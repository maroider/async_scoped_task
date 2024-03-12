use std::{
    future::Future,
    marker::PhantomData,
    mem,
    pin::Pin,
    sync::{mpsc, Arc, Mutex, Weak},
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

use maybe_dangling::ManuallyDrop;
use pin_project::{pin_project, pinned_drop};
use slab::Slab;

use lock::{TicketRwLock, TicketRwLockWriteHandle};

// TODO: Panic propogation
// TODO: Return values

/// An executor capable of paralell execution.
///
/// If [`Executor::spawn`] ends up spawning futures onto a non-multithreaded executor, your program
/// may deadlock.
pub trait Executor {
    type TaskHandle<T>: TaskHandle;

    fn spawn<F, T>(fut: F) -> Self::TaskHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static;
}

pub trait TaskHandle {
    fn abort(&self);
}

#[derive(Clone)]
pub struct Scope<'scope, 'env: 'scope> {
    _phantom: PhantomData<&'scope &'env ()>,
    spawn_tx: mpsc::Sender<ManuallyDrop<Box<dyn Future<Output = ()> + Send + 'static>>>,
}

#[pin_project(PinnedDrop)]
pub struct ScopeRunner<'scope, Ex, F, T>
where
    Ex: Executor,
{
    _phantom: PhantomData<&'scope ()>,
    #[pin]
    fut: F,
    fut_output: Option<T>,
    shared: Arc<TicketRwLock<RunnerSharedState<Ex>>>,
    shared_write: TicketRwLockWriteHandle,
    to_spawn: Vec<(usize, TaskRunner<Ex>)>,
    spawn_rx: mpsc::Receiver<ManuallyDrop<Box<dyn Future<Output = ()> + Send + 'static>>>,
    task_wake_rx: mpsc::Receiver<usize>,
    task_done_rx: mpsc::Receiver<usize>,
}

struct RunnerSharedState<Ex>
where
    Ex: Executor,
{
    tasks: Slab<Mutex<SubTask<Ex::TaskHandle<()>>>>,
    // This is only an `Option` because of delayed initialization. If we wanted to, we could
    // initialize it with a no-op waker, but I can't be arsed since there's no stable way to do
    // it in the standard library.
    scope_waker: Option<Waker>,
    task_wake_tx: mpsc::Sender<usize>,
    task_done_tx: mpsc::Sender<usize>,
}

struct SubTask<H> {
    // SAFETY: Accessing this field from a runner is UB if the non-'static data this future borrows
    // from is invalidated. Access to this field from a runner is guarded by aqcuiring a read lock
    // around `RunnerSharedState`, which only allows reads when `ScopeRunner` is being polled.
    // `<ScopeRunner as Future>::poll` will block its thread until all readers are done reading.
    future: ManuallyDrop<Option<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>>,
    // Wakes the associated `TaskRunner`
    waker: Option<Waker>,
    has_ticket: bool,
    runner_handle: Option<H>,
}

/// The future spawned directly onto the runtime.
struct TaskRunner<Ex>
where
    Ex: Executor,
{
    scope: Weak<TicketRwLock<RunnerSharedState<Ex>>>,
    idx: usize,
    done: bool,
}

/// The `ScopeRunner` won't return until both the future passed to it (`f`) and all subtasks have
/// finished running.
#[must_use = "Must .await the ScopeRunner in order to make progress in subtasks"]
pub fn scope<'env, 'scope, Ex, F, Fut, T>(f: F) -> ScopeRunner<'scope, Ex, Fut, T>
where
    'env: 'scope,
    Ex: Executor,
    F: FnOnce(Scope<'scope, 'env>) -> Fut,
    Fut: Future<Output = T> + Send + 'scope,
    T: Send,
{
    let (tx, rx) = mpsc::channel();
    let scope = Scope::new(tx);
    ScopeRunner::new(f(scope), rx)
}

impl<'scope, 'env> Scope<'scope, 'env> {
    fn new(
        spawn_tx: mpsc::Sender<ManuallyDrop<Box<dyn Future<Output = ()> + Send + 'static>>>,
    ) -> Scope<'scope, 'env> {
        Self {
            _phantom: PhantomData,
            spawn_tx,
        }
    }

    pub fn spawn<F, T>(&self, fut: F)
    where
        F: Future<Output = T> + Send + 'scope,
        T: Send + 'scope,
    {
        let fut = Box::new(async {
            fut.await;
        });
        // SAFETY: The rest of this module makes sure to only deref the `Box` when we're certain
        // that the 'scope borrow is still live/valid.
        let fut = unsafe {
            ManuallyDrop::new(Box::from_raw(
                Box::into_raw(fut) as *mut (dyn Future<Output = ()> + Send + 'static)
            ))
        };
        let _ = self.spawn_tx.send(fut);
    }
}

impl<'scope, Ex, F, T> ScopeRunner<'scope, Ex, F, T>
where
    Ex: Executor,
{
    fn new(
        fut: F,
        spawn_rx: mpsc::Receiver<ManuallyDrop<Box<dyn Future<Output = ()> + Send + 'static>>>,
    ) -> Self {
        let (task_done_tx, task_done_rx) = mpsc::channel();
        let (task_wake_tx, task_wake_rx) = mpsc::channel();
        let (shared, shared_write) = TicketRwLock::new(RunnerSharedState {
            tasks: Slab::new(),
            scope_waker: None,
            task_wake_tx,
            task_done_tx,
        });
        Self {
            _phantom: PhantomData,
            fut,
            fut_output: None,
            shared: Arc::new(shared),
            shared_write,
            to_spawn: Vec::new(),
            spawn_rx,
            task_wake_rx,
            task_done_rx,
        }
    }
}

impl<'scope, Ex, F, T> Future for ScopeRunner<'scope, Ex, F, T>
where
    Ex: Executor + 'static,
    <Ex as Executor>::TaskHandle<()>: Unpin + Send,
    F: Future<Output = T>,
    T: Send,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        // The lock can only be poisoned in the current scope.
        let shared = this.shared_write.write(this.shared);
        shared.scope_waker = Some(cx.waker().clone());

        let mut wakers = Vec::new();
        for idx in this.task_wake_rx.try_iter() {
            let task = shared.tasks.get_mut(idx).unwrap().get_mut().unwrap();
            if let Some(waker) = task.waker.take() {
                task.has_ticket = true;
                wakers.push(waker);
            }
        }

        let tickets = wakers.len() + this.to_spawn.len();
        // SAFETY: We only made one handle + lock pair, so this is safe
        let allow_reads = unsafe { this.shared_write.allow_reads(this.shared, tickets) };
        // Dispatch subtasks
        for waker in wakers {
            waker.wake();
        }
        let mut runner_handles = Vec::new();
        for (idx, runner) in this.to_spawn.drain(..) {
            runner_handles.push((idx, Ex::spawn(runner)));
        }

        // Since we're going to have to block in a second, might as well poll `self.fut` here so
        // that we at least do _something_ useful.
        if this.fut_output.is_none() {
            let poll = this.fut.poll(cx);
            if let Poll::Ready(output) = poll {
                *this.fut_output = Some(output);
            }
        }

        // Block the thread until all subtasks are done polling their futures.
        drop(allow_reads);
        let shared = this.shared_write.write(this.shared);

        for (idx, handle) in runner_handles {
            let task = shared.tasks.get_mut(idx).unwrap().get_mut().unwrap();
            task.runner_handle = Some(handle);
        }

        for task in this.spawn_rx.try_iter() {
            let entry = shared.tasks.vacant_entry();
            let idx = entry.key();
            this.to_spawn.push((
                idx,
                TaskRunner {
                    scope: Arc::downgrade(&this.shared.clone()),
                    idx,
                    done: false,
                },
            ));
            entry.insert(Mutex::new(SubTask {
                future: ManuallyDrop::new(Some(Box::into_pin(ManuallyDrop::into_inner(task)))),
                waker: None,
                has_ticket: true,
                runner_handle: None,
            }));
        }
        for idx in this.task_done_rx.try_iter() {
            shared.tasks.remove(idx);
        }
        if !this.to_spawn.is_empty() {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        if !shared.tasks.is_empty() {
            return Poll::Pending;
        }
        Poll::Ready(this.fut_output.take().unwrap())
    }
}

#[pinned_drop]
impl<'scope, Ex, F, T> PinnedDrop for ScopeRunner<'scope, Ex, F, T>
where
    Ex: Executor,
{
    // Clippy doesn't understand proc macros, I guess
    #[allow(clippy::needless_lifetimes)]
    fn drop(mut self: Pin<&mut Self>) {
        let this = self.project();
        let shared = this.shared_write.write(this.shared);
        for (_, task) in shared.tasks.iter_mut() {
            let task = task.get_mut().unwrap();
            if let Some(runner_handle) = task.runner_handle.as_ref() {
                runner_handle.abort();
            }
            // SAFETY: The lifetime on `Self` guards against invalidating the borrow of the futures
            // until we're dropped or leaked.
            unsafe { ManuallyDrop::drop(&mut task.future) };
        }
    }
}

impl<Ex> Future for TaskRunner<Ex>
where
    Ex: Executor,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.done {
            panic!("Polled TaskRunner after it completed");
        }
        if let Some(scope) = self.scope.upgrade() {
            if let Some(scope) = scope.try_read() {
                let mut task = scope.tasks.get(self.idx).unwrap().lock().unwrap();
                task.waker = Some(cx.waker().clone());
                // SAFETY: Getting read access through the lock means that it's safe to access the
                // future, since we may only get a read lock while `<ScopeRunner as Future>::poll`
                // is executing.
                let mut future = ManuallyDrop::into_inner(mem::take(&mut task.future)).unwrap();
                let poll = future
                    .as_mut()
                    .poll(&mut Context::from_waker(&scoped_task_waker(
                        scope.scope_waker.as_ref().unwrap().clone(),
                        scope.task_wake_tx.clone(),
                        self.idx,
                    )));
                task.future = ManuallyDrop::new(Some(future));
                if poll.is_ready() {
                    let _ = scope.task_done_tx.send(self.idx);
                    self.done = true;
                }
                if task.has_ticket {
                    task.has_ticket = false;
                    drop(task);
                    scope.consume_ticket();
                }
                return poll;
            }
        }
        Poll::Pending
    }
}

fn scoped_task_waker(scope_waker: Waker, tx: mpsc::Sender<usize>, idx: usize) -> Waker {
    struct ScopedTaskWakerData {
        scope_waker: Waker,
        tx: mpsc::Sender<usize>,
        idx: usize,
    }
    const VTABLE: RawWakerVTable = RawWakerVTable::new(
        |data| {
            let data = unsafe { Arc::from_raw(data.cast::<ScopedTaskWakerData>()) };
            let cloned = RawWaker::new(Arc::into_raw(Arc::clone(&data)).cast(), &VTABLE);
            let _ = Arc::into_raw(data);
            cloned
        },
        |data| {
            let data = unsafe { Arc::from_raw(data.cast::<ScopedTaskWakerData>()) };
            let _ = data.tx.send(data.idx);
            data.scope_waker.wake_by_ref();
        },
        |data| {
            let data = unsafe { &*data.cast::<ScopedTaskWakerData>() };
            let _ = data.tx.send(data.idx);
            data.scope_waker.wake_by_ref();
        },
        |data| {
            let _ = unsafe { Arc::from_raw(data.cast::<ScopedTaskWakerData>()) };
        },
    );
    let data: Arc<ScopedTaskWakerData> = Arc::new(ScopedTaskWakerData {
        scope_waker,
        tx,
        idx,
    });
    let raw = RawWaker::new(Arc::into_raw(data).cast(), &VTABLE);
    unsafe { Waker::from_raw(raw) }
}

mod lock {
    //! A specialized lock just for our use-case
    //!
    //! It lets us write by default with a `WriteHandle`, and then we can optionally give read
    //! access for a limited time with `WriteHandle::allow_reads()` to a known number of tasks.
    //! We're only given back access to our `WriteHandle` once all tasks have consumed their read
    //! tickets.

    use std::{
        cell::UnsafeCell,
        ops::{Deref, DerefMut},
        sync::atomic::{AtomicUsize, Ordering},
    };

    pub struct TicketRwLock<T> {
        item: UnsafeCell<T>,
        read_tickets: AtomicUsize,
        readers: AtomicUsize,
    }

    pub struct TicketRwLockReadPermissionGuard<'a, T> {
        inner: &'a TicketRwLock<T>,
        _handle: &'a mut TicketRwLockWriteHandle,
    }

    pub struct TicketRwLockReadGuard<'a, T> {
        inner: &'a TicketRwLock<T>,
    }

    pub struct TicketRwLockWriteHandle {}

    impl<T> TicketRwLock<T> {
        pub fn new(item: T) -> (Self, TicketRwLockWriteHandle) {
            (
                Self {
                    item: UnsafeCell::new(item),
                    read_tickets: AtomicUsize::new(0),
                    readers: AtomicUsize::new(0),
                },
                TicketRwLockWriteHandle {},
            )
        }

        pub fn try_read(&self) -> Option<TicketRwLockReadGuard<'_, T>> {
            self.readers.fetch_add(1, Ordering::Release);
            if self.read_tickets.load(Ordering::Acquire) > 0 {
                Some(TicketRwLockReadGuard { inner: self })
            } else {
                self.readers.fetch_sub(1, Ordering::Relaxed);
                None
            }
        }
    }

    unsafe impl<T: Send> Send for TicketRwLock<T> {}
    unsafe impl<T: Send + Sync> Sync for TicketRwLock<T> {}

    // These methods work based on the same principle as `qcell`'s epynomous `Qcell` type.
    impl TicketRwLockWriteHandle {
        pub fn write<'a, T>(&'a mut self, lock: &'a TicketRwLock<T>) -> &'a mut T {
            unsafe { lock.item.get().as_mut().unwrap() }
        }

        // # Safety
        //
        // The lock passed in must be the lock `self` was constructed alongside.
        pub unsafe fn allow_reads<'a, T>(
            &'a mut self,
            lock: &'a TicketRwLock<T>,
            read_tickets: usize,
        ) -> TicketRwLockReadPermissionGuard<'a, T> {
            lock.read_tickets.store(read_tickets, Ordering::Release);
            TicketRwLockReadPermissionGuard {
                inner: lock,
                _handle: self,
            }
        }
    }

    impl<'a, T> Drop for TicketRwLockReadPermissionGuard<'a, T> {
        fn drop(&mut self) {
            while self.inner.readers.load(Ordering::Acquire) > 0
                || self.inner.read_tickets.load(Ordering::Acquire) > 0
            {
                std::hint::spin_loop();
            }
        }
    }

    impl<'a, T> TicketRwLockReadGuard<'a, T> {
        pub fn consume_ticket(self) {
            self.inner.read_tickets.fetch_sub(1, Ordering::Release);
        }
    }

    impl<'a, T> Deref for TicketRwLockReadGuard<'a, T> {
        type Target = T;

        fn deref(&self) -> &Self::Target {
            unsafe { self.inner.item.get().as_ref().unwrap() }
        }
    }

    impl<'a, T> DerefMut for TicketRwLockReadGuard<'a, T> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            unsafe { self.inner.item.get().as_mut().unwrap() }
        }
    }

    impl<'a, T> Drop for TicketRwLockReadGuard<'a, T> {
        fn drop(&mut self) {
            self.inner.readers.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TokioExecutor {}
    impl Executor for TokioExecutor {
        type TaskHandle<T> = tokio::task::JoinHandle<T>;

        fn spawn<F, T>(fut: F) -> Self::TaskHandle<T>
        where
            F: Future<Output = T> + Send + 'static,
            T: Send + 'static,
        {
            tokio::task::spawn(fut)
        }
    }
    impl<T> TaskHandle for tokio::task::JoinHandle<T> {
        fn abort(&self) {
            tokio::task::JoinHandle::<T>::abort(self)
        }
    }

    fn scoped(
        data: &mut [i32],
    ) -> ScopeRunner<'_, TokioExecutor, impl Future<Output = ()> + '_, ()> {
        scope::<TokioExecutor, _, _, _>(move |s| async move {
            let split_at = data.len() / 2;
            let (left, right) = data.split_at_mut(split_at);
            s.spawn(async {
                tokio::task::yield_now().await;
                for n in left.iter_mut() {
                    *n += 1;
                }
            });
            s.spawn(async {
                tokio::task::yield_now().await;
                for n in right.iter_mut() {
                    *n += 2;
                }
            });
        })
    }

    fn mt_block_on(future: impl Future<Output = ()>) {
        let rt = tokio::runtime::Builder::new_multi_thread().build().unwrap();
        rt.block_on(future);
        // Silence spurious Miri deadlock warnings caused by `tokio` runtime cleanup.
        // Running without `MIRIFLAGS=-Zmiri-ignore-leaks` makes Miri consistently complain about
        // the main thread exiting before all other threads have exited.
        #[cfg(miri)]
        mem::forget(rt);
    }

    #[test]
    fn test_evil_fanout() {
        async fn evil_fanout(data: &mut Vec<i32>) {
            {
                let my_scope = scoped(data);
                // Once awaited, the scope will spawn all the tasks onto the runtime. On subsequent
                // polls, the scope will poll any spawned tasks. `ScopeRunner::poll` will only yield
                // once its child tasks have all yielded.
                let mut scope = Box::pin(my_scope);
                let _ = futures::poll!(&mut scope);

                // E499: Cannot borrow `*data` as mutable more than once at a time
                // data.push(0);

                // This will invalidate `scope`'s borrow of `data`. Since the child tasks may only
                // run (and thus access the futures passed to `s.spawn()`) when we poll `scope`, no
                // use-after-free is possible. This holds true even if we wrap the current future
                // (aka the current async fn) in another future that tries to do the same thing.
                mem::forget(scope);

                data.push(2);
            }
        }
        mt_block_on(async {
            let mut data = vec![1, 3, 5, 7];
            evil_fanout(&mut data).await;
            // We only polled once, which means the subtasks only got polled once, which means the
            // subtasks didn't get around to mutating the data.
            assert_eq!(data, vec![1, 3, 5, 7, 2]);
        })
    }

    #[test]
    fn test_normal_fanout() {
        mt_block_on(async {
            let mut data = [1, 2, 3, 4];
            scoped(&mut data).await;
            assert_eq!(data, [2, 3, 5, 6]);
        })
    }
}
