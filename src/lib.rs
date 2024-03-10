use std::{
    future::Future,
    marker::PhantomData,
    mem,
    pin::Pin,
    sync::{mpsc, Arc, Mutex, Weak},
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

use maybe_dangling::ManuallyDrop;
use slab::Slab;

use special_lock::{SpecialRwLock, WriteHandle};

// TODO: Panic propogation
// TODO: Return values
// TODO: Allow the spawning of subtasks within subtasks from the same scope
// TODO: Let the callback passed to `scope` return a `Future`

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

pub struct Scope<'scope, 'env: 'scope> {
    // *const is so the struct is !Send, since spawning subtasks within a subtask from the Scope
    // isn't something I want to tackle right now. Using a different inner Scope should be fine.
    _phantom: PhantomData<*const &'scope &'env ()>,
    spawn_tx: mpsc::Sender<ManuallyDrop<Box<dyn Future<Output = ()> + Send + 'static>>>,
}

pub struct ScopeGuard<'scope, Ex>
where
    Ex: Executor,
{
    _phantom: PhantomData<&'scope ()>,
    inner: Arc<SpecialRwLock<InnerScopeGuard<Ex>>>,
    inner_write: WriteHandle,
    to_spawn: Vec<(usize, ScopedTaskRunner<Ex>)>,
    spawn_rx: mpsc::Receiver<ManuallyDrop<Box<dyn Future<Output = ()> + Send + 'static>>>,
    task_wake_rx: mpsc::Receiver<usize>,
    task_done_rx: mpsc::Receiver<usize>,
}

struct InnerScopeGuard<Ex>
where
    Ex: Executor,
{
    // When this field is `true`, it signals to `ScopedTaskRunner`s that they may access and run
    // their futures. We set it to `true` when we enter the body of `<ScopeGuard as Future>::poll`,
    // and set it to `false` when we exit the function. Additionally, we make sure to block the
    // thread until all tasks have yielded back to their `ScopedTaskRunner`.
    can_run: bool,
    tasks: Slab<Mutex<SubTask<Ex::TaskHandle<()>>>>,
    // This is only an `Option` because of delayed initialization. If we wanted to, we could
    // initialize it with a no-op waker, but I can't be arsed since there's no stable way to do
    // it in the standard library.
    scope_waker: Option<Waker>,
    task_wake_tx: mpsc::Sender<usize>,
    task_done_tx: mpsc::Sender<usize>,
}

struct SubTask<H> {
    // SAFETY: Accessing this field may be UB if the non-'static data this future borrows from is
    // invalidated. Therefore, we don't touch it unless `InnerScopeGuard.can_run` is `true`.
    future: ManuallyDrop<Option<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>>,
    // Wakes the associated `ScopedTaskRunner`
    waker: Option<Waker>,
    runner_handle: Option<H>,
}

/// The future spawned directly onto the runtime.
struct ScopedTaskRunner<Ex>
where
    Ex: Executor,
{
    scope: Weak<SpecialRwLock<InnerScopeGuard<Ex>>>,
    idx: usize,
    done: bool,
}

#[must_use = "Must .await the ScopeGuard in order to make progress in subtasks"]
pub fn scope<'env, 'scope, Ex, F>(f: F) -> ScopeGuard<'scope, Ex>
where
    'env: 'scope,
    Ex: Executor,
    F: FnOnce(&Scope<'scope, 'env>),
{
    let (tx, rx) = mpsc::channel();
    let scope = Scope::new(tx);
    f(&scope);
    ScopeGuard::new(rx)
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

impl<'scope, Ex> ScopeGuard<'scope, Ex>
where
    Ex: Executor,
{
    fn new(
        spawn_rx: mpsc::Receiver<ManuallyDrop<Box<dyn Future<Output = ()> + Send + 'static>>>,
    ) -> Self {
        let (task_done_tx, task_done_rx) = mpsc::channel();
        let (task_wake_tx, task_wake_rx) = mpsc::channel();
        let (inner, inner_write) = unsafe {
            SpecialRwLock::new(InnerScopeGuard {
                can_run: false,
                tasks: Slab::new(),
                scope_waker: None,
                task_wake_tx,
                task_done_tx,
            })
        };
        Self {
            _phantom: PhantomData,
            inner: Arc::new(inner),
            inner_write,
            to_spawn: Vec::new(),
            spawn_rx,
            task_wake_rx,
            task_done_rx,
        }
    }
}

impl<'scope, Ex> Future for ScopeGuard<'scope, Ex>
where
    Ex: Executor + 'static,
    <Ex as Executor>::TaskHandle<()>: Unpin + Send,
{
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        // The lock can only be poisoned in the current scope.
        let inner = this.inner_write.write(&this.inner);
        inner.scope_waker = Some(cx.waker().clone());

        let mut wakers = Vec::new();
        for idx in this.task_wake_rx.try_iter() {
            let task = inner.tasks.get_mut(idx).unwrap().get_mut().unwrap();
            if let Some(waker) = task.waker.take() {
                wakers.push(waker);
            }
        }

        inner.can_run = true;
        let num_readers = wakers.len() + this.to_spawn.len();
        let allow_reads = this.inner_write.allow_reads(&this.inner, num_readers);
        // Dispatch subtasks
        for waker in wakers {
            waker.wake();
        }
        let mut runner_handles = Vec::new();
        for (idx, runner) in this.to_spawn.drain(..) {
            runner_handles.push((idx, Ex::spawn(runner)));
        }
        // Block the thread until all subtasks are done polling their futures.
        drop(allow_reads);
        let inner = this.inner_write.write(&this.inner);
        inner.can_run = false;

        for (idx, handle) in runner_handles {
            let task = inner.tasks.get_mut(idx).unwrap().get_mut().unwrap();
            task.runner_handle = Some(handle);
        }

        for task in this.spawn_rx.try_iter() {
            let entry = inner.tasks.vacant_entry();
            let idx = entry.key();
            this.to_spawn.push((
                idx,
                ScopedTaskRunner {
                    scope: Arc::downgrade(&this.inner.clone()),
                    idx,
                    done: false,
                },
            ));
            entry.insert(Mutex::new(SubTask {
                future: ManuallyDrop::new(Some(Box::into_pin(ManuallyDrop::into_inner(task)))),
                waker: None,
                runner_handle: None,
            }));
        }
        for idx in this.task_done_rx.try_iter() {
            inner.tasks.remove(idx);
        }
        if !this.to_spawn.is_empty() {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        if !inner.tasks.is_empty() {
            return Poll::Pending;
        }
        Poll::Ready(())
    }
}

impl<'scope, Ex> Drop for ScopeGuard<'scope, Ex>
where
    Ex: Executor,
{
    fn drop(&mut self) {
        let inner = self.inner_write.write(&self.inner);
        for (_, task) in inner.tasks.iter_mut() {
            let task = task.get_mut().unwrap();
            if let Some(runner_handle) = task.runner_handle.as_ref() {
                runner_handle.abort();
            }
            // SAFETY: The lifetime on `Self` guards against invalidating the borrow of the futures
            unsafe { ManuallyDrop::drop(&mut task.future) };
        }
    }
}

impl<Ex> Future for ScopedTaskRunner<Ex>
where
    Ex: Executor,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.done {
            panic!("Polled ScopedTaskRuner after it completed");
        }
        if let Some(scope) = self.scope.upgrade() {
            if let Some(scope) = scope.try_read() {
                let mut task = scope.tasks.get(self.idx).unwrap().lock().unwrap();
                task.waker = Some(cx.waker().clone());
                if scope.can_run {
                    // SAFETY: `can_run` signals that it's safe to access the future.
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
                    return poll;
                }
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
            data.scope_waker.wake_by_ref();
            let _ = data.tx.send(data.idx);
        },
        |data| {
            let data = unsafe { Arc::from_raw(data.cast::<ScopedTaskWakerData>()) };
            let _ = data.tx.send(data.idx);
            data.scope_waker.wake_by_ref();
            let _ = Arc::into_raw(data);
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

mod special_lock {
    //! A specialized lock just for our use-case
    //!
    //! It lets us write by default with a `WriteHandle`, and then we can optionally give read
    //! access for a limited time with `WriteHandle::allow_reads()` to a known number of readers.
    //! We're only given back access to our `WriteHandle` once all readers have dropped their read
    //! guards. This will deadlock if any reader fails to create and subsequently drop their
    //! alloted read guard.
    //!
    //! TODO: Think of some alternate shceme that avoid this potential deadlock.

    use std::{
        cell::UnsafeCell,
        ops::{Deref, DerefMut},
        sync::atomic::{AtomicUsize, Ordering},
    };

    pub struct SpecialRwLock<T> {
        item: UnsafeCell<T>,
        readers: AtomicUsize,
    }

    pub struct ReadableSpecialRwLock<'a, T> {
        inner: &'a SpecialRwLock<T>,
        _handle: &'a mut WriteHandle,
    }

    pub struct SpecialRwLockReadGuard<'a, T> {
        inner: &'a SpecialRwLock<T>,
    }

    pub struct WriteHandle {}

    impl<T> SpecialRwLock<T> {
        /// # Safety
        ///
        /// It's the caller's responsibility to make sure they use the returned handles with the lock
        /// they were created from.
        pub unsafe fn new(item: T) -> (Self, WriteHandle) {
            (
                Self {
                    item: UnsafeCell::new(item),
                    readers: AtomicUsize::new(0),
                },
                WriteHandle {},
            )
        }

        pub fn try_read(&self) -> Option<SpecialRwLockReadGuard<'_, T>> {
            if self.readers.load(Ordering::Acquire) > 0 {
                Some(SpecialRwLockReadGuard { inner: self })
            } else {
                None
            }
        }
    }

    unsafe impl<T: Send> Send for SpecialRwLock<T> {}
    unsafe impl<T: Send + Sync> Sync for SpecialRwLock<T> {}

    impl WriteHandle {
        pub fn write<'a, T>(&'a mut self, lock: &'a SpecialRwLock<T>) -> &'a mut T {
            unsafe { lock.item.get().as_mut().unwrap() }
        }

        pub fn allow_reads<'a, T>(
            &'a mut self,
            lock: &'a SpecialRwLock<T>,
            num_readers: usize,
        ) -> ReadableSpecialRwLock<'a, T> {
            lock.readers.store(num_readers, Ordering::Release);
            ReadableSpecialRwLock {
                inner: lock,
                _handle: self,
            }
        }
    }

    impl<'a, T> Drop for ReadableSpecialRwLock<'a, T> {
        fn drop(&mut self) {
            while self.inner.readers.load(Ordering::Acquire) > 0 {
                std::hint::spin_loop();
            }
        }
    }

    impl<'a, T> Deref for SpecialRwLockReadGuard<'a, T> {
        type Target = T;

        fn deref(&self) -> &Self::Target {
            unsafe { self.inner.item.get().as_ref().unwrap() }
        }
    }

    impl<'a, T> DerefMut for SpecialRwLockReadGuard<'a, T> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            unsafe { self.inner.item.get().as_mut().unwrap() }
        }
    }

    impl<'a, T> Drop for SpecialRwLockReadGuard<'a, T> {
        fn drop(&mut self) {
            self.inner.readers.fetch_sub(1, Ordering::Release);
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

    fn scoped(data: &mut [i32]) -> ScopeGuard<'_, TokioExecutor> {
        scope::<TokioExecutor, _>(|s| {
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
        // Requires running Miri with `MIRIFLAGS=-Zmiri-ignore-leaks`.
        #[cfg(miri)]
        mem::forget(rt);
    }

    #[test]
    fn test_evil_fanout() {
        async fn evil_fanout(data: &mut Vec<i32>) {
            {
                let my_scope = scoped(data);
                // Once awaited, the scope will spawn all but the first task onto the runtime, and
                // then poll the first task itself. `Scope::poll` will only yield once its child
                // tasks have all yielded.
                let mut scope = Box::pin(my_scope);
                let _ = futures::poll!(&mut scope);

                // E499: Cannot borrow `*data` as mutable more than once at a time
                // data.push(0);

                // This will invalidate `scope`'s borrow of `data`. Since the child tasks may only
                // run (and thus access the futures passed to `s.spawn()`) when we poll `scope`, no
                // use-after-free is possible.
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
