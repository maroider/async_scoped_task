use std::{
    cell::UnsafeCell,
    future::Future,
    marker::PhantomData,
    mem,
    pin::Pin,
    ptr::NonNull,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Mutex,
    },
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

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

#[must_use = "Must .await the ScopeGuard in order to make progress in subtasks"]
pub fn scope<'env, 'scope, Ex, F>(f: F) -> ScopeGuard<'scope, Ex>
where
    'env: 'scope,
    Ex: Executor,
    F: FnOnce(&Scope<'scope, 'env, Ex>),
{
    let scope = Scope::new();
    f(&scope);
    scope.guard.into_inner().unwrap()
}

pub struct ScopeGuard<'scope, Ex>
where
    Ex: Executor,
{
    _phantom: PhantomData<&'scope ()>,
    tasks_to_spawn: Vec<Box<dyn Future<Output = ()>>>,
    tasks: Vec<SubTask<Ex::TaskHandle<()>>>,
    tasks_done: usize,
    tx: mpsc::Sender<usize>,
    rx: mpsc::Receiver<usize>,
}

impl<'scope, Ex> ScopeGuard<'scope, Ex>
where
    Ex: Executor,
{
    fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        Self {
            _phantom: PhantomData,
            tasks_to_spawn: Vec::new(),
            tasks: Vec::new(),
            tasks_done: 0,
            tx,
            rx,
        }
    }
}

impl<'scope, Ex> Future for ScopeGuard<'scope, Ex>
where
    Ex: Executor,
    Ex::TaskHandle<()>: Unpin,
{
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        let n_tasks_to_spawn = this.tasks_to_spawn.len();
        if n_tasks_to_spawn > 0 {
            this.tasks.reserve(n_tasks_to_spawn);
            for (i, task) in this.tasks_to_spawn.drain(..).enumerate() {
                this.tasks.push(SubTask {
                    state: TaskState {
                        lock: AtomicBool::new(true),
                        inner: UnsafeCell::new(InnerTaskState {
                            future: Box::into_pin(task),
                            scope_waker: cx.waker().clone(),
                            waker: None,
                            done: false,
                        }),
                    },
                    runner_handle: None,
                    wake: true,
                });
                let task = this.tasks.last_mut().unwrap();
                task.runner_handle = Some(Ex::spawn(ScopedTaskRunner {
                    state: (&mut task.state).into(),
                    tx: this.tx.clone(),
                    idx: i,
                    done: false,
                }));
            }
        } else {
            while let Ok(idx) = this.rx.try_recv() {
                this.tasks[idx].wake = true;
            }
            for task in this.tasks.iter_mut() {
                if !task.wake {
                    continue;
                }
                // If there is no waker here, the runner may have panicked
                if let Some(waker) =
                    unsafe { task.state.inner.get().as_mut().unwrap().waker.take() }
                {
                    waker.wake();
                    task.wake = true;
                    task.state.lock.store(true, Ordering::Release);
                }
            }
        }
        let mut waiting_for = this.tasks.iter().filter(|task| task.wake).count();
        while waiting_for > 0 {
            for task in this.tasks.iter_mut() {
                if !task.wake {
                    continue;
                }
                while task.state.lock.load(Ordering::Acquire) {
                    // You spin me right round, baby, right round
                }
                task.wake = false;
                waiting_for -= 1;
                if unsafe { task.state.inner.get().as_ref().unwrap().done } {
                    this.tasks_done += 1;
                }
            }
        }
        if this.tasks_done == this.tasks.len() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl<'scope, Ex> Drop for ScopeGuard<'scope, Ex>
where
    Ex: Executor,
{
    fn drop(&mut self) {
        for task in self.tasks.iter() {
            if let Some(runner_handle) = task.runner_handle.as_ref() {
                runner_handle.abort();
            }
        }
    }
}

pub struct Scope<'scope, 'env: 'scope, Ex>
where
    Ex: Executor,
{
    // *const is so the struct is !Send, since spawning subtasks within a subtask from the Scope
    // isn't something I want to tackle right now. Using a different inner Scope should be fine.
    _phantom: PhantomData<*const &'scope &'env ()>,
    guard: Mutex<ScopeGuard<'scope, Ex>>,
}

impl<'scope, 'env, Ex> Scope<'scope, 'env, Ex>
where
    Ex: Executor,
{
    fn new() -> Scope<'scope, 'env, Ex> {
        Self {
            _phantom: PhantomData,
            guard: Mutex::new(ScopeGuard::new()),
        }
    }

    pub fn spawn<F, T>(&self, fut: F)
    where
        F: Future<Output = T> + Send + 'scope,
        T: Send + 'scope,
    {
        let fut = Box::new(async {
            fut.await;
            // FIXME: Do something with the output of the future instead of throwing it away
        }) as Box<dyn Future<Output = ()>>;
        // SAFETY: The 'scope lifetime is enforced by ... this entire module, really.
        let fut = unsafe {
            mem::transmute::<
                Box<dyn Future<Output = ()> + 'scope>,
                Box<dyn Future<Output = ()> + 'static>,
            >(fut)
        };
        self.guard.lock().unwrap().tasks_to_spawn.push(fut);
    }
}

struct SubTask<H> {
    state: TaskState,
    runner_handle: Option<H>,
    wake: bool,
}

unsafe impl<H> Send for SubTask<H> {}

struct TaskState {
    // Since the executor may poll the ScopedTaskRunner as it pleases, we have to guard against the
    // runtime polling us at the wrong time. The "lock" is `true` when the ScopedTaskRunner is
    // allowed to access the guarded state, and `false` when the ScopeGuard is allowed to access
    // the state.
    lock: AtomicBool,
    inner: UnsafeCell<InnerTaskState>,
}

struct InnerTaskState {
    future: Pin<Box<dyn Future<Output = ()>>>,
    scope_waker: Waker,
    waker: Option<Waker>,
    done: bool,
}

/// The future spawned directly onto the runtime.
struct ScopedTaskRunner {
    // We're only allowed to access this when `task_fence == true`
    state: NonNull<TaskState>,
    tx: mpsc::Sender<usize>,
    idx: usize,
    done: bool,
}

unsafe impl Send for ScopedTaskRunner {}

impl Future for ScopedTaskRunner {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.done {
            panic!("Polled ScopedTaskRuner after it completed");
        }
        if unsafe { self.state.as_ref().lock.load(Ordering::Acquire) } {
            let ret = {
                let this = self.as_mut().get_mut();
                let state = unsafe { this.state.as_ref().inner.get().as_mut().unwrap_unchecked() };
                let future = state.future.as_mut();
                // FIXME: Panic propogation?
                // FIXME: Release the lock on panic?
                let waker = scoped_task_waker(state.scope_waker.clone(), this.tx.clone(), this.idx);
                let ret = future.poll(&mut Context::from_waker(&waker));
                state.waker = Some(cx.waker().clone());
                state.done = ret.is_ready();
                this.done = ret.is_ready();
                ret
            };
            unsafe { self.state.as_ref().lock.store(false, Ordering::Release) };
            ret
        } else {
            Poll::Pending
        }
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

    #[test]
    fn test_evil_fanout() {
        async fn evil_fanout(data: &mut Vec<i32>) {
            {
                let my_scope = scope::<TokioExecutor, _>(|s| {
                    s.spawn(async {
                        tokio::task::yield_now().await;
                        data.push(1);
                    });
                    s.spawn(async {
                        tokio::task::yield_now().await;
                    });
                });
                // Once awaited, the scope will spawn all but the first task onto the runtime, and
                // then poll the first task itself. `Scope::poll` will only yield once its child
                // tasks have all yielded.
                let mut scope = Box::pin(my_scope);
                let _ = futures::poll!(&mut scope);

                // E501: Cannot borrow data as mutable because previous closure
                // requires unique access
                // data.push(0);

                // This will invalidate `scope`'s borrow of `data`. Since the child tasks may only
                // run (and thus access the futures passed to `s.spawn()`) when we poll `scope`, no
                // use-after-free is possible.
                mem::forget(scope);

                data.push(2);
            }
        }

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut data = vec![1, 3, 5, 7];
        runtime.block_on(evil_fanout(&mut data));
        assert_eq!(data, vec![1, 3, 5, 7, 2]);
    }
}
