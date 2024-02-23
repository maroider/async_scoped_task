use std::{
    cell::Cell,
    future::Future,
    marker::{PhantomData, PhantomPinned},
    pin::Pin,
    task::Poll,
};

pub async fn scope<F, T>(f: F) -> Scope<T, F>
where
    F: FnOnce(&mut ScopedSpawner<T>) -> T,
    T: Send,
{
    Scope::<T, F>::new(f)
}

/// The scope guard.
///
/// For the scoped tasks to actually be spawned onto the executor, you must await this future, but
/// you must also yield control all the way back to the [`ScopedSpawnCapableFuture`] at the bottom
/// of your stack of futures. Once you've done this, your future will only be polled again once all
/// sub-tasks have finished running.
pub struct Scope<T, F> {
    _phantom: PhantomData<T>,
    f: Option<F>,
    output: Option<T>,
}

impl<T, F> Scope<T, F> {
    fn new(f: F) -> Self {
        Self {
            _phantom: PhantomData,
            f: Some(f),
            output: None,
        }
    }
}

impl<T, F> Future for Scope<T, F>
where
    F: FnOnce(&mut ScopedSpawner<T>) -> T,
    T: Send,
{
    type Output = T;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // TODO: Give the `ScopedSpawnCapableFuture` a pointer to the `self` so it can take and
        // call `self.f`. Now that we've been pinned, this should be safe.
        if !IN_SUPPORTED_RUNTIME.get() {
            panic!("Scope can only be polled from within a supported async runtime");
        }
        WANTS_TO_SPAWN_SCOPED_TASKS.set(Some((
            (&mut self.f as *mut Option<F>).cast(),
            (&mut self.output as *mut Option<T>).cast(),
            |f, output, inner_spawner| {
                let f: &mut Option<F> = unsafe { &mut *f.cast() };
                let output: &mut Option<T> = unsafe { &mut *output.cast() };
                *output = Some((f.take().unwrap())(&mut ScopedSpawner::new(inner_spawner)));
            },
        )));
        // TODO: figure out wakers...
        Poll::Pending
    }
}

pub struct ScopedSpawner<T> {
    _phantom: PhantomData<T>,
    inner: ScopedSpawnerInner,
}
impl<T> ScopedSpawner<T> {
    fn new(inner: ScopedSpawnerInner) -> ScopedSpawner<T>
    where
        T: Send,
    {
        Self {
            _phantom: PhantomData,
            inner,
        }
    }

    pub fn spawn<F, U>(&mut self, fut: F) -> JoinHandle<U>
    where
        F: Future<Output = U> + Send,
    {
        self.inner.spawn()
    }
}

struct ScopedSpawnerInner {
    //
}

impl ScopedSpawnerInner {
    pub fn spawn<T>(&mut self) -> JoinHandle<T> {
        todo!()
    }
}

pub struct JoinHandle<T> {
    _phantom: PhantomData<T>,
}

impl<T> JoinHandle<T> {}

// TODO: Figure out how nested scoped tasks work
// TODO: There may be a way of relaxing the requirement where `ScopedSpawnCapableFuture` must be at
// the bottom of the futures stack.

/// A "guard" of sorts that always is the "top-level" future in a given stack of futures. It
/// allows us to defer spawning scoped tasks until we know we're about to give back control to
/// the underlying executor, and also lets us guard against running the inner stack of futures
/// while scoped tasks are still running.
pub struct ScopedSpawnCapableFuture<F> {
    // TODO: Does this need to be wrapped in an UnsafeCell?
    inner: F,
    subtasks_are_running: bool,
    // We rely on being pinned by the executor until we're either dropped, leaked, or polled to
    // completion.
    _phantom: PhantomPinned,
}

impl<F> ScopedSpawnCapableFuture<F>
where
    F: Future,
{
    /// # Safety
    ///
    /// The caller is responsible for ensuring that this future isn't polled by anything other than
    /// a supported executor, with no intermediate futures.
    pub unsafe fn new(future: F) -> Self {
        Self {
            inner: future,
            subtasks_are_running: false,
            _phantom: PhantomPinned,
        }
    }

    fn project_inner(self: Pin<&mut Self>) -> Pin<&mut F> {
        unsafe { self.map_unchecked_mut(|s| &mut s.inner) }
    }
}

thread_local! {
    static IN_SUPPORTED_RUNTIME: Cell<bool> = Cell::new(false);
    static WANTS_TO_SPAWN_SCOPED_TASKS: Cell<Option<(*mut (), *mut (), fn(*mut (), *mut (), ScopedSpawnerInner))>> = Cell::new(None);
}

impl<F> Future for ScopedSpawnCapableFuture<F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if self.subtasks_are_running {
            // TODO: register a waker
            return Poll::Pending;
        }

        IN_SUPPORTED_RUNTIME.set(true);
        let ret = self.project_inner().poll(cx);
        IN_SUPPORTED_RUNTIME.set(false);
        if let Some((f, output, callback)) = WANTS_TO_SPAWN_SCOPED_TASKS.get() {
            let inner_spawner = ();
            callback(f, output, inner_spawner);
            self.subtasks_are_running = true;
        }
        ret
    }
}

impl<F> Drop for ScopedSpawnCapableFuture<F> {
    fn drop(&mut self) {
        inner_drop(unsafe { Pin::new_unchecked(self) });
        fn inner_drop<F>(this: Pin<&mut ScopedSpawnCapableFuture<F>>) {
            // TODO: Make sure all scoped tasks are either cancelled or complete before returning from
            // this function. Leaking this future should however still be safe. Probably involves
            // looping on some shared atomic counter for the number of live scoped sub-tasks.
            while this.subtasks_are_running {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{mem, pin::pin};

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_evil_fanout() {
        async fn evil_fanout<T: Send + Sync>(data: &mut Vec<T>) {
            {
                let scope = scope(|s| {
                    for n in data.iter() {
                        s.spawn(async {
                            let n = n;
                            tokio::task::yield_now().await;
                            mem::drop(n);
                        });
                    }
                });
                // The `Scope` will register its callback with the `ScopedSpawnCapableFuture` once.
                let mut scope = Box::pin(scope);
                futures::poll!(&mut scope);
                // This will invalidate `scope`'s borrow of `data`. Since the callback given to
                // `scope` hasn't been run yet, nothing bad can happen.
                mem::forget(scope);

                let foo = data;
                // Yielding back to the `ScopedSpawnCapableFuture` must not spawn the scoped tasks.
                // However, we have no way of guaranteeing this :(
                tokio::task::yield_now().await;
                mem::drop(foo);
            }
        }
        let data = vec![1, 3, 5, 7];
        tokio::spawn(evil_fanout(&mut data));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_evil_fanout_stack_pin() {
        async fn evil_fanout<T: Send + Sync>(data: &mut Vec<T>) {
            {
                let scope = scope(|s| {
                    for n in data.iter() {
                        s.spawn(async {
                            let n = n;
                            tokio::task::yield_now().await;
                            mem::drop(n);
                        });
                    }
                });
                // The `Scope` will register its callback with the `ScopedSpawnCapableFuture` once.
                let mut scope = pin!(scope);
                futures::poll!(&mut scope);
                // This will invalidate `scope`'s borrow of `data`. Since the callback given to
                // `scope` hasn't been run yet, nothing bad can happen.
                mem::forget(scope);

                let foo = data;
                // Yielding back to the `ScopedSpawnCapableFuture` must not spawn the scoped tasks.
                tokio::task::yield_now().await;
                mem::drop(foo);
            }
        };
        let data = vec![1, 3, 5, 7];
        tokio::spawn(evil_fanout(&mut data));
    }
}
