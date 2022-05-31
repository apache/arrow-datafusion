use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::task::JoinHandle;

pub trait AsyncExecutor {
    type Output<T>;

    fn execute<F>(&self, future: F) -> Self::Output<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;
}

pub struct DefaultExecutor;

impl AsyncExecutor for DefaultExecutor {
    type Output<T> = JoinHandle<T>;

    fn execute<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tokio::spawn(future)
    }
}
