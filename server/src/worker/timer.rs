use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};
use tokio::time::{sleep, Instant, Sleep};

/// Restartable timer backed by Tokio's `Sleep`.
pub struct RestartableTimer {
    sleep: Pin<Box<Sleep>>,
    expired: bool,
}

impl RestartableTimer {
    pub fn new(after: Duration) -> Self {
        Self {
            sleep: Box::pin(sleep(after)),
            expired: after.is_zero(),
        }
    }

    pub fn restart(&mut self, after: Duration, waker: &Waker) {
        self.sleep.as_mut().reset(Instant::now() + after);
        self.expired = after.is_zero();
        // ensure the task is polled again to observe the new deadline
        waker.wake_by_ref();
    }

    pub fn is_expired(&self) -> bool {
        self.expired
    }
}

impl Future for RestartableTimer {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        match self.sleep.as_mut().poll(cx) {
            Poll::Ready(()) => {
                self.expired = true;
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

