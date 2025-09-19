use futures::future::{self};
use once_cell::sync::Lazy;
use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll, Waker},
    thread,
    time::{Duration, Instant},
};
struct Sleep {
    wake_time: Instant,
}

fn sleep(duration: Duration) -> Sleep {
    let wake_time = Instant::now() + duration;
    Sleep { wake_time }
}

static WAKE_TIMES: Lazy<Mutex<BTreeMap<Instant, Vec<Waker>>>> =
    Lazy::new(|| Mutex::new(BTreeMap::new()));

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if Instant::now() >= self.wake_time {
            Poll::Ready(())
        } else {
            let mut wake_times = WAKE_TIMES.lock().unwrap();
            let wakers_vec = wake_times.entry(self.wake_time).or_default();
            wakers_vec.push(cx.waker().clone());
            Poll::Pending
        }
    }
}

async fn foo(n: u64) {
    println!("start {n}");
    sleep(Duration::from_secs(1)).await;
    println!("End {n}");
}

fn main() {
    let mut futures = Vec::new();
    for n in 1..=10 {
        futures.push(foo(n));
    }

    let mut joined_futures = Box::pin(future::join_all(futures));
    let waker = futures::task::noop_waker();
    let mut context = Context::from_waker(&waker);

    while joined_futures.as_mut().poll(&mut context).is_pending() {
        let mut wake_times = WAKE_TIMES.lock().unwrap();
        let next_wake = wake_times.keys().next().expect("Sleep forever???");

        thread::sleep(next_wake.saturating_duration_since(Instant::now()));

        while let Some(entry) = wake_times.first_entry() {
            if *entry.key() <= Instant::now() {
                entry.remove().into_iter().for_each(Waker::wake);
            } else {
                break;
            }
        }
    }
}
