use once_cell::sync::Lazy;
use std::{
    collections::BTreeMap,
    mem,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Wake, Waker},
    thread::{self},
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

type DynFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

static NEW_TASKS: Mutex<Vec<DynFuture>> = Mutex::new(Vec::new());

enum JoinState<T> {
    Unawaited,
    Awaited(Waker),
    Ready(T),
    Done,
}

struct JoinHandle<T> {
    state: Arc<Mutex<JoinState<T>>>,
}

impl<T> Future for JoinHandle<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut guard = self.state.lock().unwrap();

        match mem::replace(&mut *guard, JoinState::Done) {
            JoinState::Ready(value) => Poll::Ready(value),
            JoinState::Unawaited | JoinState::Awaited(_) => {
                *guard = JoinState::Awaited(cx.waker().clone());
                Poll::Pending
            }
            JoinState::Done => unreachable!("polled again after ready"),
        }
    }
}

async fn wrap_with_join_state<F: Future>(future: F, join_state: Arc<Mutex<JoinState<F::Output>>>) {
    let value = future.await;
    let mut guard = join_state.lock().unwrap();

    if let JoinState::Awaited(waker) = &*guard {
        waker.wake_by_ref();
    }
    *guard = JoinState::Ready(value)
}

fn spawn<F, T>(future: F) -> JoinHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let join_state = Arc::new(Mutex::new(JoinState::Unawaited));
    let join_handle = JoinHandle {
        state: Arc::clone(&join_state),
    };

    let task = Box::pin(wrap_with_join_state(future, join_state));
    NEW_TASKS.lock().unwrap().push(task);
    join_handle
}

struct AwakeFlag(Mutex<bool>);

impl AwakeFlag {
    fn check_and_clear(&self) -> bool {
        let mut guard = self.0.lock().unwrap();
        let check = *guard;
        *guard = false;
        check
    }
}

impl Wake for AwakeFlag {
    fn wake(self: Arc<Self>) {
        *self.0.lock().unwrap() = true;
    }
}

async fn async_main() {
    let mut task_handles = Vec::new();
    for n in 1..=10 {
        task_handles.push(spawn(foo(n)));
    }

    for handle in task_handles {
        handle.await
    }
}
fn main() {
    let awake_flag = Arc::new(AwakeFlag(Mutex::new(false)));
    let waker = Waker::from(Arc::clone(&awake_flag));

    let mut context = Context::from_waker(&waker);
    let mut main_task = Box::pin(async_main());
    let mut other_tasks: Vec<DynFuture> = Vec::new();

    loop {
        if main_task.as_mut().poll(&mut context).is_ready() {
            return;
        }

        // Poll each task and remove any that are ready
        let is_pending = |task: &mut DynFuture| task.as_mut().poll(&mut context).is_pending();

        other_tasks.retain_mut(is_pending);

        loop {
            let Some(mut task) = NEW_TASKS.lock().unwrap().pop() else {
                break;
            };

            if task.as_mut().poll(&mut context).is_pending() {
                other_tasks.push(task);
            }
        }

        if awake_flag.check_and_clear() {
            continue;
        }

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
