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

type DynFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

static NEW_TASKS: Mutex<Vec<DynFuture>> = Mutex::new(Vec::new());

fn spawn<F: Future<Output = ()> + Send + 'static>(future: F) {
    NEW_TASKS.lock().unwrap().push(Box::pin(future));
}


async fn async_main() {
    for n in 1..=10{
        spawn(foo(n));
    }
}
fn main() {

    let waker = futures::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut tasks: Vec<DynFuture> = vec![Box::pin(async_main())];

    loop{
        // Poll each task and remove any that are ready
        let is_pending = |task: &mut DynFuture| {
            task.as_mut().poll(&mut context).is_pending()
        };

        tasks.retain_mut(is_pending);

        loop{
            let Some(mut task) = NEW_TASKS.lock().unwrap().pop() else {
                break;
            };

            if task.as_mut().poll(&mut context).is_pending() {
                tasks.push(task);
            }
        }   

        if tasks.is_empty() {
            break;
        }     


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
