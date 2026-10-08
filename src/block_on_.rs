//! 把 future 在当前线程的**本地作用域**上跑到完成（同步等待）。
//!
//! # 为什么不能用 `TrBlockOn::block_on`
//!
//! `abs_art` 的 `run_until` 文档写得很明确：
//!
//! > 需要阻塞等待时，用运行时值的 [`TrBlockOn::block_on`]，但要清楚它**不驱动本地
//! > 队列**。……但**不得**在其内部做阻塞驱动：tokio 的 `block_in_place` 在 `LocalSet`
//! > 内会 panic。
//!
//! 而 `abs_buff` 的环是**全被动**的：环里的数据要靠投递在本地队列（tokio 下即
//! `LocalSet`）上的循环搬进来。于是「在异步任务里同步读一个由本地循环供料的环」这条
//! 最普通的用法，用 `block_on` 要么 panic、要么把线程占住让队列再也跑不动。
//!
//! # 本模块的做法
//!
//! 用 [`TrLocalScope::run_until`] 驱动队列——它**会**推进本线程队列，且**允许嵌套**——
//! 外层只做「纯 park」的同步等待：不进入任何运行时上下文，唯一职责是别再 poll 最外层
//! future。队列的推进全交给 `run_until`。
//!
//! 两种用法都能工作，因此不需要调用方区分：
//!
//! | 场景 | 本线程队列里有什么 | `run_until` 的作用 |
//! | --- | --- | --- |
//! | 连接循环与调用方**同线程** | 连接的读 / 写循环 | 直接驱动它们，把数据喂进环 |
//! | 连接循环在**别的线程** | 空 | 本线程无事可做，等对端线程把数据喂好并唤醒即可 |
//!
//! 可运行的最小验证见 `mptp_cs_demo/examples/local_block_probe.rs`（E1 / E2 成立，E3 panic）。

use core::{
    future::Future,
    pin::pin,
    task::{Context, Poll},
};
use std::{
    sync::Arc,
    task::{Wake, Waker},
    thread,
    time::Duration,
};

use abs_art::{SPAWN_LOCAL, TrLocalScope};
use abs_art_bridge::Runtime;

/// 两次 park 之间的最长间隔。
///
/// `wake` 走的是 `Thread::unpark`，正常情况下唤醒会立刻把线程叫醒；这个超时只是兜底，
/// 避免任何一层的唤醒丢失让调用方永久挂住。
const K_PARK_TICK: Duration = Duration::from_millis(1);

/// 在当前线程的本地作用域上驱动 `future` 直到完成。
///
/// # Panics
///
/// 调用点不在所选后端的运行时上下文内时 panic（由 `Runtime::current` 给出文案）。
pub(crate) fn block_on_local_<F>(future: F) -> F::Output
where
    F: Future,
{
    let runtime = Runtime::<{ SPAWN_LOCAL }>::current();
    let scope = runtime.local_scope();
    park_on_(scope.run_until(future))
}

/// 纯 park 的同步执行器：只 poll 给定 future，未就绪就把线程挂起。
///
/// 它**不**建立运行时上下文、也**不**接管任何队列——那些是 `run_until` 的事。
fn park_on_<F>(future: F) -> F::Output
where
    F: Future,
{
    /// 唤醒 = unpark 当前线程。
    struct ThreadWaker_(thread::Thread);

    impl Wake for ThreadWaker_ {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let mut future = pin!(future);
    let waker = Waker::from(Arc::new(ThreadWaker_(thread::current())));
    let mut context = Context::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            // `unpark` 有留存语义，晚到的唤醒不会被丢掉；超时兜底的原因见
            // `K_PARK_TICK` 的说明。
            Poll::Pending => thread::park_timeout(K_PARK_TICK),
        }
    }
}
