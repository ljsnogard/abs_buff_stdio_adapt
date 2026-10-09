//! 把 future 在当前线程的**本地作用域**上跑到完成（同步等待）。
//!
//! # 为什么不能用 `TrBlockOn::block_on`
//!
//! `abs_buff` 的环是**全被动**的：环里的数据要靠投递在本地队列（tokio 下即
//! `LocalSet`）上的循环搬进来。而 `TrBlockOn::block_on` **只等待、不驱动本地队列**
//! （tokio 上它的实现还是 `block_in_place`，在 `LocalSet` 内被 tokio 直接拒绝）。
//! 于是「在异步任务里同步读一个由本地循环供料的环」这条最普通的用法，用它要么 panic、
//! 要么把线程占住让队列再也跑不动。
//!
//! # 本模块的做法：把这件事交回给作用域
//!
//! 用 [`TrLocalScope::block_on_local`]——「阻塞当前线程直到 future 完成，等待期间持续
//! 驱动本线程的本地队列」这条语义，由**各后端**按自己的运行时性质兑现：
//!
//! | 后端 | 做法 |
//! | --- | --- |
//! | tokio | `LocalSet::run_until` 驱动队列 + 纯 park |
//! | smol | `LocalExecutor::run` 驱动队列 + 纯 park |
//! | compio | 自己 tick `Runtime::run` + `poll_with` |
//!
//! **本 crate 自己选后端**：与 `smux_v1::connection::DefaultRt_` 同一套规则，由
//! `rt-tokio` / `rt-compio` / `rt-smol` feature **三选一**。
//!
//! 为什么不能像以前那样用 bridge 的裸名：bridge 更新后对「谁是默认」要求显式声明
//! （`default-backend-*`），而 bridge 自己的 `default = ["default-backend-compio"]`
//! 始终在线，于是裸名 `Runtime` 在任何装配下都解析成 compio。tokio / smol 装配下
//! `CompioRuntime::current()` 会直接 panic（「not in a compio runtime」）。
//!
//! 各后端的边界（含 tokio 在 `current_thread` 运行时下无法推进非空本地队列这一条）
//! 见 [`TrLocalScope::block_on_local`] 的文档。

use core::future::Future;

use abs_art::{SPAWN_LOCAL, TrLocalScope};

#[cfg(feature = "rt-tokio")]
use abs_art_bridge::TokioRuntime as BackendRuntime;
#[cfg(feature = "rt-smol")]
use abs_art_bridge::SmolRuntime as BackendRuntime;
#[cfg(not(any(feature = "rt-tokio", feature = "rt-smol")))]
use abs_art_bridge::CompioRuntime as BackendRuntime;

/// 在当前线程的本地作用域上驱动 `future` 直到完成。
///
/// # Panics
///
/// 调用点不在所选后端的运行时上下文内时 panic（由 `BackendRuntime::current` 给出文案）。
pub(crate) fn block_on_local_<F>(future: F) -> F::Output
where
    F: Future,
{
    // 运行时值在需要等待时才取：空缓冲 / 已取消 / EOF 这些不需要等待的路径不会走到
    // 这里，因此不该要求调用方处于运行时上下文。
    let runtime = BackendRuntime::<{ SPAWN_LOCAL }>::current();
    runtime.local_scope().block_on_local(future)
}
