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
//! # 后端从哪来
//!
//! 「取哪一个后端的运行时值」不再由本 crate 决定，而是问门面
//! [`abs_art_facade::Runtime`]——它是全图唯一决定「当前后端」的地方。下游谁点亮
//! `abs_art-facade/rt-tokio`，全图（含这里）就都是 tokio；本 crate 一行 feature
//! 都不用写、也不需要任何别名表。缺省（没人 override）时门面给的是 compio，因此本
//! crate 单独编译 / 自测同样成立。
//!
//! 各后端的边界（含 tokio 在 `current_thread` 运行时下无法推进非空本地队列这一条）
//! 见 [`TrLocalScope::block_on_local`] 的文档。

use core::future::Future;

use abs_art_facade::{Runtime, SPAWN_LOCAL, TrLocalScope};

/// 在当前线程的本地作用域上驱动 `future` 直到完成。
///
/// # Panics
///
/// 调用点不在所选后端的运行时上下文内时 panic（由门面选中的后端的 `current()` 给出文案）。
pub(crate) fn block_on_local_<F>(future: F) -> F::Output
where
    F: Future,
{
    // 运行时值在需要等待时才取：空缓冲 / 已取消 / EOF 这些不需要等待的路径不会走到
    // 这里，因此不该要求调用方处于运行时上下文。
    let runtime = Runtime::<{ SPAWN_LOCAL }>::current();
    runtime.local_scope().block_on_local(future)
}
