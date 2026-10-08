extern crate std;

use std::{io, string::ToString};

use abs_art_bridge::{BLOCK_ON, Runtime, TrBlockOn};
use abs_buff::{
    Demand, TrBuffTryWrite, TrBuffWrite,
    buffer::{TrBuffSegmMut, TrProducerState},
    x_deps::abs_cancel,
};
use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};

/// An adapter that exposes a [`TrBuffTryWrite`] buffer as a non-blocking
/// `std::io::Write`.
///
/// Each `write` call pushes as many bytes as the sink currently accepts: the
/// borrowed segment's buffer *is* the sink's own memory, and the source bytes
/// are cloned straight into it through the segment's `clone_items_from_buff`
/// primitive, which advances the segment's offset — so the sink commits
/// exactly the written amount when the segment drops (the `abs_buff`
/// per-piece reclaim granularity). Nothing is copied through an intermediate
/// buffer.
///
/// The loop stops when `buf` is exhausted, the sink is full, or the
/// cancellation token is signalled. Following the std convention, an error
/// reported by `try_write` (e.g. the sink being stuffed) is deferred: if
/// anything was already written it is returned first, and the error is only
/// surfaced by the call that makes no progress.
///
/// Being a `std::io::Write`, a call does not wait for room to become
/// available: as soon as the sink reports no free space it returns the amount
/// written so far and lets the caller retry — waiting for the peer is the
/// consumer's business. [`TrProducerState`] is what makes that check possible
/// (`is_stuffed_closing` in the previous `abs_buff` API); a sink that reports
/// no state (`None`) falls back to the async wait inside `write_async`.
pub struct AsStdWrite<'a, W, C = NonCancellableToken>
where
    W: TrBuffWrite + TrProducerState,
    C: TrCancellationToken,
{
    buff_w_: &'a mut W,
    cancel_: C,
}

type Rt = Runtime<{ BLOCK_ON } >;

impl<'a, W, C> AsStdWrite<'a, W, C>
where
    W: TrBuffWrite + TrProducerState,
    C: TrCancellationToken,
{
    pub const fn new(w: &'a mut W, cancel: C) -> Self {
        AsStdWrite {
            buff_w_: w,
            cancel_: cancel,
        }
    }

    /// Write as many bytes from `buf` as the sink currently accepts.
    pub fn write(&mut self, buf: &[u8]) -> io::Result<usize>
    where
        <W as TrBuffTryWrite>::Err: core::error::Error,
    {
        let mut c = 0usize;
        let buf_len = buf.len();
        loop {
            // 与旧版 `TrBuffWrite::is_stuffed_closing()` 等价：sink 已满（无可用
            // 空间）或对端已关闭时，本次不可能再写出任何字节，按 std 惯例立刻返回
            // 已写量、由调用方稍后重试——适配器**不得**在这里进入 `write_async`
            // 的等待路径，否则 `std::io::Write` 就变成了阻塞写（「强制等待空间」
            // 的场景由测试里的 `ConservativeTx` 单独覆盖）。
            // `producer_state()` 返回 `(可用空间, 对端是否关闭)`；`None`（该实现
            // 不报告状态）保守地不当作不可写，交给 `write_async` 决定。
            let stalled_closing = self
                .buff_w_
                .producer_state()
                .is_some_and(|(free, closed)| free == 0 || closed);
            if c >= buf_len || stalled_closing || self.cancel_.is_cancelled() {
                return Result::Ok(c);
            }
            // 「最多再写 `buf_len - c` 个」。注意新版 `abs_buff` 里
            // `Demand::less_than(n)` 的语义是**严格小于 n**（旧版是「至多 n 个」，
            // 含端点），若照搬旧写法，剩余 1 个字节时会构造出 `{0}` 这个借不到任何
            // 空间的请求（`Ring` 会直接 `debug_assert!(take > 0)` 失败）；
            // `no_more_than` 才是与旧版 `less_than` 等价的「至多 n 个」。
            let demand = Demand::no_more_than(buf_len - c);
            // 同 `AsStdRead`：`may_cancel_with` 的输出是具体的
            // `SomeOf<SegmMut, Err>`，且让等待过程真正可被取消。
            let fut = self
                .buff_w_
                .write_async(&demand)
                .may_cancel_with(self.cancel_.child_token())
                .into_future();
            let mut w_res = Rt::block_on(fut);
            if let Option::Some(segm) = w_res.as_mut().pick_left() {
                // `as_segm_mut` yields the concrete `SegmMut` over the
                // remaining free items (the borrowed segment's buffer *is*
                // the sink's own memory), on which the inherent clone
                // primitive exists.
                let mut child = segm.as_segm_mut();
                let take = core::cmp::min(child.least_count(), buf_len - c);
                // Clone (bitwise, for `u8`) the source bytes straight into
                // the segment and advance the child's offset; the child's
                // drop advances the parent's offset, and the sink commits
                // exactly `take` units when the parent drops.
                let moved = child.clone_items_from_buff(&buf[c..c + take]);
                debug_assert_eq!(moved, take);
                c += moved;
                if moved == 0 {
                    // the segment accepted nothing; no progress possible now
                    return Result::Ok(c);
                }
            }
            if let Option::Some(err) = w_res.pick_right() {
                // The sink reported an error (e.g. stuffed / closed). Per the
                // std convention, defer it: if anything was already written,
                // report that first and let the next call surface the error;
                // only fail outright when nothing was written.
                if c > 0 {
                    return Result::Ok(c);
                }
                let err = io::Error::other(err.to_string());
                return Result::Err(err);
            }
        }
    }
}

impl<'a, W> AsStdWrite<'a, W, NonCancellableToken>
where
    W: TrBuffWrite + TrProducerState,
{
    pub fn uncancellable(w: &'a mut W) -> Self {
        Self::new(w, NonCancellableToken::new())
    }
}

impl<'a, W, C> io::Write for AsStdWrite<'a, W, C>
where
    W: TrBuffWrite + TrProducerState,
    C: TrCancellationToken,
{
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        AsStdWrite::write(self, buf)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        // the written bytes are handed to the sink as soon as the borrowed
        // segment drops; the adapter itself buffers nothing
        Result::Ok(())
    }
}
