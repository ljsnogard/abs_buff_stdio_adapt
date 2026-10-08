use std::{
    io,
    mem::MaybeUninit,
    string::ToString,
};

use abs_art_bridge::{BLOCK_ON, Runtime, TrBlockOn};
use abs_buff::{
    Demand, TrBuffRead, TrBuffTryRead,
    buffer::{TrConsumerState, TrBuffSegmRef},
    x_deps::abs_cancel
};
use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};

/// An adapter that exposes a [`TrBuffTryRead`] buffer as a non-blocking
/// `std::io::Read`.
///
/// Each `read` call drains as much data as the source currently offers: the
/// borrowed segment's buffer *is* the source's own memory, and the data is
/// moved straight into the caller's `buf` through the segment's move
/// primitive (`SegmRef::move_items_to_buff`), which advances the segment's
/// offset — so the source commits exactly the moved amount when the segment
/// drops (the `abs_buff` per-piece reclaim granularity). Nothing is copied
/// through an intermediate buffer.
///
/// The loop stops when `buf` is full, the source is drained (EOF), or the
/// cancellation token is signalled. Following the std convention, an error
/// reported by `try_read` (e.g. the source being temporarily empty) is
/// deferred: if anything was already read it is returned first, and the error
/// is only surfaced by the call that makes no progress.
pub struct AsStdRead<'a, R, C = NonCancellableToken>
where
    R: TrBuffRead<u8> + TrConsumerState,
    C: TrCancellationToken,
{
    buff_r_: &'a mut R,
    cancel_: &'a mut C,
}

type Rt = Runtime<{ BLOCK_ON }>;

impl<'a, R, C> AsStdRead<'a, R, C>
where
    R: TrBuffRead<u8> + TrConsumerState,
    C: TrCancellationToken,
{
    pub const fn new(r: &'a mut R, cancel: &'a mut C) -> Self {
        AsStdRead {
            buff_r_: r,
            cancel_: cancel,
        }
    }

    /// Read as many bytes as the source currently offers into `buf`.
    pub fn read(&mut self, buf: &mut [u8]) -> io::Result<usize>
    where
        <R as TrBuffTryRead>::Err: core::error::Error,
    {
        let mut c = 0usize;
        let buf_len = buf.len();
        loop {
            // 源「已无可读数据且生产端已关闭」即 EOF：std 惯例下应当返回已读量，
            // 而不是继续等待。`consumer_state()` 是 `abs_buff` 取代旧版
            // `TrBuffRead::is_drained_closing()` 的状态查询，返回
            // `(可读数量, 生产端是否关闭)`；`None`（该实现不报告状态）保守地**不**
            // 当作 EOF——适配器的设计意图是等待异步数据，误判成 EOF 会让 `read`
            // 在第一轮 poll 就提前退出。
            let drained_closing = self
                .buff_r_
                .consumer_state()
                .is_some_and(|(count, closed)| count == 0 && closed);
            if c >= buf_len || drained_closing || self.cancel_.is_cancelled() {
                return Result::Ok(c);
            }
            // 「最多再要 `buf_len - c` 个」。注意新版 `abs_buff` 里
            // `Demand::less_than(n)` 的语义是**严格小于 n**（旧版是「至多 n 个」，
            // 含端点），若照搬旧写法，剩余 1 个字节时会构造出 `{0}` 这个无法推进
            // 任何数据的请求，让循环空转；`no_more_than` 才是与旧版 `less_than`
            // 等价的「至多 n 个」。下限交给段实现自行判定（源为空时会返回
            // `Drained` 错误，由下面的错误分支处理）。
            let demand = Demand::no_more_than(buf_len - c);
            // `may_cancel_with` 把借用的异步操作转成可取消 future，其输出类型是
            // 具体的 `SomeOf<SegmRef, Err>`（`TrBuffRead` 的 `ReadAsync` 只是
            // `TrMayCancel`，直接 `.into_future()` 的输出是无法归一化的投影类型）。
            let read_fut = self
                .buff_r_
                .read_async(&demand)
                .may_cancel_with(self.cancel_.child_token())
                .into_future();
            let mut r_res = Rt::block_on(read_fut);
            if let Option::Some(segm) = r_res.as_mut().pick_left() {
                // `as_segm_ref` yields the concrete `SegmRef` over the
                // remaining items (the borrowed segment's buffer *is* the
                // source's own memory), on which the inherent move primitive
                // exists. The remaining `buf` viewed as `MaybeUninit<u8>`
                // (`&mut [u8]` has the same layout).
                let mut child = segm.as_segm_ref();
                let dst = unsafe {
                    core::slice::from_raw_parts_mut(
                        buf[c..].as_mut_ptr().cast::<MaybeUninit<u8>>(),
                        buf_len - c,
                    )
                };
                // SAFETY: the items being moved are plain `u8` (no drop
                // needs), and `dst` is exclusively borrowed for the whole
                // move. Moving advances the child's offset, the child's drop
                // advances the parent's offset, and the source commits the
                // moved amount when the parent drops.
                let moved = unsafe { child.move_items_to_buff(dst) };
                debug_assert!(moved <= buf_len - c);
                c += moved;
                if moved == 0 {
                    // the segment yielded nothing; no progress possible now
                    return Result::Ok(c);
                }
            }
            if let Option::Some(err) = r_res.pick_right() {
                // The source reported an error (e.g. temporarily drained).
                // Per the std convention, defer it: if anything was already
                // read, report that first and let the next call surface the
                // error; only fail outright when nothing was read.
                if c > 0 {
                    return Result::Ok(c);
                }
                let err = io::Error::other(err.to_string());
                return Result::Err(err);
            }
        }
    }
}

impl<'a, R> AsStdRead<'a, R, NonCancellableToken>
where
    R: TrBuffRead<u8> + TrConsumerState,
{
    pub fn uncancellable(r: &'a mut R) -> Self {
        Self::new(r, NonCancellableToken::shared_mut())
    }
}

impl<'a, R, C> io::Read for AsStdRead<'a, R, C>
where
    R: TrBuffRead<u8> + TrConsumerState,
    C: TrCancellationToken + Clone,
{
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        AsStdRead::read(self, buf)
    }
}
