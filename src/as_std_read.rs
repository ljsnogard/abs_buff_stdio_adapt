use std::{
    io,
    mem::MaybeUninit,
    string::ToString,
};

use abs_art_bridge::{BLOCK_ON, Runtime, TrBlockOn};
use abs_buff::{
    Demand, TrBuffRead, TrBuffTryRead,
    error::{ReadErrTag, TrTaggedError},
    buffer::TrBuffSegmRef,
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
    R: TrBuffRead<u8>,
    C: TrCancellationToken,
{
    buff_r_: &'a mut R,
    cancel_: C,
}

impl<'a, R, C> AsStdRead<'a, R, C>
where
    R: TrBuffRead<u8>,
    C: TrCancellationToken,
{
    pub const fn new(r: &'a mut R, cancel: C) -> Self {
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
        // 惰性获取运行时值：不需要等待的路径（空缓冲 / 已取消 / EOF）直接返回，
        // 不该要求调用方处于运行时上下文。
        let mut rt: Option<Runtime<{ BLOCK_ON }>> = Option::None;
        let mut c = 0usize;
        let buf_len = buf.len();
        loop {
            if c >= buf_len || self.cancel_.is_cancelled() {
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
            let rt = rt.get_or_insert_with(Runtime::<{ BLOCK_ON }>::current);
            let mut r_res = rt.block_on(read_fut);
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
                // `Closing` 的定义就是「已无数据且读端已关闭」——正是 `io::Read`
                // 眼里的 EOF。这里不去问 `TrConsumerState`：那不在 `abs_smux` 的
                // 契约里（`TrChannelRx` 只承诺 `TrBuffRead`），要求它等于把所有合法
                // 的复用实现排除在外。
                if err.err_tag() == ReadErrTag::Closing {
                    eprintln!("[dbg-read] Closing，已读 {c} 字节 → 当作 EOF");
                    return Result::Ok(c);
                }
                eprintln!("[dbg-read] 错误 tag={:?}，已读 {c} 字节", err.err_tag());
                // 其它错误按 std 惯例延后：已经读到的先交出去，错误留给下一次调用。
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
    R: TrBuffRead<u8>,
{
    pub fn uncancellable(r: &'a mut R) -> Self {
        Self::new(r, NonCancellableToken::new())
    }
}

impl<'a, R, C> io::Read for AsStdRead<'a, R, C>
where
    R: TrBuffRead<u8>,
    C: TrCancellationToken,
{
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        AsStdRead::read(self, buf)
    }
}
