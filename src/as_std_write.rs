extern crate std;

use std::{io, string::ToString};

use abs_art_bridge::{BLOCK_ON, Runtime, TrBlockOn};
use abs_buff::{
    Demand, TrBuffTryWrite, TrBuffWrite,
    buffer::TrBuffSegmMut,
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
/// The loop stops when `buf` is exhausted, the sink is blocked, or the
/// cancellation token is signalled. Following the std convention, an error
/// reported by `try_write` (e.g. the sink being stuffed) is deferred: if
/// anything was already written it is returned first, and the error is only
/// surfaced by the call that makes no progress.
pub struct AsStdWrite<'a, W, C = NonCancellableToken>
where
    W: TrBuffWrite,
    C: TrCancellationToken,
{
    buff_w_: &'a mut W,
    cancel_: &'a mut C,
}

type Rt = Runtime<{ BLOCK_ON } >;

impl<'a, W, C> AsStdWrite<'a, W, C>
where
    W: TrBuffWrite,
    C: TrCancellationToken,
{
    pub const fn new(w: &'a mut W, cancel: &'a mut C) -> Self {
        AsStdWrite {
            buff_w_: w,
            cancel_: cancel,
        }
    }

    /// Write as many bytes from `buf` as the sink currently accepts.
    pub fn write(&mut self, buf: &[u8]) -> io::Result<usize>
    where
        <W as TrBuffWrite>::Err: core::error::Error,
        C: TrCancellationToken + Clone,
    {
        let mut c = 0usize;
        let buf_len = buf.len();
        loop {
            if c >= buf_len
                || self.buff_w_.is_stuffed_closing()
                || self.cancel_.is_cancelled()
            {
                return Result::Ok(c);
            }
            let demand = Demand::less_than(buf_len - c);
            // 同 `AsStdRead`：`may_cancel_with` 的输出是具体的
            // `SomeOf<SegmMut, Err>`，且让等待过程真正可被取消。
            let fut = self
                .buff_w_
                .write_async(&demand)
                .may_cancel_with(&mut *self.cancel_)
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
    W: TrBuffTryWrite,
{
    pub fn uncancellable(w: &'a mut W) -> Self {
        Self::new(w, NonCancellableToken::shared_mut())
    }
}

impl<'a, W, C> io::Write for AsStdWrite<'a, W, C>
where
    W: TrBuffTryWrite,
    C: TrCancellationToken + Clone,
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
