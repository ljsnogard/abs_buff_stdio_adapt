//! # 集成测试：`AsStdRead` / `AsStdWrite` 与「异步数据流」的真实对接
//!
//! ## 背景与测试意图
//!
//! 适配器内部通过 `abs_art-bridge` 的 `TrBlockOn::block_on` 把 `abs_buff` 的
//! 异步借用操作（`read_async` / `write_async`）同步驱动到完成。本模块要证明的
//! 不是「字节搬移」本身正确（那是 `abs_buff` / `buffex` 自己的测试范围），而是
//! **适配器真的等到了异步数据**：
//!
//! - 当数据流中的数据要**稍后**才由另一个执行体（后台线程 / compio 异步任务）
//!   产生时，一次 `read` 必须阻塞等待并返回完整正确的数据；
//! - 当数据流**暂时为空但尚未关闭**时，`read` 不能把「暂时为空」误判成 EOF
//!   提前退出（返回 0 或半截数据）；
//! - 写方向同理：`write` 写进异步接收端的数据必须完整、按序、无丢失。
//!
//! 若实现存在「第一轮 poll 没有数据就提前退出」型错误，下面的测试会在内容、
//! 数量或耗时断言上失败——这正是本模块存在的意义。
//!
//! ## 测试载体：`buffex::ring` 的 SPSC 环形管道
//!
//! [`CbTx`] / [`CbRx`]（`RingWriter` / `RingReader`）实现了 `abs_buff` 的
//! `TrBuffWrite` / `TrBuffRead`：空管道上 `read_async` 会保持 Pending 直到写端
//! 送来数据并唤醒 waker；满管道上 `write_async` 会保持 Pending 直到读端腾出空间。
//! 它是一条**真实、可延迟、可跨线程**的异步数据流，正适合用来暴露「提前退出」
//! 型错误。
//!
//! ## 运行前提
//!
//! 适配器内部的 `block_on`（compio 后端）要求当前线程处于**多线程 compio 运行时**
//! 上下文（`Handle::current()` 可用，且 `block_in_place` 需要其它 worker 承接
//! 任务）。因此所有直接调用适配器的测试都包在 [`compio 运行时上下文`] 里执行。
//! 测试所用的 `backend-compio` 由本 crate 的 `[dev-dependencies]` 启用。

use std::{
    mem::MaybeUninit,
    sync::Arc,
    thread,
    time::{Duration, Instant},
    vec::Vec,
};

use abs_buff::{
    Demand, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite,
    buffer::{TrBuffSegmMut, TrProducerState},
    x_deps::{abs_cancel, anylr::SomeOf},
};
use abs_buff_testkit::read_initialized;
use abs_cancel::CancelledToken;
use buffex::ring::{Ring, RingReader, RingWriter};

use crate::{AsStdRead, AsStdWrite};

// ===========================================================================
// 公共辅助
// ===========================================================================

/// SPSC 环形管道（`buffex::ring`）的缓冲类型：元素 `u8` 的未初始化缓冲。
type CbBuf = Box<[MaybeUninit<u8>]>;
/// 环本体：由 `Arc` 共享给读写两半（经 `Ring::split_unchecked` 拆分）。
type CbRing = Ring<CbBuf, u8>;
/// 环的写端（生产端）。
type CbTx = RingWriter<Arc<CbRing>, CbBuf, u8>;
/// 环的读端（消费端）。
type CbRx = RingReader<Arc<CbRing>, CbBuf, u8>;

// ---------------------------------------------------------------------------
// SPSC 环形管道（`buffex::ring`）载体：写端实现 TrBuffWrite、读端实现
// TrBuffRead（空/满时会真实 Pending 等待对端并注册 waker）
// ---------------------------------------------------------------------------

/// 建立一条容量为 `cap` 的 SPSC 环形管道，返回 (写端, 读端)。
///
/// 说明：`buffex::ring` 的 `Ring` 本身就是**被动设备**——两端各自 park，谁持有
/// 半部谁负责等待；`Ring::split_unchecked` 把 `Arc<Ring>` 拆成两个**拥有所有权**
/// 的半部，因而可以分别 move 进不同线程。这正是测试需要的「真实、可延迟、
/// 可跨线程」的异步数据流：`AsStdWrite` 写进写端、数据经环形缓冲流动、
/// `AsStdRead` 从读端读出。
fn make_cb_pair(cap: usize) -> (CbTx, CbRx) {
    let buff = Box::<[u8]>::new_uninit_slice(cap);
    let ring = Arc::new(Ring::new_unchecked(buff));
    // SAFETY: `split_unchecked` 要求环被智能指针独占持有、且不存在 weak 升级。
    // `ring` 被 move 进来后由内部 clone 一份给写端；返回后调用方不再保留其它
    // `Arc`，也没有 `Weak`，两个半部各持唯一的一份，满足安全前提。
    unsafe { Ring::split_unchecked(ring) }
}

/// 把 `data` 全部写入环形管道写端（同步 API，供后台线程使用）。
///
/// 经 `TrBuffTryWrite::try_write` 借出写段，数据经段原语
/// `move_items_from_as_buff` 位拷贝进缓冲，段 drop 时提交（推进写位置、唤醒
/// 读端）。可能分多次借用直到写完。
fn cb_write_all(tx: &mut CbTx, data: &[u8]) {
    let mut off = 0usize;
    while off < data.len() {
        let demand = Demand::at_least(1);
        let mut segm = TrBuffTryWrite::try_write(tx, &demand)
            .pick_left()
            .expect("写端应有可写空间");
        // 只搬本轮剩余载荷：借到的写段可能比剩余数据大。
        let n = core::cmp::min(segm.least_count(), data.len() - off);
        assert!(n > 0, "借到的写段不可能为空");
        let moved = segm.move_items_from_as_buff(&data[off..off + n]);
        assert_eq!(moved, n);
        drop(segm);
        off += n;
    }
}

/// 把环形管道读端当前可读的数据全部读出（同步 API，供后台线程使用）。
/// 读端空（且未关闭）时返回空——「等待」由调用方的循环 + 适配器负责。
fn cb_drain_available(rx: &mut CbRx) -> Vec<u8> {
    let mut out = Vec::new();
    let demand = Demand::at_least(1);
    while let Some(mut segm) = TrBuffTryRead::try_read(rx, &demand).pick_left() {
        let n = segm.least_count();
        assert!(n > 0, "读段不可能为空");
        let mut dst: Vec<MaybeUninit<u8>> = (0..n).map(|_| MaybeUninit::uninit()).collect();
        // SAFETY: 把缓冲段中的 `u8` 逐位搬进 `dst`（段 drop 时推进读位置）；
        // `u8` 无 drop 需求，`dst` 在搬移期间被独占借用。
        let moved = unsafe { segm.move_items_to_buff(&mut dst) };
        assert_eq!(moved, n);
        out.extend(read_initialized(&dst, dst.len()));
        drop(segm);
    }
    out
}

// ===========================================================================
// 写方向：std::io::Write → 异步数据流
// ===========================================================================

/// 包装写端，**不报告** `producer_state`（保持 `TrProducerState` 的默认实现，
/// 返回 `None` 表示「状态未知」）。
///
/// `AsStdWrite` 在 `producer_state()` 为 `None` 时不会提前退出，只能走
/// `write_async` 的 Pending 等待路径——这正是本类型存在的意义：单独验证适配器
/// （以及环的写等待 park + 读端提交唤醒）确实能阻塞等待异步空间。
///
/// 说明：旧版 `abs_buff` 里对应的做法是覆写 `is_stuffed_closing()` 恒返回
/// `false`；新版把「是否能继续写」统一收敛到 `TrProducerState`。
struct ConservativeTx<W>(W);

impl<W> TrProducerState for ConservativeTx<W> {}

impl<W> TrBuffTryWrite<u8> for ConservativeTx<W>
where
    W: TrBuffTryWrite<u8>,
{
    type SegmMut<'f> = W::SegmMut<'f> where Self: 'f;
    type Err = W::Err;

    fn try_write<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        self.0.try_write(demand)
    }
}

impl<W> TrBuffWrite<u8> for ConservativeTx<W>
where
    W: TrBuffWrite<u8>,
{
    type WriteAsync<'f> = W::WriteAsync<'f> where Self: 'f;

    fn write_async<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> Self::WriteAsync<'f> {
        self.0.write_async(demand)
    }
}

// ===========================================================================
// 环形管道载体：SPSC + AsStdRead / AsStdWrite（全同步代码）
// ===========================================================================

/// 目的：证明环形管道的两端分别包上 `AsStdWrite`（生产端）与 `AsStdRead`
/// （消费端）后，用**全同步**的代码就能让数据从生产端流进环形缓冲、再从消费端
/// 读出——往返内容、顺序、数量完全一致。
///
/// 测试方法：
/// 1. 建立容量 64 的 SPSC 环形管道，写端包 `AsStdWrite`、读端包
///    `AsStdRead`（两个适配器都只调用同步的 `write` / `read`）；
/// 2. 先 `write` 写入 32 字节载荷，再 `close()` 关闭写端（EOF）；
/// 3. 用一次 `read`（64 字节缓冲）读出；
///
/// 通过依据（同时满足才算通过）：
/// - 一次 `write` 返回 32（载荷小于容量，必须全部写入）；
/// - `read` 返回 32 且内容与载荷逐字节一致——数据确实经环形缓冲从生产端
///   流到了消费端；写端关闭（EOF）后读循环能正确结束，不把 EOF 误当错误。
#[compio::test]
async fn cb_sync_write_then_read_roundtrip() {
    const CAP: usize = 64;
    const PAYLOAD_LEN: usize = 32;

    let (mut tx, mut rx) = make_cb_pair(CAP);
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 7 + 1) as u8).collect();

    // 双端包装：生产端 → AsStdWrite，消费端 → AsStdRead。
    let mut writer = AsStdWrite::uncancellable(&mut tx);
    let mut reader = AsStdRead::uncancellable(&mut rx);

    // 全同步写入：数据流进环形缓冲（段 drop 即提交、唤醒读端）。
    let n = writer.write(&payload).expect("write 不应失败");
    assert_eq!(n, PAYLOAD_LEN, "载荷小于容量，一次 write 应全部写入");
    tx.close(); // 写端关闭（EOF）：让读循环能确定结束

    // 全同步读出：数据从环形缓冲流到调用者缓冲。
    let mut buf = [0u8; 64];
    let rn = reader.read(&mut buf).expect("read 不应失败");
    assert_eq!(rn, PAYLOAD_LEN, "应读出全部写入的数据");
    assert_eq!(&buf[..rn], &payload[..], "读出内容必须与写入一致");
}

/// 目的：证明环形管道读端包上 `AsStdRead` 后，面对「数据要过一段时间才由另一个
/// 执行体（后台线程）写入」的场景，一次 `read` 会通过适配器内部的 `block_on`
/// **阻塞等待**直到数据真正到达（并随写端关闭结束），而不是因为第一轮 poll
/// 没有数据就立即返回 0（提前退出）。
///
/// 测试方法：
/// 1. 建立容量 64 的环形管道，读端包上 `AsStdRead`；
/// 2. 后台生产者线程：先 `sleep(150ms)`（保证读端的 `read` 已进入
///    `read_async` 等待、第一轮 poll 必然 Pending），再用同步 API 写入 32 字节
///    载荷并 `close()` 关闭写端（EOF）；
/// 3. 主线程在 compio 运行时上下文里调用**一次** `read`，记录耗时；
///
/// 通过依据（同时满足才算通过）：
/// - `read` 返回 32（提前退出会得到 0）；
/// - 内容与载荷逐字节相同；
/// - 耗时 ≥ 130ms（150ms - 20ms 余量）：直接证明调用阻塞到了异步数据到达
///   之后才返回，且跨线程唤醒（生产者线程提交 → 消费端 waker 被 signal）生效。
#[compio::test]
async fn cb_read_waits_for_async_data_arriving_later() {
    const PAYLOAD_LEN: usize = 32;

    let (mut tx, mut rx) = make_cb_pair(64);
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 7 + 1) as u8).collect();

    // 异步生产者线程：延迟 150ms 后写入载荷并关闭写端（EOF）。
    let payload_producer = payload.clone();
    let producer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(150));
        cb_write_all(&mut tx, &payload_producer);
        tx.close(); // 写端关闭 → 数据流 EOF
    });

    let mut reader = AsStdRead::uncancellable(&mut rx);
    let mut buf = [0u8; PAYLOAD_LEN];
    let t0 = Instant::now();
    let n = reader.read(&mut buf).expect("read 不应返回错误");
    let elapsed = t0.elapsed();
    assert_eq!(n, PAYLOAD_LEN, "必须读满 {PAYLOAD_LEN} 字节；提前退出会得到 0");
    assert_eq!(&buf[..], &payload[..], "读出的内容必须与载荷一致");
    assert!(
        elapsed >= Duration::from_millis(130),
        "read 必须等待异步数据到达：耗时 {elapsed:?} 远小于 150ms 延迟，说明提前退出了"
    );

    producer.join().expect("生产者线程不应 panic");
}

/// 目的：证明环形管道读端在「数据流暂时为空但尚未关闭」的窗口期不会提前退出。
/// 一次 `read` 消费完第一批后缓冲变空（写端还开着），适配器的读循环必须继续
/// 阻塞等待第二批，而不是把「暂时为空」误判成 EOF 提前返回。
///
/// 测试方法：
/// 1. 后台生产者：睡 100ms → 写第一批 8 字节 → 等读者消费完（轮询
///    `ring_state().data_size()` 归零，此时读者必已进入第二批等待）→ 再睡 20ms
///    确保读者已 park → 写第二批 8 字节 → 关闭写端；
/// 2. 主线程在 compio 运行时上下文里**只调用一次** `read`（缓冲 32 字节）；
///
/// 通过依据：
/// - `read` 返回 16（两批之和）——若在「暂时为空」处提前退出，只会返回 8；
/// - 内容 == 第一批 ++ 第二批（按序、无丢失）。
#[compio::test]
async fn cb_read_waits_across_transient_empty_phase() {
    let (mut tx, mut rx) = make_cb_pair(64);
    let batch1: Vec<u8> = (0..8).map(|i| (i as u8) + 1).collect(); // [1..=8]
    let batch2: Vec<u8> = (0..8).map(|i| (i as u8) + 100).collect(); // [100..=107]
    let mut expect = batch1.clone();
    expect.extend_from_slice(&batch2);

    let producer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        cb_write_all(&mut tx, &batch1);
        // 等读者消费完第一批（数据量归零 = 读者已读完，即将进入第二批等待）。
        let deadline = Instant::now() + Duration::from_secs(2);
        while tx.ring_state().data_size() > 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        thread::sleep(Duration::from_millis(20)); // 确保读者已 park 进等待
        cb_write_all(&mut tx, &batch2);
        tx.close(); // EOF
    });

    let mut reader = AsStdRead::uncancellable(&mut rx);
    let mut buf = [0u8; 32];
    let n = reader.read(&mut buf).expect("read 不应返回错误");
    assert_eq!(
        n, 16,
        "必须读到两批共 16 字节；若在流暂时为空时提前退出，只会返回 8"
    );
    assert_eq!(&buf[..16], &expect[..], "两批数据必须按序完整到达");

    producer.join().expect("生产者线程不应 panic");
}

/// 目的：证明环形管道写端包上 `AsStdWrite` 后，载荷远超容量时数据
/// **完整、按序、无丢失**地送达延迟腾出空间的异步消费者——写端在管道满时
/// 返回 `Ok(0)`（std 惯例：写满当前可用空间即返回），由调用方循环重试，
/// 直到消费者腾出空间后全部写出。
///
/// 测试方法：
/// 1. 建立容量 16 的环形管道，载荷 100 字节（远超容量，写端必然反复撞上「满」）；
/// 2. 消费者线程：先 `sleep(100ms)`（保证写端已经填满管道并开始等待），
///    然后持续读出，直到收满整个载荷；
/// 3. 主线程在 compio 运行时上下文里循环 `write`：`Ok(0)`（管道满）就稍等
///    重试，直到全部写出；记录耗时；
///
/// 通过依据：
/// - 累计写出的字节数必须等于 100（若管道满时数据被弄丢 / 返回错误，循环
///   无法完成）；
/// - 消费者收到的字节与载荷逐字节相同（内容、顺序、无重复无丢失）；
/// - 耗时 ≥ 90ms：证明写端真的等到了异步消费者腾出空间。
#[compio::test]
async fn cb_write_delivers_all_to_delayed_async_consumer() {
    const CAP: usize = 16;
    const PAYLOAD_LEN: usize = 100;

    let (mut tx, mut rx) = make_cb_pair(CAP);
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 13 + 7) as u8).collect();

    // 异步消费者：延迟 100ms 才开始腾空间，直到收满整个载荷。
    let consumer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut got = Vec::new();
        while got.len() < PAYLOAD_LEN && Instant::now() < deadline {
            let chunk = cb_drain_available(&mut rx);
            got.extend_from_slice(&chunk);
            if got.len() < PAYLOAD_LEN && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(1));
            }
        }
        got
    });

    let mut writer = AsStdWrite::uncancellable(&mut tx);
    let t0 = Instant::now();
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut off = 0usize;
    while off < payload.len() && Instant::now() < deadline {
        match writer.write(&payload[off..]) {
            Ok(0) => {
                // 管道满：等待异步消费者腾出空间后重试。
                thread::sleep(Duration::from_millis(1));
            }
            Ok(n) => {
                assert!(n > 0, "write 返回的已写字节数不可能为负向增长");
                off += n;
            }
            Err(e) => panic!("write 不应返回错误：{e}"),
        }
    }
    let elapsed = t0.elapsed();
    assert_eq!(off, PAYLOAD_LEN, "必须写出全部 {PAYLOAD_LEN} 字节");
    assert!(
        elapsed >= Duration::from_millis(90),
        "写端必须等待异步消费者腾出空间（消费者延迟 100ms）：耗时 {elapsed:?}"
    );

    let got = consumer.join().expect("消费者线程不应 panic");
    assert_eq!(got.len(), PAYLOAD_LEN, "消费者必须收到全部载荷");
    assert_eq!(&got[..], &payload[..], "消费者收到的内容必须与载荷逐字节一致");
}

/// 目的：证明环形管道写端适配器内部的 `write_async` + `block_on`
/// **确实会阻塞等待**异步空间——用 [`ConservativeTx`] 包装（不报告
/// `producer_state`）强制走等待路径：当环形缓冲写满、读端要过一段时间才腾出
/// 空间时，**一次** `write` 调用必须完整写出整个载荷，而不是带着部分字节提前
/// 返回。
///
/// 说明：普通环形管道写端的 `producer_state()` 在满时会报告 `free == 0`，
/// 让适配器在调用 `write_async` 之前就提前返回（std 惯例）；本测试用
/// [`ConservativeTx`] 把状态查询变成「未知」，强制走 `write_async` 的 Pending
/// 等待路径，单独验证适配器（以及环形管道的写等待 park + 读端提交唤醒）的
/// 阻塞等待能力。
///
/// 测试方法：
/// 1. 建立容量 16 的环形管道，载荷 100 字节（远超容量）；
/// 2. 写端 `ConservativeTx(tx)` 包上 `AsStdWrite`；
/// 3. 消费者线程：先 `sleep(100ms)`（此时写端第一次 `write` 必已撞上「满」、
///    进入 `write_async` 的 Pending 等待），然后持续读出全部 100 字节；
/// 4. 主线程在 compio 运行时上下文里**只调用一次** `write`，记录耗时；
///
/// 通过依据：
/// - 这次 `write` 返回 100（若适配器没等待、在满时提前退出，返回的只是当前
///   可用空间 ≤15 字节）；
/// - 消费者收到的 100 字节与载荷逐字节一致；
/// - 耗时 ≥ 90ms：证明 `block_on` 真正阻塞到了异步消费者腾出空间。
#[compio::test]
async fn cb_write_blocks_until_async_consumer_frees_space() {
    const CAP: usize = 16;
    const PAYLOAD_LEN: usize = 100;

    let (tx, mut rx) = make_cb_pair(CAP);
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 13 + 7) as u8).collect();
    let payload_consumer = payload.clone();

    // 异步消费者：延迟 100ms 才开始腾空间，直到收满整个载荷。
    let consumer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut got = Vec::new();
        while got.len() < payload_consumer.len() && Instant::now() < deadline {
            let chunk = cb_drain_available(&mut rx);
            got.extend_from_slice(&chunk);
            if got.len() < payload_consumer.len() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(1));
            }
        }
        got
    });

    let mut sink = ConservativeTx(tx);
    let mut writer = AsStdWrite::uncancellable(&mut sink);
    let t0 = Instant::now();
    let n = writer
        .write(&payload)
        .expect("write 不应返回错误");
    let elapsed = t0.elapsed();
    assert_eq!(
        n, PAYLOAD_LEN,
        "一次 write 必须完整写出全部 {PAYLOAD_LEN} 字节（适配器应等待异步空间，而非提前退出）"
    );
    assert!(
        elapsed >= Duration::from_millis(90),
        "write 必须阻塞等待异步消费者腾出空间（消费者延迟 100ms）：耗时 {elapsed:?}"
    );

    let got = consumer.join().expect("消费者线程不应 panic");
    assert_eq!(got.len(), PAYLOAD_LEN, "消费者必须收到全部载荷");
    assert_eq!(&got[..], &payload[..], "消费者收到的内容必须与载荷逐字节一致");
}

// ===========================================================================
// 边界与设计意图回归
// ===========================================================================
//
// 下面一组用例把适配器与 `abs_buff` 新版 API 之间的**语义映射**钉死，防止
// 后续升级时再次踩到下面这些点：
//
// * `Demand::less_than(n)` 的语义由旧版的「至多 n 个」改成了「严格小于 n」，
//   适配器必须用 `no_more_than(n)` 表达「最多再搬剩余量」——否则剩余 1 个字节
//   时会构造出只允许 0 个的请求：读方向空转、写方向直接让 `Ring` 的
//   `debug_assert!(take > 0)` 失败；
// * EOF 必须表达为 `Ok(0)`（空且生产端已关闭），而不是错误、也不是继续等待；
// * `std::io::Write` 侧在「满 / 对端已关闭」时返回 `Ok(0)`，**不阻塞等待**空间；
// * 已取消的令牌必须让 `read` / `write` 立刻返回，而不是进入异步等待。

/// 目的：证明 `read` 只在调用方缓冲写满、或源已 EOF 时停止，**跨调用不丢数据**。
///
/// 测试方法：
/// 1. 容量 16 的环形管道写入 10 字节载荷并关闭写端（EOF）；
/// 2. 依次用长度为 4、4、4 的缓冲调用 `read`；
///
/// 通过依据：
/// - 前两次各返回 4，第三次返回 2（剩余量小于缓冲，靠 EOF 例外交付）；
/// - 三次读出的内容首尾相接恰好等于载荷（顺序、数量、无丢失）；
/// - 第四次 `read` 返回 0：EOF 之后不会再次产出数据，也不报错。
#[compio::test]
async fn cb_read_respects_buffer_len_and_resumes() {
    const PAYLOAD_LEN: usize = 10;

    let (mut tx, mut rx) = make_cb_pair(16);
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 5 + 3) as u8).collect();
    cb_write_all(&mut tx, &payload);
    tx.close(); // EOF

    let mut reader = AsStdRead::uncancellable(&mut rx);
    let mut got = Vec::new();
    for expect in [4usize, 4, 2] {
        let mut buf = [0u8; 4];
        let n = reader.read(&mut buf).expect("read 不应返回错误");
        assert_eq!(n, expect, "每次 read 至多交付缓冲长度，且不会提前截断数据");
        got.extend_from_slice(&buf[..n]);
    }
    assert_eq!(got, payload, "跨多次 read 必须完整、按序交付载荷");

    let mut tail = [0u8; 4];
    assert_eq!(
        reader.read(&mut tail).expect("EOF 后 read 不应返回错误"),
        0,
        "EOF 之后 read 必须返回 0（std 惯例），而不是错误或再次等待"
    );
}

/// 目的：证明「剩余 1 个字节」这一边界请求能真正推进数据——这是
/// `Demand::less_than` → `Demand::no_more_than` 语义迁移的回归用例。
///
/// 测试方法：
/// 1. 容量 16 的环形管道写入 1 字节载荷并关闭写端；
/// 2. 用长度 1 的缓冲调用一次 `read`；
///
/// 通过依据：
/// - 返回 1 且内容等于载荷。若适配器仍按旧语义构造 `less_than(1)`，新版
///   `Demand` 会把它解释成「只允许 0 个」，段不会被借出，读循环无法推进
///   （最坏表现为空转或挂死），用例必然失败。
#[compio::test]
async fn cb_read_single_byte_request_advances() {
    let (mut tx, mut rx) = make_cb_pair(16);
    cb_write_all(&mut tx, &[0xA5]);
    tx.close();

    let mut reader = AsStdRead::uncancellable(&mut rx);
    let mut buf = [0u8; 1];
    let n = reader.read(&mut buf).expect("read 不应返回错误");
    assert_eq!(n, 1, "剩余 1 个字节的请求必须被满足");
    assert_eq!(buf[0], 0xA5, "读出的内容必须与写入一致");
}

/// 目的：证明源「已空且生产端已关闭」时 `read` 立即返回 `Ok(0)`，不把 EOF
/// 当成错误，也不因为「没有数据」而进入异步等待。
///
/// 测试方法：
/// 1. 建立容量 16 的环形管道后立刻关闭写端（从未写入任何数据）；
/// 2. 用 `AsStdRead` 连续调用两次 `read`；
///
/// 通过依据：
/// - 两次调用都返回 `Ok(0)`（不是 `Err`、不 panic）；
/// - EOF 是可重复观测的状态，第二次调用同样立即返回 0。
#[compio::test]
async fn cb_read_eof_returns_zero() {
    let (mut tx, mut rx) = make_cb_pair(16);
    tx.close(); // 空管道 + EOF

    let mut reader = AsStdRead::uncancellable(&mut rx);
    let mut buf = [0u8; 8];
    assert_eq!(
        reader.read(&mut buf).expect("EOF 不应是错误"),
        0,
        "空且已关闭时 read 必须返回 Ok(0)"
    );
    assert_eq!(
        reader.read(&mut buf).expect("EOF 不应是错误"),
        0,
        "EOF 是可重复观测的状态，再次 read 仍应返回 Ok(0)"
    );
}

/// 目的：证明已取消的令牌让 `read` **立刻**返回，而不是进入 `read_async` 的
/// 等待（适配器向调用方承诺的「可取消」语义）。
///
/// 测试方法：
/// 1. 建立容量 16 的环形管道，**保持写端开启**（若适配器忽略取消信号，空管道
///    上的 `read_async` 会一直 Pending，用例将挂死而不是通过）；
/// 2. 用 `AsStdRead::new` 绑定 [`CancelledToken`]，调用一次 `read` 并记录耗时；
///
/// 通过依据：
/// - 返回 `Ok(0)`（已读 0 字节、不再等待）；
/// - 耗时 < 100ms：远小于任何真实的异步等待，证明没有进入等待路径。
#[compio::test]
async fn cb_read_cancelled_token_returns_without_waiting() {
    let (_tx, mut rx) = make_cb_pair(16);
    let mut token = CancelledToken::new();
    let mut reader = AsStdRead::new(&mut rx, &mut token);

    let mut buf = [0u8; 8];
    let t0 = Instant::now();
    let n = reader.read(&mut buf).expect("取消不应是错误");
    let elapsed = t0.elapsed();
    assert_eq!(n, 0, "已取消时 read 必须立刻返回已读量 0");
    assert!(
        elapsed < Duration::from_millis(100),
        "已取消时 read 不得进入等待：耗时 {elapsed:?}"
    );
}

/// 目的：证明 `AsStdWrite` 遵守 `std::io::Write` 的非阻塞约定——写满当前可用
/// 空间即返回，绝不为「等空间」而阻塞；调用方稍后重试仍能续写且不丢数据。
///
/// 测试方法：
/// 1. 容量 8 的环形管道，载荷 16 字节；
/// 2. 同步循环：`write` 直到返回 `Ok(0)`（满），记录已写量；
/// 3. 手动 `drain` 读端腾空空间，再继续 `write`，直到写完全部载荷；
///
/// 通过依据：
/// - 第一次 `write` 恰好返回 8（容量）；
/// - 随后的 `write` 在满环上返回 `Ok(0)`（而不是阻塞或报错）——这是与
///   [`ConservativeTx`] 那组「强制等待」用例的对照；
/// - 最终写满 16 字节，且读端收到的内容与载荷逐字节一致（无重复、无丢失）。
#[compio::test]
async fn cb_write_nonblocking_stops_when_full() {
    const CAP: usize = 8;
    const PAYLOAD_LEN: usize = 16;

    let (mut tx, mut rx) = make_cb_pair(CAP);
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 11 + 1) as u8).collect();
    let mut got = Vec::new();
    let mut off = 0usize;

    {
        let mut writer = AsStdWrite::uncancellable(&mut tx);
        let n = writer.write(&payload[off..]).expect("write 不应失败");
        assert_eq!(n, CAP, "空环上一次 write 应写满容量");
        off += n;

        let n = writer.write(&payload[off..]).expect("write 不应失败");
        assert_eq!(n, 0, "满环上 write 必须返回 Ok(0)，而不是阻塞等待空间");
    }

    got.extend_from_slice(&cb_drain_available(&mut rx));

    {
        let mut writer = AsStdWrite::uncancellable(&mut tx);
        let n = writer.write(&payload[off..]).expect("write 不应失败");
        assert_eq!(n, CAP, "读端腾空后应能继续写满容量");
        off += n;
    }

    got.extend_from_slice(&cb_drain_available(&mut rx));

    assert_eq!(off, PAYLOAD_LEN, "必须写出全部载荷");
    assert_eq!(got, payload, "读端收到的内容必须与载荷逐字节一致");
}

/// 目的：证明「剩余 1 个字节」的写请求能真正推进数据——这是
/// `Demand::less_than` → `Demand::no_more_than` 语义迁移在写方向的回归用例。
///
/// 测试方法：
/// 1. 容量 8 的环形管道，先一次写满 8 字节并读空；
/// 2. 再用 `AsStdWrite` 写入尾部的 1 个字节；
///
/// 通过依据：
/// - 尾部 `write` 返回 1，读端能取回该字节。若适配器仍按旧语义构造
///   `less_than(1)`，新版 `Demand` 会把它解释成「只允许 0 个」，`Ring` 的
///   `try_write_internal_` 会因 `take == 0` 触发 `debug_assert` 而 panic。
#[compio::test]
async fn cb_write_single_byte_tail_advances() {
    const CAP: usize = 8;
    const PAYLOAD_LEN: usize = 9;

    let (mut tx, mut rx) = make_cb_pair(CAP);
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 17 + 2) as u8).collect();
    let mut got = Vec::new();

    {
        let mut writer = AsStdWrite::uncancellable(&mut tx);
        let n = writer.write(&payload).expect("write 不应失败");
        assert_eq!(n, CAP, "第一次 write 应写满容量");
    }
    got.extend_from_slice(&cb_drain_available(&mut rx));

    {
        let mut writer = AsStdWrite::uncancellable(&mut tx);
        let n = writer.write(&payload[CAP..]).expect("write 不应失败");
        assert_eq!(n, PAYLOAD_LEN - CAP, "剩余 1 个字节的写请求必须被满足");
    }
    got.extend_from_slice(&cb_drain_available(&mut rx));

    assert_eq!(got, payload, "读端收到的内容必须与载荷逐字节一致");
}

/// 目的：证明消费端已关闭时 `write` 返回 `Ok(0)`（等价于旧版
/// `is_stuffed_closing()` 里的 closing 分支），既不阻塞也不 panic。
///
/// 测试方法：
/// 1. 建立容量 8 的环形管道后先 `close` 读端（消费端）；
/// 2. 用 `AsStdWrite` 调用一次 `write`，记录耗时；
///
/// 通过依据：
/// - 返回 `Ok(0)`：不可能再有消费者接收数据，按适配器约定报告「本次写不出」；
/// - 耗时 < 100ms：状态查询直接命中，未进入 `write_async` 的等待路径。
#[compio::test]
async fn cb_write_closed_consumer_returns_zero() {
    let (mut tx, mut rx) = make_cb_pair(8);
    rx.close(); // 消费端关闭：写端不再可能写出数据

    let mut writer = AsStdWrite::uncancellable(&mut tx);
    let t0 = Instant::now();
    let n = writer
        .write(b"abc")
        .expect("消费端关闭不应让 write 报错");
    let elapsed = t0.elapsed();
    assert_eq!(n, 0, "消费端已关闭时 write 必须返回 Ok(0)");
    assert!(
        elapsed < Duration::from_millis(100),
        "消费端已关闭时 write 不得进入等待：耗时 {elapsed:?}"
    );
}

/// 目的：证明已取消的令牌让 `write` **立刻**返回——用 [`ConservativeTx`]
/// （不报告 `producer_state`）强制走 `write_async` 路径，若适配器忽略取消信号，
/// 满环上的写等待会让用例挂死而不是通过。
///
/// 测试方法：
/// 1. 容量 8 的环形管道，先用同步 API 填满 8 字节（此后任何写都必须等待空间）；
/// 2. 用 `ConservativeTx` 包装写端、绑定 [`CancelledToken`]，调用一次 `write`
///    并记录耗时；
///
/// 通过依据：
/// - 返回 `Ok(0)`（一个字节也没写出去，且不再等待）；
/// - 耗时 < 100ms：证明取消信号在进入异步等待之前就被观察到。
#[compio::test]
async fn cb_write_cancelled_token_returns_without_waiting() {
    const CAP: usize = 8;

    let (mut tx, _rx) = make_cb_pair(CAP);
    cb_write_all(&mut tx, &[0x5Au8; CAP]); // 填满，后续写必然要等空间

    let mut sink = ConservativeTx(tx);
    let mut token = CancelledToken::new();
    let mut writer = AsStdWrite::new(&mut sink, &mut token);

    let t0 = Instant::now();
    let n = writer
        .write(&[0x11u8; 4])
        .expect("取消不应是错误");
    let elapsed = t0.elapsed();
    assert_eq!(n, 0, "已取消时 write 必须立刻返回已写量 0");
    assert!(
        elapsed < Duration::from_millis(100),
        "已取消时 write 不得进入等待：耗时 {elapsed:?}"
    );
}
