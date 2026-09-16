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
//! ## 测试载体：`buffex` 的 SPSC 环形管道
//!
//! [`RingTx`] / [`RingRx`] 实现了 `abs_buff` 的 `TrBuffWrite` / `TrBuffRead`：
//! 空管道上 `read_async` 会保持 Pending 直到写端送来数据并唤醒 waker；满管道上
//! `write_async` 会保持 Pending 直到读端腾出空间。它是一条**真实、可延迟、可跨
//! 线程**的异步数据流，正适合用来暴露「提前退出」型错误。
//!
//! ## 运行前提
//!
//! 适配器内部的 `block_on`（compio 后端）要求当前线程处于**多线程 compio 运行时**
//! 上下文（`Handle::current()` 可用，且 `block_in_place` 需要其它 worker 承接
//! 任务）。因此所有直接调用适配器的测试都包在 [`compio 运行时上下文`] 里执行。
//! 测试所用的 `backend-compio` 由本 crate 的 `[dev-dependencies]` 启用。

use std::{
    mem::MaybeUninit,
    thread,
    time::{Duration, Instant},
    vec::Vec,
};

use abs_buff::{
    Demand, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite,
    x_deps::anylr::SomeOf,
};
use abs_buff_testkit::read_initialized;
use buffex::x_deps::{
    abs_mm::mem_alloc::CoreAlloc,
    mm_ptr::Owned,
};

use crate::{AsStdRead, AsStdWrite};

// ===========================================================================
// 公共辅助
// ===========================================================================

/// circular buff（`buffex::circular_buff`）各类型的别名：双端被动、`u8` 元素、
/// `CoreAlloc` 分配器。
type CbBuf = Owned<[MaybeUninit<u8>], CoreAlloc>;
type CbPair = buffex::circular_buff::SpscPair<CbBuf>;
type CbTx = buffex::circular_buff::Producer<
    buffex::circular_buff::BufConsumer<u8>,
    CbBuf,
    u8,
    CoreAlloc,
>;
type CbRx = buffex::circular_buff::Consumer<
    buffex::circular_buff::BufProducer<u8>,
    CbBuf,
    u8,
    CoreAlloc,
>;

// ---------------------------------------------------------------------------
// circular buff（`buffex::circular_buff`）载体：双端被动，实现 TrBuffRead /
// TrBuffWrite（空/满时会真实 Pending 等待对端并注册 waker）
// ---------------------------------------------------------------------------

/// 建立一条容量为 `cap` 的**双端被动** circular buff，返回 (写端, 读端)。
///
/// 说明：`circular_buff` 的构建器以类型状态强制两端模式；双端被动是经典
/// 手动管道——`AsStdWrite` 写进生产端、数据经环形缓冲流动、`AsStdRead` 从
/// 消费端读出。
async fn make_cb_pair(cap: usize) -> CbPair {
    buffex::circular_buff::builder::CircularBuffBuilder::with_capacity(cap)
        .expect("cap 必须在 [MIN_CAPACITY, MAX_CAPACITY] 内")
        .producer_passive()
        .consumer_passive()
        .build_async()
        .await
        .expect("双端被动构建不可能失败")
}

/// 把 `data` 全部写入 circular buff 写端（同步 API，供后台线程使用）。
///
/// 经 `TrBuffTryWrite::try_write` 借出两段式写段，数据经段原语
/// `move_items_from_buff` 位拷贝进缓冲，段 drop 时提交（推进写位置、唤醒
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
        let mut staging: Vec<MaybeUninit<u8>> = data[off..off + n]
            .iter()
            .map(|&b| MaybeUninit::new(b))
            .collect();
        let moved = segm.move_items_from_buff(&mut staging);
        assert_eq!(moved, n);
        drop(segm);
        off += n;
    }
}

/// 把 circular buff 读端当前可读的数据全部读出（同步 API，供后台线程使用）。
/// 读端空（且未关闭）时返回空——「等待」由调用方的循环 + 适配器负责。
fn cb_drain_available(rx: &mut CbRx) -> Vec<u8> {
    let mut out = Vec::new();
    let demand = Demand::at_least(1);
    while let Some(mut segm) = TrBuffTryRead::try_read(rx, &demand).pick_left() {
        let n = segm.least_count();
        assert!(n > 0, "读段不可能为空");
        let mut dst: Vec<MaybeUninit<u8>> = (0..n).map(|_| MaybeUninit::uninit()).collect();
        // SAFETY: 把缓冲段中的 `u8` 逐位搬进 `dst`（段 drop 时推进读位置）；
        // `u8` 无 drop 需求。
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

struct ConservativeTx<W>(W);

impl<W: TrBuffWrite<u8>> TrBuffWrite<u8> for ConservativeTx<W> {
    type WriteAsync<'f> = W::WriteAsync<'f> where Self: 'f;
    type SegmMut<'f> = W::SegmMut<'f> where Self: 'f;
    type Err = W::Err;

    fn is_stuffed_closing(&self) -> bool {
        false // 故意保守：永不提前退出，强制走 write_async 的等待路径
    }

    fn write_async<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> Self::WriteAsync<'f> {
        self.0.write_async(demand)
    }
}

impl<W: TrBuffTryWrite<u8>> TrBuffTryWrite<u8> for ConservativeTx<W> {
    fn try_write<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        self.0.try_write(demand)
    }
}

// ===========================================================================
// circular buff 载体：双端被动 + AsStdRead / AsStdWrite（全同步代码）
// ===========================================================================

/// 目的：证明 circular buff 的两端分别包上 `AsStdWrite`（生产端）与
/// `AsStdRead`（消费端）后，用**全同步**的代码就能让数据从生产端流进
/// circular buff、再从消费端读出——往返内容、顺序、数量完全一致。
///
/// 测试方法：
/// 1. 建立容量 64 的双端被动 circular buff，写端包 `AsStdWrite`、读端包
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

    let (mut tx, mut rx) = make_cb_pair(CAP).await;
    let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|i| (i * 7 + 1) as u8).collect();

        // 双端包装：生产端 → AsStdWrite，消费端 → AsStdRead。
        let mut writer = AsStdWrite::uncancellable(&mut tx);
        let mut reader = AsStdRead::uncancellable(&mut rx);

        // 全同步写入：数据流进 circular buff（段 drop 即提交、唤醒读端）。
        let n = writer.write(&payload).expect("write 不应失败");
        assert_eq!(n, PAYLOAD_LEN, "载荷小于容量，一次 write 应全部写入");
        tx.close(); // 写端关闭（EOF）：让读循环能确定结束

        // 全同步读出：数据从 circular buff 流到调用者缓冲。
        let mut buf = [0u8; 64];
        let rn = reader.read(&mut buf).expect("read 不应失败");
        assert_eq!(rn, PAYLOAD_LEN, "应读出全部写入的数据");
        assert_eq!(&buf[..rn], &payload[..], "读出内容必须与写入一致");
}

/// 目的：证明 circular buff 的读端包上 `AsStdRead` 后，面对「数据要过一段
/// 时间才由另一个执行体（后台线程）写入」的场景，一次 `read` 会通过适配器
/// 内部的 `block_on` **阻塞等待**直到数据真正到达（并随写端关闭结束），
/// 而不是因为第一轮 poll 没有数据就立即返回 0（提前退出）。
///
/// 测试方法：
/// 1. 建立容量 64 的 circular buff，读端包上 `AsStdRead`；
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

    let (mut tx, mut rx) = make_cb_pair(64).await;
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

/// 目的：证明 circular buff 读端在「数据流暂时为空但尚未关闭」的窗口期不会
/// 提前退出。一次 `read` 消费完第一批后缓冲变空（写端还开着），适配器的读
/// 循环必须继续阻塞等待第二批，而不是把「暂时为空」误判成 EOF 提前返回。
///
/// 测试方法：
/// 1. 后台生产者：睡 100ms → 写第一批 8 字节 → 等读者消费完（轮询
///    `tx.data_size()` 归零，此时读者必已进入第二批等待）→ 再睡 20ms 确保
///    读者已 park → 写第二批 8 字节 → 关闭写端；
/// 2. 主线程在 compio 运行时上下文里**只调用一次** `read`（缓冲 32 字节）；
///
/// 通过依据：
/// - `read` 返回 16（两批之和）——若在「暂时为空」处提前退出，只会返回 8；
/// - 内容 == 第一批 ++ 第二批（按序、无丢失）。
#[compio::test]
async fn cb_read_waits_across_transient_empty_phase() {
    let (mut tx, mut rx) = make_cb_pair(64).await;
    let batch1: Vec<u8> = (0..8).map(|i| (i as u8) + 1).collect(); // [1..=8]
    let batch2: Vec<u8> = (0..8).map(|i| (i as u8) + 100).collect(); // [100..=107]
    let mut expect = batch1.clone();
    expect.extend_from_slice(&batch2);

    let producer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        cb_write_all(&mut tx, &batch1);
        // 等读者消费完第一批（数据量归零 = 读者已读完，即将进入第二批等待）。
        let deadline = Instant::now() + Duration::from_secs(2);
        while tx.data_size() > 0 && Instant::now() < deadline {
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
            n,
            16,
            "必须读到两批共 16 字节；若在流暂时为空时提前退出，只会返回 8"
        );
        assert_eq!(&buf[..16], &expect[..], "两批数据必须按序完整到达");

    producer.join().expect("生产者线程不应 panic");
}

/// 目的：证明 circular buff 写端包上 `AsStdWrite` 后，载荷远超容量时数据
/// **完整、按序、无丢失**地送达延迟腾出空间的异步消费者——写端在管道满时
/// 返回 `Ok(0)`（std 惯例：写满当前可用空间即返回），由调用方循环重试，
/// 直到消费者腾出空间后全部写出。
///
/// 测试方法：
/// 1. 建立容量 16 的 circular buff，载荷 100 字节（远超容量，写端必然反复
///    撞上「满」）；
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

    let (mut tx, mut rx) = make_cb_pair(CAP).await;
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

/// 目的：证明 circular buff 写端适配器内部的 `write_async` + `block_on`
/// **确实会阻塞等待**异步空间——用 [`ConservativeTx`] 包装（`is_stuffed_closing`
/// 恒 `false`）强制走等待路径：当环形缓冲写满、读端要过一段时间才腾出空间
/// 时，**一次** `write` 调用必须完整写出整个载荷，而不是带着部分字节提前返回。
///
/// 说明：普通 circular buff 写端的 `is_stuffed_closing` 在满时会返回 `true`，
/// 让适配器在调用 `write_async` 之前就提前返回（std 惯例）；本测试用
/// [`ConservativeTx`] 强制走 `write_async` 的 Pending 等待路径，单独验证
/// 适配器（以及 circular buff 的写等待 park + 读端提交唤醒）的阻塞等待能力。
///
/// 测试方法：
/// 1. 建立容量 16 的 circular buff，载荷 100 字节（远超容量）；
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

    let (tx, mut rx) = make_cb_pair(CAP).await;
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
