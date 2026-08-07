# Core Metering Engine 实现报告 (Phase 1)

## 概述
本报告总结了 **AI Token Flow Monitor** 核心计量引擎（Core Metering Engine）在 `D:\实时Token监控工具` 项目中的具体实现架构、核心数据模型、算法逻辑与 SQLite 持久化保障。

---

## 1. 架构管线实现

引擎严格按照 Architecture Lock V2.1 实现全解耦数据处理管线：

```
[RawSourceSample]
       │
       ▼
[UsageNormalizer]       (根据 Provider UsageSemantics 归一化 Token 字段)
       │
       ▼
[RequestCorrelator]      (生成 CorrelationResult，计算 CorrelationConfidence)
       │
       ▼
[SnapshotAccumulator]   (基于 BaselineTracker 维护 baseline, last, watermark, epoch)
       │
       ▼
[DeltaCalculator]        (结合 GapDetector 产生 CanonicalTokenDelta u64)
       │
       ▼
[CrossSourceReconciler]  (消除跨 Adapter 重复上报，保留高优先级 Source)
       │
       ▼
[RequestLedgerManager]   (维护 CanonicalRequestLedger，Final 结算产生 CanonicalCorrection i64)
       │
       ▼
[TPSEngine]              (结合 collector_run_id + monotonic_elapsed_ns 计算 1s/5s/Peak OUT TPS)
       │
       ▼
[GlobalAggregator]       (汇总 Global OUT TPS, Measured IN TPS + IN Coverage, Active Count)
       │
       ▼
[StorageManager]         (SQLite WAL mode, 事务化提交 delta / correction / ledger / run)
```

---

## 2. 关键核心模块说明

1. **`types.rs`**:
   - 定义了 `RawSourceSample`, `NormalizedUsage`, `UsageSemantics`, `CanonicalTokenDelta`, `CanonicalCorrection`, `CanonicalRequestLedger`, `AgentStatus`, `AgentRuntimeFlags`, `BaselineMode`, `GapState`, `TokenAccuracy`, `TemporalAccuracy`, `MeasurementKind`, `CorrelationConfidence`.
2. **`normalization.rs` (`UsageNormalizer`)**:
   - 处理 Provider Token 语义。例如 OpenAI/Anthropic 下 `reasoning_is_output_subset = true` 和 `cache_is_input_subset = true` 时，正确计算 `fresh_input` 与 `normalized_output`，杜绝 Cache / Reasoning 的二次重复计算。
3. **`correlation.rs` (`RequestCorrelator`)**:
   - 解决请求 ID 缺失问题（Fix 3）。优先级：`request_id` (`Exact`) > `turn_id`/`response_id`/`native_message_id` (`Strong`) > `file_offset` (`Weak`) > `sample_id` (`Unknown`).
   - 当置信度为 `Weak`/`Unknown` 时，跨 Source 去重自动 bypass，防止误合并不同的真实请求。
4. **`baseline.rs` (`BaselineTracker`) & `snapshot_accumulator.rs`**:
   - 支持 `BaselineMode` 4 种模式：`KnownZeroOrigin`, `UnknownAttach`, `ReplayRestore`, `ContinuousEpoch`.
   - 解决监控器启动附着、进程重启、历史 Session 重放（Replay）、连续 Counter Reset 的增量精确计算。
5. **`delta_calculator.rs` & `gap_detector.rs`**:
   - `GapDetector` 使用 Monotonic Time 检测 >3s 关联空档，将 `GapState` 标记为 `CatchUp`，`TemporalAccuracy` 设为 `IntervalExact`，杜绝系统休眠/进程挂起恢复后的瞬时 TPS 爆高。
6. **`request_ledger.rs` (`RequestLedgerManager`)**:
   - 维护请求级权威账本 `CanonicalRequestLedger`。
   - 当权威 `Final` 记录（如 JSONL 结算 196）到来时，与 Live 累计值（如 160 或 200）比较，生成 signed 正/负修正记录 `CanonicalCorrection`（`+36` 或 `-4`），保持 Canonical Total 为 196，且修正记录不干扰 Live 实时 TPS。
7. **`tps_engine.rs` (`TPSEngine`)**:
   - 严格使用 `collector_run_id` + Monotonic Time (`observed_monotonic_ns`)。跨运行实例不直接比较 Instant。
   - 维护 1 秒滑动窗口 Instant OUT TPS、5 秒 Average OUT TPS 及 Peak TPS。
8. **`aggregator.rs` (`GlobalAggregator`)**:
   - 结合 Agent 正交 Flags（`installed`, `running`, `request_active`, `generating`, `supported`, `adapter_healthy`）。
   - 计算 `Global Live OUT TPS`、`Global Measured IN TPS` 及其 `IN Coverage`（如 `2/4`）与 `Generating/Working Agents` 统计。
9. **`persistence.rs` (`StorageManager`)**:
   - 基于 SQLite 事务实现 `save_canonical_transaction`，单次事务同时更新 checkpoint、canonical delta、correction 和 ledger，实现 100% 崩溃一致性（Crash Safety）与幂等性（Idempotency）。

---

## 3. 验收结论
核心引擎代码已全部实现，架构完全符合 Architecture Lock V2.1 要求。
