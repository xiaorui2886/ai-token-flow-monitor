# Task Card 01-FIX — Changelog

## 概述
本文件记录了在 **Task Card 01-FIX** 阶段对 Core Metering Engine 进行了全量 P0 修正的具体变更。

---

## 变更项列表

1. **P0-1: 独立 Cumulative Token Counter Deltas**:
   - `SourceBaselineState` 升级为跟踪所有 6 个计数器 (`context_input`, `fresh_input`, `output`, `cache_read`, `cache_write`, `reasoning`) 的 last_value 与 watermark。
   - 快照累积模式下，所有 Counter 独立计算 `delta = current - previous`。
   - 新增 `Test S` (Cumulative Cache/Reasoning Delta)。
2. **P0-2: 重构 CrossSourceReconciler & Same-source / Cross-source 优先级**:
   - 删除错误的 `Hash(agent, session, request, delta_input, delta_output)` 事件解构规则。
   - `SourceNativeIdentity` (`native_event_id`, `db_row_id`, `file_path_hash + byte_offset`, `native_sequence_id`) 生成 `stable_ingestion_id` 实现同源幂等。
   - `RequestLiveSourceSelection` 根据 `TokenAccuracy` -> `TemporalAccuracy` -> `source_priority` 竞争选定唯一 Active Live Source。
   - 新增 `Test T` (Repeated Equal Real Delta) 与 `Test U` (Real Cross-Source Priority)。
3. **P0-3: 修复 Out-of-Order 快照逻辑**:
   - 当快照 Counter 变小 (`current < last`) 时，区别对待迟到旧快照与显式 Counter Reset。
   - 迟到旧快照 (`100 → 180 → 150 → 230`) Delta 被标记为 `0`，`last` 保持 180，下一条 230 Delta 为 50，总和仍等于 230。
   - 更新 `Test P`。
4. **P0-4: 适配器提供 Measurement Metadata**:
   - `RawSourceSample` 携带 `token_accuracy`, `temporal_accuracy`, `measurement_kind`。
   - Core Engine 仅执行必要降级 (`StreamExact` -> `IntervalExact`)，禁止升级 Accuracy。
5. **P0-5: Scope-Isolated GapDetector**:
   - `GapDetector` state 按照 `(collector_run_id, source_adapter_id, request_key)` 独立隔离。
   - 多 Agent 并发活动互不干扰 Gap State。新增 `Test AC`。
6. **P0-6 & P0-7: Catch-Up TPS 排除 & Option<f64> IN TPS**:
   - 1 秒 Instant Live OUT TPS 自动排除 `GapState::CatchUp`, `GapState::Stale`, `TemporalAccuracy::TurnExact`, `TemporalAccuracy::Unavailable`, `TokenizerEstimate` 及 `Correction`。
   - `InputThroughputMetric`: `PrefillExact`, `EffectiveMeasured`, `Unavailable` (`Option<f64>` 为 `None`)。
   - 新增 `Test V` (Effective IN) 与 `Test W` (IN Unavailable)。更新 `Test K`。
7. **P0-8: 权威 Final 全字段 Reconciliation & `old_source` 修复**:
   - `finalize_authoritative` 覆盖所有 5 个 Token 字段，并生成 signed `CanonicalCorrection`。
   - 修复 `winning_source` 更新顺序，保留真实 `old_source`。
   - 新增 `Test X` (Final All-Field Reconciliation)。
8. **P0-9: Provider UsageAccountingStrategy 重构**:
   - 支持 `OpenAiStyle`、`AnthropicStyle` 及 `GenericStyle`。
   - 新增 Normalization 测试 `Test Y1` (OpenAI Cached Input) 与 `Test Y2` (Anthropic Cache)。
9. **P0-10 & P0-11: 持久化幂等与 Final Ledger 全字段保存**:
   - `canonical_token_deltas` 增加 `UNIQUE(source_adapter_id, stable_ingestion_id)` 约束。
   - `canonical_request_ledgers` 补全所有字段，并在 Final 结算时无论是否有 correction 均持久化最新 Ledger。
10. **P0-12: Error Propagation (禁止吞 DB Error)**:
    - `EnginePipeline::process_sample` 返回 `Result<ProcessOutcome, EngineError>`。
11. **P0-13, P0-14 & P0-15: Crash / Replay & Ledger Reload**:
    - `EnginePipeline::new` 启动时自动从 SQLite 恢复 `CanonicalRequestLedger`, `active_sources` 和 `seen_stable_ids`。
    - 新增 `Test Z` (Stable Source Replay) 与 `Test AA` (Final Persistence Reload)。
12. **P0-16 & P0-17: Global Peak 动态追溯**:
    - `Global Peak` 追溯历史并发 `GLOBAL OUT TPS` 最大值 (新增 `test_global_peak_aggregation`)。
