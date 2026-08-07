# Phase 0 Data Source Audit Evidence Report (Updated)

## 概述
本报告为 **AI Token Flow Monitor** Phase 0 对 Windows 本机真实数据源的 **Passive Audit 证据报告**。所有数据来源、目录结构、SQLite 表结构、JSONL Payload 及字段声明均来自本机真实审查，无任何虚构、无任何硬编码推测。敏感正文内容均已做 `<REDACTED_CONTENT>` 脱敏处理。

---

## 1. 适配器实测证据总表 (区分 Target vs Observed Capability)

| 审计维 | Codex | Claude Code | ZCode | OpenCode | Hermes | CC Switch |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Agent** | Codex CLI / Code Mode | Claude Code | ZCode CLI / Engine V2 | OpenCode / OpenCodex | Hermes Agent | CC Switch Proxy Logs |
| **Installed** | `true` | `true` | `true` | `true` | `true` | `true` |
| **Running** | `true` (`codex.exe`) | `false` | `true` (`ZCode.exe`) | `true` (`opencode.exe`) | `true` (`hermes.exe`) | `true` (`cli-proxy-api.exe`) |
| **Version** | CLI 2026.x | CLI 2026.x | Engine V2 | OpenCode 2026.x | Hermes state v0 | CC-Switch DB user_version 16 |
| **Detected Path** | `C:\Users\Administrator\.codex\` | `C:\Users\Administrator\.claude\` | `C:\Users\Administrator\.zcode\cli\` | `C:\Users\Administrator\.local\share\opencode\` | `C:\Users\Administrator\.hermes\` | `C:\Users\Administrator\.cc-switch\` |
| **Primary Source** | Rollout JSONL | Project Transcript JSONL | SQLite `db.sqlite` (`model_usage`) | SQLite `opencode.db` (`session`) | SQLite `state.db` (`sessions` & `messages`) | Read-Only SQLite `cc-switch.db` |
| **Fallback Source** | `state_5.sqlite` | `history.jsonl` | Daily Log `.jsonl` | `.opencodex/usage.jsonl` | `kanban.db` / `config.yaml` | `session_log_sync` |
| **Target Capability** | `LIVE` (App Server) | `TURN` | `NEAR_LIVE` | `NEAR_LIVE` | `NEAR_LIVE` | `NEAR_LIVE` |
| **Observed Capability** | `JSONL INTERVAL / Exact usage` (App Server: `NOT OBSERVED`) | `TURN / TurnExact` | `TURN / Timing Exact` (Potential: `NEAR_LIVE`) | `TURN / Usage Exact` (Potential: `NEAR_LIVE`) | `TURN / Usage Exact` (Potential: `NEAR_LIVE`) | `INTERVAL_EXACT / Near Live` |
| **Token Counters** | `input_tokens`<br>`output_tokens`<br>`cached_input_tokens`<br>`reasoning_output_tokens` | `input_tokens`<br>`output_tokens`<br>`cache_creation_input_tokens`<br>`cache_read_input_tokens` | `input_tokens`<br>`output_tokens`<br>`reasoning_tokens`<br>`cache_creation_input_tokens`<br>`cache_read_input_tokens` | `tokens_input`<br>`tokens_output`<br>`tokens_reasoning`<br>`tokens_cache_read`<br>`tokens_cache_write` | `input_tokens`<br>`output_tokens`<br>`reasoning_tokens`<br>`cache_read_tokens`<br>`cache_write_tokens` | `input_tokens`<br>`output_tokens`<br>`cache_read_tokens`<br>`cache_creation_tokens` |
| **Counter Semantics** | Cumulative Session/Turn Snapshot | Turn Final Usage | Turn Final / Micro Snapshot | Turn Final Usage | Turn Final Usage | Request Final Log |
| **Identity Fields** | `session_id`, `turn_id`, `call_id` | `sessionId`, `message.id` | `session_id`, `logical_request_id` | `session_id`, `requestId` | `session_id`, `platform_message_id` | `session_id`, `request_id` |
| **Timing Fields** | `timestamp` (ISO8601) | `timestamp` (ISO8601) | `started_at`, `first_token_at`, `completed_at`, `duration_ms`, `time_to_first_token_ms` | `time_created`, `time_updated` | `started_at`, `ended_at`, `timestamp` | `created_at`, `latency_ms`, `first_token_ms`, `duration_ms` |
| **Update Behaviour** | Append-only on rollout snapshot | Append-only on assistant message complete | SQLite write on model usage event | SQLite update on session save | SQLite insert on message write | SQLite insert on request end |
| **Token Accuracy** | `EXACT` | `EXACT` | `EXACT` | `EXACT` | `EXACT` | `EXACT` |
| **Temporal Accuracy** | `INTERVAL_EXACT` | `TURN_EXACT` | `INTERVAL_EXACT` / `TURN_EXACT` | `TURN_EXACT` | `TURN_EXACT` | `INTERVAL_EXACT` |
| **Correlation** | `Strong` | `Strong` | `Exact` | `Strong` | `Strong` | `Exact` |
| **Read Safety** | Passive file tailing / URI `mode=ro` | Passive file tailing | Passive URI `mode=ro` | Passive URI `mode=ro` | Passive URI `mode=ro` | Passive URI `mode=ro` |

---

## 2. 特别回答项 (Special Section Answers)

### 2.1 Codex 特别回答
- **数据源获取**:
  - `state_5.sqlite`: 包含 `threads` 表（`id`, `rollout_path`, `tokens_used`, `updated_at_ms`, `model`），主要用于 Session 发现与元数据（Metadata）。
  - `rollout-*.jsonl`: 包含 `payload.info.total_token_usage`（`input_tokens`, `cached_input_tokens`, `output_tokens`, `reasoning_output_tokens`, `total_tokens`），属于 **Cumulative Session/Turn Snapshot**。
- **App Server 推送频次**:
  - **Target Capability**: `LIVE` (App Server stream)。
  - **Observed Capability**: `JSONL INTERVAL / Exact usage` (App Server: **NOT OBSERVED**)。被动审计期间未建立活跃 WebSocket 监听，因此不能声明 `STREAM_EXACT`。根据已知实测证据，标记 TemporalAccuracy 为 `INTERVAL_EXACT`（来自 JSONL 增量）。

### 2.2 Claude Code 特别回答
- **更新时机**:
  - 本机 Claude Code transcript JSONL（`.claude/projects/*/*.jsonl`）仅在 Assistant Message 完成（Turn Completion）后写入记录。
  - **运行中无持续流式 Token Counter**。
- **实际字段**:
  - `input_tokens`: 存在 (`message.usage.input_tokens`)
  - `output_tokens`: 存在 (`message.usage.output_tokens`)
  - `cache_read`: 存在 (`message.usage.cache_read_input_tokens`)
  - `cache_creation`: 存在 (`message.usage.cache_creation_input_tokens`)
  - `thinking/reasoning`: 存在 `thinking` 内容块，但静态 JSONL 未拆分为独立 numeric reasoning counter。
- **验证结论**: 标定为 `TokenAccuracy = Exact`, `TemporalAccuracy = TurnExact`, `RealtimeLevel = TURN`。

### 2.3 ZCode 特别回答
- **实际 DB 路径**: `C:\Users\Administrator\.zcode\cli\db\db.sqlite`
- **实际表名**: `model_usage`
- **Token 字段**: `input_tokens`, `output_tokens`, `reasoning_tokens`, `cache_creation_input_tokens`, `cache_read_input_tokens`
- **Identity 字段**: `logical_request_id`, `session_id`, `parent_user_message_id`
- **写入时机**: Turn 完成及 Model Usage Event 触发时写入。
- **能力结论**:
  - **Observed Capability**: `TURN / Timing Exact`
  - **Potential Capability**: `NEAR LIVE`

---

## 3. Crash Test 证据分类

在 `tests/p0_tests.rs` 中的 `test_m_sqlite_primary_key_idempotency` 与 `test_z_stable_source_replay` 属于：
- **分类**: **B. SQLite Transaction Integration Test** (基于 SQLite WAL 事务提交与重放的集成测试)。
- **Real Process Crash / Kill Test 声明**: `REAL PROCESS CRASH TEST = NOT YET VERIFIED`（将在后续 Phase 阶段作为独立测试安排）。

---

## 4. Performance Benchmark 声明

在 Phase 1 报告中记录的资源指标：
- **测量工具**: Windows Task Manager / Process Explorer
- **测试环境**: Rust Debug Build (`cargo test`) 运行期间
- **指标状态**: **PRELIMINARY**（初步测量指标，非 Production Release Benchmark）。
