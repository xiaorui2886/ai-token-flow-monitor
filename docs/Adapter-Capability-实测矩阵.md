# Adapter Capability 实测矩阵 (Phase 0 Local Data Source Audit - Updated)

## 概述
本报告为 **AI Token Flow Monitor** Phase 0 对本机（Windows 环境 `C:\Users\Administrator`）实际存在的 AI Agent 数据源进行的 **Passive Read Audit** 结果。所有检测均采用只读模式（SQLite `mode=ro` URI，JSONL offset read），未修改任何代理配置、未注入 Hook、未拦截端口、未干预正在运行的进程。

---

## 1. 详细实测矩阵

| 审计维度 | 1. Codex | 2. Claude Code | 3. ZCode | 4. OpenCode / OpenCodex | 5. Hermes | 6. CC Switch Passive |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Agent** | Codex CLI / Code Mode | Claude Code | ZCode CLI / Engine V2 | OpenCode / OpenCodex | Hermes Agent | CC Switch Proxy Logs |
| **Installed** | `true` | `true` | `true` | `true` | `true` | `true` |
| **Running** | `true` (`codex.exe`, `codex-code-mode-host.exe`) | `false` | `true` (`ZCode.exe` PIDs) | `true` (`opencode.exe` PID 1160) | `true` (`hermes.exe` PID 3184) | `true` (`cli-proxy-api.exe` PID 19212) |
| **Version** | CLI 2026.x | CLI 2026.x | Engine V2 | OpenCode 2026.x | Hermes state v0 | CC-Switch DB user_version 16 |
| **Primary Source** | Rollout JSONL (`.codex/sessions/**/rollout-*.jsonl`) | Project Transcript JSONL (`.claude/projects/*/*.jsonl`) | SQLite `db.sqlite` (`model_usage` table) | SQLite `opencode.db` (`session` table) | SQLite `state.db` (`sessions` & `messages`) | Read-Only SQLite `cc-switch.db` (`proxy_request_logs`) |
| **Fallback Source** | `state_5.sqlite` (`threads` table) | `history.jsonl` | Daily Log (`.zcode/cli/log/zcode-*.jsonl`) | `.opencodex/usage.jsonl` | `kanban.db` / `config.yaml` | `proxy_live_backup` / `session_log_sync` |
| **Target Capability** | `LIVE` (App Server) | `TURN` | `NEAR_LIVE` | `NEAR_LIVE` | `NEAR_LIVE` | `NEAR_LIVE` |
| **Observed Capability** | `JSONL INTERVAL / Exact usage` (App Server: `NOT OBSERVED`) | `TURN / TurnExact` | `TURN / Timing Exact` (Potential: `NEAR_LIVE`) | `TURN / Usage Exact` (Potential: `NEAR_LIVE`) | `TURN / Usage Exact` (Potential: `NEAR_LIVE`) | `INTERVAL_EXACT / Near Live` |
| **Schema Version** | SQLite `user_version = 0`, JSONL rollout v1 | Transcript JSONL v1 | SQLite `user_version = 0` | SQLite `user_version = 0` | SQLite `user_version = 0` | SQLite `user_version = 16` |
| **Input Tokens** | `payload.info.total_token_usage.input_tokens` | `message.usage.input_tokens` | `input_tokens` | `tokens_input` | `input_tokens` | `input_tokens` |
| **Output Tokens** | `payload.info.total_token_usage.output_tokens` | `message.usage.output_tokens` | `output_tokens` | `tokens_output` | `output_tokens` | `output_tokens` |
| **Cache** | `cached_input_tokens` | `cache_creation_input_tokens`, `cache_read_input_tokens` | `cache_creation_input_tokens`, `cache_read_input_tokens` | `tokens_cache_read`, `tokens_cache_write` | `cache_read_tokens`, `cache_write_tokens` | `cache_read_tokens`, `cache_creation_tokens` |
| **Reasoning** | `reasoning_output_tokens` | `thinking` content blocks | `reasoning_tokens` | `tokens_reasoning` | `reasoning_tokens` | N/A |
| **Request ID** | `payload.call_id` / `payload.turn_id` | `message.id` / `promptId` | `logical_request_id` | `requestId` (in `.opencodex`) | `platform_message_id` | `request_id` |
| **Session ID** | `payload.session_id` | `sessionId` | `session_id` | `session_id` | `session_id` | `session_id` |
| **Timing** | `timestamp` (ISO8601 string) | `timestamp` (ISO8601 string) | `started_at`, `first_token_at`, `completed_at`, `duration_ms`, `time_to_first_token_ms` | `time_created`, `time_updated` (Unix ms) | `started_at`, `ended_at`, `timestamp` (Unix float s) | `created_at`, `latency_ms`, `first_token_ms`, `duration_ms` |
| **Counter Semantics** | Cumulative Snapshot per session/turn | Turn Final Usage (per assistant message) | Turn Final / Micro Snapshot per request | Turn Final Usage per session/request | Turn Final Usage per session/message | Final Request Usage Log |
| **Update Behaviour** | Stream event updates during generation; append-only JSONL | Appended on assistant message completion | Written upon turn completion / model usage event | Updated in SQLite on session write; JSONL append | Written on message completion | Written upon request completion |
| **Token Accuracy** | `EXACT` | `EXACT` | `EXACT` | `EXACT` | `EXACT` | `EXACT` |
| **Temporal Accuracy** | `INTERVAL_EXACT` (JSONL) | `TURN_EXACT` | `INTERVAL_EXACT` / `TURN_EXACT` | `TURN_EXACT` | `TURN_EXACT` | `INTERVAL_EXACT` |
| **Expected Realtime Level** | `NEAR_LIVE` (Observed JSONL) / `LIVE` (Target App Server) | `TURN` | `NEAR_LIVE` / `TURN` | `NEAR_LIVE` / `TURN` | `NEAR_LIVE` / `TURN` | `NEAR_LIVE` |
| **Correlation Confidence** | `Strong` | `Strong` | `Exact` | `Strong` | `Strong` | `Exact` |
| **Read Safety** | Passive file tailing / URI `mode=ro` | Passive JSONL file tailing | Passive URI `mode=ro` (`db.sqlite`) | Passive URI `mode=ro` (`opencode.db`) | Passive URI `mode=ro` (`state.db`) | Passive URI `mode=ro` (`cc-switch.db`) |
| **Evidence Path** | `C:\Users\Administrator\.codex\state_5.sqlite`<br>`C:\Users\Administrator\.codex\sessions\...\*.jsonl` | `C:\Users\Administrator\.claude\projects\*\*.jsonl` | `C:\Users\Administrator\.zcode\cli\db\db.sqlite` | `C:\Users\Administrator\.local\share\opencode\opencode.db`<br>`C:\Users\Administrator\.opencodex\usage.jsonl` | `C:\Users\Administrator\.hermes\state.db` | `C:\Users\Administrator\.cc-switch\cc-switch.db` |

---
*结论：适配器能力矩阵已更新，明确区分 Target Capability 与 Observed Capability。*
