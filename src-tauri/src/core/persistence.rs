use crate::core::types::{
    CanonicalCorrection, CanonicalRequestLedger, CanonicalTokenDelta, RequestCorrelationKey,
    SourceCheckpoint, TemporalAccuracy, TokenAccuracy,
};
use rusqlite::{params, Connection, Result};
use std::path::Path;

pub struct StorageManager {
    conn: Connection,
}

impl StorageManager {
    pub fn new_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let storage = Self { conn };
        storage.init_schema()?;
        Ok(storage)
    }

    pub fn new_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path)?;
        let storage = Self { conn };
        storage.init_schema()?;
        Ok(storage)
    }

    pub fn init_schema(&self) -> Result<()> {
        let mut user_ver: i32 = self
            .conn
            .query_row("PRAGMA user_version;", [], |r| r.get(0))
            .unwrap_or(0);

        if user_ver < 2 {
            // Fix 7: Safe Pre-release Schema Upgrade Policy
            // Check if old incompatible schema exists and drop development tables cleanly
            let table_exists: bool = self
                .conn
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='canonical_request_ledgers';",
                    [],
                    |r| r.get::<_, i32>(0),
                )
                .map(|c| c > 0)
                .unwrap_or(false);

            if table_exists {
                // Inspect table columns to verify compatibility
                let has_new_col: bool = self
                    .conn
                    .prepare("PRAGMA table_info(canonical_request_ledgers);")
                    .and_then(|mut stmt| {
                        let rows = stmt.query_map([], |r| r.get::<_, String>(1))?;
                        let mut found = false;
                        for col in rows {
                            if col.unwrap_or_default() == "canonical_context_input_total" {
                                found = true;
                                break;
                            }
                        }
                        Ok(found)
                    })
                    .unwrap_or(false);

                if !has_new_col {
                    // Rebuild development tables for v2 schema
                    self.conn.execute_batch(
                        "
                        DROP TABLE IF EXISTS canonical_token_deltas;
                        DROP TABLE IF EXISTS canonical_corrections;
                        DROP TABLE IF EXISTS canonical_request_ledgers;
                        ",
                    )?;
                }
            }

            self.conn.execute_batch("PRAGMA user_version = 2;")?;
            user_ver = 2;
        }

        self.conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;

            CREATE TABLE IF NOT EXISTS collector_runs (
                run_id TEXT PRIMARY KEY,
                started_wall_ms INTEGER NOT NULL,
                created_at_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS source_checkpoints (
                source_id TEXT PRIMARY KEY,
                last_file_offset INTEGER NOT NULL DEFAULT 0,
                last_db_row_id TEXT,
                last_sequence_id INTEGER,
                watermark_timestamp_ms INTEGER NOT NULL DEFAULT 0,
                updated_at_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS canonical_token_deltas (
                delta_id TEXT PRIMARY KEY,
                collector_run_id TEXT NOT NULL,
                stable_ingestion_id TEXT NOT NULL,
                source_adapter_id TEXT NOT NULL,
                agent_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                request_id TEXT NOT NULL,
                observed_monotonic_ns INTEGER NOT NULL,
                wall_timestamp_ms INTEGER NOT NULL,
                delta_context_input_tokens INTEGER NOT NULL,
                delta_fresh_input_tokens INTEGER NOT NULL,
                delta_output_tokens INTEGER NOT NULL,
                delta_cache_read INTEGER NOT NULL,
                delta_cache_write INTEGER NOT NULL,
                delta_reasoning INTEGER NOT NULL,
                delta_total INTEGER NOT NULL,
                token_accuracy TEXT NOT NULL,
                temporal_accuracy TEXT NOT NULL,
                measurement_kind TEXT NOT NULL,
                gap_state TEXT NOT NULL,
                source_priority INTEGER NOT NULL,
                UNIQUE(source_adapter_id, stable_ingestion_id)
            );

            CREATE TABLE IF NOT EXISTS canonical_corrections (
                correction_id TEXT PRIMARY KEY,
                collector_run_id TEXT NOT NULL,
                agent_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                request_id TEXT NOT NULL,
                wall_timestamp_ms INTEGER NOT NULL,
                context_input_correction INTEGER NOT NULL,
                fresh_input_correction INTEGER NOT NULL,
                output_correction INTEGER NOT NULL,
                cache_read_correction INTEGER NOT NULL,
                cache_write_correction INTEGER NOT NULL,
                reasoning_correction INTEGER NOT NULL,
                reason TEXT NOT NULL,
                old_source TEXT NOT NULL,
                new_authoritative_source TEXT NOT NULL,
                old_total INTEGER NOT NULL,
                new_total INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS canonical_request_ledgers (
                agent_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                request_id TEXT NOT NULL,
                model TEXT NOT NULL,
                provider TEXT NOT NULL,
                canonical_context_input_total INTEGER NOT NULL,
                canonical_fresh_input_total INTEGER NOT NULL,
                canonical_output_total INTEGER NOT NULL,
                canonical_cache_read INTEGER NOT NULL,
                canonical_cache_write INTEGER NOT NULL,
                canonical_reasoning INTEGER NOT NULL,
                live_contributed_context_input INTEGER NOT NULL,
                live_contributed_fresh_input INTEGER NOT NULL,
                live_contributed_output INTEGER NOT NULL,
                live_contributed_cache_read INTEGER NOT NULL,
                live_contributed_cache_write INTEGER NOT NULL,
                live_contributed_reasoning INTEGER NOT NULL,
                authoritative_final_context_input INTEGER,
                authoritative_final_fresh_input INTEGER,
                authoritative_final_output INTEGER,
                authoritative_final_cache_read INTEGER,
                authoritative_final_cache_write INTEGER,
                authoritative_final_reasoning INTEGER,
                winning_source TEXT NOT NULL,
                active_live_source_priority INTEGER NOT NULL,
                active_live_token_accuracy TEXT NOT NULL,
                active_live_temporal_accuracy TEXT NOT NULL,
                is_finalized INTEGER NOT NULL,
                normalization_version INTEGER NOT NULL,
                last_reconciled_at_ms INTEGER NOT NULL,
                PRIMARY KEY (agent_id, session_id, request_id)
            );

            CREATE TABLE IF NOT EXISTS agent_status (
                agent_id TEXT PRIMARY KEY,
                agent_name TEXT NOT NULL,
                model TEXT NOT NULL,
                provider TEXT NOT NULL,
                flags_json TEXT NOT NULL,
                current_in_tps REAL,
                current_out_tps REAL NOT NULL,
                today_tokens INTEGER NOT NULL,
                session_tokens INTEGER NOT NULL,
                last_updated_at_ms INTEGER NOT NULL
            );
            ",
        )?;

        let _ = user_ver; // Suppress unused variable warning
        Ok(())
    }

    pub fn record_collector_run(&self, run_id: &str, started_wall_ms: i64) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO collector_runs (run_id, started_wall_ms, created_at_ms) VALUES (?1, ?2, ?3)",
            params![run_id, started_wall_ms, chrono::Utc::now().timestamp_millis()],
        )?;
        Ok(())
    }

    pub fn save_canonical_transaction(
        &mut self,
        deltas: &[CanonicalTokenDelta],
        corrections: &[CanonicalCorrection],
        ledgers: &[CanonicalRequestLedger],
        checkpoint: Option<&SourceCheckpoint>,
    ) -> Result<()> {
        let tx = self.conn.transaction()?;

        for d in deltas {
            tx.execute(
                "INSERT OR IGNORE INTO canonical_token_deltas (
                    delta_id, collector_run_id, stable_ingestion_id, source_adapter_id,
                    agent_id, session_id, request_id, observed_monotonic_ns, wall_timestamp_ms,
                    delta_context_input_tokens, delta_fresh_input_tokens, delta_output_tokens,
                    delta_cache_read, delta_cache_write, delta_reasoning, delta_total,
                    token_accuracy, temporal_accuracy, measurement_kind, gap_state, source_priority
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)",
                params![
                    d.delta_id,
                    d.collector_run_id,
                    d.stable_ingestion_id,
                    d.source_adapter_id,
                    d.correlation_key.agent_id,
                    d.correlation_key.session_id,
                    d.correlation_key.request_id,
                    d.observed_monotonic_ns,
                    d.wall_timestamp_ms,
                    d.delta_context_input_tokens,
                    d.delta_fresh_input_tokens,
                    d.delta_output_tokens,
                    d.delta_cache_read,
                    d.delta_cache_write,
                    d.delta_reasoning,
                    d.delta_total,
                    format!("{:?}", d.token_accuracy),
                    format!("{:?}", d.temporal_accuracy),
                    format!("{:?}", d.measurement_kind),
                    format!("{:?}", d.gap_state),
                    d.source_priority,
                ],
            )?;
        }

        for c in corrections {
            tx.execute(
                "INSERT OR REPLACE INTO canonical_corrections (
                    correction_id, collector_run_id, agent_id, session_id, request_id,
                    wall_timestamp_ms, context_input_correction, fresh_input_correction,
                    output_correction, cache_read_correction, cache_write_correction,
                    reasoning_correction, reason, old_source, new_authoritative_source, old_total, new_total
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
                params![
                    c.correction_id,
                    c.collector_run_id,
                    c.correlation_key.agent_id,
                    c.correlation_key.session_id,
                    c.correlation_key.request_id,
                    c.wall_timestamp_ms,
                    c.context_input_correction,
                    c.fresh_input_correction,
                    c.output_correction,
                    c.cache_read_correction,
                    c.cache_write_correction,
                    c.reasoning_correction,
                    c.reason,
                    c.old_source,
                    c.new_authoritative_source,
                    c.old_total,
                    c.new_total,
                ],
            )?;
        }

        for l in ledgers {
            tx.execute(
                "INSERT OR REPLACE INTO canonical_request_ledgers (
                    agent_id, session_id, request_id, model, provider,
                    canonical_context_input_total, canonical_fresh_input_total, canonical_output_total,
                    canonical_cache_read, canonical_cache_write, canonical_reasoning,
                    live_contributed_context_input, live_contributed_fresh_input, live_contributed_output,
                    live_contributed_cache_read, live_contributed_cache_write, live_contributed_reasoning,
                    authoritative_final_context_input, authoritative_final_fresh_input, authoritative_final_output,
                    authoritative_final_cache_read, authoritative_final_cache_write, authoritative_final_reasoning,
                    winning_source, active_live_source_priority, active_live_token_accuracy, active_live_temporal_accuracy,
                    is_finalized, normalization_version, last_reconciled_at_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30)",
                params![
                    l.correlation_key.agent_id,
                    l.correlation_key.session_id,
                    l.correlation_key.request_id,
                    l.model,
                    l.provider,
                    l.canonical_context_input_total,
                    l.canonical_fresh_input_total,
                    l.canonical_output_total,
                    l.canonical_cache_read,
                    l.canonical_cache_write,
                    l.canonical_reasoning,
                    l.live_contributed_context_input,
                    l.live_contributed_fresh_input,
                    l.live_contributed_output,
                    l.live_contributed_cache_read,
                    l.live_contributed_cache_write,
                    l.live_contributed_reasoning,
                    l.authoritative_final_context_input,
                    l.authoritative_final_fresh_input,
                    l.authoritative_final_output,
                    l.authoritative_final_cache_read,
                    l.authoritative_final_cache_write,
                    l.authoritative_final_reasoning,
                    l.winning_source,
                    l.active_live_source_priority,
                    format!("{:?}", l.active_live_token_accuracy),
                    format!("{:?}", l.active_live_temporal_accuracy),
                    if l.is_finalized { 1 } else { 0 },
                    l.normalization_version,
                    l.last_reconciled_at_ms,
                ],
            )?;
        }

        if let Some(cp) = checkpoint {
            tx.execute(
                "INSERT OR REPLACE INTO source_checkpoints (
                    source_id, last_file_offset, last_db_row_id, last_sequence_id, watermark_timestamp_ms, updated_at_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    cp.source_id,
                    cp.last_file_offset,
                    cp.last_db_row_id,
                    cp.last_sequence_id,
                    cp.watermark_timestamp_ms,
                    cp.updated_at_ms,
                ],
            )?;
        }

        tx.commit()?;
        Ok(())
    }

    pub fn load_ledgers(&self) -> Result<Vec<CanonicalRequestLedger>> {
        let mut stmt = self.conn.prepare(
            "SELECT agent_id, session_id, request_id, model, provider,
                    canonical_context_input_total, canonical_fresh_input_total, canonical_output_total,
                    canonical_cache_read, canonical_cache_write, canonical_reasoning,
                    live_contributed_context_input, live_contributed_fresh_input, live_contributed_output,
                    live_contributed_cache_read, live_contributed_cache_write, live_contributed_reasoning,
                    authoritative_final_context_input, authoritative_final_fresh_input, authoritative_final_output,
                    authoritative_final_cache_read, authoritative_final_cache_write, authoritative_final_reasoning,
                    winning_source, active_live_source_priority, active_live_token_accuracy, active_live_temporal_accuracy,
                    is_finalized, normalization_version, last_reconciled_at_ms
             FROM canonical_request_ledgers",
        )?;

        let rows = stmt.query_map([], |row| {
            let agent_id: String = row.get(0)?;
            let session_id: String = row.get(1)?;
            let request_id: String = row.get(2)?;
            let token_acc_str: String = row.get(25)?;
            let temp_acc_str: String = row.get(26)?;
            let is_finalized_int: i32 = row.get(27)?;

            Ok(CanonicalRequestLedger {
                correlation_key: RequestCorrelationKey {
                    agent_id: agent_id.clone(),
                    session_id,
                    request_id,
                },
                agent_id,
                model: row.get(3)?,
                provider: row.get(4)?,
                canonical_context_input_total: row.get(5)?,
                canonical_fresh_input_total: row.get(6)?,
                canonical_output_total: row.get(7)?,
                canonical_cache_read: row.get(8)?,
                canonical_cache_write: row.get(9)?,
                canonical_reasoning: row.get(10)?,
                live_contributed_context_input: row.get(11)?,
                live_contributed_fresh_input: row.get(12)?,
                live_contributed_output: row.get(13)?,
                live_contributed_cache_read: row.get(14)?,
                live_contributed_cache_write: row.get(15)?,
                live_contributed_reasoning: row.get(16)?,
                authoritative_final_context_input: row.get(17)?,
                authoritative_final_fresh_input: row.get(18)?,
                authoritative_final_output: row.get(19)?,
                authoritative_final_cache_read: row.get(20)?,
                authoritative_final_cache_write: row.get(21)?,
                authoritative_final_reasoning: row.get(22)?,
                winning_source: row.get(23)?,
                active_live_source_priority: row.get(24)?,
                active_live_token_accuracy: parse_token_acc(&token_acc_str),
                active_live_temporal_accuracy: parse_temporal_acc(&temp_acc_str),
                is_finalized: is_finalized_int != 0,
                normalization_version: row.get(28)?,
                last_reconciled_at_ms: row.get(29)?,
            })
        })?;

        let mut ledgers = Vec::new();
        for r in rows {
            ledgers.push(r?);
        }
        Ok(ledgers)
    }

    pub fn load_stable_ingestion_ids(&self) -> Result<Vec<(String, String)>> {
        let mut stmt = self
            .conn
            .prepare("SELECT source_adapter_id, stable_ingestion_id FROM canonical_token_deltas")?;
        let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        let mut ids = Vec::new();
        for r in rows {
            ids.push(r?);
        }
        Ok(ids)
    }

    pub fn load_checkpoints(&self) -> Result<Vec<SourceCheckpoint>> {
        let mut stmt = self.conn.prepare(
            "SELECT source_id, last_file_offset, last_db_row_id, last_sequence_id, watermark_timestamp_ms, updated_at_ms
             FROM source_checkpoints",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(SourceCheckpoint {
                source_id: row.get(0)?,
                last_file_offset: row.get(1)?,
                last_db_row_id: row.get(2)?,
                last_sequence_id: row.get(3)?,
                watermark_timestamp_ms: row.get(4)?,
                updated_at_ms: row.get(5)?,
            })
        })?;
        let mut cps = Vec::new();
        for r in rows {
            cps.push(r?);
        }
        Ok(cps)
    }

    pub fn get_total_output_tokens(&self, agent_id: &str) -> Result<u64> {
        let mut stmt = self.conn.prepare(
            "SELECT COALESCE(SUM(canonical_output_total), 0) FROM canonical_request_ledgers WHERE agent_id = ?1",
        )?;
        let total: u64 = stmt.query_row(params![agent_id], |row| row.get(0))?;
        Ok(total)
    }
}

fn parse_token_acc(s: &str) -> TokenAccuracy {
    match s {
        "Exact" => TokenAccuracy::Exact,
        "Measured" => TokenAccuracy::Measured,
        "Estimated" => TokenAccuracy::Estimated,
        _ => TokenAccuracy::Unavailable,
    }
}

fn parse_temporal_acc(s: &str) -> TemporalAccuracy {
    match s {
        "StreamExact" => TemporalAccuracy::StreamExact,
        "IntervalExact" => TemporalAccuracy::IntervalExact,
        "TurnExact" => TemporalAccuracy::TurnExact,
        "Estimated" => TemporalAccuracy::Estimated,
        _ => TemporalAccuracy::Unavailable,
    }
}
