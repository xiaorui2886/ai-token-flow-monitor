use crate::core::types::{
    CanonicalCorrection, CanonicalRequestLedger, CanonicalTokenDelta, NormalizedUsage,
    RawSourceSample, RequestCorrelationKey,
};
use std::collections::HashMap;
use uuid::Uuid;

pub struct RequestLedgerManager {
    ledgers: HashMap<RequestCorrelationKey, CanonicalRequestLedger>,
}

impl Default for RequestLedgerManager {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestLedgerManager {
    pub fn new() -> Self {
        Self {
            ledgers: HashMap::new(),
        }
    }

    pub fn restore_ledger(&mut self, ledger: CanonicalRequestLedger) {
        self.ledgers.insert(ledger.correlation_key.clone(), ledger);
    }

    pub fn record_live_delta(&mut self, delta: &CanonicalTokenDelta) {
        let entry = self
            .ledgers
            .entry(delta.correlation_key.clone())
            .or_insert_with(|| CanonicalRequestLedger {
                correlation_key: delta.correlation_key.clone(),
                agent_id: delta.agent_id.clone(),
                model: delta.model.clone(),
                provider: delta.provider.clone(),
                canonical_context_input_total: 0,
                canonical_fresh_input_total: 0,
                canonical_output_total: 0,
                canonical_cache_read: 0,
                canonical_cache_write: 0,
                canonical_reasoning: 0,
                live_contributed_context_input: 0,
                live_contributed_fresh_input: 0,
                live_contributed_output: 0,
                live_contributed_cache_read: 0,
                live_contributed_cache_write: 0,
                live_contributed_reasoning: 0,
                authoritative_final_context_input: None,
                authoritative_final_fresh_input: None,
                authoritative_final_output: None,
                authoritative_final_cache_read: None,
                authoritative_final_cache_write: None,
                authoritative_final_reasoning: None,
                winning_source: delta.source_adapter_id.clone(),
                active_live_source_priority: delta.source_priority,
                active_live_token_accuracy: delta.token_accuracy,
                active_live_temporal_accuracy: delta.temporal_accuracy,
                is_finalized: false,
                normalization_version: 1,
                last_reconciled_at_ms: delta.wall_timestamp_ms,
            });

        // Test R: If already finalized, do not rollback ledger on late live snapshots
        if entry.is_finalized {
            return;
        }

        entry.live_contributed_context_input += delta.delta_context_input_tokens;
        entry.live_contributed_fresh_input += delta.delta_fresh_input_tokens;
        entry.live_contributed_output += delta.delta_output_tokens;
        entry.live_contributed_cache_read += delta.delta_cache_read;
        entry.live_contributed_cache_write += delta.delta_cache_write;
        entry.live_contributed_reasoning += delta.delta_reasoning;

        entry.canonical_context_input_total += delta.delta_context_input_tokens;
        entry.canonical_fresh_input_total += delta.delta_fresh_input_tokens;
        entry.canonical_output_total += delta.delta_output_tokens;
        entry.canonical_cache_read += delta.delta_cache_read;
        entry.canonical_cache_write += delta.delta_cache_write;
        entry.canonical_reasoning += delta.delta_reasoning;

        entry.winning_source = delta.source_adapter_id.clone();
        entry.active_live_source_priority = delta.source_priority;
        entry.active_live_token_accuracy = delta.token_accuracy;
        entry.active_live_temporal_accuracy = delta.temporal_accuracy;
        entry.last_reconciled_at_ms = delta.wall_timestamp_ms;
    }

    pub fn finalize_authoritative(
        &mut self,
        sample: &RawSourceSample,
        normalized: &NormalizedUsage,
        key: &RequestCorrelationKey,
    ) -> (CanonicalRequestLedger, Option<CanonicalCorrection>) {
        let entry = self
            .ledgers
            .entry(key.clone())
            .or_insert_with(|| CanonicalRequestLedger {
                correlation_key: key.clone(),
                agent_id: sample.agent_id.clone(),
                model: sample.model.clone(),
                provider: sample.provider.clone(),
                canonical_context_input_total: 0,
                canonical_fresh_input_total: 0,
                canonical_output_total: 0,
                canonical_cache_read: 0,
                canonical_cache_write: 0,
                canonical_reasoning: 0,
                live_contributed_context_input: 0,
                live_contributed_fresh_input: 0,
                live_contributed_output: 0,
                live_contributed_cache_read: 0,
                live_contributed_cache_write: 0,
                live_contributed_reasoning: 0,
                authoritative_final_context_input: None,
                authoritative_final_fresh_input: None,
                authoritative_final_output: None,
                authoritative_final_cache_read: None,
                authoritative_final_cache_write: None,
                authoritative_final_reasoning: None,
                winning_source: sample.source_adapter_id.clone(),
                active_live_source_priority: sample.source_priority,
                active_live_token_accuracy: sample.token_accuracy,
                active_live_temporal_accuracy: sample.temporal_accuracy,
                is_finalized: false,
                normalization_version: 1,
                last_reconciled_at_ms: sample.wall_timestamp_ms,
            });

        // Save previous winning source BEFORE updating (P0-8 Fix)
        let old_source = entry.winning_source.clone();

        let target_ctx_in = normalized.normalized_context_input_tokens;
        let target_fresh_in = normalized.normalized_fresh_input_tokens;
        let target_out = normalized.normalized_output_tokens;
        let target_c_read = normalized.cache_read_tokens;
        let target_c_write = normalized.cache_write_tokens;
        let target_reason = normalized.reasoning_tokens;

        // Reconcile ALL 6 fields (P0-4 & P0-8)
        let diff_ctx_in = target_ctx_in as i64 - entry.canonical_context_input_total as i64;
        let diff_fresh_in = target_fresh_in as i64 - entry.canonical_fresh_input_total as i64;
        let diff_out = target_out as i64 - entry.canonical_output_total as i64;
        let diff_c_read = target_c_read as i64 - entry.canonical_cache_read as i64;
        let diff_c_write = target_c_write as i64 - entry.canonical_cache_write as i64;
        let diff_reason = target_reason as i64 - entry.canonical_reasoning as i64;

        entry.authoritative_final_context_input = Some(target_ctx_in);
        entry.authoritative_final_fresh_input = Some(target_fresh_in);
        entry.authoritative_final_output = Some(target_out);
        entry.authoritative_final_cache_read = Some(target_c_read);
        entry.authoritative_final_cache_write = Some(target_c_write);
        entry.authoritative_final_reasoning = Some(target_reason);

        entry.is_finalized = true;
        entry.winning_source = sample.source_adapter_id.clone();
        entry.last_reconciled_at_ms = sample.wall_timestamp_ms;

        let old_total = entry.canonical_context_input_total + entry.canonical_output_total;
        let new_total = target_ctx_in + target_out;

        entry.canonical_context_input_total = target_ctx_in;
        entry.canonical_fresh_input_total = target_fresh_in;
        entry.canonical_output_total = target_out;
        entry.canonical_cache_read = target_c_read;
        entry.canonical_cache_write = target_c_write;
        entry.canonical_reasoning = target_reason;

        // Task 02F-FIX #5: Authoritative Final reconciliation also updates model/provider
        // metadata (a changed-final rewrite with identical token numbers must converge to
        // identical_final_dedup, never stay "changed" forever). Token correction math untouched.
        entry.model = sample.model.clone();
        entry.provider = sample.provider.clone();

        let correction = if diff_ctx_in != 0
            || diff_fresh_in != 0
            || diff_out != 0
            || diff_c_read != 0
            || diff_c_write != 0
            || diff_reason != 0
        {
            Some(CanonicalCorrection {
                correction_id: format!("corr_{}", Uuid::new_v4()),
                collector_run_id: sample.collector_run_id.clone(),
                correlation_key: key.clone(),
                wall_timestamp_ms: sample.wall_timestamp_ms,
                context_input_correction: diff_ctx_in,
                fresh_input_correction: diff_fresh_in,
                output_correction: diff_out,
                cache_read_correction: diff_c_read,
                cache_write_correction: diff_c_write,
                reasoning_correction: diff_reason,
                reason: "Authoritative Final Usage Reconciliation".to_string(),
                old_source,
                new_authoritative_source: sample.source_adapter_id.clone(),
                old_total,
                new_total,
            })
        } else {
            None
        };

        (entry.clone(), correction)
    }

    pub fn get_ledger(&self, key: &RequestCorrelationKey) -> Option<&CanonicalRequestLedger> {
        self.ledgers.get(key)
    }
}
