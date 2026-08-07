use crate::core::gap_detector::GapDetector;
use crate::core::snapshot_accumulator::SnapshotAccumulator;
use uuid::Uuid;

use crate::core::types::{
    BaselineMode, CanonicalTokenDelta, CorrelationResult, NormalizedUsage, RawSourceSample,
    TemporalAccuracy, UsageAccountingStrategy,
};

pub struct DeltaCalculator {
    accumulator: SnapshotAccumulator,
    gap_detector: GapDetector,
}

impl Default for DeltaCalculator {
    fn default() -> Self {
        Self::new()
    }
}

impl DeltaCalculator {
    pub fn new() -> Self {
        Self {
            accumulator: SnapshotAccumulator::new(),
            gap_detector: GapDetector::new(),
        }
    }

    pub fn calculate(
        &mut self,
        sample: &RawSourceSample,
        normalized: &NormalizedUsage,
        correlation: &CorrelationResult,
        mode: BaselineMode,
    ) -> Option<CanonicalTokenDelta> {
        let acc_delta = self.accumulator.process_sample(
            sample,
            normalized,
            &correlation.canonical_request_key,
            mode,
        );

        if acc_delta.is_late_old_sample {
            return None;
        }

        let (gap_state, temporal_acc) = self.gap_detector.inspect(
            &sample.collector_run_id,
            &sample.source_adapter_id,
            &correlation.canonical_request_key,
            sample.observed_monotonic_ns,
            sample.temporal_accuracy,
        );

        // Skip producing empty deltas
        if acc_delta.delta_context_input == 0
            && acc_delta.delta_fresh_input == 0
            && acc_delta.delta_output == 0
            && acc_delta.delta_cache_read == 0
            && acc_delta.delta_cache_write == 0
            && acc_delta.delta_reasoning == 0
        {
            return None;
        }

        let stable_id = generate_stable_ingestion_id(sample);

        // Multi-Field Consistency Freeze: cumulative position per field ONLY if raw field is actually available.
        // None = source cannot provide this field. Never treat unavailable as known zero.
        let raw = &sample.raw_usage;
        let semantics = &normalized.usage_semantics;

        let cum_ctx = if sample.is_cumulative && raw.raw_input_tokens.is_some() {
            match semantics.accounting_strategy {
                // AnthropicStyle: full context input requires cache read & creation availability semantics
                UsageAccountingStrategy::AnthropicStyle => {
                    if raw.raw_cache_read_tokens.is_some() && raw.raw_cache_write_tokens.is_some() {
                        Some(normalized.normalized_context_input_tokens)
                    } else {
                        None
                    }
                }
                _ => Some(normalized.normalized_context_input_tokens),
            }
        } else {
            None
        };

        let cum_fresh = if sample.is_cumulative && raw.raw_input_tokens.is_some() {
            Some(normalized.normalized_fresh_input_tokens)
        } else {
            None
        };

        let cum_out = if sample.is_cumulative && raw.raw_output_tokens.is_some() {
            Some(normalized.normalized_output_tokens)
        } else {
            None
        };

        let cum_cr = if sample.is_cumulative && raw.raw_cache_read_tokens.is_some() {
            Some(normalized.cache_read_tokens)
        } else {
            None
        };

        let cum_cw = if sample.is_cumulative && raw.raw_cache_write_tokens.is_some() {
            Some(normalized.cache_write_tokens)
        } else {
            None
        };

        let cum_reason = if sample.is_cumulative && raw.raw_reasoning_tokens.is_some() {
            Some(normalized.reasoning_tokens)
        } else {
            None
        };

        Some(CanonicalTokenDelta {
            delta_id: format!("delta_{}", Uuid::new_v4()),
            collector_run_id: sample.collector_run_id.clone(),
            stable_ingestion_id: stable_id,
            source_adapter_id: sample.source_adapter_id.clone(),
            correlation_key: correlation.canonical_request_key.clone(),
            correlation_confidence: correlation.correlation_confidence,
            observed_monotonic_ns: sample.observed_monotonic_ns,
            wall_timestamp_ms: sample.wall_timestamp_ms,
            agent_id: sample.agent_id.clone(),
            agent_name: sample.agent_name.clone(),
            model: sample.model.clone(),
            provider: sample.provider.clone(),
            delta_context_input_tokens: acc_delta.delta_context_input,
            delta_fresh_input_tokens: acc_delta.delta_fresh_input,
            delta_output_tokens: acc_delta.delta_output,
            delta_cache_read: acc_delta.delta_cache_read,
            delta_cache_write: acc_delta.delta_cache_write,
            delta_reasoning: acc_delta.delta_reasoning,
            delta_total: acc_delta.delta_total,
            timing: sample.timing.clone(),
            token_accuracy: sample.token_accuracy,
            temporal_accuracy: min_temporal_acc(sample.temporal_accuracy, temporal_acc),
            measurement_kind: sample.measurement_kind,
            gap_state,
            source_priority: sample.source_priority,
            source_cumulative_context_input: cum_ctx,
            source_cumulative_fresh_input: cum_fresh,
            source_cumulative_output: cum_out,
            source_cumulative_cache_read: cum_cr,
            source_cumulative_cache_write: cum_cw,
            source_cumulative_reasoning: cum_reason,
        })
    }
}

pub fn generate_stable_ingestion_id(sample: &RawSourceSample) -> String {
    let native = &sample.native_identity;
    if let Some(ref nid) = native.native_event_id {
        if !nid.is_empty() {
            return format!("event_{}", nid);
        }
    }
    if let Some(ref row_id) = native.db_row_id {
        if !row_id.is_empty() {
            return format!("row_{}", row_id);
        }
    }
    if let (Some(ref f_hash), Some(offset)) = (&native.file_path_hash, native.byte_offset) {
        return format!("file_{}_{}", f_hash, offset);
    }
    if let Some(seq) = native.native_sequence_id {
        return format!("seq_{}", seq);
    }
    format!("sample_{}", sample.sample_id)
}

fn min_temporal_acc(a: TemporalAccuracy, b: TemporalAccuracy) -> TemporalAccuracy {
    if a < b {
        a
    } else {
        b
    }
}
