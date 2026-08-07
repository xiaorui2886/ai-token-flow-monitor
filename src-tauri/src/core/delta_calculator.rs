use crate::core::gap_detector::GapDetector;
use crate::core::snapshot_accumulator::SnapshotAccumulator;
use uuid::Uuid;

use crate::core::types::{
    BaselineMode, CanonicalTokenDelta, CorrelationResult, NormalizedUsage, RawSourceSample,
    TemporalAccuracy,
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
        if acc_delta.delta_input == 0
            && acc_delta.delta_output == 0
            && acc_delta.delta_cache_read == 0
            && acc_delta.delta_cache_write == 0
            && acc_delta.delta_reasoning == 0
        {
            return None;
        }

        let stable_id = generate_stable_ingestion_id(sample);

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
            delta_input_tokens: acc_delta.delta_input,
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
