use crate::core::baseline::BaselineTracker;
use crate::core::types::{
    BaselineMode, EventKind, NormalizedUsage, RawSourceSample, RequestCorrelationKey,
};

pub struct AccumulatedDelta {
    pub delta_context_input: u64,
    pub delta_fresh_input: u64,
    pub delta_output: u64,
    pub delta_cache_read: u64,
    pub delta_cache_write: u64,
    pub delta_reasoning: u64,
    pub delta_total: u64,
    pub baseline_mode: BaselineMode,
    pub is_late_old_sample: bool,
}

pub struct SnapshotAccumulator {
    tracker: BaselineTracker,
}

impl Default for SnapshotAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl SnapshotAccumulator {
    pub fn new() -> Self {
        Self {
            tracker: BaselineTracker::new(),
        }
    }

    pub fn process_sample(
        &mut self,
        sample: &RawSourceSample,
        normalized: &NormalizedUsage,
        key: &RequestCorrelationKey,
        mode: BaselineMode,
    ) -> AccumulatedDelta {
        if sample.event_kind == EventKind::Delta {
            // Native delta mode
            return AccumulatedDelta {
                delta_context_input: normalized.normalized_context_input_tokens,
                delta_fresh_input: normalized.normalized_fresh_input_tokens,
                delta_output: normalized.normalized_output_tokens,
                delta_cache_read: normalized.cache_read_tokens,
                delta_cache_write: normalized.cache_write_tokens,
                delta_reasoning: normalized.reasoning_tokens,
                // Multi-Field Consistency Freeze: Canonical Total = Context Input + Output (Cache/Reasoning are subsets)
                delta_total: normalized.normalized_context_input_tokens
                    + normalized.normalized_output_tokens,
                baseline_mode: mode,
                is_late_old_sample: false,
            };
        }

        // Cumulative snapshot mode across ALL counters (P0-1)
        // Multi-Field Consistency Freeze: ONLY explicit counter_reset_hint triggers reset
        let is_explicit_reset = sample.counter_reset_hint;

        let c_deltas = self.tracker.process_counters(
            &sample.source_adapter_id,
            key,
            normalized.normalized_context_input_tokens,
            normalized.normalized_fresh_input_tokens,
            normalized.normalized_output_tokens,
            normalized.cache_read_tokens,
            normalized.cache_write_tokens,
            normalized.reasoning_tokens,
            mode,
            is_explicit_reset,
        );

        // Multi-Field Consistency Freeze: Canonical Total = Context Input + Output (NOT Fresh Input + Output!)
        let d_total = c_deltas.delta_context_input + c_deltas.delta_output;

        AccumulatedDelta {
            delta_context_input: c_deltas.delta_context_input,
            delta_fresh_input: c_deltas.delta_fresh_input,
            delta_output: c_deltas.delta_output,
            delta_cache_read: c_deltas.delta_cache_read,
            delta_cache_write: c_deltas.delta_cache_write,
            delta_reasoning: c_deltas.delta_reasoning,
            delta_total: d_total,
            baseline_mode: mode,
            is_late_old_sample: c_deltas.is_late_old_sample,
        }
    }
}
