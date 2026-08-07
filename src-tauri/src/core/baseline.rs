use crate::core::types::{BaselineMode, RequestCorrelationKey};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct SourceBaselineState {
    pub baseline_context_input: u64,
    pub baseline_fresh_input: u64,
    pub baseline_output: u64,
    pub baseline_cache_read: u64,
    pub baseline_cache_write: u64,
    pub baseline_reasoning: u64,

    pub last_context_input: u64,
    pub last_fresh_input: u64,
    pub last_output: u64,
    pub last_cache_read: u64,
    pub last_cache_write: u64,
    pub last_reasoning: u64,

    pub watermark_output: u64,
    pub generation_id: u32,
    pub mode: BaselineMode,
    pub is_initialized: bool,
}

pub struct BaselineTracker {
    states: HashMap<(String, RequestCorrelationKey), SourceBaselineState>,
}

impl Default for BaselineTracker {
    fn default() -> Self {
        Self::new()
    }
}

pub struct CounterDeltas {
    pub delta_context_input: u64,
    pub delta_fresh_input: u64,
    pub delta_output: u64,
    pub delta_cache_read: u64,
    pub delta_cache_write: u64,
    pub delta_reasoning: u64,
    pub is_late_old_sample: bool,
}

impl BaselineTracker {
    pub fn new() -> Self {
        Self {
            states: HashMap::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn process_counters(
        &mut self,
        source_adapter_id: &str,
        key: &RequestCorrelationKey,
        context_input: u64,
        fresh_input: u64,
        output: u64,
        cache_read: u64,
        cache_write: u64,
        reasoning: u64,
        mode: BaselineMode,
        is_explicit_reset: bool,
    ) -> CounterDeltas {
        let state_key = (source_adapter_id.to_string(), key.clone());

        if let Some(state) = self.states.get_mut(&state_key) {
            // Check for counter decrease (e.g. output < state.last_output)
            if output < state.last_output {
                if is_explicit_reset || mode == BaselineMode::ContinuousEpoch {
                    // Explicit new epoch/generation reset
                    state.generation_id += 1;
                    state.last_context_input = context_input;
                    state.last_fresh_input = fresh_input;
                    state.last_output = output;
                    state.last_cache_read = cache_read;
                    state.last_cache_write = cache_write;
                    state.last_reasoning = reasoning;
                    state.watermark_output = output;

                    return CounterDeltas {
                        delta_context_input: context_input,
                        delta_fresh_input: fresh_input,
                        delta_output: output,
                        delta_cache_read: cache_read,
                        delta_cache_write: cache_write,
                        delta_reasoning: reasoning,
                        is_late_old_sample: false,
                    };
                } else {
                    // P0-3: Out-of-order late old snapshot (e.g. 150 < 180)
                    return CounterDeltas {
                        delta_context_input: 0,
                        delta_fresh_input: 0,
                        delta_output: 0,
                        delta_cache_read: 0,
                        delta_cache_write: 0,
                        delta_reasoning: 0,
                        is_late_old_sample: true,
                    };
                }
            }

            // Normal sequential snapshot deltas for ALL 6 counters (P0-1)
            let d_context_in = output_delta(context_input, state.last_context_input);
            let d_fresh_in = output_delta(fresh_input, state.last_fresh_input);
            let d_output = output_delta(output, state.last_output);
            let d_cache_read = output_delta(cache_read, state.last_cache_read);
            let d_cache_write = output_delta(cache_write, state.last_cache_write);
            let d_reasoning = output_delta(reasoning, state.last_reasoning);

            state.last_context_input = context_input;
            state.last_fresh_input = fresh_input;
            state.last_output = output;
            state.last_cache_read = cache_read;
            state.last_cache_write = cache_write;
            state.last_reasoning = reasoning;

            if output > state.watermark_output {
                state.watermark_output = output;
            }

            CounterDeltas {
                delta_context_input: d_context_in,
                delta_fresh_input: d_fresh_in,
                delta_output: d_output,
                delta_cache_read: d_cache_read,
                delta_cache_write: d_cache_write,
                delta_reasoning: d_reasoning,
                is_late_old_sample: false,
            }
        } else {
            // First observation initialization
            let (d_c_in, d_f_in, d_out, d_c_read, d_c_write, d_reason) = match mode {
                BaselineMode::KnownZeroOrigin => (
                    context_input,
                    fresh_input,
                    output,
                    cache_read,
                    cache_write,
                    reasoning,
                ),
                BaselineMode::UnknownAttach => (0, 0, 0, 0, 0, 0),
                BaselineMode::ReplayRestore => (0, 0, 0, 0, 0, 0),
                BaselineMode::ContinuousEpoch => (
                    context_input,
                    fresh_input,
                    output,
                    cache_read,
                    cache_write,
                    reasoning,
                ),
            };

            let new_state = SourceBaselineState {
                baseline_context_input: context_input,
                baseline_fresh_input: fresh_input,
                baseline_output: output,
                baseline_cache_read: cache_read,
                baseline_cache_write: cache_write,
                baseline_reasoning: reasoning,

                last_context_input: context_input,
                last_fresh_input: fresh_input,
                last_output: output,
                last_cache_read: cache_read,
                last_cache_write: cache_write,
                last_reasoning: reasoning,

                watermark_output: output,
                generation_id: 1,
                mode,
                is_initialized: true,
            };

            self.states.insert(state_key, new_state);

            CounterDeltas {
                delta_context_input: d_c_in,
                delta_fresh_input: d_f_in,
                delta_output: d_out,
                delta_cache_read: d_c_read,
                delta_cache_write: d_c_write,
                delta_reasoning: d_reason,
                is_late_old_sample: false,
            }
        }
    }
}

fn output_delta(curr: u64, last: u64) -> u64 {
    if curr >= last {
        curr - last
    } else {
        curr
    }
}
