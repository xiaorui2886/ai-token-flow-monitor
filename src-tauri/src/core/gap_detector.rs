use crate::core::types::{GapState, RequestCorrelationKey, TemporalAccuracy};
use std::collections::HashMap;

pub struct GapDetector {
    source_last_ns: HashMap<(String, String, RequestCorrelationKey), u64>,
}

impl Default for GapDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl GapDetector {
    pub fn new() -> Self {
        Self {
            source_last_ns: HashMap::new(),
        }
    }

    pub fn inspect(
        &mut self,
        collector_run_id: &str,
        source_adapter_id: &str,
        key: &RequestCorrelationKey,
        current_monotonic_ns: u64,
        source_temporal_acc: TemporalAccuracy,
    ) -> (GapState, TemporalAccuracy) {
        let state_key = (
            collector_run_id.to_string(),
            source_adapter_id.to_string(),
            key.clone(),
        );

        if let Some(&last_ns) = self.source_last_ns.get(&state_key) {
            if current_monotonic_ns < last_ns {
                self.source_last_ns.insert(state_key, current_monotonic_ns);
                return (GapState::Stale, TemporalAccuracy::Unavailable);
            }

            let diff_ns = current_monotonic_ns - last_ns;
            self.source_last_ns.insert(state_key, current_monotonic_ns);

            if diff_ns > 3_000_000_000 {
                // Gap > 3 seconds -> CatchUp state with IntervalExact (P0-5)
                (GapState::CatchUp, TemporalAccuracy::IntervalExact)
            } else {
                (GapState::Normal, source_temporal_acc)
            }
        } else {
            self.source_last_ns.insert(state_key, current_monotonic_ns);
            (GapState::Normal, source_temporal_acc)
        }
    }
}
