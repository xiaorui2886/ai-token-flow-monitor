use crate::core::types::{
    CanonicalTokenDelta, GapState, InputThroughputMetric, IntervalAverageMetric, MeasurementKind,
    TemporalAccuracy,
};
use std::collections::{HashMap, VecDeque};

#[derive(Debug, Clone)]
struct TokenSampleRecord {
    collector_run_id: String,
    monotonic_ns: u64,
    delta_out: u64,
    #[allow(dead_code)]
    delta_in: u64,
    gap_state: GapState,
    temporal_accuracy: TemporalAccuracy,
    measurement_kind: MeasurementKind,
}

#[derive(Debug, Clone, Default)]
pub struct AgentTpsMetrics {
    pub current_out_tps: f64,
    pub avg_5s_out_tps: f64,
    pub current_in_tps: Option<f64>,
    pub input_metric: InputThroughputMetric,
    pub interval_avg_metric: Option<IntervalAverageMetric>,
    pub peak_out_tps: f64,
}

pub struct TPSEngine {
    agent_buffers: HashMap<String, VecDeque<TokenSampleRecord>>,
    agent_peaks: HashMap<String, f64>,
    agent_last_in_metrics: HashMap<String, (Option<f64>, InputThroughputMetric)>,
    agent_interval_metrics: HashMap<String, IntervalAverageMetric>,
    global_peak: f64,
}

impl Default for TPSEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl TPSEngine {
    pub fn new() -> Self {
        Self {
            agent_buffers: HashMap::new(),
            agent_peaks: HashMap::new(),
            agent_last_in_metrics: HashMap::new(),
            agent_interval_metrics: HashMap::new(),
            global_peak: 0.0,
        }
    }

    pub fn push_delta(&mut self, delta: &CanonicalTokenDelta) {
        let record = TokenSampleRecord {
            collector_run_id: delta.collector_run_id.clone(),
            monotonic_ns: delta.observed_monotonic_ns,
            delta_out: delta.delta_output_tokens,
            delta_in: delta.delta_context_input_tokens,
            gap_state: delta.gap_state,
            temporal_accuracy: delta.temporal_accuracy,
            measurement_kind: delta.measurement_kind,
        };

        // P0-1: Compute IntervalAverageMetric for non-stream interval samples (e.g., IntervalExact)
        if delta.temporal_accuracy == TemporalAccuracy::IntervalExact
            && delta.delta_output_tokens > 0
        {
            let interval_tps = delta.delta_output_tokens as f64 / 2.0; // 2s sample window default
            self.agent_interval_metrics.insert(
                delta.agent_id.clone(),
                IntervalAverageMetric {
                    interval_tokens: delta.delta_output_tokens,
                    interval_duration_sec: 2.0,
                    interval_tps,
                },
            );
        }

        // P0-3: Compute & store InputThroughputMetric
        let in_metric = self.compute_input_metric(delta);
        let in_tps_val = match in_metric {
            InputThroughputMetric::PrefillExact(v) => Some(v),
            InputThroughputMetric::EffectiveMeasured(v) => Some(v),
            InputThroughputMetric::Unavailable => None,
        };
        if in_tps_val.is_some() {
            self.agent_last_in_metrics
                .insert(delta.agent_id.clone(), (in_tps_val, in_metric));
        }

        let buffer = self
            .agent_buffers
            .entry(delta.agent_id.clone())
            .or_default();

        buffer.push_back(record);

        if let Some(latest) = buffer.back().cloned() {
            buffer.retain(|r| {
                r.collector_run_id == latest.collector_run_id
                    && (latest.monotonic_ns.saturating_sub(r.monotonic_ns)) <= 5_000_000_000
            });
        }
    }

    pub fn calculate_agent_tps(
        &mut self,
        agent_id: &str,
        current_monotonic_ns: u64,
        current_run_id: &str,
    ) -> AgentTpsMetrics {
        let buffer = match self.agent_buffers.get(agent_id) {
            Some(b) => b,
            None => return AgentTpsMetrics::default(),
        };

        if buffer.is_empty() {
            return AgentTpsMetrics::default();
        }

        let window_1s_ns = 1_000_000_000u64;
        let mut out_1s_tokens = 0u64;

        for r in buffer.iter().rev() {
            if r.collector_run_id != current_run_id {
                continue;
            }
            if current_monotonic_ns.saturating_sub(r.monotonic_ns) <= window_1s_ns {
                // P0-1: ONLY StreamExact with GapState::Normal and not TokenizerEstimate can enter Instant 1s Live OUT TPS!
                if r.temporal_accuracy != TemporalAccuracy::StreamExact {
                    continue;
                }
                if r.gap_state != GapState::Normal {
                    continue;
                }
                if r.measurement_kind == MeasurementKind::TokenizerEstimate {
                    continue;
                }
                out_1s_tokens += r.delta_out;
            } else {
                break;
            }
        }

        let current_out_tps = out_1s_tokens as f64;

        // Calculate 5s average OUT TPS
        let mut out_5s_tokens = 0u64;
        let mut oldest_ns = current_monotonic_ns;

        for r in buffer.iter() {
            if r.collector_run_id == current_run_id {
                out_5s_tokens += r.delta_out;
                if r.monotonic_ns < oldest_ns {
                    oldest_ns = r.monotonic_ns;
                }
            }
        }

        let elapsed_sec =
            (current_monotonic_ns.saturating_sub(oldest_ns) as f64 / 1_000_000_000.0).max(1.0);
        let avg_5s_out_tps = out_5s_tokens as f64 / elapsed_sec;

        let peak = self.agent_peaks.entry(agent_id.to_string()).or_insert(0.0);
        if current_out_tps > *peak {
            *peak = current_out_tps;
        }

        let (in_val, in_metric) = self
            .agent_last_in_metrics
            .get(agent_id)
            .cloned()
            .unwrap_or((None, InputThroughputMetric::Unavailable));

        let interval_metric = self.agent_interval_metrics.get(agent_id).cloned();

        AgentTpsMetrics {
            current_out_tps,
            avg_5s_out_tps,
            current_in_tps: in_val,
            input_metric: in_metric,
            interval_avg_metric: interval_metric,
            peak_out_tps: *peak,
        }
    }

    pub fn compute_input_metric(&self, delta: &CanonicalTokenDelta) -> InputThroughputMetric {
        if let (Some(start), Some(end)) =
            (delta.timing.prefill_start_ms, delta.timing.prefill_end_ms)
        {
            let dur_sec = (end - start) as f64 / 1000.0;
            if dur_sec > 0.0 {
                return InputThroughputMetric::PrefillExact(
                    delta.delta_context_input_tokens as f64 / dur_sec,
                );
            }
        }
        if let (Some(start), Some(first)) =
            (delta.timing.request_start_ms, delta.timing.first_token_ms)
        {
            let ttft_sec = (first - start) as f64 / 1000.0;
            if ttft_sec > 0.0 {
                return InputThroughputMetric::EffectiveMeasured(
                    delta.delta_context_input_tokens as f64 / ttft_sec,
                );
            }
        }
        InputThroughputMetric::Unavailable
    }

    pub fn update_global_peak(&mut self, current_global_out_tps: f64) {
        if current_global_out_tps > self.global_peak {
            self.global_peak = current_global_out_tps;
        }
    }

    pub fn get_global_peak(&self) -> f64 {
        self.global_peak
    }
}
