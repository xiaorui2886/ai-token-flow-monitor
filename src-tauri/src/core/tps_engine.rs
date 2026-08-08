use crate::core::types::{
    CanonicalTokenDelta, GapState, InputThroughputMetric, IntervalAverageMetric, MeasurementKind,
    TemporalAccuracy, TimingInfo,
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
    agent_last_in_metrics: HashMap<String, (String, u64, Option<f64>, InputThroughputMetric)>,
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

        // Fix 1: Compute IntervalAverageMetric dynamically from measurement_interval_ms (no hardcoded 2s!)
        if delta.temporal_accuracy == TemporalAccuracy::IntervalExact
            && delta.delta_output_tokens > 0
        {
            let (dur_sec, tps) = if let Some(ms) = delta.timing.measurement_interval_ms {
                if ms > 0 {
                    let sec = ms as f64 / 1000.0;
                    (Some(sec), Some(delta.delta_output_tokens as f64 / sec))
                } else {
                    (None, None)
                }
            } else {
                (None, None)
            };

            self.agent_interval_metrics.insert(
                delta.agent_id.clone(),
                IntervalAverageMetric {
                    interval_tokens: delta.delta_output_tokens,
                    interval_duration_sec: dur_sec,
                    interval_tps: tps,
                },
            );
        }

        // Fix 4 & Task 02F §25: single shared helper records InputThroughputMetric (freshness ns).
        self.record_input_measurement(
            &delta.agent_id,
            &delta.collector_run_id,
            delta.observed_monotonic_ns,
            delta.delta_context_input_tokens,
            &delta.timing,
        );

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
        // Fix 4 & Task 02F §25/§28: Current IN TPS Freshness window (1s) is computed BEFORE the
        // OUT buffer lookup — a Final-only agent (TurnExact, e.g. ZCode SQLite adapter) has no
        // live OUT buffer but MUST still report its EffectiveMeasured/PrefillExact IN metric.
        let (in_val, in_metric) = if let Some((run_id, ns, val, metric)) =
            self.agent_last_in_metrics.get(agent_id)
        {
            if run_id == current_run_id && current_monotonic_ns.saturating_sub(*ns) <= 1_000_000_000
            {
                (*val, metric.clone())
            } else {
                (None, InputThroughputMetric::Unavailable)
            }
        } else {
            (None, InputThroughputMetric::Unavailable)
        };

        let buffer = match self.agent_buffers.get(agent_id) {
            Some(b) => b,
            None => {
                return AgentTpsMetrics {
                    current_in_tps: in_val,
                    input_metric: in_metric,
                    ..Default::default()
                }
            }
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
                // P0-1 & Fix 5: ONLY StreamExact with GapState::Normal and not TokenizerEstimate can enter Instant 1s Live OUT TPS!
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

        // Fix 5: Calculate 5s Live Average using Live Eligibility ONLY (StreamExact, GapState::Normal, not TokenizerEstimate)
        let mut out_5s_tokens = 0u64;
        let mut oldest_ns = current_monotonic_ns;

        for r in buffer.iter() {
            if r.collector_run_id == current_run_id {
                if r.temporal_accuracy != TemporalAccuracy::StreamExact {
                    continue;
                }
                if r.gap_state != GapState::Normal {
                    continue;
                }
                if r.measurement_kind == MeasurementKind::TokenizerEstimate {
                    continue;
                }

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

    /// Compute an InputThroughputMetric from raw counters + timing using the frozen rules:
    /// PrefillExact (prefill start/end) > EffectiveMeasured (request start -> first token) > Unavailable.
    /// Task 02F §25: shared by BOTH `push_delta` (live delta path) and the Final authoritative path.
    pub fn compute_input_metric_from_timing(
        context_input_tokens: u64,
        timing: &TimingInfo,
    ) -> InputThroughputMetric {
        if let (Some(start), Some(end)) = (timing.prefill_start_ms, timing.prefill_end_ms) {
            let dur_sec = (end - start) as f64 / 1000.0;
            if dur_sec > 0.0 {
                return InputThroughputMetric::PrefillExact(context_input_tokens as f64 / dur_sec);
            }
        }
        if let (Some(start), Some(first)) = (timing.request_start_ms, timing.first_token_ms) {
            let ttft_sec = (first - start) as f64 / 1000.0;
            if ttft_sec > 0.0 {
                return InputThroughputMetric::EffectiveMeasured(
                    context_input_tokens as f64 / ttft_sec,
                );
            }
        }
        InputThroughputMetric::Unavailable
    }

    /// Task 02F §25: record an input measurement with its freshness timestamp (ns).
    /// Stored ONLY when a measurable metric exists (never for `Unavailable`).
    /// The Final path calls this AFTER the durable transaction committed — a storage failure
    /// must never produce an IN metric.
    pub fn record_input_measurement(
        &mut self,
        agent_id: &str,
        collector_run_id: &str,
        observed_monotonic_ns: u64,
        context_input_tokens: u64,
        timing: &TimingInfo,
    ) {
        let metric = Self::compute_input_metric_from_timing(context_input_tokens, timing);
        let tps_val = match metric {
            InputThroughputMetric::PrefillExact(v)
            | InputThroughputMetric::EffectiveMeasured(v) => Some(v),
            InputThroughputMetric::Unavailable => None,
        };
        if let Some(v) = tps_val {
            self.agent_last_in_metrics.insert(
                agent_id.to_string(),
                (
                    collector_run_id.to_string(),
                    observed_monotonic_ns,
                    Some(v),
                    metric,
                ),
            );
        }
    }

    pub fn compute_input_metric(&self, delta: &CanonicalTokenDelta) -> InputThroughputMetric {
        Self::compute_input_metric_from_timing(delta.delta_context_input_tokens, &delta.timing)
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
