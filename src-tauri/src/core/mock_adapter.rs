use crate::core::types::{
    EventKind, MeasurementKind, RawSourceSample, RawUsage, SourceNativeIdentity, SourceType,
    TemporalAccuracy, TimingInfo, TokenAccuracy,
};
use uuid::Uuid;

pub struct MockAdapter {
    collector_run_id: String,
}

impl MockAdapter {
    pub fn new(collector_run_id: &str) -> Self {
        Self {
            collector_run_id: collector_run_id.to_string(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_sample(
        &self,
        agent_id: &str,
        agent_name: &str,
        model: &str,
        session_id: &str,
        request_id: Option<&str>,
        turn_id: Option<&str>,
        monotonic_ns: u64,
        wall_ms: i64,
        kind: EventKind,
        is_cumulative: bool,
        input: u64,
        output: u64,
        cache_read: u64,
        cache_write: u64,
        reasoning: u64,
        source_priority: u8,
    ) -> RawSourceSample {
        self.create_sample_with_native(
            agent_id,
            agent_name,
            model,
            session_id,
            request_id,
            turn_id,
            monotonic_ns,
            wall_ms,
            kind,
            is_cumulative,
            input,
            output,
            cache_read,
            cache_write,
            reasoning,
            source_priority,
            SourceNativeIdentity::default(),
            format!("{}_mock_adapter", agent_id),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_sample_with_native(
        &self,
        agent_id: &str,
        agent_name: &str,
        model: &str,
        session_id: &str,
        request_id: Option<&str>,
        turn_id: Option<&str>,
        monotonic_ns: u64,
        wall_ms: i64,
        kind: EventKind,
        is_cumulative: bool,
        input: u64,
        output: u64,
        cache_read: u64,
        cache_write: u64,
        reasoning: u64,
        source_priority: u8,
        native_identity: SourceNativeIdentity,
        source_adapter_id: String,
    ) -> RawSourceSample {
        let (token_acc, temp_acc, measure_kind) = if kind == EventKind::Delta {
            (
                TokenAccuracy::Exact,
                TemporalAccuracy::StreamExact,
                MeasurementKind::NativeCounter,
            )
        } else if kind == EventKind::Snapshot {
            (
                TokenAccuracy::Exact,
                TemporalAccuracy::IntervalExact,
                MeasurementKind::SnapshotDelta,
            )
        } else {
            (
                TokenAccuracy::Exact,
                TemporalAccuracy::TurnExact,
                MeasurementKind::TurnAverage,
            )
        };

        RawSourceSample {
            sample_id: format!("mock_{}", Uuid::new_v4()),
            collector_run_id: self.collector_run_id.clone(),
            source_adapter_id,
            source_type: SourceType::Mock,
            observed_monotonic_ns: monotonic_ns,
            wall_timestamp_ms: wall_ms,
            source_timestamp_ms: Some(wall_ms),
            process_id: Some(1000),
            agent_id: agent_id.to_string(),
            agent_name: agent_name.to_string(),
            session_id: session_id.to_string(),
            request_id: request_id.map(|s| s.to_string()),
            turn_id: turn_id.map(|s| s.to_string()),
            response_id: None,
            native_identity,
            model: model.to_string(),
            provider: "mock_provider".to_string(),
            event_kind: kind,
            is_cumulative,
            is_final: kind == EventKind::Final,
            counter_reset_hint: false,
            raw_usage: RawUsage {
                raw_input_tokens: Some(input),
                raw_output_tokens: Some(output),
                raw_cache_read_tokens: Some(cache_read),
                raw_cache_write_tokens: Some(cache_write),
                raw_reasoning_tokens: Some(reasoning),
                raw_total_tokens: Some(input + output),
            },
            timing: TimingInfo {
                request_start_ms: Some(wall_ms - 1000),
                first_token_ms: Some(wall_ms - 800),
                last_token_ms: Some(wall_ms),
                prefill_start_ms: None,
                prefill_end_ms: None,
            },
            source_priority,
            token_accuracy: token_acc,
            temporal_accuracy: temp_acc,
            measurement_kind: measure_kind,
        }
    }
}
