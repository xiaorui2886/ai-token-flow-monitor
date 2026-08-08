use std::time::Instant;

use uuid::Uuid;

use super::types::ObservationTime;
/// The single collector clock for the whole runtime.
///
/// - `run_id`: unique collector run (`run_<uuid>`), shared by EnginePipeline + all adapters.
/// - `started_wall_ms`: wall time of runtime start (persisted via `record_collector_run`).
/// - `started_instant`: monotonic origin — `observe().monotonic_ns` is elapsed since here.
#[derive(Debug, Clone)]
pub struct CollectorClock {
    run_id: String,
    started_wall_ms: i64,
    started_instant: Instant,
}

impl CollectorClock {
    pub fn new() -> Self {
        Self::with_run_id(format!("run_{}", Uuid::new_v4()))
    }

    pub fn with_run_id(run_id: String) -> Self {
        Self {
            run_id,
            started_wall_ms: chrono::Utc::now().timestamp_millis(),
            started_instant: Instant::now(),
        }
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn started_wall_ms(&self) -> i64 {
        self.started_wall_ms
    }

    /// One observation on the SHARED clock timeline.
    pub fn observe(&self) -> ObservationTime {
        ObservationTime {
            monotonic_ns: self.started_instant.elapsed().as_nanos() as u64,
            wall_timestamp_ms: chrono::Utc::now().timestamp_millis(),
        }
    }
}

impl Default for CollectorClock {
    fn default() -> Self {
        Self::new()
    }
}
