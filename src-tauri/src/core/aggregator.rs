use crate::core::tps_engine::TPSEngine;
use crate::core::types::AgentStatus;
use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct GlobalAggregatedMetrics {
    pub global_out_tps: f64,
    pub global_in_tps: Option<f64>,
    pub in_coverage_measured: usize,
    pub in_coverage_total: usize,
    pub generating_agents_count: usize,
    pub working_agents_count: usize,
    pub today_tokens: u64,
    pub session_tokens: u64,
    pub peak_out_tps: f64,
}

pub struct GlobalAggregator {
    agent_statuses: HashMap<String, AgentStatus>,
}

impl Default for GlobalAggregator {
    fn default() -> Self {
        Self::new()
    }
}

impl GlobalAggregator {
    pub fn new() -> Self {
        Self {
            agent_statuses: HashMap::new(),
        }
    }

    pub fn update_agent_status(&mut self, status: AgentStatus) {
        self.agent_statuses.insert(status.agent_id.clone(), status);
    }

    pub fn compute_global_metrics(
        &mut self,
        tps_engine: &mut TPSEngine,
        current_monotonic_ns: u64,
        current_run_id: &str,
    ) -> GlobalAggregatedMetrics {
        let mut global_out_tps = 0.0;
        let mut global_in_tps_sum = 0.0;
        let mut in_measured = 0;
        let mut generating_count = 0;
        let mut working_count = 0;
        let mut today_tokens = 0u64;
        let mut session_tokens = 0u64;

        let total_agents = self.agent_statuses.len();

        for (agent_id, status) in self.agent_statuses.iter_mut() {
            let metrics =
                tps_engine.calculate_agent_tps(agent_id, current_monotonic_ns, current_run_id);
            status.current_out_tps = metrics.current_out_tps;
            status.current_in_tps = metrics.current_in_tps;
            status.interval_avg_metric = metrics.interval_avg_metric;

            status.flags.generating = metrics.current_out_tps > 0.0;
            if status.flags.generating {
                generating_count += 1;
            } else if status.flags.request_active {
                working_count += 1;
            }

            global_out_tps += metrics.current_out_tps;

            if let Some(in_val) = metrics.current_in_tps {
                global_in_tps_sum += in_val;
                in_measured += 1;
            }

            today_tokens += status.today_tokens;
            session_tokens += status.session_tokens;
        }

        // P0-16 Fix: Global Peak is the max of historical GLOBAL OUT TPS over time!
        tps_engine.update_global_peak(global_out_tps);
        let global_peak = tps_engine.get_global_peak();

        let global_in_tps = if in_measured > 0 {
            Some(global_in_tps_sum)
        } else {
            None
        };

        GlobalAggregatedMetrics {
            global_out_tps,
            global_in_tps,
            in_coverage_measured: in_measured,
            in_coverage_total: total_agents,
            generating_agents_count: generating_count,
            working_agents_count: working_count,
            today_tokens,
            session_tokens,
            peak_out_tps: global_peak,
        }
    }

    pub fn get_agent_statuses(&self) -> Vec<AgentStatus> {
        self.agent_statuses.values().cloned().collect()
    }
}
