use crate::core::types::{
    CanonicalTokenDelta, CorrelationConfidence, RequestCorrelationKey, TemporalAccuracy,
    TokenAccuracy,
};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct ActiveSourceInfo {
    pub source_adapter_id: String,
    pub token_accuracy: TokenAccuracy,
    pub temporal_accuracy: TemporalAccuracy,
    pub priority: u8,
    pub contributed_context_input: u64,
    pub contributed_output: u64,
}

pub struct CrossSourceReconciler {
    seen_stable_ids: HashSet<(String, String)>,
    request_active_sources: HashMap<RequestCorrelationKey, ActiveSourceInfo>,
}

impl Default for CrossSourceReconciler {
    fn default() -> Self {
        Self::new()
    }
}

impl CrossSourceReconciler {
    pub fn new() -> Self {
        Self {
            seen_stable_ids: HashSet::new(),
            request_active_sources: HashMap::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn restore_state(
        &mut self,
        request_key: RequestCorrelationKey,
        source_id: String,
        token_acc: TokenAccuracy,
        temp_acc: TemporalAccuracy,
        priority: u8,
        contributed_context_input: u64,
        contributed_output: u64,
    ) {
        self.request_active_sources.insert(
            request_key,
            ActiveSourceInfo {
                source_adapter_id: source_id,
                token_accuracy: token_acc,
                temporal_accuracy: temp_acc,
                priority,
                contributed_context_input,
                contributed_output,
            },
        );
    }

    pub fn restore_stable_id(&mut self, source_id: String, stable_id: String) {
        self.seen_stable_ids.insert((source_id, stable_id));
    }

    pub fn reconcile(&mut self, delta: &CanonicalTokenDelta) -> Option<CanonicalTokenDelta> {
        // 1. Same-source Event Identity Deduplication (P0-2 & P0-10)
        let same_source_key = (
            delta.source_adapter_id.clone(),
            delta.stable_ingestion_id.clone(),
        );
        if self.seen_stable_ids.contains(&same_source_key) {
            return None;
        }
        self.seen_stable_ids.insert(same_source_key);

        // 2. Correlation Confidence Check (Bypass cross-source dedup for Weak/Unknown confidence)
        if delta.correlation_confidence <= CorrelationConfidence::Weak {
            return Some(delta.clone());
        }

        // 3. Active Source Ranking: TokenAccuracy -> TemporalAccuracy -> source_priority (P0-2 & Fix 5)
        let key = &delta.correlation_key;
        if let Some(active) = self.request_active_sources.get_mut(key) {
            if delta.source_adapter_id == active.source_adapter_id {
                active.contributed_context_input += delta.delta_context_input_tokens;
                active.contributed_output += delta.delta_output_tokens;
                Some(delta.clone())
            } else if is_better_source(
                delta.token_accuracy,
                delta.temporal_accuracy,
                delta.source_priority,
                active.token_accuracy,
                active.temporal_accuracy,
                active.priority,
            ) {
                // Source Handoff Reconciliation (Freeze Patch Fix 1: SAFE handoff with cumulative position alignment)
                let mut adjusted_delta = delta.clone();

                let prev_c_in = active.contributed_context_input;
                let prev_out = active.contributed_output;

                // Only handoff when new source cumulative position can safely prove alignment
                let can_align_out = match delta.source_cumulative_output {
                    Some(b_cum_out) => b_cum_out >= prev_out,
                    None => false, // Native Delta without cumulative position: NO auto handoff!
                };
                let can_align_in = match delta.source_cumulative_context_input {
                    Some(b_cum_in) => b_cum_in >= prev_c_in,
                    None => true, // Context input may be absent for pure output streams
                };

                if !(can_align_out && can_align_in) {
                    // Uncertain handoff: suppress live contribution & DO NOT switch active source!
                    // Mark source B as Pending/Uncertain by simply not switching.
                    return None;
                }

                if let Some(b_cum_in) = delta.source_cumulative_context_input {
                    adjusted_delta.delta_context_input_tokens = b_cum_in - prev_c_in;
                } else {
                    adjusted_delta.delta_context_input_tokens = 0;
                }

                if let Some(b_cum_out) = delta.source_cumulative_output {
                    adjusted_delta.delta_output_tokens = b_cum_out - prev_out;
                } else {
                    adjusted_delta.delta_output_tokens = 0;
                }

                adjusted_delta.delta_total =
                    adjusted_delta.delta_fresh_input_tokens + adjusted_delta.delta_output_tokens;

                active.source_adapter_id = delta.source_adapter_id.clone();
                active.token_accuracy = delta.token_accuracy;
                active.temporal_accuracy = delta.temporal_accuracy;
                active.priority = delta.source_priority;
                active.contributed_context_input += adjusted_delta.delta_context_input_tokens;
                active.contributed_output += adjusted_delta.delta_output_tokens;

                Some(adjusted_delta)
            } else {
                // Lower ranked source live delta suppressed to prevent double counting
                None
            }
        } else {
            // First source for this request
            self.request_active_sources.insert(
                key.clone(),
                ActiveSourceInfo {
                    source_adapter_id: delta.source_adapter_id.clone(),
                    token_accuracy: delta.token_accuracy,
                    temporal_accuracy: delta.temporal_accuracy,
                    priority: delta.source_priority,
                    contributed_context_input: delta.delta_context_input_tokens,
                    contributed_output: delta.delta_output_tokens,
                },
            );
            Some(delta.clone())
        }
    }
}

fn is_better_source(
    t1: TokenAccuracy,
    tp1: TemporalAccuracy,
    p1: u8,
    t2: TokenAccuracy,
    tp2: TemporalAccuracy,
    p2: u8,
) -> bool {
    if t1 != t2 {
        return t1 > t2;
    }
    if tp1 != tp2 {
        return tp1 > tp2;
    }
    p1 > p2
}
