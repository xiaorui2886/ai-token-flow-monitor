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
    // Multi-Field Consistency Freeze: 6-field canonical contribution cursor
    pub contributed_context_input: u64,
    pub contributed_fresh_input: u64,
    pub contributed_output: u64,
    pub contributed_cache_read: u64,
    pub contributed_cache_write: u64,
    pub contributed_reasoning: u64,
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
        contributed_fresh_input: u64,
        contributed_output: u64,
        contributed_cache_read: u64,
        contributed_cache_write: u64,
        contributed_reasoning: u64,
    ) {
        self.request_active_sources.insert(
            request_key,
            ActiveSourceInfo {
                source_adapter_id: source_id,
                token_accuracy: token_acc,
                temporal_accuracy: temp_acc,
                priority,
                contributed_context_input,
                contributed_fresh_input,
                contributed_output,
                contributed_cache_read,
                contributed_cache_write,
                contributed_reasoning,
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

        // 3. Active Source Ranking: TokenAccuracy -> TemporalAccuracy -> source_priority (P0-2)
        let key = &delta.correlation_key;
        if let Some(active) = self.request_active_sources.get_mut(key) {
            if delta.source_adapter_id == active.source_adapter_id {
                // Same active source: accumulate all 6 fields
                active.contributed_context_input += delta.delta_context_input_tokens;
                active.contributed_fresh_input += delta.delta_fresh_input_tokens;
                active.contributed_output += delta.delta_output_tokens;
                active.contributed_cache_read += delta.delta_cache_read;
                active.contributed_cache_write += delta.delta_cache_write;
                active.contributed_reasoning += delta.delta_reasoning;
                Some(delta.clone())
            } else if is_better_source(
                delta.token_accuracy,
                delta.temporal_accuracy,
                delta.source_priority,
                active.token_accuracy,
                active.temporal_accuracy,
                active.priority,
            ) {
                // Multi-Field Consistency Freeze: SAFE 6-field handoff alignment
                // A field the old source has contributed MUST be provable in the new source
                // (new cumulative Some and >= contributed). Otherwise: NO handoff, suppress live contribution.
                let can_align = |active_contrib: u64, new_cum: &Option<u64>| -> bool {
                    if active_contrib > 0 {
                        match new_cum {
                            Some(c) => *c >= active_contrib,
                            None => false, // cannot prove alignment for a contributed field!
                        }
                    } else {
                        true
                    }
                };

                if !(can_align(
                    active.contributed_context_input,
                    &delta.source_cumulative_context_input,
                ) && can_align(
                    active.contributed_fresh_input,
                    &delta.source_cumulative_fresh_input,
                ) && can_align(active.contributed_output, &delta.source_cumulative_output)
                    && can_align(
                        active.contributed_cache_read,
                        &delta.source_cumulative_cache_read,
                    )
                    && can_align(
                        active.contributed_cache_write,
                        &delta.source_cumulative_cache_write,
                    )
                    && can_align(
                        active.contributed_reasoning,
                        &delta.source_cumulative_reasoning,
                    ))
                {
                    // Uncertain handoff: suppress live contribution & DO NOT switch active source!
                    return None;
                }

                // Compute per-field deltas from new source cumulative positions (only contributed deltas)
                let mut adjusted_delta = delta.clone();

                adjusted_delta.delta_context_input_tokens = delta
                    .source_cumulative_context_input
                    .map(|c| c - active.contributed_context_input)
                    .unwrap_or(0);
                adjusted_delta.delta_fresh_input_tokens = delta
                    .source_cumulative_fresh_input
                    .map(|c| c - active.contributed_fresh_input)
                    .unwrap_or(0);
                adjusted_delta.delta_output_tokens = delta
                    .source_cumulative_output
                    .map(|c| c - active.contributed_output)
                    .unwrap_or(0);
                adjusted_delta.delta_cache_read = delta
                    .source_cumulative_cache_read
                    .map(|c| c - active.contributed_cache_read)
                    .unwrap_or(0);
                adjusted_delta.delta_cache_write = delta
                    .source_cumulative_cache_write
                    .map(|c| c - active.contributed_cache_write)
                    .unwrap_or(0);
                adjusted_delta.delta_reasoning = delta
                    .source_cumulative_reasoning
                    .map(|c| c - active.contributed_reasoning)
                    .unwrap_or(0);

                // Multi-Field Consistency Freeze: Canonical Total = Context Input + Output
                adjusted_delta.delta_total =
                    adjusted_delta.delta_context_input_tokens + adjusted_delta.delta_output_tokens;

                // Switch active source AFTER safe alignment
                active.source_adapter_id = delta.source_adapter_id.clone();
                active.token_accuracy = delta.token_accuracy;
                active.temporal_accuracy = delta.temporal_accuracy;
                active.priority = delta.source_priority;
                active.contributed_context_input += adjusted_delta.delta_context_input_tokens;
                active.contributed_fresh_input += adjusted_delta.delta_fresh_input_tokens;
                active.contributed_output += adjusted_delta.delta_output_tokens;
                active.contributed_cache_read += adjusted_delta.delta_cache_read;
                active.contributed_cache_write += adjusted_delta.delta_cache_write;
                active.contributed_reasoning += adjusted_delta.delta_reasoning;

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
                    contributed_fresh_input: delta.delta_fresh_input_tokens,
                    contributed_output: delta.delta_output_tokens,
                    contributed_cache_read: delta.delta_cache_read,
                    contributed_cache_write: delta.delta_cache_write,
                    contributed_reasoning: delta.delta_reasoning,
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
