use crate::core::types::{CanonicalTokenDelta, CorrelationConfidence, RequestCorrelationKey};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct ActiveSourceInfo {
    pub source_adapter_id: String,
    pub priority: u8,
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

    pub fn restore_state(
        &mut self,
        request_key: RequestCorrelationKey,
        source_id: String,
        priority: u8,
    ) {
        self.request_active_sources.insert(
            request_key,
            ActiveSourceInfo {
                source_adapter_id: source_id,
                priority,
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

        // 3. Cross-source Priority Live Source Selection (P0-2)
        let key = &delta.correlation_key;
        if let Some(active) = self.request_active_sources.get_mut(key) {
            if delta.source_adapter_id == active.source_adapter_id {
                Some(delta.clone())
            } else if delta.source_priority > active.priority {
                active.source_adapter_id = delta.source_adapter_id.clone();
                active.priority = delta.source_priority;
                Some(delta.clone())
            } else {
                None
            }
        } else {
            self.request_active_sources.insert(
                key.clone(),
                ActiveSourceInfo {
                    source_adapter_id: delta.source_adapter_id.clone(),
                    priority: delta.source_priority,
                },
            );
            Some(delta.clone())
        }
    }
}
