pub mod aggregator;
pub mod baseline;
pub mod correlation;
pub mod delta_calculator;
pub mod gap_detector;
pub mod mock_adapter;
pub mod normalization;
pub mod persistence;
pub mod reconciler;
pub mod request_ledger;
pub mod snapshot_accumulator;
pub mod tps_engine;
pub mod types;

use crate::core::aggregator::GlobalAggregator;
use crate::core::correlation::RequestCorrelator;
use crate::core::delta_calculator::DeltaCalculator;
use crate::core::normalization::UsageNormalizer;
use crate::core::persistence::StorageManager;
use crate::core::reconciler::CrossSourceReconciler;
use crate::core::request_ledger::RequestLedgerManager;
use crate::core::tps_engine::TPSEngine;
use crate::core::types::{
    AgentRuntimeFlags, AgentStatus, BaselineMode, CommittedDetails, EngineError, EventKind,
    ProcessOutcome, RawSourceSample, SourceCheckpoint, UsageSemantics,
};
use parking_lot::Mutex;
use std::sync::Arc;

pub struct EnginePipeline {
    pub collector_run_id: String,
    pub delta_calculator: DeltaCalculator,
    pub request_ledger: RequestLedgerManager,
    pub reconciler: CrossSourceReconciler,
    pub tps_engine: TPSEngine,
    pub global_aggregator: GlobalAggregator,
    pub storage: Arc<Mutex<StorageManager>>,
}

impl EnginePipeline {
    pub fn new(
        collector_run_id: &str,
        storage: Arc<Mutex<StorageManager>>,
    ) -> Result<Self, EngineError> {
        let mut request_ledger = RequestLedgerManager::new();
        let mut reconciler = CrossSourceReconciler::new();

        // P0-15 Startup Restoration of Canonical Ledgers, Checkpoints & Dedup Stable IDs from SQLite
        {
            let storage_guard = storage.lock();
            if let Ok(ledgers) = storage_guard.load_ledgers() {
                for l in ledgers {
                    reconciler.restore_state(
                        l.correlation_key.clone(),
                        l.winning_source.clone(),
                        l.active_live_token_accuracy,
                        l.active_live_temporal_accuracy,
                        l.active_live_source_priority,
                    );
                    request_ledger.restore_ledger(l);
                }
            }
            if let Ok(stable_ids) = storage_guard.load_stable_ingestion_ids() {
                for (source_id, stable_id) in stable_ids {
                    reconciler.restore_stable_id(source_id, stable_id);
                }
            }
        }

        Ok(Self {
            collector_run_id: collector_run_id.to_string(),
            delta_calculator: DeltaCalculator::new(),
            request_ledger,
            reconciler,
            tps_engine: TPSEngine::new(),
            global_aggregator: GlobalAggregator::new(),
            storage,
        })
    }

    pub fn process_sample(
        &mut self,
        sample: &RawSourceSample,
        semantics: &UsageSemantics,
        mode: BaselineMode,
    ) -> Result<ProcessOutcome, EngineError> {
        self.process_sample_with_checkpoint(sample, semantics, mode, None)
    }

    pub fn process_sample_with_checkpoint(
        &mut self,
        sample: &RawSourceSample,
        semantics: &UsageSemantics,
        mode: BaselineMode,
        checkpoint: Option<&SourceCheckpoint>,
    ) -> Result<ProcessOutcome, EngineError> {
        let normalized = UsageNormalizer::normalize(&sample.raw_usage, semantics);
        let correlation = RequestCorrelator::correlate(sample);

        self.global_aggregator.update_agent_status(AgentStatus {
            agent_id: sample.agent_id.clone(),
            agent_name: sample.agent_name.clone(),
            model: sample.model.clone(),
            provider: sample.provider.clone(),
            flags: AgentRuntimeFlags {
                installed: true,
                running: true,
                request_active: true,
                generating: false,
                supported: true,
                adapter_healthy: true,
            },
            current_in_tps: None,
            current_out_tps: 0.0,
            interval_avg_metric: None,
            today_tokens: 0,
            session_tokens: 0,
            token_accuracy: sample.token_accuracy,
            temporal_accuracy: sample.temporal_accuracy,
            last_updated_at_ms: sample.wall_timestamp_ms,
        });

        if sample.event_kind == EventKind::Final {
            let (updated_ledger, correction) = self.request_ledger.finalize_authoritative(
                sample,
                &normalized,
                &correlation.canonical_request_key,
            );

            let mut storage_guard = self.storage.lock();
            let corrections_slice = match correction.as_ref() {
                Some(c) => vec![c.clone()],
                None => vec![],
            };

            // P0-11 & P0-5: Final ledger and Checkpoint are persisted in the SAME transaction!
            if let Err(e) = storage_guard.save_canonical_transaction(
                &[],
                &corrections_slice,
                std::slice::from_ref(&updated_ledger),
                checkpoint,
            ) {
                return Err(EngineError::StorageError(e.to_string()));
            }

            return Ok(ProcessOutcome::Committed(Box::new(CommittedDetails {
                delta: None,
                correction,
            })));
        }

        if let Some(raw_delta) =
            self.delta_calculator
                .calculate(sample, &normalized, &correlation, mode)
        {
            if let Some(canonical_delta) = self.reconciler.reconcile(&raw_delta) {
                self.request_ledger.record_live_delta(&canonical_delta);
                self.tps_engine.push_delta(&canonical_delta);

                let mut storage_guard = self.storage.lock();
                if let Some(ledger) = self
                    .request_ledger
                    .get_ledger(&canonical_delta.correlation_key)
                {
                    if let Err(e) = storage_guard.save_canonical_transaction(
                        std::slice::from_ref(&canonical_delta),
                        &[],
                        std::slice::from_ref(ledger),
                        checkpoint,
                    ) {
                        return Err(EngineError::StorageError(e.to_string()));
                    }
                }

                return Ok(ProcessOutcome::Committed(Box::new(CommittedDetails {
                    delta: Some(canonical_delta),
                    correction: None,
                })));
            }
        }

        Ok(ProcessOutcome::Committed(Box::new(CommittedDetails {
            delta: None,
            correction: None,
        })))
    }
}
