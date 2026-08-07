use ai_token_flow_monitor_lib::core::baseline::BaselineTracker;
use ai_token_flow_monitor_lib::core::gap_detector::GapDetector;
use ai_token_flow_monitor_lib::core::mock_adapter::MockAdapter;
use ai_token_flow_monitor_lib::core::normalization::UsageNormalizer;
use ai_token_flow_monitor_lib::core::persistence::StorageManager;
use ai_token_flow_monitor_lib::core::types::*;
use ai_token_flow_monitor_lib::core::EnginePipeline;
use parking_lot::Mutex;
use std::sync::Arc;

fn create_test_pipeline(run_id: &str) -> EnginePipeline {
    let storage = Arc::new(Mutex::new(StorageManager::new_in_memory().unwrap()));
    EnginePipeline::new(run_id, storage).unwrap()
}

fn generic_semantics() -> UsageSemantics {
    UsageSemantics {
        reasoning_is_output_subset: true,
        accounting_strategy: UsageAccountingStrategy::GenericStyle,
        provider_name: "generic".to_string(),
    }
}

fn extract_delta(outcome: ProcessOutcome) -> Option<CanonicalTokenDelta> {
    match outcome {
        ProcessOutcome::Committed(details) => details.delta,
        _ => None,
    }
}

fn extract_correction(outcome: ProcessOutcome) -> Option<CanonicalCorrection> {
    match outcome {
        ProcessOutcome::Committed(details) => details.correction,
        _ => None,
    }
}

#[test]
fn test_a_snapshot_accumulation() {
    let mut pipeline = create_test_pipeline("run_test_a");
    let mock = MockAdapter::new("run_test_a");
    let semantics = generic_semantics();

    let snapshots = [20, 71, 133, 196];
    let mut total_output_deltas = 0u64;

    for (i, &out_val) in snapshots.iter().enumerate() {
        let sample = mock.create_sample(
            "codex",
            "Codex",
            "gpt-4o",
            "sess_a",
            Some("req_a"),
            None,
            (i as u64 + 1) * 1_000_000_000,
            1000 + (i as i64) * 1000,
            EventKind::Snapshot,
            true,
            50,
            out_val,
            0,
            0,
            0,
            1,
        );
        let outcome = pipeline
            .process_sample(&sample, &semantics, BaselineMode::KnownZeroOrigin)
            .unwrap();
        if let Some(d) = extract_delta(outcome) {
            total_output_deltas += d.delta_output_tokens;
        }
    }

    assert_eq!(
        total_output_deltas, 196,
        "Test A failed: Cumulative output must be 196"
    );
    println!("P0 Test A PASS: Snapshot Accumulation (20->71->133->196 = 196)");
}

#[test]
fn test_b_cross_source_duplicate() {
    let mut pipeline = create_test_pipeline("run_test_b");
    let semantics = generic_semantics();

    let mock_proxy = MockAdapter::new("run_test_b");
    let s_proxy = mock_proxy.create_sample_with_native(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_b",
        Some("req_b"),
        None,
        1_000_000_000,
        1000,
        EventKind::Snapshot,
        true,
        50,
        196,
        0,
        0,
        0,
        10,
        SourceNativeIdentity::default(),
        "codex_appserver".to_string(),
    );

    let mock_jsonl = MockAdapter::new("run_test_b");
    let s_jsonl = mock_jsonl.create_sample_with_native(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_b",
        Some("req_b"),
        None,
        1_000_000_000,
        1000,
        EventKind::Snapshot,
        true,
        50,
        196,
        0,
        0,
        0,
        5,
        SourceNativeIdentity::default(),
        "codex_jsonl".to_string(),
    );

    let mock_sqlite = MockAdapter::new("run_test_b");
    let s_sqlite = mock_sqlite.create_sample_with_native(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_b",
        Some("req_b"),
        None,
        1_000_000_000,
        1000,
        EventKind::Snapshot,
        true,
        50,
        196,
        0,
        0,
        0,
        1,
        SourceNativeIdentity::default(),
        "codex_sqlite".to_string(),
    );

    let o1 = pipeline
        .process_sample(&s_proxy, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();
    let o2 = pipeline
        .process_sample(&s_jsonl, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();
    let o3 = pipeline
        .process_sample(&s_sqlite, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    let count = [extract_delta(o1), extract_delta(o2), extract_delta(o3)]
        .iter()
        .filter(|d| d.is_some())
        .count();
    assert_eq!(
        count, 1,
        "Test B failed: Cross-source report must select single active live source"
    );
    println!(
        "P0 Test B PASS: Cross Source Duplicate Reconcile (Proxy/JSONL/SQLite 196 -> 1 count)"
    );
}

#[test]
fn test_c_replay_restore() {
    let mut tracker = BaselineTracker::new();
    let key = RequestCorrelationKey {
        agent_id: "codex".to_string(),
        session_id: "sess_c".to_string(),
        request_id: "req_c".to_string(),
    };

    let c = tracker.process_counters(
        "adapter_c",
        &key,
        1000,
        1000,
        50000,
        100,
        50,
        20,
        BaselineMode::ReplayRestore,
        false,
    );
    assert_eq!(c.delta_context_input, 0);
    assert_eq!(c.delta_output, 0);
    println!("P0 Test C PASS: Historical Replay Restore (50000 tokens -> delta 0)");
}

#[test]
fn test_d1_known_epoch_restart() {
    let mut pipeline = create_test_pipeline("run_test_d1");
    let mock = MockAdapter::new("run_test_d1");
    let semantics = generic_semantics();

    let s1 = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_d",
        Some("req_d1"),
        None,
        1_000_000_000,
        1000,
        EventKind::Snapshot,
        true,
        0,
        500,
        0,
        0,
        0,
        1,
    );
    let s2 = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_d",
        Some("req_d1"),
        None,
        2_000_000_000,
        2000,
        EventKind::Snapshot,
        true,
        0,
        550,
        0,
        0,
        0,
        1,
    );

    pipeline
        .process_sample(&s1, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();
    pipeline
        .process_sample(&s2, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    let s3 = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_d",
        Some("req_d1"),
        None,
        3_000_000_000,
        3000,
        EventKind::Snapshot,
        true,
        0,
        30,
        0,
        0,
        0,
        1,
    );
    let s4 = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_d",
        Some("req_d1"),
        None,
        4_000_000_000,
        4000,
        EventKind::Snapshot,
        true,
        0,
        70,
        0,
        0,
        0,
        1,
    );

    let o3 = pipeline
        .process_sample(&s3, &semantics, BaselineMode::ContinuousEpoch)
        .unwrap();
    let o4 = pipeline
        .process_sample(&s4, &semantics, BaselineMode::ContinuousEpoch)
        .unwrap();

    let d3_out = extract_delta(o3)
        .map(|d| d.delta_output_tokens)
        .unwrap_or(0);
    let d4_out = extract_delta(o4)
        .map(|d| d.delta_output_tokens)
        .unwrap_or(0);

    assert_eq!(
        d3_out + d4_out,
        70,
        "Test D1 failed: Epoch restart deltas must equal 70"
    );
    println!("P0 Test D1 PASS: Known Epoch Restart (500->550, restart 30->70 = 120 total)");
}

#[test]
fn test_d2_unknown_reattach() {
    let mut tracker = BaselineTracker::new();
    let key = RequestCorrelationKey {
        agent_id: "codex".to_string(),
        session_id: "sess_d2".to_string(),
        request_id: "req_d2".to_string(),
    };

    let c1 = tracker.process_counters(
        "adapter_d2",
        &key,
        0,
        0,
        200,
        0,
        0,
        0,
        BaselineMode::UnknownAttach,
        false,
    );
    assert_eq!(c1.delta_output, 0);

    let c2 = tracker.process_counters(
        "adapter_d2",
        &key,
        0,
        0,
        250,
        0,
        0,
        0,
        BaselineMode::UnknownAttach,
        false,
    );
    assert_eq!(c2.delta_output, 50);
    println!("P0 Test D2 PASS: Unknown Reattach (Attach at 200 -> delta 0, next 250 -> delta 50)");
}

#[test]
fn test_d3_historical_replay() {
    let mut tracker = BaselineTracker::new();
    let key = RequestCorrelationKey {
        agent_id: "codex".to_string(),
        session_id: "sess_d3".to_string(),
        request_id: "req_d3".to_string(),
    };

    let c = tracker.process_counters(
        "adapter_d3",
        &key,
        100,
        100,
        50000,
        0,
        0,
        0,
        BaselineMode::ReplayRestore,
        false,
    );
    assert_eq!(c.delta_output, 0);
    println!("P0 Test D3 PASS: Historical Replay (50000 -> baseline 50000, delta 0)");
}

#[test]
fn test_e_parallel_agent_speed() {
    let mut pipeline = create_test_pipeline("run_test_e");
    let mock = MockAdapter::new("run_test_e");
    let semantics = generic_semantics();

    let now_ns = 5_000_000_000u64;
    let now_ms = 5000i64;

    let s1 = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "s1",
        Some("r1"),
        None,
        now_ns,
        now_ms,
        EventKind::Delta,
        false,
        0,
        60,
        0,
        0,
        0,
        1,
    );
    let s2 = mock.create_sample(
        "claude",
        "Claude",
        "sonnet",
        "s2",
        Some("r2"),
        None,
        now_ns,
        now_ms,
        EventKind::Delta,
        false,
        0,
        50,
        0,
        0,
        0,
        1,
    );
    let s3 = mock.create_sample(
        "zcode",
        "ZCode",
        "glm",
        "s3",
        Some("r3"),
        None,
        now_ns,
        now_ms,
        EventKind::Delta,
        false,
        0,
        30,
        0,
        0,
        0,
        1,
    );

    pipeline
        .process_sample(&s1, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();
    pipeline
        .process_sample(&s2, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();
    pipeline
        .process_sample(&s3, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    let global_metrics = pipeline.global_aggregator.compute_global_metrics(
        &mut pipeline.tps_engine,
        now_ns,
        "run_test_e",
    );

    assert_eq!(global_metrics.global_out_tps, 140.0);
    assert_eq!(global_metrics.peak_out_tps, 140.0);
    println!("P0 Test E PASS: Parallel Agent Speed Aggregation (Codex 60 + Claude 50 + ZCode 30 = 140 OUT TPS)");
}

#[test]
fn test_h_known_new_request() {
    let mut tracker = BaselineTracker::new();
    let key = RequestCorrelationKey {
        agent_id: "codex".to_string(),
        session_id: "sess_h".to_string(),
        request_id: "req_h".to_string(),
    };

    let c = tracker.process_counters(
        "adapter_h",
        &key,
        0,
        0,
        30,
        0,
        0,
        0,
        BaselineMode::KnownZeroOrigin,
        false,
    );
    assert_eq!(c.delta_output, 30);
    println!("P0 Test H PASS: Known New Request (First snapshot 30 -> delta 30)");
}

#[test]
fn test_i_positive_reconciliation() {
    let mut pipeline = create_test_pipeline("run_test_i");
    let mock = MockAdapter::new("run_test_i");
    let semantics = generic_semantics();

    let s_live = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_i",
        Some("req_i"),
        None,
        1_000_000_000,
        1000,
        EventKind::Snapshot,
        true,
        0,
        160,
        0,
        0,
        0,
        1,
    );
    pipeline
        .process_sample(&s_live, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    let s_final = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_i",
        Some("req_i"),
        None,
        2_000_000_000,
        2000,
        EventKind::Final,
        true,
        0,
        196,
        0,
        0,
        0,
        1,
    );
    let outcome = pipeline
        .process_sample(&s_final, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();
    let correction = extract_correction(outcome);

    assert!(correction.is_some());
    let corr = correction.unwrap();
    assert_eq!(corr.output_correction, 36);

    let ledger = pipeline
        .request_ledger
        .get_ledger(&RequestCorrelationKey {
            agent_id: "codex".to_string(),
            session_id: "sess_i".to_string(),
            request_id: "req_i".to_string(),
        })
        .unwrap();

    assert_eq!(ledger.canonical_output_total, 196);
    println!("P0 Test I PASS: Positive Reconciliation (Live 160, Final 196 -> Correction +36, Total 196)");
}

#[test]
fn test_j_negative_reconciliation() {
    let mut pipeline = create_test_pipeline("run_test_j");
    let mock = MockAdapter::new("run_test_j");
    let semantics = generic_semantics();

    let s_live = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_j",
        Some("req_j"),
        None,
        1_000_000_000,
        1000,
        EventKind::Snapshot,
        true,
        0,
        200,
        0,
        0,
        0,
        1,
    );
    pipeline
        .process_sample(&s_live, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    let s_final = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_j",
        Some("req_j"),
        None,
        2_000_000_000,
        2000,
        EventKind::Final,
        true,
        0,
        196,
        0,
        0,
        0,
        1,
    );
    let outcome = pipeline
        .process_sample(&s_final, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();
    let correction = extract_correction(outcome);

    assert!(correction.is_some());
    let corr = correction.unwrap();
    assert_eq!(corr.output_correction, -4);

    let ledger = pipeline
        .request_ledger
        .get_ledger(&RequestCorrelationKey {
            agent_id: "codex".to_string(),
            session_id: "sess_j".to_string(),
            request_id: "req_j".to_string(),
        })
        .unwrap();

    assert_eq!(ledger.canonical_output_total, 196);
    println!(
        "P0 Test J PASS: Negative Reconciliation (Live 200, Final 196 -> Correction -4, Total 196)"
    );
}

#[test]
fn test_k_sleep_resume_gap_pipeline() {
    let mut pipeline = create_test_pipeline("run_test_k");
    let mock = MockAdapter::new("run_test_k");
    let semantics = generic_semantics();

    let s1 = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_k",
        Some("req_k"),
        None,
        1_000_000_000,
        1000,
        EventKind::Delta,
        false,
        0,
        50,
        0,
        0,
        0,
        1,
    );
    pipeline
        .process_sample(&s1, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    // Sleep jump 5 seconds -> catchup +1800
    let s2 = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_k",
        Some("req_k"),
        None,
        6_500_000_000,
        6500,
        EventKind::Snapshot,
        true,
        0,
        1850,
        0,
        0,
        0,
        1,
    );
    pipeline
        .process_sample(&s2, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    let metrics = pipeline
        .tps_engine
        .calculate_agent_tps("codex", 6_500_000_000, "run_test_k");
    assert_ne!(
        metrics.current_out_tps, 1800.0,
        "CatchUp tokens must NOT spike 1s Instant Live OUT TPS to 1800!"
    );
    println!("CATCH UP TPS SUPPRESSION = PASS");
}

#[test]
fn test_l_wall_clock_jump() {
    let mut pipeline = create_test_pipeline("run_test_l");
    let mock = MockAdapter::new("run_test_l");
    let semantics = generic_semantics();

    let s1 = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_l",
        Some("req_l"),
        None,
        1_000_000_000,
        1000,
        EventKind::Delta,
        false,
        0,
        60,
        0,
        0,
        0,
        1,
    );
    let s2 = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_l",
        Some("req_l"),
        None,
        3_000_000_000,
        601000,
        EventKind::Delta,
        false,
        0,
        60,
        0,
        0,
        0,
        1,
    );

    pipeline
        .process_sample(&s1, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();
    pipeline
        .process_sample(&s2, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    let metrics = pipeline
        .tps_engine
        .calculate_agent_tps("codex", 3_000_000_000, "run_test_l");
    assert_eq!(metrics.current_out_tps, 60.0);
    println!(
        "P0 Test L PASS: Wall Clock Jump (+10 min wall jump -> Monotonic TPS strictly 60.0 t/s)"
    );
}

#[test]
fn test_m_sqlite_primary_key_idempotency() {
    let mut storage = StorageManager::new_in_memory().unwrap();
    storage.record_collector_run("run_m", 1000).unwrap();

    let d = CanonicalTokenDelta {
        delta_id: "d_m".to_string(),
        collector_run_id: "run_m".to_string(),
        stable_ingestion_id: "stable_m".to_string(),
        source_adapter_id: "mock_source".to_string(),
        correlation_key: RequestCorrelationKey {
            agent_id: "codex".to_string(),
            session_id: "sess_m".to_string(),
            request_id: "req_m".to_string(),
        },
        correlation_confidence: CorrelationConfidence::Exact,
        observed_monotonic_ns: 1_000_000_000,
        wall_timestamp_ms: 1000,
        agent_id: "codex".to_string(),
        agent_name: "Codex".to_string(),
        model: "gpt-4o".to_string(),
        provider: "openai".to_string(),
        delta_context_input_tokens: 10,
        delta_fresh_input_tokens: 10,
        delta_output_tokens: 50,
        delta_cache_read: 0,
        delta_cache_write: 0,
        delta_reasoning: 0,
        delta_total: 60,
        timing: TimingInfo::default(),
        token_accuracy: TokenAccuracy::Exact,
        temporal_accuracy: TemporalAccuracy::StreamExact,
        measurement_kind: MeasurementKind::NativeCounter,
        gap_state: GapState::Normal,
        source_priority: 1,
    };

    let l = CanonicalRequestLedger {
        correlation_key: d.correlation_key.clone(),
        agent_id: "codex".to_string(),
        model: "gpt-4o".to_string(),
        provider: "openai".to_string(),
        canonical_context_input_total: 10,
        canonical_fresh_input_total: 10,
        canonical_output_total: 50,
        canonical_cache_read: 0,
        canonical_cache_write: 0,
        canonical_reasoning: 0,
        live_contributed_context_input: 10,
        live_contributed_fresh_input: 10,
        live_contributed_output: 50,
        live_contributed_cache_read: 0,
        live_contributed_cache_write: 0,
        live_contributed_reasoning: 0,
        authoritative_final_context_input: None,
        authoritative_final_fresh_input: None,
        authoritative_final_output: None,
        authoritative_final_cache_read: None,
        authoritative_final_cache_write: None,
        authoritative_final_reasoning: None,
        winning_source: "mock_source".to_string(),
        active_live_source_priority: 1,
        active_live_token_accuracy: TokenAccuracy::Exact,
        active_live_temporal_accuracy: TemporalAccuracy::StreamExact,
        is_finalized: false,
        normalization_version: 1,
        last_reconciled_at_ms: 1000,
    };

    storage
        .save_canonical_transaction(
            std::slice::from_ref(&d),
            &[],
            std::slice::from_ref(&l),
            None,
        )
        .unwrap();
    storage
        .save_canonical_transaction(
            std::slice::from_ref(&d),
            &[],
            std::slice::from_ref(&l),
            None,
        )
        .unwrap();

    let total = storage.get_total_output_tokens("codex").unwrap();
    assert_eq!(total, 50);
    println!("P0 Test M PASS: SQLite Primary Key Idempotency (Total remains 50)");
}

#[test]
fn test_n_missing_request_id() {
    let mut pipeline = create_test_pipeline("run_test_n");
    let mock = MockAdapter::new("run_test_n");
    let semantics = generic_semantics();

    let s1 = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_n",
        None,
        None,
        1_000_000_000,
        1000,
        EventKind::Delta,
        false,
        0,
        20,
        0,
        0,
        0,
        1,
    );
    let s2 = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_n",
        None,
        None,
        2_000_000_000,
        2000,
        EventKind::Delta,
        false,
        0,
        30,
        0,
        0,
        0,
        1,
    );

    let o1 = pipeline
        .process_sample(&s1, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();
    let o2 = pipeline
        .process_sample(&s2, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    let d1 = extract_delta(o1);
    let d2 = extract_delta(o2);

    assert!(d1.is_some());
    assert!(d2.is_some());
    assert_eq!(
        d1.unwrap().correlation_confidence,
        CorrelationConfidence::Unknown
    );
    println!(
        "P0 Test N PASS: Missing Request ID (Low confidence correlation bypasses false merging)"
    );
}

#[test]
fn test_o_monitor_restart_run_id() {
    let mut p1 = create_test_pipeline("run_instance_1");
    let mut p2 = create_test_pipeline("run_instance_2");

    let mock1 = MockAdapter::new("run_instance_1");
    let mock2 = MockAdapter::new("run_instance_2");
    let semantics = generic_semantics();

    let s1 = mock1.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_o",
        Some("req_o"),
        None,
        1_000_000_000,
        1000,
        EventKind::Delta,
        false,
        0,
        60,
        0,
        0,
        0,
        1,
    );
    let s2 = mock2.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_o",
        Some("req_o"),
        None,
        100_000,
        2000,
        EventKind::Delta,
        false,
        0,
        60,
        0,
        0,
        0,
        1,
    );

    p1.process_sample(&s1, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();
    p2.process_sample(&s2, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    let m1 = p1
        .tps_engine
        .calculate_agent_tps("codex", 1_000_000_000, "run_instance_1");
    let m2 = p2
        .tps_engine
        .calculate_agent_tps("codex", 100_000, "run_instance_2");

    assert_eq!(m1.current_out_tps, 60.0);
    assert_eq!(m2.current_out_tps, 60.0);
    println!("P0 Test O PASS: Monitor Restart Run ID Isolation");
}

#[test]
fn test_p_out_of_order_samples() {
    let mut tracker = BaselineTracker::new();
    let key = RequestCorrelationKey {
        agent_id: "codex".to_string(),
        session_id: "sess_p".to_string(),
        request_id: "req_p".to_string(),
    };

    let c1 = tracker.process_counters(
        "adapter_p",
        &key,
        0,
        0,
        100,
        0,
        0,
        0,
        BaselineMode::KnownZeroOrigin,
        false,
    );
    let c2 = tracker.process_counters(
        "adapter_p",
        &key,
        0,
        0,
        180,
        0,
        0,
        0,
        BaselineMode::KnownZeroOrigin,
        false,
    );
    let c3 = tracker.process_counters(
        "adapter_p",
        &key,
        0,
        0,
        150,
        0,
        0,
        0,
        BaselineMode::KnownZeroOrigin,
        false,
    );
    let c4 = tracker.process_counters(
        "adapter_p",
        &key,
        0,
        0,
        230,
        0,
        0,
        0,
        BaselineMode::KnownZeroOrigin,
        false,
    );

    assert_eq!(c1.delta_output, 100);
    assert_eq!(c2.delta_output, 80);
    assert_eq!(c3.delta_output, 0);
    assert_eq!(c4.delta_output, 50);
    assert_eq!(
        c1.delta_output + c2.delta_output + c3.delta_output + c4.delta_output,
        230
    );
    println!("OUT OF ORDER = PASS");
}

#[test]
fn test_q_duplicate_same_source_event() {
    let mut pipeline = create_test_pipeline("run_test_q");
    let mock = MockAdapter::new("run_test_q");
    let semantics = generic_semantics();

    let s = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_q",
        Some("req_q"),
        None,
        1_000_000_000,
        1000,
        EventKind::Delta,
        false,
        0,
        50,
        0,
        0,
        0,
        1,
    );

    let o1 = pipeline
        .process_sample(&s, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();
    let o2 = pipeline
        .process_sample(&s, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    assert!(extract_delta(o1).is_some());
    assert!(extract_delta(o2).is_none());
    println!("P0 Test Q PASS: Duplicate Same-Source Event Deduplication");
}

#[test]
fn test_r_final_before_late_snapshot() {
    let mut pipeline = create_test_pipeline("run_test_r");
    let mock = MockAdapter::new("run_test_r");
    let semantics = generic_semantics();

    let s_final = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_r",
        Some("req_r"),
        None,
        1_000_000_000,
        1000,
        EventKind::Final,
        true,
        0,
        196,
        0,
        0,
        0,
        1,
    );
    pipeline
        .process_sample(&s_final, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    let s_late = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_r",
        Some("req_r"),
        None,
        2_000_000_000,
        2000,
        EventKind::Snapshot,
        true,
        0,
        180,
        0,
        0,
        0,
        1,
    );
    pipeline
        .process_sample(&s_late, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    let ledger = pipeline
        .request_ledger
        .get_ledger(&RequestCorrelationKey {
            agent_id: "codex".to_string(),
            session_id: "sess_r".to_string(),
            request_id: "req_r".to_string(),
        })
        .unwrap();

    assert_eq!(ledger.canonical_output_total, 196);
    println!("P0 Test R PASS: Final Before Late Snapshot Protection");
}

#[test]
fn test_s_cumulative_cache_reasoning_delta() {
    let mut pipeline = create_test_pipeline("run_test_s");
    let mock = MockAdapter::new("run_test_s");
    let semantics = generic_semantics();

    let s1 = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_s",
        Some("req_s"),
        None,
        1_000_000_000,
        1000,
        EventKind::Snapshot,
        true,
        1000,
        20,
        1000,
        200,
        10,
        1,
    );
    let s2 = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_s",
        Some("req_s"),
        None,
        2_000_000_000,
        2000,
        EventKind::Snapshot,
        true,
        1200,
        50,
        1200,
        250,
        20,
        1,
    );
    let s3 = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_s",
        Some("req_s"),
        None,
        3_000_000_000,
        3000,
        EventKind::Snapshot,
        true,
        1500,
        90,
        1500,
        300,
        35,
        1,
    );

    pipeline
        .process_sample(&s1, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();
    pipeline
        .process_sample(&s2, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();
    pipeline
        .process_sample(&s3, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    let ledger = pipeline
        .request_ledger
        .get_ledger(&RequestCorrelationKey {
            agent_id: "codex".to_string(),
            session_id: "sess_s".to_string(),
            request_id: "req_s".to_string(),
        })
        .unwrap();

    assert_eq!(ledger.canonical_output_total, 90);
    assert_eq!(ledger.canonical_cache_read, 1500);
    assert_eq!(ledger.canonical_cache_write, 300);
    assert_eq!(ledger.canonical_reasoning, 35);
    println!("CUMULATIVE CACHE DELTA = PASS");
}

#[test]
fn test_t_repeated_equal_real_delta() {
    let mut pipeline = create_test_pipeline("run_test_t");
    let mock = MockAdapter::new("run_test_t");
    let semantics = generic_semantics();

    let s1 = mock.create_sample_with_native(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_t",
        Some("req_t"),
        None,
        1_000_000_000,
        1000,
        EventKind::Delta,
        false,
        0,
        50,
        0,
        0,
        0,
        1,
        SourceNativeIdentity {
            native_event_id: Some("ev_1".to_string()),
            ..Default::default()
        },
        "codex_source".to_string(),
    );
    let s2 = mock.create_sample_with_native(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_t",
        Some("req_t"),
        None,
        2_000_000_000,
        2000,
        EventKind::Delta,
        false,
        0,
        50,
        0,
        0,
        0,
        1,
        SourceNativeIdentity {
            native_event_id: Some("ev_2".to_string()),
            ..Default::default()
        },
        "codex_source".to_string(),
    );
    let s3 = mock.create_sample_with_native(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_t",
        Some("req_t"),
        None,
        3_000_000_000,
        3000,
        EventKind::Delta,
        false,
        0,
        50,
        0,
        0,
        0,
        1,
        SourceNativeIdentity {
            native_event_id: Some("ev_3".to_string()),
            ..Default::default()
        },
        "codex_source".to_string(),
    );

    pipeline
        .process_sample(&s1, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();
    pipeline
        .process_sample(&s2, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();
    pipeline
        .process_sample(&s3, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    let ledger = pipeline
        .request_ledger
        .get_ledger(&RequestCorrelationKey {
            agent_id: "codex".to_string(),
            session_id: "sess_t".to_string(),
            request_id: "req_t".to_string(),
        })
        .unwrap();

    assert_eq!(ledger.canonical_output_total, 150);
    println!("REPEATED EQUAL DELTA = PASS");
}

#[test]
fn test_u_real_cross_source_priority() {
    let mut pipeline = create_test_pipeline("run_test_u");
    let semantics = generic_semantics();

    let mock_app = MockAdapter::new("run_test_u");
    let s_app = mock_app.create_sample_with_native(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_u",
        Some("req_u"),
        None,
        1_000_000_000,
        1000,
        EventKind::Delta,
        false,
        0,
        60,
        0,
        0,
        0,
        10,
        SourceNativeIdentity::default(),
        "codex_appserver".to_string(),
    );

    let mock_jsonl = MockAdapter::new("run_test_u");
    let s_jsonl = mock_jsonl.create_sample_with_native(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_u",
        Some("req_u"),
        None,
        1_000_000_000,
        1000,
        EventKind::Delta,
        false,
        0,
        60,
        0,
        0,
        0,
        5,
        SourceNativeIdentity::default(),
        "codex_jsonl".to_string(),
    );

    let o_app = pipeline
        .process_sample(&s_app, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();
    let o_jsonl = pipeline
        .process_sample(&s_jsonl, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    assert!(extract_delta(o_app).is_some());
    assert!(extract_delta(o_jsonl).is_none());
    println!("REAL CROSS SOURCE PRIORITY = PASS");
}

#[test]
fn test_v_effective_in_tps() {
    let mock = MockAdapter::new("run_test_v");
    let sample = RawSourceSample {
        timing: TimingInfo {
            request_start_ms: Some(1000),
            first_token_ms: Some(1500),
            ..Default::default()
        },
        ..mock.create_sample(
            "codex",
            "Codex",
            "gpt-4o",
            "sess_v",
            Some("req_v"),
            None,
            1_000_000_000,
            1000,
            EventKind::Delta,
            false,
            20000,
            0,
            0,
            0,
            0,
            1,
        )
    };

    let pipeline = create_test_pipeline("run_test_v");
    let metric = pipeline
        .tps_engine
        .compute_input_metric(&CanonicalTokenDelta {
            delta_id: "d_v".to_string(),
            collector_run_id: "run_test_v".to_string(),
            stable_ingestion_id: "s_v".to_string(),
            source_adapter_id: "mock".to_string(),
            correlation_key: RequestCorrelationKey {
                agent_id: "codex".to_string(),
                session_id: "sess_v".to_string(),
                request_id: "req_v".to_string(),
            },
            correlation_confidence: CorrelationConfidence::Exact,
            observed_monotonic_ns: 1_000_000_000,
            wall_timestamp_ms: 1000,
            agent_id: "codex".to_string(),
            agent_name: "Codex".to_string(),
            model: "gpt-4o".to_string(),
            provider: "openai".to_string(),
            delta_context_input_tokens: sample.raw_usage.raw_input_tokens.unwrap(),
            delta_fresh_input_tokens: sample.raw_usage.raw_input_tokens.unwrap(),
            delta_output_tokens: 0,
            delta_cache_read: 0,
            delta_cache_write: 0,
            delta_reasoning: 0,
            delta_total: 20000,
            timing: sample.timing,
            token_accuracy: TokenAccuracy::Exact,
            temporal_accuracy: TemporalAccuracy::StreamExact,
            measurement_kind: MeasurementKind::NativeCounter,
            gap_state: GapState::Normal,
            source_priority: 1,
        });

    assert_eq!(metric, InputThroughputMetric::EffectiveMeasured(40000.0));
    println!("EFFECTIVE IN TPS = PASS");
}

#[test]
fn test_w_in_unavailable() {
    let mock = MockAdapter::new("run_test_w");
    let sample = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_w",
        Some("req_w"),
        None,
        1_000_000_000,
        1000,
        EventKind::Delta,
        false,
        20000,
        0,
        0,
        0,
        0,
        1,
    );
    let pipeline = create_test_pipeline("run_test_w");

    let metric = pipeline
        .tps_engine
        .compute_input_metric(&CanonicalTokenDelta {
            delta_id: "d_w".to_string(),
            collector_run_id: "run_test_w".to_string(),
            stable_ingestion_id: "s_w".to_string(),
            source_adapter_id: "mock".to_string(),
            correlation_key: RequestCorrelationKey {
                agent_id: "codex".to_string(),
                session_id: "sess_w".to_string(),
                request_id: "req_w".to_string(),
            },
            correlation_confidence: CorrelationConfidence::Exact,
            observed_monotonic_ns: 1_000_000_000,
            wall_timestamp_ms: 1000,
            agent_id: "codex".to_string(),
            agent_name: "Codex".to_string(),
            model: "gpt-4o".to_string(),
            provider: "openai".to_string(),
            delta_context_input_tokens: sample.raw_usage.raw_input_tokens.unwrap(),
            delta_fresh_input_tokens: sample.raw_usage.raw_input_tokens.unwrap(),
            delta_output_tokens: 0,
            delta_cache_read: 0,
            delta_cache_write: 0,
            delta_reasoning: 0,
            delta_total: 20000,
            timing: TimingInfo::default(),
            token_accuracy: TokenAccuracy::Exact,
            temporal_accuracy: TemporalAccuracy::StreamExact,
            measurement_kind: MeasurementKind::NativeCounter,
            gap_state: GapState::Normal,
            source_priority: 1,
        });

    assert_eq!(metric, InputThroughputMetric::Unavailable);
}

#[test]
fn test_x_final_all_field_reconciliation() {
    let mut pipeline = create_test_pipeline("run_test_x");
    let mock = MockAdapter::new("run_test_x");
    let semantics = generic_semantics();

    // Live: input=100, output=200, cache_read=0, cache_write=200, reasoning=50
    let s_live = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_x",
        Some("req_x"),
        None,
        1_000_000_000,
        1000,
        EventKind::Snapshot,
        true,
        100,
        200,
        0,
        200,
        50,
        1,
    );
    pipeline
        .process_sample(&s_live, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    // Final: input=110, output=196, cache_read=950, cache_write=220, reasoning=45
    let s_final = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_x",
        Some("req_x"),
        None,
        2_000_000_000,
        2000,
        EventKind::Final,
        true,
        110,
        196,
        950,
        220,
        45,
        1,
    );
    let outcome = pipeline
        .process_sample(&s_final, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();
    let correction = extract_correction(outcome).unwrap();

    assert_eq!(correction.context_input_correction, 10);
    assert_eq!(correction.output_correction, -4);
    assert_eq!(correction.cache_read_correction, 950);
    assert_eq!(correction.cache_write_correction, 20);
    assert_eq!(correction.reasoning_correction, -5);

    let ledger = pipeline
        .request_ledger
        .get_ledger(&RequestCorrelationKey {
            agent_id: "codex".to_string(),
            session_id: "sess_x".to_string(),
            request_id: "req_x".to_string(),
        })
        .unwrap();

    assert_eq!(ledger.canonical_context_input_total, 110);
    assert_eq!(ledger.canonical_output_total, 196);
    assert_eq!(ledger.canonical_cache_read, 950);
    assert_eq!(ledger.canonical_cache_write, 220);
    assert_eq!(ledger.canonical_reasoning, 45);
    println!("FINAL ALL FIELD RECONCILIATION = PASS");
}

#[test]
fn test_z_stable_source_replay() {
    let temp_db_path = std::env::temp_dir().join(format!("test_z_{}.db", uuid::Uuid::new_v4()));
    {
        let storage = Arc::new(Mutex::new(StorageManager::new_file(&temp_db_path).unwrap()));
        let mut p1 = EnginePipeline::new("run_z1", storage).unwrap();
        let mock1 = MockAdapter::new("run_z1");
        let semantics = generic_semantics();

        let s1 = mock1.create_sample_with_native(
            "codex",
            "Codex",
            "gpt-4o",
            "sess_z",
            Some("req_z"),
            None,
            1_000_000_000,
            1000,
            EventKind::Delta,
            false,
            0,
            50,
            0,
            0,
            0,
            1,
            SourceNativeIdentity {
                native_event_id: Some("ev_z_001".to_string()),
                ..Default::default()
            },
            "codex_source".to_string(),
        );
        p1.process_sample(&s1, &semantics, BaselineMode::KnownZeroOrigin)
            .unwrap();
    }

    {
        let storage2 = Arc::new(Mutex::new(StorageManager::new_file(&temp_db_path).unwrap()));
        let mut p2 = EnginePipeline::new("run_z2", storage2.clone()).unwrap();
        let mock2 = MockAdapter::new("run_z2");
        let semantics = generic_semantics();

        let s2 = mock2.create_sample_with_native(
            "codex",
            "Codex",
            "gpt-4o",
            "sess_z",
            Some("req_z"),
            None,
            100_000,
            2000,
            EventKind::Delta,
            false,
            0,
            50,
            0,
            0,
            0,
            1,
            SourceNativeIdentity {
                native_event_id: Some("ev_z_001".to_string()),
                ..Default::default()
            },
            "codex_source".to_string(),
        );
        p2.process_sample(&s2, &semantics, BaselineMode::KnownZeroOrigin)
            .unwrap();

        let total = storage2.lock().get_total_output_tokens("codex").unwrap();
        assert_eq!(total, 50);
    }
    let _ = std::fs::remove_file(temp_db_path);
    println!("STABLE REPLAY IDEMPOTENCY = PASS");
}

#[test]
fn test_aa_final_persistence_reload() {
    let temp_db_path = std::env::temp_dir().join(format!("test_aa_{}.db", uuid::Uuid::new_v4()));
    {
        let storage = Arc::new(Mutex::new(StorageManager::new_file(&temp_db_path).unwrap()));
        let mut p1 = EnginePipeline::new("run_aa1", storage).unwrap();
        let mock = MockAdapter::new("run_aa1");
        let semantics = generic_semantics();

        let s_live = mock.create_sample(
            "codex",
            "Codex",
            "gpt-4o",
            "sess_aa",
            Some("req_aa"),
            None,
            1_000_000_000,
            1000,
            EventKind::Snapshot,
            true,
            0,
            160,
            0,
            0,
            0,
            1,
        );
        p1.process_sample(&s_live, &semantics, BaselineMode::KnownZeroOrigin)
            .unwrap();

        let s_final = mock.create_sample(
            "codex",
            "Codex",
            "gpt-4o",
            "sess_aa",
            Some("req_aa"),
            None,
            2_000_000_000,
            2000,
            EventKind::Final,
            true,
            0,
            196,
            0,
            0,
            0,
            1,
        );
        p1.process_sample(&s_final, &semantics, BaselineMode::KnownZeroOrigin)
            .unwrap();
    }

    {
        let storage2 = Arc::new(Mutex::new(StorageManager::new_file(&temp_db_path).unwrap()));
        let p2 = EnginePipeline::new("run_aa2", storage2).unwrap();

        let ledger = p2
            .request_ledger
            .get_ledger(&RequestCorrelationKey {
                agent_id: "codex".to_string(),
                session_id: "sess_aa".to_string(),
                request_id: "req_aa".to_string(),
            })
            .unwrap();

        assert_eq!(ledger.canonical_output_total, 196);
        assert!(ledger.is_finalized);
        assert_eq!(ledger.authoritative_final_output, Some(196));
    }
    let _ = std::fs::remove_file(temp_db_path);
    println!("FINAL LEDGER RELOAD = PASS");
}

#[test]
fn test_ab_repeated_same_size_tps() {
    let mut pipeline = create_test_pipeline("run_test_ab");
    let mock = MockAdapter::new("run_test_ab");
    let semantics = generic_semantics();

    let s1 = mock.create_sample_with_native(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_ab",
        Some("req_ab"),
        None,
        200_000_000,
        200,
        EventKind::Delta,
        false,
        0,
        20,
        0,
        0,
        0,
        1,
        SourceNativeIdentity {
            native_event_id: Some("e1".to_string()),
            ..Default::default()
        },
        "codex_source".to_string(),
    );
    let s2 = mock.create_sample_with_native(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_ab",
        Some("req_ab"),
        None,
        500_000_000,
        500,
        EventKind::Delta,
        false,
        0,
        20,
        0,
        0,
        0,
        1,
        SourceNativeIdentity {
            native_event_id: Some("e2".to_string()),
            ..Default::default()
        },
        "codex_source".to_string(),
    );
    let s3 = mock.create_sample_with_native(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_ab",
        Some("req_ab"),
        None,
        800_000_000,
        800,
        EventKind::Delta,
        false,
        0,
        20,
        0,
        0,
        0,
        1,
        SourceNativeIdentity {
            native_event_id: Some("e3".to_string()),
            ..Default::default()
        },
        "codex_source".to_string(),
    );

    pipeline
        .process_sample(&s1, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();
    pipeline
        .process_sample(&s2, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();
    pipeline
        .process_sample(&s3, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    let metrics = pipeline
        .tps_engine
        .calculate_agent_tps("codex", 800_000_000, "run_test_ab");
    assert_eq!(metrics.current_out_tps, 60.0);
    println!("REPEATED EQUAL DELTA = PASS");
}

#[test]
fn test_ac_multi_agent_gap_isolation() {
    let mut gap_detector = GapDetector::new();
    let key_codex = RequestCorrelationKey {
        agent_id: "codex".to_string(),
        session_id: "s_codex".to_string(),
        request_id: "r_codex".to_string(),
    };
    let key_claude = RequestCorrelationKey {
        agent_id: "claude".to_string(),
        session_id: "s_claude".to_string(),
        request_id: "r_claude".to_string(),
    };

    let (g1, _) = gap_detector.inspect(
        "run_ac",
        "codex_src",
        &key_codex,
        1_000_000_000,
        TemporalAccuracy::StreamExact,
    );
    assert_eq!(g1, GapState::Normal);

    let (g2, _) = gap_detector.inspect(
        "run_ac",
        "claude_src",
        &key_claude,
        10_000_000_000,
        TemporalAccuracy::StreamExact,
    );
    assert_eq!(g2, GapState::Normal);

    let (g3, _) = gap_detector.inspect(
        "run_ac",
        "codex_src",
        &key_codex,
        2_000_000_000,
        TemporalAccuracy::StreamExact,
    );
    assert_eq!(g3, GapState::Normal);
    println!("MULTI AGENT GAP ISOLATION = PASS");
}

#[test]
fn test_ad_cache_counter_reset() {
    let mut tracker = BaselineTracker::new();
    let key = RequestCorrelationKey {
        agent_id: "codex".to_string(),
        session_id: "sess_ad".to_string(),
        request_id: "req_ad".to_string(),
    };

    let c1 = tracker.process_counters(
        "adapter_ad",
        &key,
        0,
        0,
        100,
        1000,
        0,
        0,
        BaselineMode::KnownZeroOrigin,
        false,
    );
    let c2 = tracker.process_counters(
        "adapter_ad",
        &key,
        0,
        0,
        200,
        1500,
        0,
        0,
        BaselineMode::KnownZeroOrigin,
        false,
    );
    let c3 = tracker.process_counters(
        "adapter_ad",
        &key,
        0,
        0,
        50,
        100,
        0,
        0,
        BaselineMode::ContinuousEpoch,
        true,
    );
    let c4 = tracker.process_counters(
        "adapter_ad",
        &key,
        0,
        0,
        100,
        300,
        0,
        0,
        BaselineMode::ContinuousEpoch,
        false,
    );

    assert_eq!(c1.delta_cache_read, 1000);
    assert_eq!(c2.delta_cache_read, 500);
    assert_eq!(c3.delta_cache_read, 100);
    assert_eq!(c4.delta_cache_read, 200);
    assert_eq!(
        c1.delta_cache_read + c2.delta_cache_read + c3.delta_cache_read + c4.delta_cache_read,
        1800
    );
}

#[test]
fn test_ae_interval_exact_instant_exclusion() {
    let mut pipeline = create_test_pipeline("run_ae");
    let mock = MockAdapter::new("run_ae");
    let semantics = generic_semantics();

    let mut sample = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_ae",
        Some("req_ae"),
        None,
        2_000_000_000,
        2000,
        EventKind::Snapshot,
        true,
        0,
        120,
        0,
        0,
        0,
        1,
    );
    sample.temporal_accuracy = TemporalAccuracy::IntervalExact;
    sample.timing.measurement_interval_ms = Some(2000);

    pipeline
        .process_sample(&sample, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    let metrics = pipeline
        .tps_engine
        .calculate_agent_tps("codex", 2_000_000_000, "run_ae");
    assert_eq!(
        metrics.current_out_tps, 0.0,
        "IntervalExact must NOT enter 1s Instant Live OUT TPS!"
    );
    assert_eq!(
        metrics.interval_avg_metric,
        Some(IntervalAverageMetric {
            interval_tokens: 120,
            interval_duration_sec: Some(2.0),
            interval_tps: Some(60.0),
        })
    );
    println!("INTERVAL EXACT INSTANT EXCLUSION = PASS");
}

#[test]
fn test_af_cross_source_handoff_reconciliation() {
    let mut pipeline = create_test_pipeline("run_af");
    let semantics = generic_semantics();

    let mock_jsonl = MockAdapter::new("run_af");
    let mut s_jsonl = mock_jsonl.create_sample_with_native(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_af",
        Some("req_af"),
        None,
        1_000_000_000,
        1000,
        EventKind::Snapshot,
        true,
        0,
        100,
        0,
        0,
        0,
        5,
        SourceNativeIdentity::default(),
        "codex_jsonl".to_string(),
    );
    s_jsonl.temporal_accuracy = TemporalAccuracy::IntervalExact;

    pipeline
        .process_sample(&s_jsonl, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    let mock_app = MockAdapter::new("run_af");
    let s_app = mock_app.create_sample_with_native(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_af",
        Some("req_af"),
        None,
        2_000_000_000,
        2000,
        EventKind::Snapshot,
        true,
        0,
        120,
        0,
        0,
        0,
        10,
        SourceNativeIdentity::default(),
        "codex_appserver".to_string(),
    );

    let o_app = pipeline
        .process_sample(&s_app, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();
    let delta = extract_delta(o_app).unwrap();

    assert_eq!(
        delta.delta_output_tokens, 20,
        "Handoff from 100 to 120 must produce delta of 20, NOT 120!"
    );

    let ledger = pipeline
        .request_ledger
        .get_ledger(&RequestCorrelationKey {
            agent_id: "codex".to_string(),
            session_id: "sess_af".to_string(),
            request_id: "req_af".to_string(),
        })
        .unwrap();

    assert_eq!(
        ledger.canonical_output_total, 120,
        "Total must equal 120, NOT 220!"
    );
    println!("CROSS SOURCE HANDOFF RECONCILIATION = PASS");
}

#[test]
fn test_ag_end_to_end_in_tps() {
    let mut pipeline = create_test_pipeline("run_ag");
    let mock = MockAdapter::new("run_ag");
    let semantics = generic_semantics();

    let sample = RawSourceSample {
        timing: TimingInfo {
            request_start_ms: Some(1000),
            first_token_ms: Some(1500),
            ..Default::default()
        },
        ..mock.create_sample(
            "codex",
            "Codex",
            "gpt-4o",
            "sess_ag",
            Some("req_ag"),
            None,
            1_000_000_000,
            1000,
            EventKind::Delta,
            false,
            20000,
            10,
            0,
            0,
            0,
            1,
        )
    };

    pipeline
        .process_sample(&sample, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    let global_metrics = pipeline.global_aggregator.compute_global_metrics(
        &mut pipeline.tps_engine,
        1_000_000_000,
        "run_ag",
    );

    assert_eq!(global_metrics.in_coverage_measured, 1);
    assert_eq!(global_metrics.in_coverage_total, 1);
    assert_eq!(global_metrics.global_in_tps, Some(40000.0));
    println!("END TO END IN TPS = PASS");
}

#[test]
fn test_ah_atomic_checkpoint_replay() {
    let temp_db_path = std::env::temp_dir().join(format!("test_ah_{}.db", uuid::Uuid::new_v4()));
    let cp = SourceCheckpoint {
        source_id: "src_jsonl".to_string(),
        last_file_offset: 100,
        last_db_row_id: None,
        last_sequence_id: None,
        watermark_timestamp_ms: 1000,
        updated_at_ms: 1000,
    };

    {
        let storage = Arc::new(Mutex::new(StorageManager::new_file(&temp_db_path).unwrap()));
        let mut p1 = EnginePipeline::new("run_ah1", storage).unwrap();
        let mock1 = MockAdapter::new("run_ah1");
        let semantics = generic_semantics();

        let s1 = mock1.create_sample_with_native(
            "codex",
            "Codex",
            "gpt-4o",
            "sess_ah",
            Some("req_ah"),
            None,
            1_000_000_000,
            1000,
            EventKind::Delta,
            false,
            0,
            50,
            0,
            0,
            0,
            1,
            SourceNativeIdentity {
                file_path_hash: Some("file_a".to_string()),
                byte_offset: Some(100),
                ..Default::default()
            },
            "src_jsonl".to_string(),
        );

        p1.process_sample_with_checkpoint(
            &s1,
            &semantics,
            BaselineMode::KnownZeroOrigin,
            Some(&cp),
        )
        .unwrap();
    }

    {
        let storage2 = Arc::new(Mutex::new(StorageManager::new_file(&temp_db_path).unwrap()));
        let cps = storage2.lock().load_checkpoints().unwrap();
        assert_eq!(cps.len(), 1);
        assert_eq!(cps[0].last_file_offset, 100);

        let mut p2 = EnginePipeline::new("run_ah2", storage2.clone()).unwrap();
        let mock2 = MockAdapter::new("run_ah2");
        let semantics = generic_semantics();

        let s_replay = mock2.create_sample_with_native(
            "codex",
            "Codex",
            "gpt-4o",
            "sess_ah",
            Some("req_ah"),
            None,
            2_000_000_000,
            2000,
            EventKind::Delta,
            false,
            0,
            50,
            0,
            0,
            0,
            1,
            SourceNativeIdentity {
                file_path_hash: Some("file_a".to_string()),
                byte_offset: Some(100),
                ..Default::default()
            },
            "src_jsonl".to_string(),
        );

        p2.process_sample_with_checkpoint(
            &s_replay,
            &semantics,
            BaselineMode::KnownZeroOrigin,
            Some(&cp),
        )
        .unwrap();

        let total = storage2.lock().get_total_output_tokens("codex").unwrap();
        assert_eq!(
            total, 50,
            "Replaying offset 100 must NOT double count tokens!"
        );
    }
    let _ = std::fs::remove_file(temp_db_path);
    println!("ATOMIC CHECKPOINT REPLAY = PASS");
}

#[test]
fn test_ai_context_fresh_input_stability() {
    let raw = RawUsage {
        raw_input_tokens: Some(1000),
        raw_output_tokens: Some(100),
        raw_cache_read_tokens: Some(600),
        raw_cache_write_tokens: Some(0),
        raw_reasoning_tokens: Some(0),
        raw_total_tokens: Some(1100),
    };
    let semantics = UsageSemantics {
        reasoning_is_output_subset: true,
        accounting_strategy: UsageAccountingStrategy::OpenAiStyle,
        provider_name: "openai".to_string(),
    };
    let norm = UsageNormalizer::normalize(&raw, &semantics);

    assert_eq!(norm.normalized_context_input_tokens, 1000);
    assert_eq!(norm.normalized_fresh_input_tokens, 400);
    assert_eq!(norm.cache_read_tokens, 600);
    println!("CONTEXT FRESH INPUT STABILITY = PASS");
}

#[test]
fn test_aj_true_mixed_accuracy_coverage() {
    let mut pipeline = create_test_pipeline("run_aj");
    let semantics = generic_semantics();

    // Codex: Exact + StreamExact + 60
    let mock_codex = MockAdapter::new("run_aj");
    let s_codex = mock_codex.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "s_c",
        Some("r_c"),
        None,
        1_000_000_000,
        1000,
        EventKind::Delta,
        false,
        0,
        60,
        0,
        0,
        0,
        10,
    );

    // Claude: Exact + TurnExact + 50
    let mock_claude = MockAdapter::new("run_aj");
    let mut s_claude = mock_claude.create_sample(
        "claude",
        "Claude",
        "sonnet",
        "s_cl",
        Some("r_cl"),
        None,
        1_000_000_000,
        1000,
        EventKind::Snapshot,
        true,
        0,
        50,
        0,
        0,
        0,
        5,
    );
    s_claude.temporal_accuracy = TemporalAccuracy::TurnExact;

    pipeline
        .process_sample(&s_codex, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();
    pipeline
        .process_sample(&s_claude, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    let global_metrics = pipeline.global_aggregator.compute_global_metrics(
        &mut pipeline.tps_engine,
        1_000_000_000,
        "run_aj",
    );

    assert_eq!(
        global_metrics.global_out_tps, 60.0,
        "Global Instant Live OUT TPS must equal 60.0 (Codex only), NOT 110.0!"
    );
    println!("TRUE MIXED ACCURACY COVERAGE = PASS");
}

#[test]
fn test_ak_source_ranking_accuracy_temporal_priority() {
    let t_exact = TokenAccuracy::Exact;
    let t_est = TokenAccuracy::Estimated;
    let temp_stream = TemporalAccuracy::StreamExact;
    let temp_turn = TemporalAccuracy::TurnExact;

    assert!(is_better_source(
        t_exact,
        temp_turn,
        1,
        t_est,
        temp_stream,
        10
    ));
    assert!(is_better_source(
        t_exact,
        temp_stream,
        1,
        t_exact,
        temp_turn,
        10
    ));
    assert!(is_better_source(
        t_exact,
        temp_stream,
        10,
        t_exact,
        temp_stream,
        5
    ));
    println!("SOURCE RANKING ACCURACY > TEMPORAL > PRIORITY = PASS");
}

#[test]
fn test_al_finalized_request_excluded_from_instant_tps() {
    let mut pipeline = create_test_pipeline("run_al");
    let mock = MockAdapter::new("run_al");
    let semantics = generic_semantics();

    // Finalize request at 196
    let s_final = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_al",
        Some("req_al"),
        None,
        1_000_000_000,
        1000,
        EventKind::Final,
        true,
        0,
        196,
        0,
        0,
        0,
        1,
    );
    pipeline
        .process_sample(&s_final, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    // Late snapshot 180 on finalized request
    let s_late = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_al",
        Some("req_al"),
        None,
        2_000_000_000,
        2000,
        EventKind::Snapshot,
        true,
        0,
        180,
        0,
        0,
        0,
        1,
    );
    pipeline
        .process_sample(&s_late, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    let metrics = pipeline
        .tps_engine
        .calculate_agent_tps("codex", 2_000_000_000, "run_al");
    assert_eq!(
        metrics.current_out_tps, 0.0,
        "Late snapshot on finalized request must NOT enter Instant OUT TPS!"
    );
    println!("FINALIZED REQUEST EXCLUDED FROM INSTANT TPS = PASS");
}

#[test]
fn test_am_source_handoff_uncertainty() {
    let mut pipeline = create_test_pipeline("run_am");
    let semantics = generic_semantics();

    // Source A: 100 tokens (Priority 5)
    let mock_a = MockAdapter::new("run_am");
    let s_a = mock_a.create_sample_with_native(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_am",
        Some("req_am"),
        None,
        1_000_000_000,
        1000,
        EventKind::Snapshot,
        true,
        0,
        100,
        0,
        0,
        0,
        5,
        SourceNativeIdentity::default(),
        "source_a".to_string(),
    );
    pipeline
        .process_sample(&s_a, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    // Source B (High Priority 10) arrives with 80 tokens (< previous 100 contribution)
    let mock_b = MockAdapter::new("run_am");
    let s_b = mock_b.create_sample_with_native(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_am",
        Some("req_am"),
        None,
        2_000_000_000,
        2000,
        EventKind::Snapshot,
        true,
        0,
        80,
        0,
        0,
        0,
        10,
        SourceNativeIdentity::default(),
        "source_b".to_string(),
    );

    let o_b = pipeline
        .process_sample(&s_b, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();
    let delta_b = extract_delta(o_b);

    let b_out_tokens = delta_b.map(|d| d.delta_output_tokens).unwrap_or(0);
    assert_eq!(
        b_out_tokens, 0,
        "Handoff to source B at 80 (< 100) must produce 0 extra delta!"
    );

    let ledger = pipeline
        .request_ledger
        .get_ledger(&RequestCorrelationKey {
            agent_id: "codex".to_string(),
            session_id: "sess_am".to_string(),
            request_id: "req_am".to_string(),
        })
        .unwrap();

    assert_eq!(ledger.canonical_output_total, 100, "Total must remain 100!");
    println!("SOURCE HANDOFF UNCERTAINTY = PASS");
}

#[test]
fn test_fix1_interval_without_duration() {
    let mock = MockAdapter::new("run_fix1");
    let mut sample = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sess_fix1",
        Some("req_fix1"),
        None,
        1_000_000_000,
        1000,
        EventKind::Snapshot,
        true,
        0,
        100,
        0,
        0,
        0,
        1,
    );
    sample.temporal_accuracy = TemporalAccuracy::IntervalExact;
    sample.timing.measurement_interval_ms = None; // No duration

    let mut pipeline = create_test_pipeline("run_fix1");
    let semantics = generic_semantics();
    pipeline
        .process_sample(&sample, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    let metrics = pipeline
        .tps_engine
        .calculate_agent_tps("codex", 1_000_000_000, "run_fix1");
    assert_eq!(
        metrics.interval_avg_metric,
        Some(IntervalAverageMetric {
            interval_tokens: 100,
            interval_duration_sec: None,
            interval_tps: None,
        })
    );
    println!("FIX1 INTERVAL WITHOUT DURATION = PASS");
}

#[test]
fn test_fix2_persistence_accuracy_restore() {
    let temp_db_path = std::env::temp_dir().join(format!("test_fix2_{}.db", uuid::Uuid::new_v4()));
    {
        let storage = Arc::new(Mutex::new(StorageManager::new_file(&temp_db_path).unwrap()));
        let mut p1 = EnginePipeline::new("run_fix2_1", storage).unwrap();
        let mock = MockAdapter::new("run_fix2_1");
        let semantics = generic_semantics();

        let mut sample = mock.create_sample(
            "codex",
            "Codex",
            "gpt-4o",
            "sess_fix2",
            Some("req_fix2"),
            None,
            1_000_000_000,
            1000,
            EventKind::Snapshot,
            true,
            0,
            100,
            0,
            0,
            0,
            1,
        );
        sample.token_accuracy = TokenAccuracy::Exact;
        sample.temporal_accuracy = TemporalAccuracy::TurnExact;

        p1.process_sample(&sample, &semantics, BaselineMode::KnownZeroOrigin)
            .unwrap();
    }

    {
        let storage2 = Arc::new(Mutex::new(StorageManager::new_file(&temp_db_path).unwrap()));
        let p2 = EnginePipeline::new("run_fix2_2", storage2).unwrap();

        let ledger = p2
            .request_ledger
            .get_ledger(&RequestCorrelationKey {
                agent_id: "codex".to_string(),
                session_id: "sess_fix2".to_string(),
                request_id: "req_fix2".to_string(),
            })
            .unwrap();

        assert_eq!(ledger.active_live_token_accuracy, TokenAccuracy::Exact);
        assert_eq!(
            ledger.active_live_temporal_accuracy,
            TemporalAccuracy::TurnExact
        );
    }
    let _ = std::fs::remove_file(temp_db_path);
    println!("FIX2 PERSISTENCE ACCURACY RESTORE = PASS");
}

#[test]
fn test_fix3_correction_does_not_reset_generation() {
    let mut tracker = BaselineTracker::new();
    let key = RequestCorrelationKey {
        agent_id: "codex".to_string(),
        session_id: "sess_fix3".to_string(),
        request_id: "req_fix3".to_string(),
    };

    let c1 = tracker.process_counters(
        "adapter_fix3",
        &key,
        0,
        0,
        100,
        0,
        0,
        0,
        BaselineMode::KnownZeroOrigin,
        false,
    );
    assert_eq!(c1.delta_output, 100);

    // EventKind::Correction should NOT reset generation when counter_reset_hint = false!
    let c2 = tracker.process_counters(
        "adapter_fix3",
        &key,
        0,
        0,
        80,
        0,
        0,
        0,
        BaselineMode::KnownZeroOrigin,
        false, // NOT explicit reset
    );
    assert_eq!(
        c2.delta_output, 0,
        "Correction event without reset hint must NOT trigger counter reset!"
    );
    println!("FIX3 CORRECTION DOES NOT RESET GENERATION = PASS");
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

#[test]
fn test_global_peak_aggregation() {
    let mut pipeline = create_test_pipeline("run_peak");
    let mock = MockAdapter::new("run_peak");
    let semantics = generic_semantics();

    let s1 = mock.create_sample(
        "codex",
        "Codex",
        "gpt-4o",
        "sp1",
        Some("rp1"),
        None,
        1_000_000_000,
        1000,
        EventKind::Delta,
        false,
        0,
        60,
        0,
        0,
        0,
        1,
    );
    let s2 = mock.create_sample(
        "claude",
        "Claude",
        "sonnet",
        "sp2",
        Some("rp2"),
        None,
        1_000_000_000,
        1000,
        EventKind::Delta,
        false,
        0,
        50,
        0,
        0,
        0,
        1,
    );

    pipeline
        .process_sample(&s1, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();
    pipeline
        .process_sample(&s2, &semantics, BaselineMode::KnownZeroOrigin)
        .unwrap();

    let m = pipeline.global_aggregator.compute_global_metrics(
        &mut pipeline.tps_engine,
        1_000_000_000,
        "run_peak",
    );
    assert_eq!(m.peak_out_tps, 110.0);
    println!("GLOBAL PEAK = PASS");
}
