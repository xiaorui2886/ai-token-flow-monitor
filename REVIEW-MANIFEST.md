# REVIEW MANIFEST — Phase01 FIX Review Pack

Generation Timestamp: 2026-08-07 20:59:16
Git Status: Baseline commit ready.

| Relative Path | Size (bytes) | SHA-256 Hash | Purpose |
| :--- | :--- | :--- | :--- |
| `FIX-CHANGELOG.md` | 3801 | `5b1a4d5b76ebf66c6d996f6d1bcc3d105084b7f0837e0329aaaafb6c1a5a36ca` | Changelog/Verification |
| `FIX-VERIFICATION.md` | 2765 | `9344ad87574e9870712f2b719a6e839a423edf4e3628646047f4072f3cf8fd6c` | Changelog/Verification |
| `docs/Adapter-Capability-实测矩阵.md` | 5596 | `560807db1261bc40ab13dac0ce5572d06af4b949d0198439e1d2345711b4b210` | Documentation |
| `docs/Core-Engine-实现报告.md` | 4303 | `450e73a387fab3d0dd18207a0cac0c4565a1a8e8224042f553d4694c7d1f1e64` | Documentation |
| `docs/P0-Ground-Truth-测试报告.md` | 5490 | `ea279cfa6dfcb40aec40cae01144f28755db1f6eb07f4c96bdd40601daa48535` | Documentation |
| `docs/本阶段文件变更清单.md` | 2491 | `e943e3ef37f2bb41a6c825168d7c78a5c483905ff06da383df3fa5b9e15b4964` | Documentation |
| `docs/已知限制与待验证项.md` | 2415 | `8747612e343310093db11accdd5829810102f7fc5335b53ae77025b0755024ab` | Documentation |
| `src-tauri/Cargo.toml` | 862 | `57779c706422285c3985fcae05e38f8dd1a75486cd4ff5769e205971b7c5d7af` | Source Code |
| `src-tauri/src/lib.rs` | 283 | `ddd07bc04d86ae2a7ceeabadf56451dab8eb5e4c761c74f9438c0f051601a6aa` | Source Code |
| `src-tauri/src/main.rs` | 120 | `b1b5553b77f4fa07689efa7b5fbf7fd70aa945923eabb4109caf35ece0ffd7a0` | Source Code |
| `src-tauri/src/core/types.rs` | 11477 | `c903182d208f3831d4056f1a971c6172356f6b92268bd483fca0e455992f1ce6` | Source Code |
| `src-tauri/src/core/normalization.rs` | 3473 | `10963b485dd2fee763f9193d5012e0e99e8f0dc1c0e4a281ba255d535999d4ed` | Source Code |
| `src-tauri/src/core/correlation.rs` | 3724 | `497d9fef8720545841d32b2f15fce25db3fab841119414ed889b1ff491906e00` | Source Code |
| `src-tauri/src/core/baseline.rs` | 6862 | `d744dff0b5d79438800eea068ab3780502700df677176dbee96ebd8d8b8df828` | Source Code |
| `src-tauri/src/core/snapshot_accumulator.rs` | 2671 | `c28c5906cb58e5004d639457f2a3c288fc62c349f049f76eb618f455fc71fb9a` | Source Code |
| `src-tauri/src/core/delta_calculator.rs` | 3992 | `066f2c98ef2207ffddba1f817c035774cc92519a7e64488b6c9c17891b30cafd` | Source Code |
| `src-tauri/src/core/gap_detector.rs` | 1686 | `66fbc9f6271359760ad94b771985fda997261599ecb5e1833f4c75bd42049154` | Source Code |
| `src-tauri/src/core/request_ledger.rs` | 7541 | `53c44a7058826454eaab298eb7f59f396de46583c877c13b824742e329678aa0` | Source Code |
| `src-tauri/src/core/reconciler.rs` | 2780 | `275770d230e5053de361aba97feb0a5f612572ec04972e905dfdecc90e2de5bd` | Source Code |
| `src-tauri/src/core/tps_engine.rs` | 5656 | `bdec39d8d42a75f2c87dea8df8df4a0ad0e2d43412e9a3d53b99473a8aec5bb9` | Source Code |
| `src-tauri/src/core/aggregator.rs` | 3160 | `cb83966109822708b350a4de201434febd57716c83706908bb3cec4c629d4524` | Source Code |
| `src-tauri/src/core/persistence.rs` | 16131 | `39e5ac90621fdbac19bad21e21c3787a893862da32486fbf5b4184e19f5113ee` | Source Code |
| `src-tauri/src/core/mock_adapter.rs` | 4417 | `29513591cb108e2efda1ff0d24bb9f8dd93e84cad83a3c083a9afda6ff9f1114` | Source Code |
| `src-tauri/src/core/mod.rs` | 6149 | `68f4a38b3bd7151eba4fbb4b0f7257387ed5d44db1e890517216e3efaaf314c9` | Source Code |
| `src-tauri/tests/p0_tests.rs` | 49190 | `0a45c4ea0893cd66dd43dcc38ef5a1e771677744f259f6ba5ca97211e2d970b7` | Source Code |
| `review-evidence/cargo-fmt.txt` | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | Review Evidence |
| `review-evidence/cargo-clippy.txt` | 153 | `890cda81e3aff5a425f1ab894f06bbd3d741548def64b713769b778ca89f57d8` | Review Evidence |
| `review-evidence/cargo-test-p0.txt` | 3207 | `ba4c7dffb6531bdd615f092f48ab4473b885d980af82a05dd094e2bdc16ac227` | Review Evidence |
| `review-evidence/cargo-test-all.txt` | 3917 | `791cdc74ebc58a2d4459b73a08b8911ef805b337666deb0a136f719f36218b9f` | Review Evidence |
| `review-evidence/phase0-source-evidence.md` | 7047 | `25b04b82b90065e371c70b9488bf8ff4aac0be500b48f99fe004c6276b3f3439` | Review Evidence |
| `review-evidence/final-schema-check.md` | 2828 | `b9605f9438bb183ba60c5fde8588c53595b72ff765714c0187edb69a7187ffc7` | Review Evidence |
| `review-evidence/git-status.txt` | 41 | `7aa69ed7b78a039eefd9d0584b73b153addec62dfd598a57e7f64b35e5a2d41a` | Review Evidence |
| `review-evidence/git-log.txt` | 64 | `7a6350b6c54cf232aebe4286ec8567c7842edf2a5f522fe06b1289700fd09b86` | Review Evidence |
