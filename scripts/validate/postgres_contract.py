#!/usr/bin/env python3
"""Validate the M0 PostgreSQL capture/backfill contract and executable fixtures."""
from __future__ import annotations

import hashlib
import importlib.util
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OWNER = "boring-cdc-m0-pg-contract"
CONTRACT = ROOT / "contracts/postgres/capture-backfill.json"
SCHEMA = ROOT / "contracts/postgres/capture-backfill.schema.json"
FIXTURES = ROOT / "fixtures/m0/postgres/capture-backfill.json"
FIXTURE_SCHEMA = ROOT / "contracts/postgres/capture-backfill-fixtures.schema.json"
EVIDENCE_SCHEMA = ROOT / "contracts/postgres/postgres-contract-evidence.schema.json"
SQL = ROOT / "contracts/postgres/roles-grants.sql"
EVIDENCE = ROOT / "artifacts/boring-cdc-m0-pg-contract/spec/evidence.json"
VALIDATOR = ROOT / "scripts/validate/postgres_contract.py"
CORE_SPEC = importlib.util.spec_from_file_location("core_validator", ROOT / "scripts/lib/core_validator.py")
CORE = importlib.util.module_from_spec(CORE_SPEC)
CORE_SPEC.loader.exec_module(CORE)


def load(path: Path):
    return CORE.read_strict(path)[0]


def finding(items, code, path, message):
    items.append({"code": code, "path": path, "message": message})


PROCESS_FIELDS = {"exporter_backend_pid", "guard_backend_pid", "importer_backend_pids", "importer_acknowledged", "importer_expected"}
LIFECYCLE_EXPECTATIONS = {'SCN-M0-PG-ADVISORY-PROBE-LATE': {'fault_action': 'delay_advisory_probe_beyond_ownership_deadline',
                                   'fault_phase': 'advisory_probe_late',
                                   'phase': 'advisory_probe_late',
                                   'pre_state': 'capture_safe_stopped_probe_overdue',
                                   'process': {}},
 'SCN-M0-PG-AMBIGUOUS-SLOT': {'fault_action': 'disconnect_after_slot_creation_response_before_intent_commit',
                              'fault_phase': 'after_slot_server_response',
                              'phase': 'after_slot_server_response',
                              'pre_state': 'slot_creation_response_unpersisted',
                              'process': {}},
 'SCN-M0-PG-BEFORE-SLOT-CREATE': {'fault_action': 'crash_before_create_replication_slot',
                                  'fault_phase': 'before_slot_create',
                                  'phase': 'before_slot_create',
                                  'pre_state': 'bootstrap_prepared_no_slot',
                                  'process': {}},
 'SCN-M0-PG-BOOTSTRAP-EXPORTER-LOSS': {'fault_action': 'disconnect_exporter_before_final_importer_ack',
                                       'fault_phase': 'before_all_import_ack',
                                       'phase': 'before_all_import_ack',
                                       'pre_state': 'snapshot_imports_pending',
                                       'process': {'exporter_backend_pid': 4101,
                                                   'guard_backend_pid': 4102,
                                                   'importer_acknowledged': 1,
                                                   'importer_backend_pids': [4103, 4104],
                                                   'importer_expected': 2}},
 'SCN-M0-PG-BOOTSTRAP-IMPORTS': {'fault_action': 'acknowledge_final_snapshot_importer',
                                 'fault_phase': 'after_import_ack',
                                 'phase': 'after_import_ack',
                                 'pre_state': 'snapshot_imports_pending',
                                 'process': {'exporter_backend_pid': 4101,
                                             'guard_backend_pid': 4102,
                                             'importer_acknowledged': 2,
                                             'importer_backend_pids': [4103, 4104],
                                             'importer_expected': 2}},
 'SCN-M0-PG-CAPTURE-FENCE': {'fault_action': 'crash_after_fence_journal_before_anchor_commit',
                             'fault_phase': 'after_fence_journal_before_anchor',
                             'phase': 'after_fence_journal_before_anchor',
                             'pre_state': 'fence_journaled_anchor_pending',
                             'process': {'guard_backend_pid': 4102}},
 'SCN-M0-PG-CHUNK-COMMIT': {'fault_action': 'crash_after_snapshot_chunk_commit',
                            'fault_phase': 'after_chunk_commit',
                            'phase': 'after_chunk_commit',
                            'pre_state': 'snapshot_copying_chunk_committed',
                            'process': {'exporter_backend_pid': 4101,
                                        'guard_backend_pid': 4102,
                                        'importer_acknowledged': 2,
                                        'importer_backend_pids': [4103, 4104],
                                        'importer_expected': 2}},
 'SCN-M0-PG-COMMIT-FEEDBACK-CRASH': {'fault_action': 'crash_after_transaction_commit_before_status_update',
                                     'fault_phase': 'after_journal_commit_before_feedback',
                                     'phase': 'after_journal_commit_before_feedback',
                                     'pre_state': 'transaction_journal_committed',
                                     'process': {}},
 'SCN-M0-PG-CONTROL-CARDINALITY': {'fault_action': 'apply_control_update_with_zero_and_two_row_results',
                                   'fault_phase': 'control_update_row_count',
                                   'phase': 'control_update_row_count',
                                   'pre_state': 'control_relation_update_attempted',
                                   'process': {}},
 'SCN-M0-PG-CONTROL-PRIVILEGES': {'fault_action': 'execute_forbidden_capture_role_statements',
                                  'fault_phase': 'roles_sql_probe',
                                  'phase': 'roles_sql_probe',
                                  'pre_state': 'capture_role_provisioned',
                                  'process': {}},
 'SCN-M0-PG-COPYBOTH-FRAMES': {'fault_action': 'deliver_complete_copyboth_xlog_frame',
                               'fault_phase': 'copyboth_frame',
                               'phase': 'copyboth_frame',
                               'pre_state': 'replication_start_requested',
                               'process': {}},
 'SCN-M0-PG-CREATION-FLOOR-EQUAL': {'fault_action': 'reconcile_confirmed_flush_equal_creation_floor',
                                    'fault_phase': 'startup_reconcile',
                                    'phase': 'startup_reconcile',
                                    'pre_state': 'bootstrap_nonterminal_reconcile',
                                    'process': {}},
 'SCN-M0-PG-CREATION-FLOOR-INVALID-SLOT': {'fault_action': 'reject_reconcile_when_slot_identity_invalid',
                                           'fault_phase': 'startup_reconcile',
                                           'phase': 'startup_reconcile',
                                           'pre_state': 'bootstrap_nonterminal_reconcile',
                                           'process': {}},
 'SCN-M0-PG-CREATION-FLOOR-NULL': {'fault_action': 'reconcile_null_confirmed_flush_at_creation_floor',
                                   'fault_phase': 'startup_reconcile',
                                   'phase': 'startup_reconcile',
                                   'pre_state': 'bootstrap_nonterminal_reconcile',
                                   'process': {}},
 'SCN-M0-PG-CREATION-FLOOR-WAL-UNAVAILABLE': {'fault_action': 'reject_reconcile_when_resume_wal_unavailable',
                                              'fault_phase': 'startup_reconcile',
                                              'phase': 'startup_reconcile',
                                              'pre_state': 'bootstrap_nonterminal_reconcile',
                                              'process': {}},
 'SCN-M0-PG-DDL-CONFLICT-MATRIX': {'fault_action': 'request_access_exclusive_ddl_while_guard_held',
                                   'fault_phase': 'before_snapshot_export',
                                   'phase': 'before_snapshot_export',
                                   'pre_state': 'ddl_guard_held_before_snapshot',
                                   'process': {'guard_backend_pid': 4102}},
 'SCN-M0-PG-DDL-IDLE-POLL': {'fault_action': 'poll_relation_fingerprint_after_idle_interval',
                             'fault_phase': 'catalog_poll',
                             'phase': 'catalog_poll',
                             'pre_state': 'ddl_guard_idle_between_catalog_polls',
                             'process': {}},
 'SCN-M0-PG-DDL-IMMEDIATE-DML': {'fault_action': 'deliver_dml_immediately_after_changed_relation_message',
                                 'fault_phase': 'relation_then_dml',
                                 'phase': 'relation_then_dml',
                                 'pre_state': 'relation_metadata_pending_dml',
                                 'process': {}},
 'SCN-M0-PG-DDL-WAITER-INVALIDATE': {'fault_action': 'hold_access_exclusive_waiter_past_limit',
                                     'fault_phase': 'ddl_waiter_limit',
                                     'phase': 'ddl_waiter_limit',
                                     'pre_state': 'ddl_guard_waiter_observed',
                                     'process': {}},
 'SCN-M0-PG-EXISTING-SLOT-STITCH': {'fault_action': 'crash_after_lower_stitch_journal_commit',
                                    'fault_phase': 'after_lower_stitch_persist',
                                    'phase': 'after_lower_stitch_persist',
                                    'pre_state': 'existing_slot_lower_stitch_persisted',
                                    'process': {'exporter_backend_pid': 4101,
                                                'guard_backend_pid': 4102,
                                                'importer_acknowledged': 2,
                                                'importer_backend_pids': [4103, 4104],
                                                'importer_expected': 2}},
 'SCN-M0-PG-EXPECTED-CLOSE-TOKEN': {'fault_action': 'close_after_expected_close_token_persisted',
                                    'fault_phase': 'after_expected_close_token',
                                    'phase': 'after_expected_close_token',
                                    'pre_state': 'capture_safe_stopped_token_persisted',
                                    'process': {}},
 'SCN-M0-PG-EXPORTER-RELEASE-AFTER': {'fault_action': 'crash_after_permitted_exporter_release',
                                      'fault_phase': 'after_exporter_release',
                                      'phase': 'after_exporter_release',
                                      'pre_state': 'exporter_released',
                                      'process': {'guard_backend_pid': 4102,
                                                  'importer_acknowledged': 2,
                                                  'importer_expected': 2}},
 'SCN-M0-PG-EXPORTER-RELEASE-BEFORE': {'fault_action': 'crash_before_permitted_exporter_release',
                                       'fault_phase': 'before_exporter_release',
                                       'phase': 'before_exporter_release',
                                       'pre_state': 'imports_complete_exporter_release_permitted',
                                       'process': {'exporter_backend_pid': 4101,
                                                   'guard_backend_pid': 4102,
                                                   'importer_acknowledged': 2,
                                                   'importer_backend_pids': [4103, 4104],
                                                   'importer_expected': 2}},
 'SCN-M0-PG-FENCE-UPDATE-AFTER': {'fault_action': 'crash_after_fence_update_before_durable_observation',
                                  'fault_phase': 'after_fence_update_before_observe',
                                  'phase': 'after_fence_update_before_observe',
                                  'pre_state': 'fence_updated_not_durably_observed',
                                  'process': {'guard_backend_pid': 4102}},
 'SCN-M0-PG-FENCE-UPDATE-BEFORE': {'fault_action': 'crash_before_unique_fence_update',
                                   'fault_phase': 'before_fence_update',
                                   'phase': 'before_fence_update',
                                   'pre_state': 'snapshot_copy_complete_fence_not_updated',
                                   'process': {'guard_backend_pid': 4102}},
 'SCN-M0-PG-FIRST-FEEDBACK': {'fault_action': 'request_first_feedback_while_importer_gate_active',
                              'fault_phase': 'before_first_feedback',
                              'phase': 'before_first_feedback',
                              'pre_state': 'replication_started_feedback_gated',
                              'process': {'exporter_backend_pid': 4101,
                                          'guard_backend_pid': 4102,
                                          'importer_acknowledged': 0,
                                          'importer_backend_pids': [4103, 4104],
                                          'importer_expected': 2}},
 'SCN-M0-PG-GUARD-LOSS': {'fault_action': 'disconnect_guard_before_durable_fence',
                          'fault_phase': 'guard_connection_loss',
                          'phase': 'guard_connection_loss',
                          'pre_state': 'ddl_guard_active_before_durable_fence',
                          'process': {'guard_backend_pid': 4102}},
 'SCN-M0-PG-KEEPALIVE-REPLY': {'fault_action': 'send_status_update_with_reply_flag_false',
                               'fault_phase': 'keepalive_reply_requested',
                               'phase': 'keepalive_reply_requested',
                               'pre_state': 'replication_keepalive_received',
                               'process': {}},
 'SCN-M0-PG-ORIGIN-ORDINAL': {'fault_action': 'decode_origin_between_begin_and_insert',
                              'fault_phase': 'origin_message',
                              'phase': 'origin_message',
                              'pre_state': 'pgoutput_transaction_open',
                              'process': {}},
 'SCN-M0-PG-PUBLICATION-DRIFT': {'fault_action': 'poll_mismatched_publication_fingerprint',
                                 'fault_phase': 'catalog_poll',
                                 'phase': 'catalog_poll',
                                 'pre_state': 'publication_contract_active',
                                 'process': {}},
 'SCN-M0-PG-RESTART-MAX': {'fault_action': 'select_max_requested_and_confirmed_restart_lsn',
                           'fault_phase': 'restart_request',
                           'phase': 'restart_request',
                           'pre_state': 'replication_restart_requested',
                           'process': {}},
 'SCN-M0-PG-RETAINED-SLOT-RECOVERY': {'fault_action': 'restart_with_retained_slot_and_ambiguous_intent',
                                      'fault_phase': 'after_slot_server_response',
                                      'phase': 'after_slot_server_response',
                                      'pre_state': 'retained_slot_bootstrap_ambiguous',
                                      'process': {}},
 'SCN-M0-PG-SAFE-STOP-CLOSE': {'fault_action': 'close_copyboth_after_matching_safe_stop_token',
                               'fault_phase': 'after_safe_stop_persist_before_close',
                               'phase': 'after_safe_stop_persist_before_close',
                               'pre_state': 'capture_safe_stop_persisted',
                               'process': {}},
 'SCN-M0-PG-SAFE-STOP-PERSIST-BEFORE': {'fault_action': 'crash_before_safe_stop_journal_commit',
                                        'fault_phase': 'before_safe_stop_persist',
                                        'phase': 'before_safe_stop_persist',
                                        'pre_state': 'capture_stopping_before_persist',
                                        'process': {}},
 'SCN-M0-PG-SLOW-COMMIT-REQUESTED-REPLY': {'fault_action': 'request_keepalive_reply_before_spool_commit',
                                           'fault_phase': 'during_spool_before_commit',
                                           'phase': 'during_spool_before_commit',
                                           'pre_state': 'transaction_spooling_uncommitted',
                                           'process': {}},
 'SCN-M0-PG-SNAPSHOT-EXPORT-AFTER-GUARD': {'fault_action': 'attempt_snapshot_export_after_guard_acquisition',
                                           'fault_phase': 'before_snapshot_export',
                                           'phase': 'before_snapshot_export',
                                           'pre_state': 'ddl_guard_acquired_snapshot_not_exported',
                                           'process': {'guard_backend_pid': 4102}},
 'SCN-M0-PG-START-REPLICATION': {'fault_action': 'crash_before_start_replication',
                                 'fault_phase': 'before_start_replication',
                                 'phase': 'before_start_replication',
                                 'pre_state': 'snapshot_exported_replication_not_started',
                                 'process': {'exporter_backend_pid': 4101, 'guard_backend_pid': 4102}},
 'SCN-M0-PG-TOKEN-PERSIST': {'fault_action': 'crash_after_snapshot_token_journal_commit',
                             'fault_phase': 'after_snapshot_token_persist',
                             'phase': 'after_snapshot_token_persist',
                             'pre_state': 'snapshot_exported_token_persisted',
                             'process': {'exporter_backend_pid': 4101, 'guard_backend_pid': 4102}},
 'SCN-M0-PG-UNEXPECTED-COPYBOTH-LOSS': {'fault_action': 'drop_copyboth_without_expected_close_token',
                                        'fault_phase': 'unexpected_copyboth_eof',
                                        'phase': 'unexpected_copyboth_eof',
                                        'pre_state': 'replication_streaming',
                                        'process': {}},
 'SCN-M0-PG-WAL-HEADROOM': {'fault_action': 'sample_fresh_wal_headroom_window',
                            'fault_phase': 'wal_metric_sample',
                            'phase': 'wal_metric_sample',
                            'pre_state': 'wal_headroom_monitoring',
                            'process': {}}}

FIXTURE_INPUT_SHA256 = {'SCN-M0-PG-ADVISORY-PROBE-LATE': 'cc6e5c4b63ca974fd0acf4da6bb8873186df29b0a2741510dcc54c3efd44d2cf',
 'SCN-M0-PG-AMBIGUOUS-SLOT': '6aa95c54bd8e38df574d9fc1882fb5d79ae170f3258ae53b4c92a0f884b198f9',
 'SCN-M0-PG-BEFORE-SLOT-CREATE': '3d42e75e258b6ab671ae162581f632a049f83735a3bdd922e428faa19799131f',
 'SCN-M0-PG-BOOTSTRAP-EXPORTER-LOSS': '2fad730d1b4fe213953c7c7db672e92c6a2b09a812e125b53b406b3bdbf8b6ac',
 'SCN-M0-PG-BOOTSTRAP-IMPORTS': '8b6aeeace4c5dc493cb44a8eeef7b546abf5afabf2e8e1e6a4da1c74cc398ff8',
 'SCN-M0-PG-CAPTURE-FENCE': '6fb23524ab6d8850e61f6837c166cfc790a547e751698f16c118b42b58554ed5',
 'SCN-M0-PG-CHUNK-COMMIT': 'b44cbeca37593ebc61ae79786793f9514e9d82fe20835b3a53c2dd47f0c5d9a6',
 'SCN-M0-PG-COMMIT-FEEDBACK-CRASH': '9d834a5d5f5444e2509c43e82e2715237579d51e1ce7296c530a3f1e5eb2bd86',
 'SCN-M0-PG-CONTROL-CARDINALITY': '4d255de544edc167c6998207040e2b7753006e7a7f8dfc9dc969963407a3d50d',
 'SCN-M0-PG-CONTROL-PRIVILEGES': '9b5643775ff6992944f0152c38377f056f52697d309bd4b836046884cd67bee5',
 'SCN-M0-PG-COPYBOTH-FRAMES': '3f23b5c1a83e138efded795f0d2dbb3921abfb08439d0a5f16f4bf0d424a3f4a',
 'SCN-M0-PG-CREATION-FLOOR-EQUAL': 'a3fec8190bbd35ec04849dacc030b1bed89648bd6930f995e8d3e841d6f522d5',
 'SCN-M0-PG-CREATION-FLOOR-INVALID-SLOT': '1fecbdbfb58cd60198651e04280b5af7ea46cedfa4cc5d768478f61947d30209',
 'SCN-M0-PG-CREATION-FLOOR-NULL': 'd44e0d8ddda8c0393c130f4efb075053758ff9e4deb0ebf9e32a40cd2d8fb44b',
 'SCN-M0-PG-CREATION-FLOOR-WAL-UNAVAILABLE': 'ed10b2453fb59d8861f679dd6502b6223605ca17a20bff2edbfa621aeee1135d',
 'SCN-M0-PG-DDL-CONFLICT-MATRIX': 'dc540c0a95db620e92d8ee81e140f563506a91c142e24dfb51e2a1ef85dc765c',
 'SCN-M0-PG-DDL-IDLE-POLL': 'ea8a5e07d416ff74f95b29bd1a9f079afa3ca44fc9c718d197763c56373ffa18',
 'SCN-M0-PG-DDL-IMMEDIATE-DML': '818fdca16d864d3a7b9c008e85d1697717d44c8174beecce1e14dacd3cd63bf4',
 'SCN-M0-PG-DDL-WAITER-INVALIDATE': '2583a02801c599567515ce63cb37a84a8a86ceee1c4f1225243c41838af92440',
 'SCN-M0-PG-EXISTING-SLOT-STITCH': 'a597090e7c235c1ef45b6495fcc71cb7696a0165bf2aaee1c346499f7cbb8212',
 'SCN-M0-PG-EXPECTED-CLOSE-TOKEN': '8eae4fa44100eea6897f2dc4ddefc98b1862b50ca3ff40de9e98cd388a727697',
 'SCN-M0-PG-EXPORTER-RELEASE-AFTER': '6ce346b4699bfcf9e82ad62be369240c5d1a29316fb068026671deb1dc0a77d7',
 'SCN-M0-PG-EXPORTER-RELEASE-BEFORE': '4a22cf61cdde1e3534a72ca49fc516e3c0088b379c6d099fe3974da3ee00237f',
 'SCN-M0-PG-FENCE-UPDATE-AFTER': '8a1382be58c83f1e931c8fd2752503847f20c37d498a10a5f399ca393e9c8972',
 'SCN-M0-PG-FENCE-UPDATE-BEFORE': '06fdeb9154e7e8cf375ed808a08f5375182e644955edc82d6643a096356c975f',
 'SCN-M0-PG-FIRST-FEEDBACK': '478b5e175631afe6ce434747e6ea9c877df5ef44f56b99d7e2eaf1baaff33b2d',
 'SCN-M0-PG-GUARD-LOSS': 'd5734ea37c8502f648d9166a68abff63008391907ea97bff5d4fc3058a8955a3',
 'SCN-M0-PG-KEEPALIVE-REPLY': 'd50f69b76d4f164e057fbe1f7fcd48e04f44aab860a1e1cf9ef744114d4e40a7',
 'SCN-M0-PG-ORIGIN-ORDINAL': 'fd763458daa1e992cfa93ea62c90f5e6122ad24c401ce1214fe5b773f8e13847',
 'SCN-M0-PG-PUBLICATION-DRIFT': '5642dcb3abfaba7018d61db01eb17a7d474672c60951631c88909402d621df64',
 'SCN-M0-PG-RESTART-MAX': 'c2d8a2e1f56c885a9745202c39887dac9d28f59e62e0cd208344251401e58bbf',
 'SCN-M0-PG-RETAINED-SLOT-RECOVERY': '10e0b3ce0b0ac00b4b08dca1a25963c8f74a3fa8caffb53c7f05e777e48252d4',
 'SCN-M0-PG-SAFE-STOP-CLOSE': 'ab26ef6720011ff8619f03de3fad069ac7e28774c461a9e2feea88b2e571481f',
 'SCN-M0-PG-SAFE-STOP-PERSIST-BEFORE': '730cf13390059172447b2102a982adbb573fc480338b5876b3b3147c5db5c02d',
 'SCN-M0-PG-SLOW-COMMIT-REQUESTED-REPLY': '8fc7471fcc782f2cc5429c71a08b02c5025c777e21e2e7abddd35a051b2c96ad',
 'SCN-M0-PG-SNAPSHOT-EXPORT-AFTER-GUARD': '270202d20884b30f86c08073753963fa3e6ea98c8b362fb14c69dc1ce2f42f9b',
 'SCN-M0-PG-START-REPLICATION': '89072fbf16e2cc7737c414f06c1dbdc6674c6d48531877da00eaccb0f74c7ec7',
 'SCN-M0-PG-TOKEN-PERSIST': 'fbc17dbda596d360083bd08ee64427edd9b992d9444e9350ddc7807de5606e0d',
 'SCN-M0-PG-UNEXPECTED-COPYBOTH-LOSS': '25bb7bcdc79379a514ca301defb88e355ab6e77d499e9f7937cb62a82ae9b842',
 'SCN-M0-PG-WAL-HEADROOM': '905de8830979cca6e1e4ec9ac5067113196934b1564e4211f1e579395e231801'}

FIXTURE_CASE_SHA256 = {'SCN-M0-PG-ADVISORY-PROBE-LATE': '88c46d63e5ab7614bfea9f43ad19bece4b0f5087018d51b81767f1f6a1346ecd',
 'SCN-M0-PG-AMBIGUOUS-SLOT': '1ded8eec66e700be5435f054063d95d13d6af2e07a6171b1efd786cb59579e68',
 'SCN-M0-PG-BEFORE-SLOT-CREATE': '3b6c74a0e4646f1bccff8f959d80f1047e810560fe0e49ab1e32c5964fee7f78',
 'SCN-M0-PG-BOOTSTRAP-EXPORTER-LOSS': '49b450e2a0d8f6f7c2957f8afc0e2bf59a4e04e2cd46499abc0cdd59dac35ebc',
 'SCN-M0-PG-BOOTSTRAP-IMPORTS': 'ed75cf7a490b551e9eee68f0ba2e13aa9dc0a418026adc78aad0be6a4da826c8',
 'SCN-M0-PG-CAPTURE-FENCE': 'd9c00e42b92fdadd6944c0c3242f10d5a5d5a9d507695746c5b2b73358713d85',
 'SCN-M0-PG-CHUNK-COMMIT': '495d50b973cdf734deccc77db9e4e3010e1b7e691a012fd62a9199c450530cb2',
 'SCN-M0-PG-COMMIT-FEEDBACK-CRASH': 'c84ba62a719587b66981d418fa260c6772aa782fc9cf3f83ec9c17de190f2168',
 'SCN-M0-PG-CONTROL-CARDINALITY': '21df9b3e5cf29bec7032c5d7c7e05fc581cb90d515803940f662a7013b366587',
 'SCN-M0-PG-CONTROL-PRIVILEGES': 'ef861da4f063d19c2ff7239fb2870072128498352f87f9026605b8b904b9c784',
 'SCN-M0-PG-COPYBOTH-FRAMES': '62fe1e0782dc064468213ae36662a57174ecf6e0515b0f0930b58a7e9b5fc304',
 'SCN-M0-PG-CREATION-FLOOR-EQUAL': '5fd37cb022f1883ed0d2875bd8e22a5ede1512aaa3eafa64880093e43492b816',
 'SCN-M0-PG-CREATION-FLOOR-INVALID-SLOT': '6fb1e8926cd74c59c9c3d0158353395f290df349c9492532d6c6dcc4a3b9bbce',
 'SCN-M0-PG-CREATION-FLOOR-NULL': '08701e73b48301c5c9f67eb8350c84fe2b2328d3c108ba4e4b3c3b79dbad072d',
 'SCN-M0-PG-CREATION-FLOOR-WAL-UNAVAILABLE': 'db3e037dc09f54712c8e5758acf8e42d6df96e642766aeb323bdacb3749c30cb',
 'SCN-M0-PG-DDL-CONFLICT-MATRIX': '3561175c9a3ad42ee5d124632e816144b3a2d5884df64b10bd209c182e87a560',
 'SCN-M0-PG-DDL-IDLE-POLL': '297cb6bd16e6dbd4a68c65a60e6c1c0ff1053e9d1a54866105659f40dbc8156a',
 'SCN-M0-PG-DDL-IMMEDIATE-DML': 'b034fd5ea21662ce860d032b7d72293aeb2f3c1de181bf0c3a00938209976a10',
 'SCN-M0-PG-DDL-WAITER-INVALIDATE': '3f3cd4ac63b447f66b751e76c0deb9c6cf810af3133d935c0eabb1e370a05b3a',
 'SCN-M0-PG-EXISTING-SLOT-STITCH': '6d9fd4d19961d1f77a8b0139ed6bc01bff731b653e591097da6949239d55ae76',
 'SCN-M0-PG-EXPECTED-CLOSE-TOKEN': '38f9a6c4b8c1401f69fc992b5e01a6dec3bee8009ef9f7acac6862ed91c14a6f',
 'SCN-M0-PG-EXPORTER-RELEASE-AFTER': 'fcd57891ab746a1b4bc4c5a7eea5b8bd7eb6fa7bcfc9d6278bcba3e7d115e56c',
 'SCN-M0-PG-EXPORTER-RELEASE-BEFORE': '98283f41b6cec78b9e5afb314ba039e3ad4e2120d7d8b72e141b53c7dbd298cf',
 'SCN-M0-PG-FENCE-UPDATE-AFTER': 'af60de1289c397fec768076370d2b33115a18f63d18ff6117cca0b202fcd23b3',
 'SCN-M0-PG-FENCE-UPDATE-BEFORE': '9723804f63b71274735f73d110b06b9c5751e5247c25cbcf21ef5454e37f1422',
 'SCN-M0-PG-FIRST-FEEDBACK': '457b29b5544d810be87be6eddfa456f65059dc59290dfca60d32a0cdc3805884',
 'SCN-M0-PG-GUARD-LOSS': '86afa11cce2a5402514917b21a2ccbe5044a417b6dcd12d12e132f26b579f491',
 'SCN-M0-PG-KEEPALIVE-REPLY': '4759e804ce85f0d7b08f596d418d0c4e7302fbb83353e4fbdb9fafd52ce7dc6b',
 'SCN-M0-PG-ORIGIN-ORDINAL': '2ad8a854d8ce6f4f356e1c3270414458ccd84a5b11dcc3e133d212b0901ef5bf',
 'SCN-M0-PG-PUBLICATION-DRIFT': '1d453d3aeefc38a6fcf62ecc54038a682d062ab692977abaf54a1d5e2354f629',
 'SCN-M0-PG-RESTART-MAX': '6b8faf6cbbb30886f4ce36568cdee8b518ad2851c9298b21d57121f2e709d457',
 'SCN-M0-PG-RETAINED-SLOT-RECOVERY': 'b3343c7f60da63e7d28109ac2451fc0efc2c5ac3f9d83fd978d921c3fb1cffa5',
 'SCN-M0-PG-SAFE-STOP-CLOSE': '4d091c322b620747b2da2075d56d61a5698bb8068fa3dc006f383767669a688c',
 'SCN-M0-PG-SAFE-STOP-PERSIST-BEFORE': 'fde0c702efd106beb37d8280c9b7d0cc51450922279b6dc96d7df06986d59954',
 'SCN-M0-PG-SLOW-COMMIT-REQUESTED-REPLY': '8da8faf7c3a2c1d2c21e61ac4756af59bd0d7f57d6c20808db264c16669d95ea',
 'SCN-M0-PG-SNAPSHOT-EXPORT-AFTER-GUARD': 'bdbc17180fd582ec97da4c099deb85f50eeefe0c291cb4075fb51f5c79113ded',
 'SCN-M0-PG-START-REPLICATION': '4b2a3200c83f9082841b47b8cdfc1a3906c34cc93d2892abf9f9869bf470f8d3',
 'SCN-M0-PG-TOKEN-PERSIST': '533f5c98313072bd58aa3ea72d0b3380c6cc454ab384c656c31292e17bea8599',
 'SCN-M0-PG-UNEXPECTED-COPYBOTH-LOSS': '4fa1d6f723d830e45133020084903c4b0571fa8da808fa59545980dc8974d36a',
 'SCN-M0-PG-WAL-HEADROOM': '5cc8bdb1f1f5cc4cd5a8f4203f7dae60196b24122ea6c13ce55111e03ae447f1'}

def validate_fixture_semantics(cases, findings):
    """Reject phase-impossible process state and nominal lifecycle placeholders."""
    for index, case in enumerate(cases):
        fixture_id = case.get("fixture_id")
        expected = LIFECYCLE_EXPECTATIONS.get(fixture_id)
        if expected is None:
            finding(findings, "E_FIXTURE_LIFECYCLE", f"cases/{index}", "unknown lifecycle profile")
            continue
        inputs = case.get("inputs", {})
        encoded_inputs = json.dumps(inputs, sort_keys=True, separators=(",", ":")).encode()
        if hashlib.sha256(encoded_inputs).hexdigest() != FIXTURE_INPUT_SHA256[fixture_id]:
            finding(findings, "E_FIXTURE_INPUT_DIGEST", fixture_id, "exact branch input vector changed")
        case_semantics = {key: case.get(key) for key in ("executor_id", "seed", "inputs", "preconditions", "hook", "expected", "redaction")}
        encoded_case = json.dumps(case_semantics, sort_keys=True, separators=(",", ":")).encode()
        if hashlib.sha256(encoded_case).hexdigest() != FIXTURE_CASE_SHA256[fixture_id]:
            finding(findings, "E_FIXTURE_CASE_DIGEST", fixture_id, "exact prerequisites/outcome/redaction vector changed")
        for field in ("pre_state", "phase", "fault_phase", "fault_action"):
            if inputs.get(field) != expected[field]:
                finding(findings, "E_FIXTURE_LIFECYCLE", fixture_id, f"{field} must be {expected[field]!r}")
        actual_process = {key: inputs[key] for key in PROCESS_FIELDS if key in inputs}
        if actual_process != expected["process"]:
            finding(findings, "E_FIXTURE_PROCESS_STATE", fixture_id, f"phase process state differs: {actual_process!r}")
        for key, value in inputs.items():
            if isinstance(value, str) and not value:
                finding(findings, "E_FIXTURE_EMPTY_LITERAL", fixture_id, f"{key} must not be empty")
        if "disconnect_backend_pid" in inputs and inputs["disconnect_backend_pid"] not in {inputs.get("exporter_backend_pid"), inputs.get("guard_backend_pid")} :
            finding(findings, "E_FIXTURE_DISCONNECT_PID", fixture_id, "disconnect target is not a live phase backend")

def validate():
    findings = []
    contract, schema, fixtures = load(CONTRACT), load(SCHEMA), load(FIXTURES)
    schema_findings = []
    CORE.validate_schema_instance(contract, schema, schema_findings, base=SCHEMA.parent, root=schema)
    for item in schema_findings:
        finding(findings, "E_SCHEMA", "contract", item["pointer"] + ": " + item["message"])
    fixture_schema = load(FIXTURE_SCHEMA)
    fixture_schema_findings = []
    CORE.validate_schema_instance(fixtures, fixture_schema, fixture_schema_findings, base=FIXTURE_SCHEMA.parent, root=fixture_schema)
    for item in fixture_schema_findings:
        finding(findings, "E_FIXTURE_SCHEMA", "fixtures", item["pointer"] + ": " + item["message"])

    text = CONTRACT.read_text() + SQL.read_text()
    if "M0-" + "PROVISIONAL" in text:
        finding(findings, "E_PROVISIONAL_MARKER", "contract", "reconciled artifact contains a provisional marker")

    protocol = contract["protocol"]
    if contract["supported_postgresql"] != [{
        "exact_version": "17.6", "image_manifest": "sha256:00bc86618629af00d2937fdc5a5d63db3ff8450acf52f0636ec813c7f4902929",
        "major": 17, "platform": "linux/amd64"
    }]:
        finding(findings, "E_SERVER_MATRIX", "supported_postgresql", "only pinned PostgreSQL 17.6 linux/amd64 is admitted")
    required_options = ["proto_version", "publication_names", "binary", "messages", "streaming", "two_phase", "origin"]
    if protocol["option_order"] != required_options or any(value is not False for value in (protocol["binary"], protocol["streaming"], protocol["two_phase"])) or protocol["origin"] != "any":
        finding(findings, "E_PROTOCOL_OPTIONS", "protocol", "logical-replication options changed")
    transport = protocol["transport"]
    if (transport["crate"], transport["version"], transport["feature"], transport.get("adapter")) != ("pgwire-replication", "0.4.0", "tls-rustls,scram", "boring-cdc-pgwire-v1"):
        finding(findings, "E_TRANSPORT", "protocol/transport", "CopyBoth transport/adapter pin changed")
    if not transport.get("stock_high_level_worker", "").startswith("forbidden:") or "always passes false" not in transport.get("implementation_boundary", ""):
        finding(findings, "E_TRANSPORT_ADAPTER", "protocol/transport", "stock-worker incompatibility or safe status adapter requirement absent")
    if "START_REPLICATION SLOT" not in protocol["start_replication_template"] or "origin 'any'" not in protocol["start_replication_template"]:
        finding(findings, "E_START_REPLICATION", "protocol/start_replication_template", "template is incomplete")

    state_fields = {item["name"]: item for item in contract["source_state_fields"]}
    for name in ("capture_epoch", "durable_transaction_end_lsn", "slot_creation_floor_lsn", "last_feedback_lsn", "server_confirmed_flush_lsn", "wal_status", "safe_wal_size_bytes"):
        if name not in state_fields:
            finding(findings, "E_SOURCE_FIELD", "source_state_fields", name)
    if state_fields.get("durable_transaction_end_lsn", {}).get("constructor") != "atomic journal commit only":
        finding(findings, "E_TYPED_BOUNDARY", "source_state_fields/durable_transaction_end_lsn", "constructor widened")

    feedback = contract["feedback"]
    if feedback["effective_server_restart"] != "max(requested_lsn, server_confirmed_flush_lsn)":
        finding(findings, "E_RESTART_RULE", "feedback", "restart rule changed")
    if set(feedback["forbidden_inputs"]) != {"primary keepalive wal_end", "last_received_lsn", "slot creation floor as durable transaction", "destination checkpoint", "clock-derived LSN"}:
        finding(findings, "E_FEEDBACK_SOURCE", "feedback/forbidden_inputs", "unsafe feedback input set changed")

    publication = contract["publication"]
    if publication["publish"] != ["insert", "update", "delete", "truncate"] or publication["truncate"] != "detection_only_requires_reseed":
        finding(findings, "E_PUBLICATION", "publication", "publication safety operations changed")
    for table in ("heartbeat", "capture_fences"):
        if contract["control_relations"][table]["immutable_key"] != {"id": 1}:
            finding(findings, "E_CONTROL_KEY", f"control_relations/{table}", "fixed key changed")

    sql = SQL.read_text()
    required_sql = (
        "CREATE ROLE boring_cdc_admin NOLOGIN", "CREATE ROLE boring_cdc_capture LOGIN REPLICATION",
        "REVOKE ALL ON ALL TABLES IN SCHEMA boring_cdc_control FROM PUBLIC",
        "GRANT SELECT (id) ON boring_cdc_control.heartbeat, boring_cdc_control.capture_fences",
        "GRANT UPDATE (nonce, updated_at) ON boring_cdc_control.heartbeat",
        "GRANT UPDATE (capture_epoch, generation, table_set_fingerprint, unique_nonce)",
        "publish = 'insert, update, delete, truncate'", "ALTER PUBLICATION boring_cdc OWNER TO boring_cdc_admin",
        "configure_selected_relations(selected regclass[])", "GRANT SELECT ON TABLE %s TO boring_cdc_capture",
        "ALTER PUBLICATION boring_cdc ADD TABLE %s",
    )
    for fragment in required_sql:
        if fragment not in sql:
            finding(findings, "E_GRANT_SQL", "contracts/postgres/roles-grants.sql", fragment)
    if re.search(r"GRANT\s+(INSERT|DELETE|ALL).*boring_cdc_control", sql, re.I):
        finding(findings, "E_EXCESS_GRANT", "contracts/postgres/roles-grants.sql", "control writer has excess privilege")

    ddl = contract["ddl_matrix"]
    if len(ddl) < 10 or any(row["minimum_lock"] != "ACCESS EXCLUSIVE" or row["guard_conflicts"] is not True for row in ddl):
        finding(findings, "E_DDL_MATRIX", "ddl_matrix", "finite admitted DDL conflict proof is incomplete")
    if any("ATTACH" in row["operation"] or "DETACH" in row["operation"] for row in ddl):
        finding(findings, "E_DDL_PARTITION_LOCK", "ddl_matrix", "PG17 partition attach/detach is not admitted under ACCESS SHARE")
    if not all(term in contract["bootstrap"]["initial"] for term in ("acquire canonical ACCESS SHARE DDL guard and verify contracts", "each importer SET TRANSACTION SNAPSHOT as first statement", "publish and durably observe unique capture fence")):
        finding(findings, "E_BOOTSTRAP_SEQUENCE", "bootstrap/initial", "required ordered bootstrap phases absent")

    wal = contract["wal_headroom"]
    expected_wal = (68719476736, 300, 30, 30, 120, {"warning": 1800, "action": 900, "critical": 300, "hard": 0})
    observed_wal = (wal["max_slot_wal_keep_size_bytes"], wal["rate_window_seconds"], wal["metric_freshness_seconds"], wal["monitoring_delay_seconds"], wal["reaction_reserve_seconds"], wal["horizons_seconds"])
    if observed_wal != expected_wal:
        finding(findings, "E_WAL_LITERALS", "wal_headroom", "recommended WAL constants changed")

    fixture_ids = contract["fixture_ids"]
    cases = fixtures.get("cases", [])
    case_ids = [case.get("fixture_id") for case in cases]
    if fixture_ids != case_ids or len(set(case_ids)) != len(case_ids):
        finding(findings, "E_FIXTURE_INVENTORY", "fixtures", "ordered contract/fixture inventory differs or duplicates")
    used_executors = {case.get("executor_id") for case in cases}
    if used_executors != set(contract["executors"]):
        finding(findings, "E_EXECUTOR_MAP", "fixtures", "fixture/executor mapping is not total")
    used_hooks = {case.get("hook") for case in cases}
    if used_hooks != set(contract["hooks"]):
        finding(findings, "E_HOOK_MAP", "fixtures", "declared hooks and fixture hooks differ")
    required_expected = {"state", "exit_code", "checkpoint", "feedback", "external_effect", "status_code", "metric"}
    for index, case in enumerate(cases):
        if set(case.get("expected", {})) != required_expected or not case.get("hook") or not case.get("preconditions") or case.get("seed") != "0x50474344434d305631":
            finding(findings, "E_FIXTURE_SHAPE", f"cases/{index}", "fixture is not mechanically executable")
        if case["expected"]["checkpoint"] not in ("unchanged", "advanced_to_fence_transaction"):
            finding(findings, "E_CHECKPOINT_BOUNDARY", f"cases/{index}", "fixture permits partial checkpoint")
    required_runtime_inputs = {
        "pre_state", "run_id", "capture_connection_generation", "capture_epoch", "generation",
        "slot_name", "publication_name", "durable_transaction_end_lsn", "last_feedback_lsn",
        "journal_checkpoint_seq", "creation_floor_lsn", "ownership_deadline_remaining_ms",
        "operation_deadline_ms", "table_set_fingerprint", "fault_action", "fault_phase", "phase",
    }
    incomplete_inputs = [case["fixture_id"] for case in cases if required_runtime_inputs - set(case.get("inputs", {}))]
    if incomplete_inputs:
        finding(findings, "E_FIXTURE_INPUTS", "fixtures", "incomplete inputs: " + ",".join(incomplete_inputs))
    validate_fixture_semantics(cases, findings)
    required_case_inputs = {
        "SCN-M0-PG-PUBLICATION-DRIFT": {"expected_publication_fingerprint", "observed_publication_fingerprint", "catalog_poll_age_ms"},
        "SCN-M0-PG-CONTROL-CARDINALITY": {"attempted_affected_row_counts", "required_affected_rows"},
        "SCN-M0-PG-DDL-CONFLICT-MATRIX": {"ddl_operation", "requested_lock", "held_guard_lock", "observed"},
        "SCN-M0-PG-BOOTSTRAP-EXPORTER-LOSS": {"disconnect_backend_pid", "importer_acknowledged", "importer_expected"},
        "SCN-M0-PG-GUARD-LOSS": {"disconnect_backend_pid", "durable_fence_observed"},
    }
    for case in cases:
        missing = required_case_inputs.get(case["fixture_id"], set()) - set(case["inputs"])
        if missing:
            finding(findings, "E_FIXTURE_BRANCH_INPUT", case["fixture_id"], ",".join(sorted(missing)))
    copyboth = cases[0].get("inputs", {}) if cases else {}
    if not copyboth.get("start_replication_hex") or not copyboth.get("server_copyboth_response_hex") or not copyboth.get("xlog_copydata_payload_hex"):
        finding(findings, "E_PROTOCOL_GOLDEN", "fixtures/0", "exact START_REPLICATION and CopyBoth bytes absent")
    else:
        try:
            start_bytes = bytes.fromhex(copyboth["start_replication_hex"])
            response = bytes.fromhex(copyboth["server_copyboth_response_hex"])
            xlog = bytes.fromhex(copyboth["xlog_copydata_payload_hex"])
            if start_bytes.decode() != copyboth["start_replication_utf8"] or response != b"W\x00\x00\x00\x07\x00\x00\x00":
                raise ValueError("START_REPLICATION or CopyBothResponse bytes differ")
            if len(xlog) < 26 or xlog[0] != ord("w") or xlog[25] != ord("B") or len(xlog[25:]) != 21:
                raise ValueError("XLogData does not contain one complete 21-byte Begin message")
        except (ValueError, UnicodeDecodeError) as error:
            finding(findings, "E_PROTOCOL_GOLDEN", "fixtures/0", str(error))
    origin_case = next((case for case in cases if case.get("fixture_id") == "SCN-M0-PG-ORIGIN-ORDINAL"), {})
    try:
        origin_inputs = origin_case["inputs"]
        begin, origin, insert = (bytes.fromhex(origin_inputs[name]) for name in ("begin_hex", "origin_hex", "insert_hex"))
        if len(begin) != 21 or begin[0] != ord("B") or len(origin) < 11 or origin[0] != ord("O") or origin[-1] != 0:
            raise ValueError("Begin/Origin message truncated")
        if len(insert) < 13 or insert[0] != ord("I") or insert[5] != ord("N") or int.from_bytes(insert[6:8], "big") != 1 or insert[8] != ord("t") or int.from_bytes(insert[9:13], "big") != len(insert[13:]):
            raise ValueError("Insert tuple message truncated")
    except (KeyError, ValueError) as error:
        finding(findings, "E_PGOUTPUT_GOLDEN", "fixtures", str(error))
    keepalive = next((case for case in cases if case.get("fixture_id") == "SCN-M0-PG-KEEPALIVE-REPLY"), {})
    if keepalive.get("inputs", {}).get("expected_status_packet_bytes") != 34 or not keepalive.get("inputs", {}).get("expected_status_packet_hex", "").endswith("00"):
        finding(findings, "E_STATUS_GOLDEN", "fixtures", "complete 34-byte status packet with outgoing reply=false absent")
    compound = [case for case in cases if case["fixture_id"] in {
        "SCN-M0-PG-CREATION-FLOOR-NULL", "SCN-M0-PG-CREATION-FLOOR-EQUAL",
        "SCN-M0-PG-CREATION-FLOOR-INVALID-SLOT", "SCN-M0-PG-CREATION-FLOOR-WAL-UNAVAILABLE",
    }]
    if len(compound) != 4 or any(case["inputs"].get("durable_transaction_end_lsn") is not None for case in compound):
        finding(findings, "E_CREATION_FLOOR", "fixtures", "compound creation-floor matrix incomplete")
    rejected = {case["fixture_id"]: case["expected"]["state"] for case in compound}
    if rejected.get("SCN-M0-PG-CREATION-FLOOR-INVALID-SLOT") != "requires_reseed" or rejected.get("SCN-M0-PG-CREATION-FLOOR-WAL-UNAVAILABLE") != "requires_reseed":
        finding(findings, "E_CREATION_FLOOR_PREDICATES", "fixtures", "slot validity and WAL availability are not independently required")

    inputs = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in (CONTRACT, SCHEMA, FIXTURE_SCHEMA, EVIDENCE_SCHEMA, FIXTURES, SQL)}
    return findings, inputs


def resolve_source_parent(prior, inputs, validator_sha256):
    """Bind generated evidence to the exact immediate source commit or reject tampering."""
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    prior_matches_inputs = prior.get("inputs") == inputs and prior.get("validator_sha256") == validator_sha256
    if not prior_matches_inputs:
        return head, None
    expected_parent = subprocess.check_output(["git", "rev-parse", "HEAD^"], cwd=ROOT, text=True).strip()
    candidate = prior.get("source_parent_git_commit")
    if candidate != expected_parent:
        return candidate or "", f"stored source parent must equal immediate parent {expected_parent}"
    try:
        subprocess.run(
            ["git", "cat-file", "-e", f"{candidate}^{{commit}}"],
            cwd=ROOT,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError:
        return candidate, "stored source parent is not an existing commit"
    return candidate, None


def main():
    findings, inputs = validate()
    prior = load(EVIDENCE) if EVIDENCE.exists() else {}
    validator_sha256 = hashlib.sha256(VALIDATOR.read_bytes()).hexdigest()
    source_parent, provenance_error = resolve_source_parent(prior, inputs, validator_sha256)
    if provenance_error:
        finding(findings, "E_EVIDENCE_PROVENANCE", "evidence/source_parent_git_commit", provenance_error)
    tree_material = b"".join((path + "\0" + digest + "\n").encode() for path, digest in sorted(inputs.items()))
    evidence = {
        "schema_version": "m0-postgres-contract-evidence/v1", "owner_bead": OWNER,
        "status": "pass" if not findings else "fail", "validator": "scripts/validate/postgres_contract.py",
        "validator_sha256": validator_sha256,
        "source_parent_git_commit": source_parent,
        "input_tree_sha256": hashlib.sha256(tree_material).hexdigest(),
        "inputs": inputs, "fixture_count": len(load(FIXTURES)["cases"]), "findings": findings,
        "runtime_observed": False, "product_faults": "fault_not_applicable",
    }
    evidence_schema_findings = []
    CORE.validate_schema_instance(evidence, load(EVIDENCE_SCHEMA), evidence_schema_findings, base=EVIDENCE_SCHEMA.parent, root=load(EVIDENCE_SCHEMA))
    if evidence_schema_findings:
        evidence["status"] = "fail"
        evidence["findings"].extend({"code": "E_EVIDENCE_SCHEMA", "path": item["pointer"], "message": item["message"]} for item in evidence_schema_findings)
    EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    EVIDENCE.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    print(json.dumps(evidence, sort_keys=True, separators=(",", ":")))
    return 0 if evidence["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
