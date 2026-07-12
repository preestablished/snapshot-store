#!/usr/bin/env python3
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from m8_joint_fork_integrity_evidence import validate_evidence


H = "0" * 64
H1 = "1" * 64
H2 = "2" * 64
H3 = "3" * 64
H4 = "4" * 64


def row(index=0, *, replay_ref=H1, original_ref=H1, state_replay=H2, result="pass"):
    return {
        "child_index": index,
        "seed_hex": H,
        "original_ref_hex": original_ref,
        "replay_ref_hex": replay_ref,
        "input_log_id_hex": H3,
        "state_hash_original_hex": H2,
        "state_hash_replay_hex": state_replay,
        "restore_mode": "baseline_delta",
        "baseline_ref_hex": H4,
        "manifest_kind": "DELTA",
        "chain_depth": 1,
        "dirty_pages": 7,
        "shared_page_ratio": 0.98,
        "timing_ms": {
            "fork": 1.0,
            "run": 2.0,
            "original_commit": 3.0,
            "restore": 4.0,
            "replay": 5.0,
            "replay_commit": 6.0,
        },
        "result": result,
    }


def bars(overrides=None):
    values = {
        "m8_command_status": True,
        "m8_child_count": True,
        "m8_ref_identity": True,
        "m8_replay_done": True,
        "m8_shared_page_ratio_aggregate": True,
        "m8_restore_delta_used": True,
        "m8_full_manifest_cadence": True,
        "m8_semantic_negative_red": True,
        "m8_store_root_qualified": True,
        "m8_fork_commit_p99": True,
        "m8_restore_delta_p99": True,
    }
    values.update(overrides or {})
    return [{"name": name, "ok": ok} for name, ok in values.items()]


def evidence(run_kind="fake", expected=1, bar_overrides=None):
    return {
        "schema_version": 1,
        "request": ".agents/requests/phase2-closeout-m8-joint-fork-integrity",
        "run_kind": run_kind,
        "expected_child_count": expected,
        "run_id": "m8-test",
        "started_at": "2026-07-09T00:00:00Z",
        "finished_at": "2026-07-09T00:01:00Z",
        "repos": {
            "snapshot-store": {"rev": "a" * 40, "dirty": False},
            "determinism-hypervisor": {"rev": "b" * 40, "dirty": False},
            "control-plane": {"rev": "c" * 40, "dirty": False},
            "guest-sdk": {"rev": "d" * 40, "dirty": False},
        },
        "host": {"hostname": "test-host"},
        "guest": {"kind": "fake"},
        "store_root": {"path": "/reference/m8", "disk_class": "sata", "qualified": True},
        "config": {"jobs": expected, "max_delta_chain": 64},
        "child_table": {"jsonl": "child-ref-table.jsonl", "csv": "child-ref-table.csv"},
        "bars": bars(bar_overrides),
        "commands": [],
        "artifacts": [],
        "semantic_negative": {
            "command": "fake-negative",
            "mutated_input": "changed burst",
            "expected_red_result": True,
            "actual_red_result": True,
        },
        "deviations": [],
    }


def latency():
    return {
        "policy": "telemetry",
        "fork_to_original_commit": {
            "count": 1,
            "p50": 6.0,
            "p95": 6.0,
            "p99": 6.0,
            "max": 6.0,
        },
        "restore_delta": {
            "count": 1,
            "p50": 4.0,
            "p95": 4.0,
            "p99": 4.0,
            "max": 4.0,
        },
        "restore_full": {
            "count": 0,
            "p50": None,
            "p95": None,
            "p99": None,
            "max": None,
        },
        "replay_restore_to_commit": {
            "count": 1,
            "p50": 15.0,
            "p95": 15.0,
            "p99": 15.0,
            "max": 15.0,
        },
    }


def write_case(root, ev, rows):
    root.mkdir(parents=True, exist_ok=True)
    (root / "evidence.json").write_text(json.dumps(ev))
    (root / "child-ref-table.jsonl").write_text(
        "".join(json.dumps(item) + "\n" for item in rows)
    )


class M8EvidenceValidatorTest(unittest.TestCase):
    def validate_case(self, ev, rows):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_case(root, ev, rows)
            return validate_evidence(root)

    def test_accepts_valid_fake_positive_evidence(self):
        self.assertEqual([], self.validate_case(evidence(), [row()]))

    def test_rejects_missing_replay_ref_in_positive_run(self):
        errors = self.validate_case(evidence(), [row(replay_ref=None)])
        self.assertTrue(any("replay_ref_hex" in error for error in errors), errors)
        self.assertTrue(any("must equal original_ref_hex" in error for error in errors), errors)

    def test_rejects_state_hash_only_success(self):
        errors = self.validate_case(evidence(), [row(state_replay=H4)])
        self.assertTrue(any("state_hash_replay_hex must equal" in error for error in errors), errors)

    def test_rejects_non_contiguous_child_indices(self):
        ev = evidence(expected=2)
        errors = self.validate_case(ev, [row(index=0), row(index=2)])
        self.assertTrue(any("unique and contiguous" in error for error in errors), errors)

    def test_accepts_resume_metadata_and_row_sources(self):
        ev = evidence(expected=2)
        ev["resume"] = {
            "enabled": True,
            "resumed_child_count": 1,
            "fresh_child_count": 1,
        }
        resumed = row(index=0)
        resumed["row_source"] = "resumed"
        fresh = row(index=1)
        fresh["row_source"] = "fresh"
        self.assertEqual([], self.validate_case(ev, [resumed, fresh]))

    def test_rejects_invalid_row_source_or_resume_counts(self):
        invalid_source = row()
        invalid_source["row_source"] = "cached"
        errors = self.validate_case(evidence(), [invalid_source])
        self.assertTrue(any("row_source" in error for error in errors), errors)

        ev = evidence(expected=2)
        ev["resume"] = {
            "enabled": True,
            "resumed_child_count": 2,
            "fresh_child_count": 1,
        }
        errors = self.validate_case(ev, [row(index=0), row(index=1)])
        self.assertTrue(any("must equal child row count" in error for error in errors), errors)

    def test_accepts_latency_metadata(self):
        ev = evidence()
        ev["latency_ms"] = latency()
        self.assertEqual([], self.validate_case(ev, [row()]))

    def test_rejects_invalid_latency_metadata(self):
        ev = evidence()
        ev["latency_ms"] = latency()
        ev["latency_ms"]["restore_delta"]["p95"] = 3.0
        ev["latency_ms"]["restore_delta"]["p99"] = 2.0
        errors = self.validate_case(ev, [row()])
        self.assertTrue(any("percentiles must be monotonic" in error for error in errors), errors)

        ev = evidence()
        ev["latency_ms"] = latency()
        ev["latency_ms"]["restore_full"]["count"] = 0
        ev["latency_ms"]["restore_full"]["p50"] = 1.0
        errors = self.validate_case(ev, [row()])
        self.assertTrue(any("empty stats must use null" in error for error in errors), errors)

    def test_full_acceptance_requires_1000_and_qualified_store_root(self):
        ev = evidence(run_kind="full_acceptance", expected=1)
        ev["store_root"]["qualified"] = False
        errors = self.validate_case(ev, [row()])
        self.assertTrue(any("full_acceptance requires 1000" in error for error in errors), errors)
        self.assertTrue(any("qualified=true" in error for error in errors), errors)

    def test_full_acceptance_requires_complete_host_guest_and_store_identity(self):
        ev = evidence(run_kind="full_acceptance", expected=1000)
        errors = self.validate_case(ev, [row(index=i) for i in range(1000)])
        for field in [
            "host.kernel",
            "host.cpu",
            "host.memory_bytes",
            "host.kvm_read_write",
            "guest.machine_config_hash",
            "guest.images",
            "store_root.resolved_path",
            "store_root.mount",
            "config.restore_mode",
            "config.slot_cores_env",
        ]:
            self.assertTrue(any(field in error for error in errors), (field, errors))

    def test_rejects_unstructured_deviation(self):
        ev = evidence()
        ev["deviations"] = [{"id": "partial"}]
        errors = self.validate_case(ev, [row()])
        self.assertTrue(any("deviations[0].reason" in error for error in errors), errors)

    def test_semantic_negative_requires_committed_ref_mismatch(self):
        ev = evidence(run_kind="semantic_negative", expected=1)
        mismatch = row(replay_ref=H4, original_ref=H1, result="ref_mismatch")
        self.assertEqual([], self.validate_case(ev, [mismatch]))

        errors = self.validate_case(ev, [row(result="replay_divergence")])
        self.assertTrue(any("committed replay_ref mismatch" in error for error in errors), errors)

    def test_rejects_failed_required_bar(self):
        errors = self.validate_case(evidence(bar_overrides={"m8_ref_identity": False}), [row()])
        self.assertTrue(any("bars.m8_ref_identity" in error for error in errors), errors)


if __name__ == "__main__":
    unittest.main()
