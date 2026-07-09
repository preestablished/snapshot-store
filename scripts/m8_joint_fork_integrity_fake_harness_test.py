#!/usr/bin/env python3
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from m8_joint_fork_integrity_evidence import validate_evidence
from m8_joint_fork_integrity_fake_harness import build_parser, run_harness


def parse_args(root, *extra):
    return build_parser().parse_args(["--evidence-root", str(root), *extra])


def read_rows(root):
    return [
        json.loads(line)
        for line in (root / "child-ref-table.jsonl").read_text().splitlines()
        if line.strip()
    ]


class M8FakeHarnessTest(unittest.TestCase):
    def test_writes_validator_valid_fake_evidence_with_full_cadence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args = parse_args(
                root,
                "--expected-child-count",
                "5",
                "--max-delta-chain",
                "2",
            )

            self.assertEqual(0, run_harness(args, repo_root=Path.cwd()))
            self.assertEqual([], validate_evidence(root))
            rows = read_rows(root)
            self.assertEqual(5, len(rows))
            self.assertTrue(any(row["restore_mode"] == "baseline_delta" for row in rows))
            self.assertTrue(any(row["manifest_kind"] == "FULL" for row in rows))
            self.assertGreaterEqual(
                sum(row["shared_page_ratio"] for row in rows) / len(rows),
                0.94,
            )
            self.assertTrue(all(row["row_source"] == "fresh" for row in rows))

    def test_resume_reuses_completed_rows_and_finishes_coherent_table(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = parse_args(
                root,
                "--expected-child-count",
                "5",
                "--max-delta-chain",
                "2",
                "--stop-after",
                "2",
                "--no-validate",
            )
            self.assertEqual(3, run_harness(first, repo_root=Path.cwd()))
            self.assertEqual(2, len(read_rows(root)))

            resumed = parse_args(
                root,
                "--expected-child-count",
                "5",
                "--max-delta-chain",
                "2",
                "--resume",
            )
            self.assertEqual(0, run_harness(resumed, repo_root=Path.cwd()))
            self.assertEqual([], validate_evidence(root))
            rows = read_rows(root)
            self.assertEqual(["resumed", "resumed", "fresh", "fresh", "fresh"], [
                row["row_source"] for row in rows
            ])

    def test_semantic_negative_commits_ref_mismatch_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args = parse_args(
                root,
                "--run-kind",
                "semantic_negative",
                "--expected-child-count",
                "1",
            )

            self.assertEqual(0, run_harness(args, repo_root=Path.cwd()))
            self.assertEqual([], validate_evidence(root))
            row = read_rows(root)[0]
            self.assertEqual("ref_mismatch", row["result"])
            self.assertNotEqual(row["original_ref_hex"], row["replay_ref_hex"])


if __name__ == "__main__":
    unittest.main()
