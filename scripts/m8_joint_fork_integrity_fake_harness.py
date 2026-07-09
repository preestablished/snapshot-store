#!/usr/bin/env python3
import argparse
import csv
import hashlib
import json
import platform
import socket
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from m8_joint_fork_integrity_evidence import REQUEST, REQUIRED_BARS, validate_evidence


REPO_NAMES = [
    "snapshot-store",
    "determinism-hypervisor",
    "control-plane",
    "guest-sdk",
]

TIMING_KEYS = [
    "fork",
    "run",
    "original_commit",
    "restore",
    "replay",
    "replay_commit",
]


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def hex32(label):
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def run_git(repo, *args):
    try:
        return subprocess.run(
            ["git", "-C", str(repo), *args],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (FileNotFoundError, subprocess.CalledProcessError):
        return ""


def repo_info(repo):
    rev = run_git(repo, "rev-parse", "HEAD") or "unknown"
    dirty = bool(run_git(repo, "status", "--short"))
    return {"rev": rev, "dirty": dirty}


def repo_roots(snapshot_root):
    parent = snapshot_root.parent
    return {
        "snapshot-store": snapshot_root,
        "determinism-hypervisor": parent / "determinism-hypervisor",
        "control-plane": parent / "control-plane",
        "guest-sdk": parent / "guest-sdk",
    }


def row_for(index, *, run_kind, max_delta_chain, row_source):
    seed = hex32(f"m8-fake-seed:{index}")
    original_ref = hex32(f"m8-fake-original-ref:{index}")
    state_hash = hex32(f"m8-fake-state:{index}")
    input_log = hex32(f"m8-fake-input-log:{index}")
    is_full_rollover = index > 0 and index % max_delta_chain == 0
    semantic_negative = run_kind == "semantic_negative"
    replay_ref = (
        hex32(f"m8-fake-mutated-replay-ref:{index}")
        if semantic_negative
        else original_ref
    )
    replay_state = (
        hex32(f"m8-fake-mutated-state:{index}") if semantic_negative else state_hash
    )
    base_timing = float(index + 1)
    return {
        "child_index": index,
        "seed_hex": seed,
        "original_ref_hex": original_ref,
        "replay_ref_hex": replay_ref,
        "input_log_id_hex": input_log,
        "state_hash_original_hex": state_hash,
        "state_hash_replay_hex": replay_state,
        "restore_mode": "full" if is_full_rollover else "baseline_delta",
        "baseline_ref_hex": None if is_full_rollover else hex32("m8-fake-root-baseline"),
        "manifest_kind": "FULL" if is_full_rollover else "DELTA",
        "chain_depth": 0 if is_full_rollover else (index % max_delta_chain) + 1,
        "dirty_pages": 7 + index,
        "shared_page_ratio": 0.99 if not is_full_rollover else 0.97,
        "timing_ms": {
            key: round(base_timing + offset * 0.25, 3)
            for offset, key in enumerate(TIMING_KEYS)
        },
        "result": "ref_mismatch" if semantic_negative else "pass",
        "row_source": row_source,
    }


def load_existing_rows(path):
    rows = {}
    if not path.exists():
        return rows
    for line_number, line in enumerate(path.read_text().splitlines(), start=1):
        if not line.strip():
            continue
        row = json.loads(line)
        index = row.get("child_index")
        if not isinstance(index, int):
            raise ValueError(f"{path}:{line_number}: child_index must be an integer")
        rows[index] = row
    return rows


def compatible_existing_row(row, expected):
    fields = [
        "child_index",
        "seed_hex",
        "original_ref_hex",
        "replay_ref_hex",
        "input_log_id_hex",
        "state_hash_original_hex",
        "state_hash_replay_hex",
        "restore_mode",
        "baseline_ref_hex",
        "manifest_kind",
        "chain_depth",
        "result",
    ]
    return all(row.get(field) == expected.get(field) for field in fields)


def build_rows(root, *, expected_child_count, stop_after, resume, run_kind, max_delta_chain):
    existing = load_existing_rows(root / "child-ref-table.jsonl") if resume else {}
    target = expected_child_count if stop_after is None else min(stop_after, expected_child_count)
    rows = []
    for index in range(target):
        expected = row_for(
            index,
            run_kind=run_kind,
            max_delta_chain=max_delta_chain,
            row_source="fresh",
        )
        existing_row = existing.get(index)
        if existing_row is not None:
            if not compatible_existing_row(existing_row, expected):
                raise ValueError(
                    f"existing child row {index} does not match this fake run config"
                )
            expected["row_source"] = "resumed"
        rows.append(expected)
    return rows


def percentile(values, pct):
    if not values:
        return 0.0
    values = sorted(values)
    rank = int(round((len(values) - 1) * pct))
    return values[rank]


def aggregate_shared_page_ratio(rows):
    if not rows:
        return 0.0
    return sum(row["shared_page_ratio"] for row in rows) / len(rows)


def has_full_cadence(rows, expected_child_count, max_delta_chain):
    if expected_child_count <= max_delta_chain:
        return True
    return any(row["manifest_kind"] == "FULL" and row["chain_depth"] == 0 for row in rows)


def bars_for(rows, *, run_kind, expected_child_count, max_delta_chain, store_root_qualified):
    semantic_negative = run_kind == "semantic_negative"
    completed = len(rows) == expected_child_count
    refs_equal = all(row["replay_ref_hex"] == row["original_ref_hex"] for row in rows)
    states_equal = all(
        row["state_hash_replay_hex"] == row["state_hash_original_hex"] for row in rows
    )
    saw_delta = any(row["restore_mode"] == "baseline_delta" for row in rows)
    saw_ref_mismatch = any(
        row["result"] == "ref_mismatch"
        and row["replay_ref_hex"] != row["original_ref_hex"]
        for row in rows
    )
    fork_commit_p99 = percentile(
        [
            row["timing_ms"]["fork"] + row["timing_ms"]["original_commit"]
            for row in rows
        ],
        0.99,
    )
    restore_delta_p99 = percentile(
        [
            row["timing_ms"]["restore"]
            for row in rows
            if row["restore_mode"] == "baseline_delta"
        ],
        0.99,
    )
    values = {
        "m8_command_status": completed,
        "m8_child_count": completed,
        "m8_ref_identity": refs_equal and states_equal and not semantic_negative,
        "m8_replay_done": all(row["replay_ref_hex"] for row in rows),
        "m8_shared_page_ratio_aggregate": aggregate_shared_page_ratio(rows) >= 0.94,
        "m8_restore_delta_used": saw_delta,
        "m8_full_manifest_cadence": has_full_cadence(
            rows, expected_child_count, max_delta_chain
        ),
        "m8_semantic_negative_red": saw_ref_mismatch if semantic_negative else True,
        "m8_store_root_qualified": store_root_qualified,
        "m8_fork_commit_p99": fork_commit_p99 < 100.0,
        "m8_restore_delta_p99": restore_delta_p99 < 100.0,
    }
    if semantic_negative:
        values["m8_command_status"] = completed
        values["m8_child_count"] = completed
        values["m8_replay_done"] = all(row["replay_ref_hex"] for row in rows)
    return [{"name": name, "ok": bool(values[name])} for name in REQUIRED_BARS]


def write_child_tables(root, rows):
    jsonl = root / "child-ref-table.jsonl"
    jsonl.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows))

    csv_path = root / "child-ref-table.csv"
    fields = [
        "child_index",
        "original_ref_hex",
        "replay_ref_hex",
        "input_log_id_hex",
        "restore_mode",
        "manifest_kind",
        "chain_depth",
        "shared_page_ratio",
        "result",
        "row_source",
    ]
    with csv_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fields})


def build_evidence(args, rows, *, started_at, finished_at, repo_root):
    roots = repo_roots(repo_root)
    run_id = args.run_id or f"m8-fake-{started_at.replace(':', '').replace('-', '')}"
    semantic_red = (
        any(row["result"] == "ref_mismatch" for row in rows)
        if args.run_kind == "semantic_negative"
        else True
    )
    semantic_negative = {
        "command": "scripts/m8_joint_fork_integrity_fake_harness.py --run-kind semantic_negative",
        "mutated_input": "fake harness changes the replay commit ref after re-execution",
        "expected_red_result": True,
        "actual_red_result": semantic_red,
    }
    return {
        "schema_version": 1,
        "request": REQUEST,
        "run_kind": args.run_kind,
        "expected_child_count": args.expected_child_count,
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "repos": {name: repo_info(roots[name]) for name in REPO_NAMES},
        "host": {
            "hostname": socket.gethostname(),
            "system": platform.system(),
            "machine": platform.machine(),
        },
        "guest": {"kind": "fake", "deterministic_child_model": "sha256-ref-table"},
        "store_root": {
            "path": str(args.store_root),
            "disk_class": args.store_root_disk_class,
            "qualified": bool(args.store_root_qualified),
        },
        "config": {
            "jobs": args.expected_child_count,
            "max_delta_chain": args.max_delta_chain,
            "restore_mode": "baseline_delta",
            "child_batch_size": args.child_batch_size,
            "resume": args.resume,
        },
        "child_table": {
            "jsonl": "child-ref-table.jsonl",
            "csv": "child-ref-table.csv",
        },
        "bars": bars_for(
            rows,
            run_kind=args.run_kind,
            expected_child_count=args.expected_child_count,
            max_delta_chain=args.max_delta_chain,
            store_root_qualified=args.store_root_qualified,
        ),
        "commands": [" ".join(sys.argv)],
        "artifacts": [
            {"path": "evidence.json", "kind": "summary"},
            {"path": "child-ref-table.jsonl", "kind": "child_table"},
            {"path": "child-ref-table.csv", "kind": "child_table_csv"},
        ],
        "semantic_negative": semantic_negative,
        "deviations": [
            {
                "id": "fake_harness",
                "reason": "Host-only deterministic fake harness; not live KVM/NVMe acceptance evidence.",
            }
        ],
    }


def write_evidence(root, evidence):
    (root / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")


def run_harness(args, *, repo_root=None):
    if args.expected_child_count < 1:
        raise ValueError("--expected-child-count must be positive")
    if args.max_delta_chain < 1:
        raise ValueError("--max-delta-chain must be positive")
    root = Path(args.evidence_root)
    root.mkdir(parents=True, exist_ok=True)
    repo_root = Path(repo_root or Path.cwd()).resolve()
    started_at = utc_now()
    rows = build_rows(
        root,
        expected_child_count=args.expected_child_count,
        stop_after=args.stop_after,
        resume=args.resume,
        run_kind=args.run_kind,
        max_delta_chain=args.max_delta_chain,
    )
    write_child_tables(root, rows)
    finished_at = utc_now()
    evidence = build_evidence(
        args,
        rows,
        started_at=started_at,
        finished_at=finished_at,
        repo_root=repo_root,
    )
    write_evidence(root, evidence)

    if args.stop_after is not None and args.stop_after < args.expected_child_count:
        print(
            f"stopped after {len(rows)}/{args.expected_child_count} fake child rows at {root}",
            file=sys.stderr,
        )
        return 3
    if not args.no_validate:
        errors = validate_evidence(root)
        if errors:
            for error in errors:
                print(error, file=sys.stderr)
            return 1
    print(f"wrote fake M8 evidence to {root}")
    return 0


def build_parser():
    parser = argparse.ArgumentParser(
        description="Generate fake M8 joint fork-integrity evidence."
    )
    parser.add_argument("--evidence-root", required=True)
    parser.add_argument("--expected-child-count", type=int, default=4)
    parser.add_argument("--run-kind", choices=["fake", "semantic_negative"], default="fake")
    parser.add_argument("--max-delta-chain", type=int, default=3)
    parser.add_argument("--child-batch-size", type=int, default=2)
    parser.add_argument("--store-root", default="target/m8-fake-store")
    parser.add_argument("--store-root-disk-class", default="fake")
    parser.add_argument("--store-root-qualified", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--stop-after", type=int)
    parser.add_argument("--run-id")
    parser.add_argument("--no-validate", action="store_true")
    return parser


def main(argv):
    args = build_parser().parse_args(argv)
    try:
        return run_harness(args)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
