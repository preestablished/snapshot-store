#!/usr/bin/env python3
import json
import re
import sys
from pathlib import Path


REQUEST = ".agents/requests/phase2-closeout-m8-joint-fork-integrity"
RUN_KINDS = {"fake", "bounded_ci", "full_acceptance", "semantic_negative"}
RESTORE_MODES = {"baseline_delta", "full"}
MANIFEST_KINDS = {"FULL", "DELTA"}
RESULTS = {"pass", "ref_mismatch", "state_mismatch", "replay_divergence", "error"}
ROW_SOURCES = {"fresh", "resumed"}
HEX32_RE = re.compile(r"^[0-9a-f]{64}$")

REQUIRED_REPOS = [
    "snapshot-store",
    "determinism-hypervisor",
    "control-plane",
    "guest-sdk",
]

REQUIRED_BARS = [
    "m8_command_status",
    "m8_child_count",
    "m8_ref_identity",
    "m8_replay_done",
    "m8_shared_page_ratio_aggregate",
    "m8_restore_delta_used",
    "m8_full_manifest_cadence",
    "m8_semantic_negative_red",
    "m8_store_root_qualified",
    "m8_fork_commit_p99",
    "m8_restore_delta_p99",
]


def load_json(path):
    try:
        return json.loads(Path(path).read_text())
    except Exception as exc:
        raise ValueError(f"{path}: cannot read JSON: {exc}") from exc


def is_hex32(value):
    return isinstance(value, str) and HEX32_RE.match(value) is not None


def is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def rel_path(root, value, field, errors):
    if not isinstance(value, str) or not value:
        errors.append(f"{field}: must be a non-empty relative path")
        return None
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        errors.append(f"{field}: must stay inside evidence root")
        return None
    return root / path


def bar_index(evidence, errors):
    bars = evidence.get("bars")
    if not isinstance(bars, list):
        errors.append("bars: must be a list")
        return {}
    indexed = {}
    for i, bar in enumerate(bars):
        if not isinstance(bar, dict):
            errors.append(f"bars[{i}]: must be an object")
            continue
        name = bar.get("name", bar.get("id"))
        if not isinstance(name, str) or not name:
            errors.append(f"bars[{i}]: missing name")
            continue
        ok = bar.get("ok", bar.get("passed", bar.get("pass")))
        if not isinstance(ok, bool):
            errors.append(f"bars[{name}]: ok must be boolean")
            continue
        indexed[name] = ok
    return indexed


def validate_repos(evidence, errors):
    repos = evidence.get("repos")
    if not isinstance(repos, dict):
        errors.append("repos: must be an object")
        return
    for name in REQUIRED_REPOS:
        repo = repos.get(name)
        if not isinstance(repo, dict):
            errors.append(f"repos.{name}: missing object")
            continue
        if not isinstance(repo.get("rev"), str) or not repo["rev"]:
            errors.append(f"repos.{name}.rev: must be a non-empty string")
        if not isinstance(repo.get("dirty"), bool):
            errors.append(f"repos.{name}.dirty: must be boolean")


def validate_store_root(evidence, run_kind, errors):
    store_root = evidence.get("store_root")
    if not isinstance(store_root, dict):
        errors.append("store_root: must be an object")
        return False
    qualified = store_root.get("qualified")
    if not isinstance(store_root.get("path"), str) or not store_root["path"]:
        errors.append("store_root.path: must be a non-empty string")
    if not isinstance(store_root.get("disk_class"), str) or not store_root["disk_class"]:
        errors.append("store_root.disk_class: must be a non-empty string")
    if not isinstance(qualified, bool):
        errors.append("store_root.qualified: must be boolean")
    if run_kind == "full_acceptance" and qualified is not True:
        errors.append("store_root.qualified: full_acceptance requires qualified=true")
    return qualified is True


def validate_child_row(row, index):
    errors = []
    if not isinstance(row, dict):
        return [f"child[{index}]: must be an object"]
    if not isinstance(row.get("child_index"), int) or row["child_index"] < 0:
        errors.append(f"child[{index}].child_index: must be a non-negative integer")
    for field in [
        "seed_hex",
        "original_ref_hex",
        "input_log_id_hex",
        "state_hash_original_hex",
    ]:
        if not is_hex32(row.get(field)):
            errors.append(f"child[{index}].{field}: must be 32-byte lowercase hex")
    if row.get("replay_ref_hex") is not None and not is_hex32(row.get("replay_ref_hex")):
        errors.append(f"child[{index}].replay_ref_hex: must be 32-byte lowercase hex")
    if row.get("state_hash_replay_hex") is not None and not is_hex32(
        row.get("state_hash_replay_hex")
    ):
        errors.append(f"child[{index}].state_hash_replay_hex: must be 32-byte lowercase hex")
    if row.get("restore_mode") not in RESTORE_MODES:
        errors.append(f"child[{index}].restore_mode: invalid value")
    baseline = row.get("baseline_ref_hex")
    if baseline is not None and not is_hex32(baseline):
        errors.append(f"child[{index}].baseline_ref_hex: must be null or 32-byte lowercase hex")
    if row.get("manifest_kind") not in MANIFEST_KINDS:
        errors.append(f"child[{index}].manifest_kind: invalid value")
    if not isinstance(row.get("chain_depth"), int) or row["chain_depth"] < 0:
        errors.append(f"child[{index}].chain_depth: must be a non-negative integer")
    dirty = row.get("dirty_pages")
    if dirty is not None and (not isinstance(dirty, int) or dirty < 0):
        errors.append(f"child[{index}].dirty_pages: must be null or non-negative integer")
    ratio = row.get("shared_page_ratio")
    if not is_number(ratio) or ratio < 0 or ratio > 1:
        errors.append(f"child[{index}].shared_page_ratio: must be a number in [0,1]")
    timing = row.get("timing_ms")
    if not isinstance(timing, dict) or not timing:
        errors.append(f"child[{index}].timing_ms: must be a non-empty object")
    elif any(not is_number(v) or v < 0 for v in timing.values()):
        errors.append(f"child[{index}].timing_ms: values must be non-negative numbers")
    if row.get("result") not in RESULTS:
        errors.append(f"child[{index}].result: invalid value")
    row_source = row.get("row_source")
    if row_source is not None and row_source not in ROW_SOURCES:
        errors.append(f"child[{index}].row_source: invalid value")
    return errors


def load_child_rows(root, evidence, errors):
    table = evidence.get("child_table")
    path_value = table.get("jsonl") if isinstance(table, dict) else None
    path = rel_path(root, path_value, "child_table.jsonl", errors)
    if path is None:
        return []
    try:
        lines = path.read_text().splitlines()
    except FileNotFoundError:
        errors.append(f"child_table.jsonl: missing file {path_value}")
        return []
    rows = []
    for i, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError as exc:
            errors.append(f"{path_value}:{i}: invalid JSON: {exc}")
    return rows


def validate_positive_rows(rows, expected_child_count, errors):
    if len(rows) != expected_child_count:
        errors.append(
            f"child_table: expected {expected_child_count} rows, found {len(rows)}"
        )
    indices = []
    saw_baseline_delta = False
    for i, row in enumerate(rows):
        errors.extend(validate_child_row(row, i))
        if not isinstance(row, dict):
            continue
        indices.append(row.get("child_index"))
        saw_baseline_delta = saw_baseline_delta or row.get("restore_mode") == "baseline_delta"
        if row.get("result") != "pass":
            errors.append(f"child[{i}].result: positive run requires pass")
        if row.get("replay_ref_hex") != row.get("original_ref_hex"):
            errors.append(f"child[{i}]: replay_ref_hex must equal original_ref_hex")
        if row.get("state_hash_replay_hex") != row.get("state_hash_original_hex"):
            errors.append(f"child[{i}]: state_hash_replay_hex must equal state_hash_original_hex")
    if sorted(indices) != list(range(expected_child_count)):
        errors.append("child_table: child_index values must be unique and contiguous")
    if expected_child_count > 0 and not saw_baseline_delta:
        errors.append("child_table: at least one positive row must use baseline_delta")


def validate_semantic_negative_rows(rows, errors):
    saw_ref_mismatch = False
    for i, row in enumerate(rows):
        errors.extend(validate_child_row(row, i))
        if not isinstance(row, dict):
            continue
        if row.get("result") == "ref_mismatch" and row.get("replay_ref_hex") != row.get(
            "original_ref_hex"
        ):
            saw_ref_mismatch = True
    if not saw_ref_mismatch:
        errors.append("semantic_negative: must include a committed replay_ref mismatch row")


def validate_resume_metadata(evidence, rows, errors):
    resume = evidence.get("resume")
    if resume is None:
        return
    if not isinstance(resume, dict):
        errors.append("resume: must be an object")
        return
    if not isinstance(resume.get("enabled"), bool):
        errors.append("resume.enabled: must be boolean")
    counts = {}
    for field in ["resumed_child_count", "fresh_child_count"]:
        value = resume.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            errors.append(f"resume.{field}: must be a non-negative integer")
        else:
            counts[field] = value
    if (
        len(counts) == 2
        and counts["resumed_child_count"] + counts["fresh_child_count"] != len(rows)
    ):
        errors.append("resume: resumed_child_count + fresh_child_count must equal child row count")
    if resume.get("enabled") is False and counts.get("resumed_child_count", 0) != 0:
        errors.append("resume.resumed_child_count: must be 0 when resume.enabled=false")


def validate_evidence(root):
    root = Path(root)
    evidence_path = root / "evidence.json"
    errors = []
    try:
        evidence = load_json(evidence_path)
    except ValueError as exc:
        return [str(exc)]
    if not isinstance(evidence, dict):
        return ["evidence.json: must contain an object"]

    if evidence.get("schema_version") != 1:
        errors.append("schema_version: must be 1")
    if evidence.get("request") != REQUEST:
        errors.append(f"request: must be {REQUEST}")
    run_kind = evidence.get("run_kind")
    if run_kind not in RUN_KINDS:
        errors.append("run_kind: invalid value")
    expected = evidence.get("expected_child_count")
    if not isinstance(expected, int) or expected < 0:
        errors.append("expected_child_count: must be a non-negative integer")
        expected = 0
    if run_kind == "full_acceptance" and expected != 1000:
        errors.append("expected_child_count: full_acceptance requires 1000")
    for field in ["run_id", "started_at", "finished_at"]:
        if not isinstance(evidence.get(field), str) or not evidence[field]:
            errors.append(f"{field}: must be a non-empty string")
    validate_repos(evidence, errors)
    if not isinstance(evidence.get("host"), dict):
        errors.append("host: must be an object")
    if not isinstance(evidence.get("guest"), dict):
        errors.append("guest: must be an object")
    validate_store_root(evidence, run_kind, errors)
    if not isinstance(evidence.get("config"), dict):
        errors.append("config: must be an object")
    if not isinstance(evidence.get("commands"), list):
        errors.append("commands: must be a list")
    if not isinstance(evidence.get("artifacts"), list):
        errors.append("artifacts: must be a list")
    if not isinstance(evidence.get("deviations"), list):
        errors.append("deviations: must be a list")

    bars = bar_index(evidence, errors)
    for name in REQUIRED_BARS:
        if name not in bars:
            errors.append(f"bars.{name}: missing")
    if run_kind == "semantic_negative":
        if bars.get("m8_semantic_negative_red") is not True:
            errors.append("bars.m8_semantic_negative_red: semantic_negative requires ok=true")
    else:
        for name in REQUIRED_BARS:
            if name == "m8_store_root_qualified" and run_kind in {"fake", "bounded_ci"}:
                continue
            if bars.get(name) is not True:
                errors.append(f"bars.{name}: must pass for {run_kind}")

    rows = load_child_rows(root, evidence, errors)
    validate_resume_metadata(evidence, rows, errors)
    if run_kind == "semantic_negative":
        validate_semantic_negative_rows(rows, errors)
    else:
        validate_positive_rows(rows, expected, errors)

    semantic = evidence.get("semantic_negative")
    if run_kind != "semantic_negative":
        if not isinstance(semantic, dict):
            errors.append("semantic_negative: must be an object")
        elif semantic.get("actual_red_result") is not True:
            errors.append("semantic_negative.actual_red_result: must be true")
    return errors


def main(argv):
    if len(argv) != 2:
        print("usage: m8_joint_fork_integrity_evidence.py <evidence-root>", file=sys.stderr)
        return 2
    errors = validate_evidence(argv[1])
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    print("M8 evidence valid")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
