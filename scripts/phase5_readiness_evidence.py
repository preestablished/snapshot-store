#!/usr/bin/env python3
import hashlib
import json
import os
import platform
import subprocess
import sys
from pathlib import Path


PSEUDO_FSTYPES = {
    "autofs",
    "cgroup",
    "cgroup2",
    "devpts",
    "devtmpfs",
    "fusectl",
    "nfs",
    "nfs4",
    "overlay",
    "proc",
    "ramfs",
    "securityfs",
    "sysfs",
    "tmpfs",
}


def read(path, default=""):
    try:
        return Path(path).read_text(errors="replace").strip()
    except FileNotFoundError:
        return default


def sh(*args):
    return subprocess.run(args, capture_output=True, text=True).stdout.strip()


def load_json(path):
    try:
        return json.loads(Path(path).read_text())
    except Exception:
        return None


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def artifact_list(root):
    out = []
    for path in sorted(root.rglob("*")):
        if path.is_file() and path.name != "evidence.json":
            out.append({"path": str(path.relative_to(root)), "sha256": sha256(path)})
    return out


def bar(id, target, measured, unit, ok, evidence_path, attribution=""):
    if measured is None:
        status = "not_run"
    else:
        status = "pass" if ok else "fail"
    return {
        "id": id,
        "target": target,
        "measured": measured,
        "unit": unit,
        "status": status,
        "attribution": attribution,
        "evidence_path": evidence_path,
    }


def clean(value):
    if value is None:
        return ""
    return str(value).strip()


def first_mount(mount_json):
    filesystems = (mount_json or {}).get("filesystems") or []
    if filesystems:
        return filesystems[0]
    return {}


def normalize_mount_source(source):
    source = clean(source)
    if "[" in source:
        source = source.split("[", 1)[0]
    return source


def mount_sources(mount):
    sources = []
    raw_sources = mount.get("sources")
    if isinstance(raw_sources, list):
        sources.extend(raw_sources)
    elif raw_sources:
        sources.extend(str(raw_sources).replace(",", "\n").splitlines())
    sources.insert(0, mount.get("source"))

    normalized = []
    for source in sources:
        source = normalize_mount_source(source)
        if source and source not in normalized:
            normalized.append(source)
    return normalized


def source_candidates(sources):
    candidates = set(sources)
    for source in sources:
        if source.startswith("/dev/"):
            candidates.add(os.path.realpath(source))
    for value in list(candidates):
        if value.startswith("/dev/"):
            candidates.add(value.removeprefix("/dev/"))
        name = Path(value).name
        if name:
            candidates.add(name)
            candidates.add(f"/dev/{name}")
    return {value for value in candidates if value}


def device_label(dev):
    for key in ("path", "kname", "name"):
        value = clean(dev.get(key))
        if value:
            return value if value.startswith("/dev/") else f"/dev/{value}"
    return "unknown"


def block_paths(lsblk_json):
    def walk(devices, parents):
        for dev in devices or []:
            path = parents + [dev]
            yield path
            yield from walk(dev.get("children"), path)

    yield from walk((lsblk_json or {}).get("blockdevices"), [])


def device_matches(dev, majmin, candidates):
    if majmin and clean(dev.get("maj:min")) == majmin:
        return True
    for key in ("path", "kname", "name"):
        value = clean(dev.get(key))
        if value and (value in candidates or f"/dev/{value}" in candidates):
            return True
    return False


def descendant_transport_pairs(dev):
    pairs = []
    tran = clean(dev.get("tran"))
    if tran:
        pairs.append((tran, device_label(dev)))
    for child in dev.get("children") or []:
        pairs.extend(descendant_transport_pairs(child))
    return pairs


def transport_pairs_for_path(path):
    for dev in reversed(path):
        tran = clean(dev.get("tran"))
        if tran:
            return [(tran, device_label(dev))]
    return descendant_transport_pairs(path[-1]) if path else []


def resolve_disk_info(mount_json, lsblk_json):
    mount = first_mount(mount_json)
    sources = mount_sources(mount)
    source = sources[0] if sources else ""
    fstype = clean(mount.get("fstype"))
    majmin = clean(mount.get("maj:min"))
    candidates = source_candidates(sources)

    matches = [
        path
        for path in block_paths(lsblk_json)
        if device_matches(path[-1], majmin, candidates)
    ]

    transport_pairs = []
    matched_devices = []
    for path in matches:
        matched_devices.append(device_label(path[-1]))
        transport_pairs.extend(transport_pairs_for_path(path))

    transports = sorted({tran for tran, _ in transport_pairs if tran})
    backing_devices = sorted({dev for _, dev in transport_pairs if dev})

    if transports:
        disk_class = transports[0] if len(transports) == 1 else ",".join(transports)
    elif fstype in PSEUDO_FSTYPES or (source and not source.startswith("/dev/")):
        disk_class = fstype or source
    else:
        disk_class = "unknown"

    return {
        "disk_class": disk_class or "unknown",
        "mount_source": source,
        "mount_sources": sources,
        "mount_majmin": majmin,
        "mount_fstype": fstype,
        "matched_devices": sorted(set(matched_devices)),
        "backing_devices": backing_devices,
        "backing_transports": transports,
    }


def load_mount_json(root, bench_root):
    mount_json = load_json(root / "hardware" / "mount.json")
    if mount_json:
        return mount_json
    out = sh(
        "findmnt",
        "-J",
        "-T",
        str(bench_root),
        "-o",
        "SOURCE,SOURCES,FSTYPE,MAJ:MIN,TARGET,FSROOT",
    )
    try:
        return json.loads(out)
    except Exception:
        return {}


def assemble_evidence(root, bench_root):
    m5 = load_json(root / "m5-transport" / "results.json")
    m7 = load_json(root / "m7-gc-benchmark" / "results.json")
    flake_summary = read(root / "flake" / "postfix-50x-summary.txt")
    attestation = read(root / "hardware" / "phase5-host-attestation.txt")
    att = {}
    for line in attestation.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            att[k] = v

    stat = os.statvfs(bench_root)
    free_bytes = stat.f_bavail * stat.f_frsize
    mount_json = load_mount_json(root, bench_root)
    lsblk = load_json(root / "hardware" / "lsblk.json") or {}
    disk_info = resolve_disk_info(mount_json, lsblk)
    disk_class = disk_info["disk_class"]
    qualified = (
        disk_class == "nvme"
        and free_bytes >= 70 * 1024**3
        and att.get("actual_soak_host") == "true"
    )
    qualification_reason = (
        "qualified"
        if qualified
        else "requires benchmark mount backed only by NVMe, >=70 GiB free, and actual_soak_host=true attestation"
    )

    bars = []
    bars.append(
        bar(
            "page_channel_fallback_50x",
            "50 green runs",
            50 if "failures=0" in flake_summary else None,
            "runs",
            "failures=0" in flake_summary,
            "flake/postfix-50x-summary.txt",
        )
    )
    if m5:
        bars.extend(
            [
                bar(
                    "put_batch_warm_sustained",
                    ">= 1.5",
                    m5.get("put_batch_warm_sustained_gbps"),
                    "GB/s",
                    m5.get("put_batch_warm_sustained_gbps", 0) >= 1.5,
                    "m5-transport/results.json",
                ),
                bar(
                    "get_batch_warm_sustained",
                    ">= 2.5",
                    m5.get("get_batch_warm_sustained_gbps"),
                    "GB/s",
                    m5.get("get_batch_warm_sustained_gbps", 0) >= 2.5,
                    "m5-transport/results.json",
                ),
                bar(
                    "commit_16x8mib_p99",
                    "< 40",
                    m5.get("commit_16x8mib_p99_ms"),
                    "ms",
                    m5.get("commit_16x8mib_p99_ms", 1e9) < 40,
                    "m5-transport/results.json",
                ),
                bar(
                    "commit_16x8mib_aggregate",
                    ">= 1.2",
                    m5.get("commit_16x8mib_aggregate_gbps"),
                    "GB/s",
                    m5.get("commit_16x8mib_aggregate_gbps", 0) >= 1.2,
                    "m5-transport/results.json",
                ),
                bar(
                    "create_node_inline_log_p50",
                    "< 1.5",
                    m5.get("create_node_inline_log_p50_ms"),
                    "ms",
                    m5.get("create_node_inline_log_p50_ms", 1e9) < 1.5,
                    "m5-transport/results.json",
                ),
                bar(
                    "update_nodes_256_p50",
                    "< 3",
                    m5.get("update_nodes_256_p50_ms"),
                    "ms",
                    m5.get("update_nodes_256_p50_ms", 1e9) < 3,
                    "m5-transport/results.json",
                ),
            ]
        )
    else:
        for name in [
            "put_batch_warm_sustained",
            "get_batch_warm_sustained",
            "commit_16x8mib_p99",
            "commit_16x8mib_aggregate",
            "create_node_inline_log_p50",
            "update_nodes_256_p50",
        ]:
            bars.append(bar(name, "", None, "", False, "m5-transport/not-run.txt"))
    if m7:
        reclaim = m7.get("reclaiming_gc_run", {})
        idle = m7.get("idle_commit", {})
        bars.extend(
            [
                bar(
                    "m7_gc_reclaiming_duration",
                    "< 60000",
                    reclaim.get("duration_ms"),
                    "ms",
                    reclaim.get("duration_ms", 1e18) < 60000,
                    "m7-gc-benchmark/results.json",
                ),
                bar(
                    "m7_gc_nodes_reaped",
                    "> 0",
                    reclaim.get("nodes_reaped"),
                    "nodes",
                    reclaim.get("nodes_reaped", 0) > 0,
                    "m7-gc-benchmark/results.json",
                ),
                bar(
                    "m7_gc_ingest_during_gc",
                    ">= target",
                    reclaim.get("ingest_mbps"),
                    "MB/s",
                    reclaim.get("ingest_mbps", 0)
                    >= m7.get("config", {}).get("ingest_target_mbps", 200),
                    "m7-gc-benchmark/results.json",
                ),
                bar(
                    "m7_gc_commit_p99_vs_idle",
                    "< 2x idle",
                    reclaim.get("commit_p99_ms"),
                    "ms",
                    idle.get("p99_ms", 0) > 0
                    and reclaim.get("commit_p99_ms", 1e18)
                    < 2 * idle.get("p99_ms", 0),
                    "m7-gc-benchmark/results.json",
                ),
            ]
        )
    else:
        for name in [
            "m7_gc_reclaiming_duration",
            "m7_gc_nodes_reaped",
            "m7_gc_ingest_during_gc",
            "m7_gc_commit_p99_vs_idle",
        ]:
            bars.append(bar(name, "", None, "", False, "m7-gc-benchmark/not-run.txt"))

    evidence = {
        "run_id": root.name,
        "request": ".agents/requests/phase5-readiness-gc-benchmark-and-transport-revalidation",
        "started_at": os.environ.get("PHASE5_STARTED_AT", ""),
        "finished_at": sh("date", "-u", "+%Y-%m-%dT%H:%M:%SZ"),
        "git": {
            "rev": sh("git", "rev-parse", "HEAD"),
            "status_clean": sh("git", "status", "--porcelain") == "",
            "status_short": sh("git", "status", "--short", "--branch"),
        },
        "host": {
            "hostname": read(root / "hardware" / "hostname.txt") or platform.node(),
            "phase5_soak_host": att.get("phase5_soak_host", "UNSET"),
            "same_as_i5_sata_reference": att.get("same_as_i5_sata_reference", "UNSET"),
            "actual_soak_host": att.get("actual_soak_host", "UNSET"),
            "operator_attestation": "hardware/phase5-host-attestation.txt",
            "kernel": read(root / "hardware" / "kernel.txt"),
            "rustc": read(root / "hardware" / "rustc.txt"),
        },
        "bench_root": {
            "path": str(bench_root),
            "mount": "hardware/mount.txt",
            "mount_json": "hardware/mount.json",
            "mount_source": disk_info["mount_source"],
            "mount_majmin": disk_info["mount_majmin"],
            "mount_fstype": disk_info["mount_fstype"],
            "free_bytes": free_bytes,
        },
        "hardware_qualification": {
            "qualified": qualified,
            "reason": qualification_reason,
            "disk_class": disk_class,
            "disk_resolution": disk_info,
            "cpu_governor": read(root / "hardware" / "cpu-governor.txt"),
            "thermal_or_throttle_notes": read(root / "hardware" / "thermal.txt"),
        },
        "commands": [
            {
                "id": "flake_50x",
                "argv": "cargo test -p snapstore-client --test page_channel_fallback -- --test-threads=1",
                "env": {},
                "log": "flake/postfix-50x.log",
            },
            {
                "id": "m5_transport",
                "argv": "cargo test -p snapstore-server --test page_channel_perf --release -- --ignored --nocapture",
                "env": {"SNAPSTORE_BENCH_ROOT": str(bench_root)},
                "log": "m5-transport/page_channel_perf.log",
            },
            {
                "id": "m7_gc",
                "argv": "cargo test -p snapstore-server --test gc_readiness_bench --release -- --ignored --nocapture",
                "env": {"SNAPSTORE_BENCH_ROOT": str(bench_root)},
                "log": "m7-gc-benchmark/gc_readiness_bench.log",
            },
        ],
        "artifacts": artifact_list(root),
        "bar_results": bars,
        "flake": {
            "summary": flake_summary,
            "root_cause": read(root / "flake" / "root-cause.txt"),
        },
        "m5_transport": m5 or {},
        "m7_gc": m7 or {},
        "risk_statement": "",
    }
    (root / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print(f"wrote {root / 'evidence.json'}")


def main():
    root = Path(sys.argv[1])
    bench_root = Path(sys.argv[2])
    assemble_evidence(root, bench_root)


if __name__ == "__main__":
    main()
