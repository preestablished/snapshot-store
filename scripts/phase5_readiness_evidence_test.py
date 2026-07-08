#!/usr/bin/env python3
import sys
import json
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from phase5_readiness_evidence import evaluate_fio_artifacts, resolve_disk_info


def mount(source, majmin, fstype="ext4"):
    return {
        "filesystems": [
            {
                "source": source,
                "maj:min": majmin,
                "fstype": fstype,
                "target": "/bench",
            }
        ]
    }


def multi_source_mount(source, sources, majmin, fstype="btrfs"):
    return {
        "filesystems": [
            {
                "source": source,
                "sources": sources,
                "maj:min": majmin,
                "fstype": fstype,
                "target": "/bench",
            }
        ]
    }


def lsblk(*devices):
    return {"blockdevices": list(devices)}


def disk(name, tran, children=None):
    return {
        "name": name,
        "kname": name,
        "path": f"/dev/{name}",
        "type": "disk",
        "tran": tran,
        "children": children or [],
    }


def part(name, majmin, children=None):
    return {
        "name": name,
        "kname": name,
        "path": f"/dev/{name}",
        "maj:min": majmin,
        "type": "part",
        "children": children or [],
    }


def lvm(name, majmin):
    return {
        "name": name,
        "kname": "dm-0",
        "path": f"/dev/mapper/{name}",
        "maj:min": majmin,
        "type": "lvm",
    }


class ResolveDiskInfoTest(unittest.TestCase):
    def test_sata_mount_with_unrelated_nvme_is_sata(self):
        info = resolve_disk_info(
            mount("/dev/sda1", "8:1"),
            lsblk(
                disk("sda", "sata", [part("sda1", "8:1")]),
                disk("nvme0n1", "nvme", [part("nvme0n1p1", "259:1")]),
            ),
        )
        self.assertEqual(info["disk_class"], "sata")
        self.assertEqual(info["backing_transports"], ["sata"])

    def test_nvme_partition_is_nvme(self):
        info = resolve_disk_info(
            mount("/dev/nvme0n1p1", "259:1"),
            lsblk(disk("nvme0n1", "nvme", [part("nvme0n1p1", "259:1")])),
        )
        self.assertEqual(info["disk_class"], "nvme")

    def test_bind_mount_suffix_matches_by_majmin(self):
        info = resolve_disk_info(
            mount("/dev/nvme0n1p1[/scratch]", "259:1"),
            lsblk(disk("nvme0n1", "nvme", [part("nvme0n1p1", "259:1")])),
        )
        self.assertEqual(info["mount_source"], "/dev/nvme0n1p1")
        self.assertEqual(info["disk_class"], "nvme")

    def test_lvm_on_nvme_is_nvme(self):
        info = resolve_disk_info(
            mount("/dev/mapper/vg-root", "253:0"),
            lsblk(
                disk(
                    "nvme0n1",
                    "nvme",
                    [part("nvme0n1p2", "259:2", [lvm("vg-root", "253:0")])],
                )
            ),
        )
        self.assertEqual(info["disk_class"], "nvme")

    def test_lvm_on_sata_is_sata(self):
        info = resolve_disk_info(
            mount("/dev/mapper/vg-root", "253:0"),
            lsblk(disk("sda", "sata", [part("sda2", "8:2", [lvm("vg-root", "253:0")])])),
        )
        self.assertEqual(info["disk_class"], "sata")

    def test_tmpfs_with_unrelated_nvme_is_tmpfs(self):
        info = resolve_disk_info(
            mount("tmpfs", "0:42", "tmpfs"),
            lsblk(disk("nvme0n1", "nvme", [part("nvme0n1p1", "259:1")])),
        )
        self.assertEqual(info["disk_class"], "tmpfs")
        self.assertEqual(info["backing_transports"], [])

    def test_dev_root_matches_by_majmin(self):
        info = resolve_disk_info(
            mount("/dev/root", "8:1"),
            lsblk(disk("sda", "sata", [part("sda1", "8:1")])),
        )
        self.assertEqual(info["disk_class"], "sata")

    def test_mixed_lvm_backing_is_not_nvme(self):
        info = resolve_disk_info(
            mount("/dev/mapper/vg-root", "253:0"),
            lsblk(
                disk(
                    "nvme0n1",
                    "nvme",
                    [part("nvme0n1p2", "259:2", [lvm("vg-root", "253:0")])],
                ),
                disk("sda", "sata", [part("sda2", "8:2", [lvm("vg-root", "253:0")])]),
            ),
        )
        self.assertEqual(info["disk_class"], "nvme,sata")

    def test_multiple_sources_are_all_resolved(self):
        info = resolve_disk_info(
            multi_source_mount(
                "/dev/nvme0n1p1",
                ["/dev/nvme0n1p1", "/dev/sda1"],
                "",
            ),
            lsblk(
                disk("nvme0n1", "nvme", [part("nvme0n1p1", "259:1")]),
                disk("sda", "sata", [part("sda1", "8:1")]),
            ),
        )
        self.assertEqual(info["disk_class"], "nvme,sata")


class FioQualificationTest(unittest.TestCase):
    def write_fio(self, root, name, *, error=0, read_bytes=0, write_bytes=0):
        hardware = root / "hardware"
        hardware.mkdir(parents=True, exist_ok=True)
        (hardware / f"{name}.json").write_text(
            json.dumps(
                {
                    "jobs": [
                        {
                            "error": error,
                            "read": {"io_bytes": read_bytes},
                            "write": {"io_bytes": write_bytes},
                        }
                    ]
                }
            )
        )
        (hardware / f"{name}.status").write_text("0\n")

    def write_valid_fio_set(self, root):
        self.write_fio(root, "fio-seqwrite", write_bytes=1024)
        self.write_fio(root, "fio-seqread", read_bytes=1024)
        self.write_fio(root, "fio-randrw", read_bytes=1024, write_bytes=1024)

    def assert_fio_failure(self, qualification, expected):
        self.assertTrue(
            any(expected in reason for reason in qualification["failure_reasons"]),
            qualification["failure_reasons"],
        )

    def test_all_required_fio_artifacts_qualify(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.write_valid_fio_set(root)

            qualification = evaluate_fio_artifacts(root)

            self.assertTrue(qualification["ok"])
            self.assertEqual(qualification["failure_reasons"], [])

    def test_missing_fio_artifact_does_not_qualify(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.write_fio(root, "fio-seqwrite", write_bytes=1024)
            self.write_fio(root, "fio-seqread", read_bytes=1024)

            qualification = evaluate_fio_artifacts(root)

            self.assertFalse(qualification["ok"])
            self.assert_fio_failure(qualification, "fio-randrw.json: missing fio JSON")

    def test_malformed_fio_json_does_not_qualify(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.write_valid_fio_set(root)
            (root / "hardware" / "fio-seqread.json").write_text("{")

            qualification = evaluate_fio_artifacts(root)

            self.assertFalse(qualification["ok"])
            self.assert_fio_failure(qualification, "fio-seqread.json: malformed fio JSON")

    def test_empty_fio_jobs_do_not_qualify(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.write_valid_fio_set(root)
            (root / "hardware" / "fio-seqread.json").write_text(json.dumps({"jobs": []}))

            qualification = evaluate_fio_artifacts(root)

            self.assertFalse(qualification["ok"])
            self.assert_fio_failure(
                qualification, "fio-seqread.json: fio JSON contains no jobs"
            )

    def test_nonzero_fio_job_error_does_not_qualify(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.write_valid_fio_set(root)
            self.write_fio(root, "fio-randrw", error=5, read_bytes=1024, write_bytes=1024)

            qualification = evaluate_fio_artifacts(root)

            self.assertFalse(qualification["ok"])
            self.assert_fio_failure(
                qualification, "fio-randrw.json: fio job error is nonzero"
            )

    def test_nonzero_fio_command_status_does_not_qualify(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.write_valid_fio_set(root)
            (root / "hardware" / "fio-seqwrite.status").write_text("7\n")

            qualification = evaluate_fio_artifacts(root)

            self.assertFalse(qualification["ok"])
            self.assert_fio_failure(qualification, "fio-seqwrite.json: fio exited 7")

    def test_skipped_or_unavailable_fio_does_not_qualify(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.write_valid_fio_set(root)
            (root / "hardware" / "fio-skipped.txt").write_text("RUN_FIO=0\n")

            qualification = evaluate_fio_artifacts(root)

            self.assertFalse(qualification["ok"])
            self.assertIn("fio skipped", qualification["failure_reasons"])


if __name__ == "__main__":
    unittest.main()
