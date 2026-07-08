#!/usr/bin/env python3
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from phase5_readiness_evidence import resolve_disk_info


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


if __name__ == "__main__":
    unittest.main()
