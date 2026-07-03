# Plan Review Log

Two independent subagent reviews of the 45007a1 draft, 2026-07-03, per the
goal's review requirement. Reviewer A: adversarial correctness (races,
crash windows). Reviewer B: implementability + acceptance-criteria
coverage. Both verified findings against `main` @ 9b2e55a before
reporting. All plan anchors were re-verified accurate by both.

## Blockers (all fixed in the plan)

| # | Finding (condensed) | Disposition |
|---|---|---|
| A1/B1 | Late-roots protocol protected the registered manifest but not its **dependency closure**: (a) CreateNode of a pre-fence orphan after its pages' pack finalized → live node, pages gone; (b) delta commit on an orphan parent — presence check covers only the delta's own entries; (c) delta parent check (put step 2) was outside the widened lock. | 02 §1 rewritten: `register_live_ref` is a gated, **validating** registration (register in late_roots, then verify the full chain: manifests present + `contains_batch` all chain hashes; fail the op on miss). put_snapshot's widened lock now covers steps 2–6, and the idempotent early-return path registers too. |
| A2/B1 | Server-side registration not serialized with the gate: registrations during finalize's write hold were only read at the *next* drain; a CreateNode in flight across `begin_gc_epoch` could be both unregistered (no epoch yet) and invisible to the root snapshot. | 03 §7 rewritten: create_node and pin handlers hold `gc_commit_gate.read()` across register → validate → meta write. |
| A3 | Manifest sweep unlinked doomed refs **after releasing** the write lock; idempotent re-put (`spm exists → early Ok`) + CreateNode could reference a doomed manifest before its unlink. | 02 §6 rewritten: unlinks happen under the gate write lock in batches with re-drain; candidates computed outside the lock; early-return path registers. |

## Majors (all fixed)

| # | Finding | Disposition |
|---|---|---|
| A4 | Pin handler has NO manifest validation today (service.rs:870-900) — "fails NOT_FOUND either way" was factually wrong; dangling pins creatable, would flake harness invariant 2. | 03 §7: pin validates via `register_live_ref` (FAILED_PRECONDITION); behavior change disclosed in resolution. 05 §3 notes the dependency. |
| A5/B2 | `create_gc_pack`'s "same monotonic counter rotation uses" doesn't exist — rotation computes `old+1` inline (ingest.rs:310); collision/clobber guaranteed eventually. | 01 §2: explicit `next_pack_id` allocator in ActiveState; rotation and open() refactored to use it. |
| A6 | GcPackWriter sidecar written via `ShardedIndex::write_sidecar` would be CRC-valid but **empty** (index still points at the old pack at publish time) → post-crash silent loss of all compacted pages. | 01 §2: sidecar written from the writer's own record list, explicitly forbidden path documented; new fsck assertion (05 §3 item 5): sidecar entry count == pack record count. |
| A7 | R4 latch acquired at step 3 but reap/rotate run at steps 1–2 → racing triggers double-reap/rotate. | 02 §2/§3: cycle-scope try-lock at `run_gc_cycle` entry; `begin_gc_epoch` stays as store-level backstop. |
| A8/B4 | Migration runner cannot apply a second migration — `Some(_) => return Ok(())` (schema.rs:66); "follow the pattern" was a trap. | 01 §4: version-stepping loop spec'd; SUPPORTED_VERSION → 2; both first-open and upgrade paths tested. |
| B3 | `gc-reap-txn` failpoint impossible as written — snapstore-meta has no failpoints feature/dep/macro. | 01 §4: feature + dep + shim added; crash feature list + ci.yaml lines updated (06 §1). |

## Minors / nits (all folded in)

- Gate poisoning: GC panics under write lock would brick commits with
  std RwLock → switch gate to `parking_lot::RwLock` (02 §2). [A11]
- `gc-test-hooks` plumbing: feature forwarding (`gc-test-hooks =
  ["snapstore-store/gc-test-hooks"]`), `[[test]] required-features`,
  separate clippy step (04 intro, 06 §1). [B5]
- `physical_page_bytes` exact accounting owed by M7 (service.rs:957-959)
  → unique_pages × 4133, disclosed (03 §2). [B6]
- Joint-verification populate: decided as `snapstore-crash
  populate-gc-fixture` subcommand with spec'd outputs (06 §3). [B7]
- `.idx`-before-`.spk` unlink order (orphan-sidecar leak) + failpoint
  placement contradiction between 01/02 (01 §2, 02 §7). [A9]
- Zero-record pack: explicit delete branch, not `NaN >= t` luck; threshold
  1.01 compacts 100%-live packs — intended, commented (02 §5, 04 §3). [A10]
- Acked-PutPages-later-collected is a legal outcome the model must encode;
  deviation disclosed (04 §3). [A12]
- `gc_done` journal line carries reaped node ids — no grace-arithmetic
  re-derivation in replay (05 §1/§3). [A13]
- Straggler rounds skip empty pack creation (02 §5). [A14]
- Shutdown is a broadcast internally; subscribe, don't consume the oneshot
  (03 §5). [B9]
- Duplicated "no TOCTOU" comment also at service.rs:1015-1018 (01 §3). [B10]
- `TombstoneRow` is new; fields listed (01 §4). [B11]
- Sequencing: WI4's RPC smoke needs WI3 — follow 06 §6's linear order,
  not the 00 diagram (06 §6). [B8]
- Anchor drift: fsck violations at fsck.rs:4-13 (05 §3). [B10]

## What survived scrutiny (Reviewer A, verified not just plausible)

- Race A core closed by the widened read lock (dead index entries removed
  only under gate write; racing puts either drained or fail MissingPages).
- No deadlock between the gate and GroupCommit; consistent lock order
  gate → active → shard everywhere traced; no writer starvation with the
  futex RwLock; no read-reentrancy anywhere.
- `read_sealed_with_retry` sound for repoint-then-unlink; `Arc<File>`
  keeps unlinked inodes readable for in-flight reads.
- Crashed-GC-pack recovery adoption (01 §2) correct as stated, modulo A6.
- Reap-before-mark crash-safe; pins independent roots so pinned refs in
  pruned subtrees survive.
- All six failpoint windows leave recoverable states.
- Completeness config (1.01 + rotate-first + grace 0) yields exact
  physical == reachable on the quiescent path, modulo A9/A10/A12.
- Decisions D1–D4, D6–D7 sound; D5 needed the three blocker repairs.

## Acceptance-criteria coverage (Reviewer B)

All five Gate-Required items and all five Acceptance Criteria mapped to
plan sections and judged covered; the single thin spot (AC5 joint
verification populate mechanism) was resolved as the fixture subcommand.
