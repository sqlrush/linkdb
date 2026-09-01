# Stage 8 CTRC terminal-reference release implementation plan

> **Private design handoff. Never copy this plan or its reasoning into the
> public repository.** Public commits may contain only product code, tests and
> user-facing behavior/diagnostics.

## 0. Authorization and fixed boundaries

- User result: `APPROVE_CTRC`, 2026-09-01.
- Architecture authority: AD-024 §§21–31.
- Executable contract: Spec 8.4D and
  `specs/matrices/spec-8.4d-authority-matrix-v2.json`.
- Writer is the sole public-code/test modifier.
- Reader/private-design author does not modify product/test bytes.
- Preserve the current safe WIP that retains peer-mode ABORTED. Remove that
  permanent hold only in Task 9, after Tasks 1–8 are GREEN.
- No new transaction-status authority, daemon, correctness GUC, terminal
  archive, commit-time all-node barrier, full-heap scan, TTL/LRU eviction or
  Stage 9 recovery positive.
- Existing canonical ACTIVE, exact COMMIT/ABORT, current/rolled locator,
  prepared undo receipt, resolver, compositor and native restart code is kept
  unless a focused RED proves a contract violation.

Observed public WIP baseline (read-only design snapshot):

```text
tree: /Users/sqlrush/pgrac/.writer-public
branch: codex/stage8-r4-ipmi
HEAD: ff95f806d8ff3e7ba308f91ca80f147e0e27a110
tracked diff sha256:
  5158eff7b5e025e5b69655e9814740df93a3469952698236fad50bf8606d0958
```

The eight tracked WIP files implement safe permanent retention. Do not revert
them. Adjust them incrementally after positive CTRC release exists.

## 1. Gate order

```mermaid
flowchart LR
    M[Private matrix/checker GREEN] --> R[Focused REDs T20..T35]
    R --> G[Grant + receipt GREEN]
    G --> S[Seal + cleanout GREEN]
    S --> C[ACK + 0xA0 certificate GREEN]
    C --> GC[L11/L12 current + rolled GC GREEN]
    GC --> X[Source census GREEN]
    X --> E[t/405 deterministic four-node GREEN]
    E --> T400[unchanged t/400 236/236]
    T400 --> R11[unchanged R11 7/7]
    R11 --> PRE[Fresh runtime PRE]
```

Never skip from a unit GREEN to PRE. PRE is integration/performance evidence,
not a state-discovery loop.

## 2. Task 0 — preflight and immutable inventory

Files read, no product edits:

- private Spec 8.4D, AD-024 and JSON matrix;
- current public `git status`, HEAD and tracked diff digest;
- all paths/symbols in `MXA-K01..K21` and `MXA-T00..T35`.

Commands/evidence:

```sh
python3 tools/check-spec-8.4d-authority-matrix.py
python3 tools/check-spec-8.4d-authority-matrix.py \
  --public-tree /Users/sqlrush/pgrac/.writer-public
```

The second command intentionally skips `REQUIRED_NEW` anchors and validates
every existing/extension anchor. Record the exact missing-test/contract list.
Do not turn a missing required-new symbol into a design deviation.

Exit:

- matrix counts/digest GREEN;
- current WIP preserved;
- exact public baseline and missing list recorded.

## 3. Task 1 — focused RED suite and source manifest skeleton

Create first:

- `src/test/cluster_unit/test_cluster_terminal_ref_census.c`;
- its Makefile registration;
- a generated/static source-census manifest owned by the focused test (choose
  a public product-test path, not a private spec path).

Add REDs with the exact JSON symbols:

- `MXA-T20..T32`, `T34`, `T35` in the new test;
- `T33` in `test_cluster_undo_record.c`;
- extend existing `T14/T15` only at their existing anchors.

RED discipline:

1. Each test must fail because the named production symbol/transition is
   missing or wrong, not because a placeholder returns false.
2. Concurrency cuts use deterministic barriers/hooks; sleeps are not verdict
   evidence.
3. The source manifest initially reports all unclassified hits and is RED.
4. Keep the existing permanent-retention tests GREEN until Task 9; add future
   positive expectations separately rather than weakening the safe WIP.

Exit: every new invariant has a deterministic RED and the pre-existing focused
suite still builds/runs.

## 4. Task 2 — exact types, bounded shared memory and activation gate

Create:

- `src/include/cluster/cluster_terminal_ref_census.h`;
- `src/backend/cluster/cluster_terminal_ref_census.c`;
- Makefile/shared-memory registration entries.

Implement only types/storage, no positive publication yet:

- exact 96-byte field-encoded `ClusterCtrcTxnKeyV1`, canonical publication/
  target TLVs, 128-byte seal request, 64-byte reply header and 416-byte ACK;
- closed reference/target/result/receipt/seal enums;
- touched-key registry;
- fixed receipt slots and frozen ACK summaries;
- monotonic nonzero grant/seal/global-journal/per-key/slot generations with
  no-wrap handling; per-key sequence must be exact `1..N` despite inter-key
  global-journal gaps;
- checked sizing formulas from Spec 8.4D §12.12, using the compile-time
  `CLUSTER_UNDO_SEGS_PER_INSTANCE` encoding ceiling rather than the SIGHUP GUC;
- shmem init/reinit checks with all reserved bytes zero;
- capability `PGRAC_IC_HELLO_CAP_MULTIXACT_CTRC_V1 = 0x00400000`;
- `PGRAC_IC_HELLO_CAP_DEFINED_COUNT = 21`, masks/sums/static asserts;
- target R4 activation requires current-MX plus CTRC from one capability
  record and all admitted node ids in the exact 16-entry dependency-vector
  domain; partial/mixed/wider formation refuses.

Do not add a GUC. Allocation failure blocks target OPEN; it does not downsize.

Tests GREEN now: layout/generation/capability/capacity portions of `T21/T28/T34`.

## 5. Task 3 — origin grant/touched registry before ACTIVE proof

Modify the existing current-MX proof path, primarily:

- `cluster_multixact_current.h`;
- `cluster_multixact_current_wire.h/.c`;
- `cluster_gcs_block.c` origin proof state;
- `cluster_sf_dep.h/.c` only for the current capability-record generation
  accessor/bit family; do not use its BufferTag dependency store.

Implement:

1. current-MX wire version 2;
2. little-endian `ctrc_grant_generation` in
   `ClusterCurrentMemberProof.reserved8[0..3]` without changing 48-byte size;
3. reserve the origin-key entry before canonical ACTIVE publication and open it
   with nonzero grant plus a positively empty touched set before publication
   success;
4. ACTIVE/SELF positive proof path exact-samples canonical ACTIVE;
5. before reply enqueue or local ordinary-ITL receipt prepare, atomically find
   the exact open key and record requester
   node/boot/capability generation in touched registry;
6. only after successful record, emit/use nonzero grant;
7. terminal/UNKNOWN emits zero grant;
8. duplicate exact request is idempotent; conflicting duplicate or same-node
   boot/capability/formation/admission change blocks;
9. closed/drift/capacity/overflow returns existing UNKNOWN/DENIED polarity.

Do not change the status decision table or make registry state a verdict.

Exit: `T20` and the grant portions of `T21/T28` GREEN; existing current-MX
authority/composition tests remain GREEN.

## 6. Task 4 — participant receipt prepare/APPLY/cancel and all heap producers

Implement APIs exactly:

```c
ClusterCtrcPrepareResult cluster_ctrc_receipt_prepare(..., ClusterCtrcReceipt *);
ClusterCtrcApplyResult cluster_ctrc_receipt_apply_prepared(...);
void cluster_ctrc_receipt_cancel_prepared(...);
```

`prepare` owns shared allocation/locking and runs outside heap content.
`apply_prepared` performs only a nonmutating page-successor plan, field writes
to the owned slot and one release-CAS after exact target/grant recheck. `cancel`
is legal only before any descriptor/page/WAL mutation.

Wire ordinary ITL/UBA first. Every shared ITL allocation or reuse must:

1. prepare a `CTRC_REF_HEAP_ITL_UBA` pending-ITL receipt outside heap content;
2. under heap content, choose predecessor/successor bytes without modifying the
   page;
3. finalize the exact ITL index/xid/wrap/UBA and APPLY the receipt;
4. only then apply the planned ITL/undo/tuple mutation.

Repeated same-transaction writes to one page may reuse an existing APPLIED
receipt only after the under-lock pure plan matches the exact ITL target. A
likely-map miss/mismatch releases and retries before mutation; the local map is
never census authority.

Wire the four existing heap caller families without changing native retry
boundaries:

- INSERT/new tuple: pending offnum receipt, finalize exact TID under lock;
- DELETE: release and return to existing `l1` tuple/page selection;
- UPDATE: release both content locks and use existing complete reacquire path;
- `heap_lock_tuple`: return to existing `l3` and revalidate tuple/page.

Also wire every `cluster_current_mx_make_stamp` publication/recomposition path.
Every retained ACTIVE member gets a receipt; terminal members do not. Pending
offnum/ITL targets must be exact before APPLIED.

Source-order gate: receipt APPLIED must dominate descriptor/MultiXact creation
that becomes reachable, heap-header mutation and heap WAL insert. A RETRY cannot
occur afterward.

Exit: `T21/T22` and the registration half of `T23` GREEN; existing
prepared-undo lock-order tests remain GREEN.

## 7. Task 5 — ordinary ITL/UBA exact discharge

Treat existing AD-024 xact terminal hooks as eager cleaners of already-APPLIED
ordinary receipts, never as the registration/census owner.

Inspect/classify:

- xact precommit/abort ITL finish calls;
- `cluster_itl_cleanout_lazy`;
- exact TT terminal transition order;
- page-local commit SCN/abort representation consumers.

Required polarity:

- exact ABORTED/terminal-lock-only or stable COMMITTED page projection plus its
  WAL/dependency frontier may move APPLIED to CLEANED;
- data COMMIT `NEEDS_CLEANOUT` remains APPLIED until exact lazy cleanout;
- invalid/missing touch proof, a record never appended to the legacy touch
  list, typed stamp skip, backend loss or timeout remains APPLIED;
- the later participant cleaner uses the receipt's exact ITL target under a
  new Resource-X/page authority round.

Exit: `T23` GREEN with a real controlled transition, not string order alone.

## 8. Task 6 — CLOSE wire, PREPARED drain and participant state

Allocate exact appended ABI values:

- forward kind `11` after fresh-ref kind `10`;
- reply status `30` after current-MX stats `29`;
- two suboperations: `CLOSE_AND_CLEAN`, `CERTIFICATE_COMMITTED`.

Implement byte-array encoders/decoders for the exact offset tables: 128-byte
request; BLCKSZ reply with 64-byte header; 416-byte ACK only for
`LOCAL_RELEASE_ACK`; all-zero bytes from offset 480 to BLCKSZ. Validate version,
length, source/destination, full selector, connection/capability generation,
suboperation, result/ACK-length pairing, flags, reserves, selector digest, both
CRCs and all-zero tail.

CLOSE steps:

1. exact idempotency lookup;
2. atomically close key/grant so later prepare/APPLY refuses; if no local key
   exists, install an exact zero-range closed tombstone until certificate
   notification, so close-before-proof cannot reopen;
3. freeze exact per-key sequence `1..N` only after PREPARED reaches zero;
   global journal min/max may have inter-key gaps; the sole `N=0` form is the
   persistent close-before-proof tombstone;
4. never cancel PREPARED from timeout/backend death;
5. duplicate CLOSE returns the byte-identical frozen ACK only after cleanup is
   complete.

Use existing asynchronous GCS/LMON/LMS continuation facilities; no new process
and no blocking heap call chain.

Exit: `T24/T28` wire/race/loss rows GREEN.

## 9. Task 7 — exact target cleanout and successor transfer

Implement `cluster_ctrc_clean_reference` as a typed continuation, not one
lock-held monolith:

```text
capture target
→ unlock
→ resolve descriptor/members and prepare successor receipts/descriptors
→ acquire Resource-X/current page
→ heap content lock
→ exact revalidate
→ mutate + WAL insert
→ unlock
→ return dependency receipt
```

Implement every Spec 8.4D §12.8 cell:

- exact absent;
- terminal lock-only;
- aborted updater;
- committed updater with ACTIVE/UNKNOWN companion (retain);
- sole committed updater with existing page-local terminal projection;
- zero/one/many ACTIVE survivors;
- wrong descriptor/HOT/page authority/identity (retain).

For every move/rewrite, successor receipts and durable descriptor precede
predecessor removal. The old descriptor remains immutable.

An ACTIVE survivor gets a successor receipt. A terminal/UNKNOWN proof carries
zero grant and can never authorize a new dependent target: project/remove it
terminal-independently in the same WAL-protected mutation, leave the old target
untouched, or refuse before mutation.

Extend the existing KO/SMGR drop/truncate chokepoints without adding wire:
before local/peer KO `DONE`, bounded-scan matching CTRC receipts and require
them cancelled/cleaned with durable dependencies. PREPARED, ACTIVE or ambiguous
entries refuse before buffers/storage are removed. A later `ENOENT` is not a
substitute for this source-censused gate.

If the existing page-local representation cannot preserve the sole committed
updater consequence without continued TT dependency, stop and report the
design defect; do not invent a second terminal projection.

Exit: `T25/T26/T27` GREEN, including deterministic concurrent schedules.

## 10. Task 8 — immutable ACK, WAL dependencies and release certificate

Participant ACK:

- exact frozen 416-byte field layout, key/grant/seal/node/boot/formation/
  admission/capability and CRC32C;
- exact per-key `1..N`, exhaustive state counts, and legal inter-key gaps in
  global journal min/max;
- sole persistent `N=0` tombstone bytes and SHA-256 empty digest;
- exact Spec 8.4D row encoding/disposition values and nonempty digest formula,
  canonical rows sorted by key sequence (32 bytes); only the persistent `N=0`
  tombstone uses `SHA256(empty)`;
- highest local cleanout WAL LSN;
- exactly 16 per-origin `ClusterSfDepVec`-semantic required-LSN entries;
- typed result/first failed predicate/CRC/zero reserves.

Wait all dependency frontiers with no page or CTRC lock. Double-sample the
range/count/digest before freezing ACK.

Origin certificate:

1. require exactly one ACK per frozen touched node;
2. reject missing/extra/duplicate/conflicting/stale ACK or illegal empty/
   nonempty count/range polarity;
3. sort by node id and apply the exact count/length-delimited ACK-set formula
   over full 416-byte ACKs including CRC; copy the first 16 hash-output bytes
   without integer conversion;
4. release CTRC locks;
5. acquire exact block0 X-current and revalidate terminal identity/brackets;
6. emit/flush fixed 96-byte `0xA0`;
7. set/write-through `TT_SLOT_FLAG_CTRC_RELEASE_PROVEN = 0x01`;
8. verify disk/resident equality;
9. send `CERTIFICATE_COMMITTED` only for participant summary reclamation.

Implement WAL identify/desc/redo/side decoder/RF route inventory/static layout
asserts in every existing RM_CLUSTER_UNDO consumer. Redo applies only exact
terminal predecessor, is idempotent, skips proven newer identity and PANICs on
same-generation conflict/malformed bytes.

Exit: `T29/T30/T31/T32/T33` certificate/redo portions GREEN.

## 11. Task 9 — L11/L12 and current/rolled recycle integration

Only now replace permanent retention.

Current allocator GC becomes two-phase:

1. snapshot exact shmem candidate under allocator lock;
2. unlock;
3. sample canonical terminal/release and horizon outside lock;
4. relock and exact-revalidate entry;
5. mark free.

Rolled slot/whole-segment recycle checks every terminal slot:

- COMMITTED: exact release bit + valid SCN + same-epoch horizon;
- ABORTED: exact release bit + durable ABORTED;
- any missing/unknown bit/flag/status retains;
- every terminal/recyclable/fresh transition clears/rechecks the bit;
- whole-segment old/resident byte equality and fresh template remain mandatory.

Remove the WIP's unconditional peer-mode ABORTED skip/segment hold only in the
same change that makes positive CTRC release tests GREEN. Keep negative retain
tests.

Exit: complete `T31/T33/T34` GREEN; finite slots recycle on the positive path
without early reuse.

## 12. Task 10 — complete source census

Generate the `T35` manifest from actual symbols/call sites. Classify every:

- current-MX proof sender and decoder;
- every ordinary heap ITL allocation/reuse and UBA publication, including the
  branch where legacy touch proof capture returns invalid and appends nothing;
- heap/MultiXact publisher/recomposer;
- HOT/prune/freeze/move/rewrite remover/transfer;
- KO local/peer DONE, `cluster_ko_flush_and_wait_ack`, relation drop/truncate,
  `smgrdounlinkall` and `smgrtruncate2` physical-removal/reuse chokepoints;
- ITL/UBA terminal projection/discharge and slot reuse;
- TT status/flags writer;
- current/rolled GC/recycle/reuse writer.

Allowed classes are exactly the five JSON values. Record exact owner and test.
Zero unclassified hits is the only GREEN. Do not whitelist by directory or
comment text.

Exit: `T35` GREEN and any source-count assertions updated from the complete
manifest, not ad hoc grep totals.

## 13. Task 11 — deterministic four-node causal witness

Extend existing `t/405`; do not create a broad parallel campaign. Add barriers
for:

1. origin touched record before ACTIVE proof consumption;
2. ordinary ITL/UBA receipt and requester member receipt APPLIED before their
   page/tuple publications;
3. close racing a delayed old grant and forcing pre-mutation retry;
4. terminal lock-only/aborted updater cleanup;
5. committed updater retain then sole-updater page projection;
6. successor-before-predecessor recomposition;
7. participant WAL/vector durability before ACK;
8. complete ACK set before `0xA0` and release bit;
9. wrong boot/grant leg remains retained;
10. normal L11 and L12 recycle/reuse eventually progress.

Require all four nodes participate, zero client errors, zero stuck PREPARED,
zero wait edge/Resource-X debt and exact causal log fields.

Exit: `T01..T35` and extended `T15` GREEN from the same eligible bytes.

## 14. Task 12 — immutable gates and PRE

Run exactly:

```text
unchanged t/400 = 236/236
→ R11 t/430 = 7/7 and source removal
→ clean build/install four fresh nodes from the same eligible commit/diff
→ PRE correctness sample
→ only after correctness, adaptive saturation/performance scoring
```

Do not edit workload, judge, timeout/error polarity, item counts, sample
validity or resource lower bounds. Use the frozen resource floor:

```text
shared_buffers = 1GB
pcm_grd_max_entries = 131072
```

An INVALID sample is preserved and diagnosed from the earliest common causal
event. A new state absent from the frozen matrix stops product edits; a C
behavior mapped to an existing row is an implementation bug.

## 15. Stop/escalation conditions

Stop product modification and return to private design only if:

- a reference producer/remover fits none of the five source classes;
- a Stage 8 physical-TT dependency cannot be represented by the five frozen
  reference kinds and four target kinds;
- successor-before-predecessor cannot be achieved before shared mutation;
- a sole committed updater has no exact page-local terminal-independent
  representation;
- a positive normal path requires reconstructing lost volatile CTRC state;
- the frozen lock order cannot avoid a blocking heap/CTRC nest;
- an exact runtime state lies outside either closed FSM/matrix;
- implementation would need a new authority, daemon or Stage 9 recovery
  positive.

Normal missing code, test failures and wrong transitions are not deviations;
fix them against the frozen rows.

## 16. Single Writer handoff prompt

Use this prompt once, after the private design commit is pushed:

```text
Resume as the sole PGRAC coding Writer. The user approved APPROVE_CTRC.

Before any product edit, read the private design commit named in the handoff,
especially:
- docs/ad-024-canonical-active-transaction-authority.md §§21–31
- specs/spec-8.4d-current-mx-transaction-authority-state-matrix.md
  §§12.4–12.15 and §§15–17
- specs/matrices/spec-8.4d-authority-matrix-v2.json
- docs/superpowers/plans/2026-09-01-stage8-ctrc-terminal-reference-release.md

Preserve the current canonical ACTIVE/COMMIT/ABORT implementation and the
existing fail-closed permanent peer-mode ABORTED retention WIP. Do not revert
or start over. Implement the plan strictly Task 0→12 with RED first. Remove the
permanent retention only at Task 9 after T20..T34 prove exact positive release.

Critical correction: ordinary shared ITL/UBA is a registered CTRC reference,
not a presumed pre-terminal discharge. The current legacy touch path can drop
an invalid proof and the terminal stamper can skip; neither proves release.
Before every ordinary ITL/UBA page mutation, prepare outside the heap lock,
pure-plan/finalize/APPLY under the lock, then mutate. A terminal hint only
cleans that existing receipt after exact terminal-independent projection and
WAL/dependency capture.

ACTIVE survivors require successor receipts before predecessor removal.
Terminal/UNKNOWN proofs have grant zero and cannot authorize a copied/moved
dependent target. Extend the existing KO/SMGR drop/truncate chokepoints so KO
DONE and physical removal occur only after matching CTRC receipts are durably
cancelled/cleaned; do not treat ENOENT as release proof and do not add KO wire.

The canonical physical TT remains the sole transaction-status authority. CTRC
is release evidence only. Do not add a new daemon, authority, correctness GUC,
terminal archive, commit-time all-node barrier, full-heap scan, TTL/LRU
eviction, Stage 9 recovery positive, or wait/network/I/O under heap content.

Run the private matrix checker first, then add T20..T35 REDs. Do not enter
t/405, t/400, R11 or PRE until the preceding focused gate is GREEN. If a stop
condition in plan §15 is proven, pause product changes and report one precise
design defect; otherwise continue coding without another r-loop discovery
campaign.
```
