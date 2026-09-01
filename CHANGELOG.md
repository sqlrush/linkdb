# Changelog

## 2026-09-01 — `APPROVE_CTRC`: Stage 8 canonical terminal-reference census design freeze

- **User decision recorded:** `APPROVE_CTRC`. Writer stays paused until one
  complete private-design handoff; no public product/test byte is modified by
  this change.
- **Design gap closed:** AD-024 now names the exact owner of
  `COMMITTED_RETAINED/ABORTED_RETAINED → RECYCLABLE`. The existing
  `cluster_undo_cleaner` closes the ACTIVE grant generation at origin-recorded
  touched nodes, drains PREPARED receipts, cleans every APPLIED ordinary
  ITL/UBA and current-MX reference under exact page authority, waits all local/per-origin WAL
  dependencies durable, and writes the sole canonical release certificate.
- **Authority boundary:** physical TT remains the only transaction-status
  authority. Receipt/journal/ACK/digest/dependency/certificate bytes prove
  release only. Missing/lost/drifted state retains; Stage 8 does not reconstruct
  a lost volatile journal positively.
- **Executable matrix v2:** Spec 8.4D adds invariants I13–I22, five reference
  kinds, four target kinds, 49-pair receipt and seal machines with 40/38 explicit
  fail-closed defaults, five source classes, nine crash cuts, sixteen new tests
  `T20..T35`, and contracts `K09..K21`. The checker validates exact identities,
  totality, capability/wire allocations, capacity formulas and WAL byte layout;
  frozen canonical JSON SHA-256 is
  `7c56f3d686804297814043c0454fc8024a6a660ae22e156a10b44f46b144f90d`.
- **Corrected completeness boundary:** the existing ITL touch path can discard
  an invalid ownership capture and the terminal stamper can skip on drift, so
  it is an eager projection hint, not census authority. Ordinary shared ITL/UBA
  now requires a pre-mutation CTRC receipt; only exact terminal-independent
  projection discharges it.
- **Fixed ABI:** capability `0x00400000`; current-MX wire v2; forward kind 11;
  reply status 30; 128-byte request, 64-byte reply header, 416-byte participant
  ACK; `XLOG_UNDO_TT_SLOT_CTRC_RELEASE=0xA0`; fixed 96-byte payload;
  `TT_SLOT_FLAG_CTRC_RELEASE_PROVEN=0x01`; SHA-256 canonical digests.
- **TDD handoff:** new private implementation plan freezes Task 0→12. Existing
  permanent peer-mode ABORTED retention remains until positive release tests are
  GREEN; then unchanged `t/405 → t/400 236/236 → R11 7/7 → fresh PRE` order.
