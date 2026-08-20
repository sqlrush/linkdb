#!/bin/bash
#-------------------------------------------------------------------------
#
# check-wal-state-correctness-census.sh
#    CI helper: static census of wal-state registry correctness
#    reader/writer call sites (RF-ROOT P7 G4 / STOP-01 §17.9).
#
#    Post-bit22 (PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1),
#    EVERY wal-state correctness reader/writer must be statically
#    unreachable — the canonical control root carries the checkpoint/tail/
#    FPW bounds (CHECKPOINT_ADVANCE + FPW_STICKY publications), and the
#    registry is telemetry only.
#
#    Batch 3 / contract §C / follow-up: the census is the POST-bit22 static
#    proof (gate modeling), NOT a pre-bit22 precondition.  The frozen
#    §17.8 keeps the registry as the SELECTED authority before bit22 opens,
#    so pre-bit22 registry reads are legal.  The modeling object is
#    "no UNGATED correctness call site":
#
#      - GATE-BOUND sites: their registry reads sit inside the recognized
#        gate idiom `cluster_r4_bit22_cutover_active()` — legal pre-bit22
#        and statically unreachable post-bit22 (the latch is monotonic), so
#        the census does NOT count them; each file must still contain the
#        idiom anchor (drift check below).
#      - KNOWN-DEFERRED sites: ungated registry reads that stay
#        §17.8-correct until the bit22 cutover round closes them in the
#        same commit.  strict mode counts them (RED = the post-bit22 proof
#        is not established yet); the runtime latch apply refuses to flip
#        while they are listed (same table, lockstep).
#      - the TELEMETRY-ONLY whitelist (allowed forever):
#        W1-W5 writers, xlogrecovery raw_ignored LOG, the R4 migration
#        binding (merge_recovered_lsn == 0 source-zero evidence).
#
#    The default (strict) mode is the post-bit22 proof gate: GREEN = every
#    correctness call site is telemetry, GATE-BOUND behind the idiom, or
#    closed.  `--deferred-ok` prints (not fails) the GATE-BOUND/KNOWN-
#    DEFERRED sites for the cutover audit.
#
# IDENTIFICATION
#    scripts/ci/check-wal-state-correctness-census.sh
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#    Strategy: enumerate the production correctness call sites of the
#    wal-state registry read/update APIs and classify each against the
#    telemetry whitelist, the GATE-BOUND list and the KNOWN-DEFERRED list.
#
#-------------------------------------------------------------------------

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

MODE="${1:-strict}"
case "$MODE" in
	--deferred-ok|-d) MODE="deferred-ok" ;;
esac

# The registry read/update entry points whose call sites this census counts.
APIS='cluster_wal_state_read_slot|cluster_wal_state_update_own'

# The gate idiom anchor.  Every GATE-BOUND file must contain this call; a
# refactor that removes the gate without updating the list is a violation.
GATE_ANCHOR='cluster_r4_bit22_cutover_active'

# Telemetry-only whitelist (file:line-prefix or file).  Every other
# production call site of the APIs above is a census violation.
TELEMETRY_OK=(
	'src/backend/cluster/cluster_wal_state.c'      # the W1-W5 writers themselves
	'src/test/cluster_unit/test_cluster_wal_state'
	'src/test/cluster_unit/test_cluster_wal_state_rmw'
	'src/backend/access/transam/xlogrecovery.c'    # raw_ignored diagnostic LOG only
	'src/backend/access/transam/xlog.c'            # W5a/W5b registry writers + EOR FPW evidence read
	'src/backend/cluster/cluster_control_root.c'   # R4 migration binding (merge_recovered_lsn==0)
	'src/backend/cluster/cluster_stats.c'          # W2/W4 telemetry publishers
	'src/backend/cluster/cluster_recovery_anchor.c' # own-slot ACTIVE/timestamp status read (phase-4 gate)
	'src/backend/cluster/cluster_debug.c'          # observability SRF dump
)

# GATE-BOUND correctness sites (contract §B / follow-up): registry reads
# restored behind the bit22 gate idiom — legal pre-bit22 (§17.8), statically
# unreachable post-bit22 (monotonic latch).  Not counted; anchor-checked.
# implementation (2026-08-18): cluster_hw_remaster.c joined — its registry watermark
# read is now inside the gate idiom (pre-bit22 frozen behavior; post-bit22
# root-only with the contract ABSENT binary).  KNOWN_DEFERRED is now empty:
# the census strict proof (post-bit22 exactly-zero, gate-modeled) holds.
GATE_BOUND=(
	'src/backend/cluster/cluster_recovery_plan.c'
	'src/backend/cluster/cluster_recovery_worker.c'
	'src/backend/cluster/cluster_thread_recovery_orchestrator.c'
	'src/backend/cluster/cluster_hw_remaster.c'
)

# KNOWN-DEFERRED correctness sites: ungated registry reads that stay
# §17.8-correct until the bit22 cutover round closes them in the same commit.
# implementation: empty — every correctness site is GATE-BOUND; the runtime latch
# apply self-check (cluster_r4_bit22_cutover_latch_apply) is GREEN.
KNOWN_DEFERRED=(
)

violations=0

while IFS=: read -r file line rest; do
	[ -n "$file" ] || continue
	case "$file" in
		src/backend/*|src/test/cluster_unit/*)
			;;
		*)
			continue
			;;
	esac
	ok=0
	for w in "${TELEMETRY_OK[@]}"; do
		case "$file" in
			"$w"*) ok=1 ;;
		esac
		[ "$ok" = 1 ] && break
	done
	if [ "$ok" = 1 ]; then
		continue
	fi
	gated=0
	for g in "${GATE_BOUND[@]}"; do
		case "$file" in
			"$g"*) gated=1 ;;
		esac
		[ "$gated" = 1 ] && break
	done
	if [ "$gated" = 1 ]; then
		if [ "$MODE" = "deferred-ok" ]; then
			echo "gate-bound: $file:$line"
		fi
		continue
	fi
	deferred=0
	for d in "${KNOWN_DEFERRED[@]}"; do
		case "$file" in
			"$d"*) deferred=1 ;;
		esac
		[ "$deferred" = 1 ] && break
	done
	if [ "$deferred" = 1 ]; then
		if [ "$MODE" = "deferred-ok" ]; then
			echo "deferred: $file:$line"
		else
			echo "VIOLATION (KNOWN-DEFERRED, must close in the bit22 cutover round): $file:$line"
			violations=$((violations + 1))
		fi
		continue
	fi
	echo "VIOLATION (ungated correctness read/write): $file:$line"
	violations=$((violations + 1))
done < <(grep -nE "$APIS" src/backend --include='*.c' -r 2>/dev/null || true)

# GATE-BOUND anchor drift check: every listed file must contain the gate
# idiom call, otherwise its registry reads are NOT proven post-bit22-
# unreachable and the modeling breaks silently.
for g in "${GATE_BOUND[@]}"; do
	if ! grep -q "$GATE_ANCHOR" "$g"; then
		echo "VIOLATION (GATE-BOUND drift: $g no longer contains the $GATE_ANCHOR gate idiom)"
		violations=$((violations + 1))
	fi
done

if [ "$violations" -gt 0 ]; then
	echo "wal-state correctness census: $violations violation(s) — post-bit22 exactly-zero proof NOT established."
	exit 1
fi

# Lockstep check: the runtime mirror table in cluster_wal_state.c
# (cluster_wal_state_correctness_census_ok, consulted by the latch apply)
# must list EXACTLY the same KNOWN-DEFERRED sites as this script.  Closing a
# deferred site removes it from both in the same commit; a mismatch means
# the runtime self-check and the static census disagree about the cutover.
CENSUS_TABLE='src/backend/cluster/cluster_wal_state.c'
if [ -f "$CENSUS_TABLE" ]; then
	table_sites=$(sed -n '/cluster_wal_state_census_deferred_sites\[\]/,/^};/p' "$CENSUS_TABLE" \
		| grep -oE '"[a-z_./]+\.c"' | tr -d '"' || true)
	# The script's KNOWN_DEFERRED entries carry src/... paths; the runtime
	# table uses basenames — compare normalized basenames.  (The array is
	# empty in the post-batch-4 GREEN state; the ${var[@]+...} guard keeps
	# set -u happy.)
	script_sites=$(printf '%s\n' ${KNOWN_DEFERRED[@]+"${KNOWN_DEFERRED[@]}"} \
		| sed 's#^.*/##' | grep -v '^$' || true)
	table_diff=$(comm -3 <(printf '%s\n' $table_sites | sort) \
		<(printf '%s\n' "$script_sites" | sort))
	if [ -n "$table_diff" ]; then
		echo "VIOLATION (lockstep drift between the runtime census table and the script KNOWN_DEFERRED list):"
		printf '%s\n' "$table_diff" | sed 's/^/  /'
		echo "the cutover round cannot close cleanly."
		exit 1
	fi
fi

echo "wal-state correctness census: clean (every correctness call site is telemetry, GATE-BOUND behind the bit22 idiom, or closed)."
exit 0
