/*-------------------------------------------------------------------------
 *
 * cluster_debug.h
 *	  pgrac cluster diagnostic snapshot framework (Stage 0.29).
 *
 *	  Backs the pg_cluster_state view: a one-stop snapshot of every
 *	  cluster subsystem's runtime state expressed as
 *	  (category, key, value) triples.  Read-only; safe to call in
 *	  production for diagnostics.  Categories at stage 0 cover shmem
 *	  control block, cluster.* GUCs, the active interconnect tier,
 *	  injection-point registry (spec-0.27), pgstat counter registry
 *	  (spec-0.28), pgrac.conf topology summary, and the cluster
 *	  lifecycle phase.
 *
 *	  Stage 1+ subsystems append their own categories (e.g. "scn" /
 *	  "ges" / "pcm" / "buffer") as they ship.  See
 *	  pgrac:docs/cluster-debug-design.md §3 for the SSOT category
 *	  roster and the procedure for adding a new category.
 *
 *	  See specs/spec-0.29-debug-tools.md for stage-0 scope and
 *	  docs/cluster-debug-design.md for the dump format contract,
 *	  field semantics, gdb python helper integration, and Stage 1+
 *	  extension guide.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_debug.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  The SRF entry point is unconditionally compiled because pg_proc.dat
 *	  references it in both build modes; its body is #ifdef USE_PGRAC_
 *	  CLUSTER guarded and raises ERRCODE_FEATURE_NOT_SUPPORTED on
 *	  --disable-cluster builds (spec-0.3 contract).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_DEBUG_H
#define CLUSTER_DEBUG_H

#include "fmgr.h"


/* ----------
 * SRF entry point.  Always linked (pg_proc.dat references this
 * unconditionally); body is #ifdef USE_PGRAC_CLUSTER guarded.
 *
 *	cluster_dump_state()
 *	    RETURNS SETOF (category text, key text, value text)
 *	    Returns one row per (category, key) pair for every cluster
 *	    subsystem.  Output is ordered by category (fixed order:
 *	    shmem, guc, ic, inject, pgstat, conf, phase) and then by
 *	    key (lexicographic, except injection-point children which
 *	    follow the registry order).  Powers the pg_cluster_state
 *	    view.  See spec-0.29 §3.1 for the order contract.
 * ---------- */
extern Datum cluster_dump_state(PG_FUNCTION_ARGS);
extern Datum pg_cluster_gcs_block_dedup_debug_exact(PG_FUNCTION_ARGS);

#endif /* CLUSTER_DEBUG_H */
