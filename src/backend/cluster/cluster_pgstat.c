/*-------------------------------------------------------------------------
 *
 * cluster_pgstat.c
 *	  pgrac cluster performance-stats framework (Stage 0.28).
 *
 *	  Implements:
 *	    - The compile-time counter registry (one entry initially:
 *	      cluster.inject.armed_count, mirroring cluster_injection_armed_count
 *	      from spec-0.27).
 *	    - Public helpers (lookup / inc / set / read) backed by
 *	      pg_atomic_uint64.
 *	    - The cluster_get_pgstat_counters SRF backing
 *	      pg_stat_cluster_counters.
 *
 *	  Stage 1+ subsystems extend the registry by appending entries to
 *	  cluster_pgstat_counters[] and calling the helpers from their hot
 *	  paths.  Cross-process / shmem aggregation is deferred to the
 *	  cluster shmem coordination protocol (Stage 2+).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_pgstat.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  The SRF entry point is unconditionally compiled because pg_proc.dat
 *	  references it in both build modes; its body is #ifdef USE_PGRAC_
 *	  CLUSTER guarded.  The internal registry, helpers, and lazy-init
 *	  logic are compiled out completely on --disable-cluster builds
 *	  (spec-0.3 contract).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"

#include "cluster/cluster_pgstat.h"


/* SRF info-V1 declaration -- always linked because pg_proc.dat
 * references this regardless of build mode. */
PG_FUNCTION_INFO_V1(cluster_get_pgstat_counters);


#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_inject.h"		  /* cluster_injection_armed_count */
#include "cluster/storage/cluster_smgr.h" /* spec-2.7 remote invalidation stub counter */


/* ============================================================
 * Registry (compile-time static array).
 * ============================================================ */

/*
 * The stage-0 counter registry.  Each entry has a stable name and a
 * lazy-initialised pg_atomic_uint64.  Stage 1+ subsystems append
 * entries here when their first stat-emitting code path lands; the
 * SSOT roster is in pgrac:docs/performance-views-design.md.
 */
static ClusterPgstatCounter cluster_pgstat_counters[] = {
	{ .name = "cluster.inject.armed_count" },
	/*
	 * spec-2.7 D6 (v0.2 frozen 2026-05-09):  mirror of the
	 * cross-instance broadcast STUB call counter owned by
	 * cluster_smgr.c.  Synchronised at SRF entry via
	 * cluster_pgstat_sync_mirrors so SQL queries against
	 * pg_stat_cluster_counters always reflect the live atomic.
	 */
	{ .name = "cluster.smgr.remote_invalidation_stub_call_count" },
	/*
	 * spec-2.6 Sprint A Step 4 D10:  4 qvotec / quorum-lite counters.
	 * Incremented by cluster_qvotec.c (poll cycle / quorum loss event /
	 * collision detect event) and cluster_voting_disk_io.c (disk I/O
	 * failure).  Read-only via pg_stat_cluster_counters; no mirror sync
	 * since these are written directly to the registry atomic.
	 */
	{ .name = "cluster.qvotec.poll_cycle_count" },
	{ .name = "cluster.qvotec.quorum_loss_event_count" },
	{ .name = "cluster.qvotec.collision_detect_event_count" },
	{ .name = "cluster.qvotec.disk_io_failure_count" },
	/*
	 * spec-2.28 Sprint A Step 4 D11:  4 fence-lite counters.
	 *
	 * Important storage rule: this registry is process-local.  A backend
	 * SELECT from pg_stat_cluster_counters sees counters for that backend
	 * process unless a subsystem explicitly syncs a mirror before SRF
	 * output.  Fence-lite's cross-process observability source of truth is
	 * pg_cluster_fence_state, which reads freeze/thaw/self-fence counts from
	 * ClusterFenceShmem; freeze_signal_received_count remains local to the
	 * receiving backend in v0.15.0.
	 *
	 *   freeze_broadcast_count          : LMON cluster_fence_broadcast_freeze
	 *   thaw_broadcast_count            : LMON cluster_fence_broadcast_thaw
	 *   self_fence_initiated_count      : postmaster cluster_fence_postmaster_check
	 *                                     after grace_ms elapsed (kill SIGINT)
	 *   freeze_signal_received_count    : current backend's cluster_fence_check_
	 *                                     interrupts ereport(ERROR, 53R50)
	 */
	{ .name = "cluster.fence.freeze_broadcast_count" },
	{ .name = "cluster.fence.thaw_broadcast_count" },
	{ .name = "cluster.fence.self_fence_initiated_count" },
	{ .name = "cluster.fence.freeze_signal_received_count" },
	/*
	 * RF-PAGE PGDEL-08 (spec-rf-page-crash-safe-page-replay-journal.md
	 * §9.1): recovery-only counters, G2-classified (EVENT/GAUGE/
	 * TIMESTAMP) in the single vocabulary table of
	 * cluster_page_stats_describe().  Producers are the
	 * cluster_page_stats_* functions, fired by the PGDEL-09 wiring at
	 * the owning proof points (source selection/invalid/missing/conflict,
	 * skip/apply/mismatch, unknown-class blocked, authority stale, early
	 * release, retire denial, D3′ rebuild/hit, stable-base unresolved,
	 * contributor gauges, retained pinned bytes, last write/durability/
	 * post-read timestamps).  Names must stay in sync with the
	 * vocabulary table (G4′: one producer + one consumer per counter).
	 */
	{ .name = "cluster.page.source_selected_current" },
	{ .name = "cluster.page.source_selected_pi" },
	{ .name = "cluster.page.source_selected_storage" },
	{ .name = "cluster.page.source_invalid" },
	{ .name = "cluster.page.source_missing" },
	{ .name = "cluster.page.source_conflict" },
	{ .name = "cluster.page.result_skip" },
	{ .name = "cluster.page.apply_count" },
	{ .name = "cluster.page.version_mismatch" },
	{ .name = "cluster.page.unknown_class_blocked" },
	{ .name = "cluster.page.authority_stale_rejected" },
	{ .name = "cluster.page.resource_early_release" },
	{ .name = "cluster.page.retire_denied" },
	{ .name = "cluster.page.d3_rebuild" },
	{ .name = "cluster.page.d3_optimization_hit" },
	{ .name = "cluster.page.stable_base_unresolved" },
	{ .name = "cluster.page.contributor_records" },
	{ .name = "cluster.page.contributor_threads" },
	{ .name = "cluster.page.contributor_gaps" },
	{ .name = "cluster.page.retained_pinned_bytes" },
	{ .name = "cluster.page.last_page_write_ts" },
	{ .name = "cluster.page.last_durability_barrier_ts" },
	{ .name = "cluster.page.last_post_read_ts" },
};

#define CLUSTER_PGSTAT_COUNT lengthof(cluster_pgstat_counters)


/*
 * Lazy initialisation flag.  pg_atomic_init_u64 must be called before
 * the first read or write; we run init the first time any helper is
 * invoked.  Since helpers funnel through this guard there is no
 * fork-time race -- all writers serialise through ordinary control
 * flow.
 */
static bool cluster_pgstat_initialised = false;

static void
cluster_pgstat_initialise(void)
{
	if (cluster_pgstat_initialised)
		return;

	for (int i = 0; i < CLUSTER_PGSTAT_COUNT; i++)
		pg_atomic_init_u64(&cluster_pgstat_counters[i].value, 0);

	cluster_pgstat_initialised = true;
}


/* ============================================================
 * Public helpers.
 * ============================================================ */

ClusterPgstatCounter *
cluster_pgstat_lookup(const char *name)
{
	if (name == NULL)
		return NULL;

	cluster_pgstat_initialise();

	for (int i = 0; i < CLUSTER_PGSTAT_COUNT; i++) {
		if (strcmp(cluster_pgstat_counters[i].name, name) == 0)
			return &cluster_pgstat_counters[i];
	}
	return NULL;
}


void
cluster_pgstat_inc(ClusterPgstatCounter *c)
{
	if (c == NULL)
		return;

	cluster_pgstat_initialise();
	(void)pg_atomic_fetch_add_u64(&c->value, 1);
}


void
cluster_pgstat_set(ClusterPgstatCounter *c, uint64 value)
{
	if (c == NULL)
		return;

	cluster_pgstat_initialise();
	pg_atomic_write_u64(&c->value, value);
}


uint64
cluster_pgstat_read(const ClusterPgstatCounter *c)
{
	if (c == NULL)
		return 0;

	/*
	 * cluster_pgstat_initialise() takes a non-const pointer to mutate
	 * the init flag; cast away const here because the read path is
	 * conceptually idempotent and the lazy-init writes are
	 * serialised by control flow, not by the caller's const-ness.
	 */
	cluster_pgstat_initialise();
	return pg_atomic_read_u64((pg_atomic_uint64 *)(uintptr_t)&c->value);
}


/*
 * cluster_pgstat_get_count -- registry size accessor (stage 0.29).
 *
 *	Returns the compile-time number of registered counters.  Used by
 *	cluster_debug.c for enumerating the registry without exposing the
 *	static array.
 */
int
cluster_pgstat_get_count(void)
{
	return CLUSTER_PGSTAT_COUNT;
}

/*
 * cluster_pgstat_get_at -- read one counter entry (stage 0.29).
 *
 *	Iterator companion to cluster_pgstat_get_count.  Returns false
 *	if `idx` is out of range; otherwise fills *name_out / *value_out
 *	from the entry at index `idx`.  Value is read atomically; mirror
 *	counters reflect whatever sync state is currently stored (caller
 *	can re-trigger mirror sync via cluster_get_pgstat_counters SRF).
 *
 *	`name_out` points into the compile-time registry's string literal.
 */
bool
cluster_pgstat_get_at(int idx, const char **name_out, uint64 *value_out)
{
	ClusterPgstatCounter *c;

	if (idx < 0 || idx >= CLUSTER_PGSTAT_COUNT)
		return false;

	cluster_pgstat_initialise();

	c = &cluster_pgstat_counters[idx];

	if (name_out != NULL)
		*name_out = c->name;
	if (value_out != NULL)
		*value_out = pg_atomic_read_u64(&c->value);

	return true;
}


/* ============================================================
 * Mirror sync: pull external state into framework counters.
 *
 *	Some counters in the registry are convenient names for state that
 *	lives elsewhere (e.g. cluster_injection_armed_count is the live
 *	int from cluster_inject.c, which the inject framework owns
 *	directly).  We refresh those mirror values at SRF entry so the
 *	view reads always reflect the current truth.
 *
 *	This is a one-way pull: cluster_pgstat depends on cluster_inject,
 *	not the other way around.  cluster_inject.c does not include
 *	cluster_pgstat.h.
 * ============================================================ */
static void
cluster_pgstat_sync_mirrors(void)
{
	ClusterPgstatCounter *armed_count;

	CLUSTER_INJECTION_POINT("cluster-pgstat-mirror-sync");

	armed_count = cluster_pgstat_lookup("cluster.inject.armed_count");
	if (armed_count != NULL)
		cluster_pgstat_set(armed_count, (uint64)cluster_injection_armed_count);

	{
		ClusterPgstatCounter *remote_inval;

		remote_inval = cluster_pgstat_lookup("cluster.smgr.remote_invalidation_stub_call_count");
		if (remote_inval != NULL)
			cluster_pgstat_set(remote_inval,
							   cluster_smgr_get_remote_invalidation_stub_call_count());
	}
}

#endif /* USE_PGRAC_CLUSTER */


/* ============================================================
 * SRF entry point (always linked; body guarded by USE_PGRAC_CLUSTER).
 * ============================================================ */

Datum
cluster_get_pgstat_counters(PG_FUNCTION_ARGS)
{
	InitMaterializedSRF(fcinfo, 0);

#ifdef USE_PGRAC_CLUSTER
	{
		ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

		cluster_pgstat_sync_mirrors();

		for (int i = 0; i < CLUSTER_PGSTAT_COUNT; i++) {
			ClusterPgstatCounter *c = &cluster_pgstat_counters[i];
			Datum values[2];
			bool nulls[2] = { false, false };
			uint64 v;

			v = pg_atomic_read_u64(&c->value);

			values[0] = CStringGetTextDatum(c->name);
			values[1] = Int64GetDatum((int64)v);

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		}
	}
#else
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("pg_stat_cluster_counters requires --enable-cluster")));
#endif

	return (Datum)0;
}
