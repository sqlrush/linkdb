/*-------------------------------------------------------------------------
 *
 * cluster_gcs_block_dedup.c
 *	  pgrac cluster GCS block reliability hardening — master-side dedup HTAB
 *	  implementation (spec-2.34 D2 + D5 + D6).
 *
 *	  Implements:
 *	    - shmem region "pgrac cluster gcs block dedup" with a per-worker
 *	      shard array (spec-7.3 D5): dedup_shards[worker_id] each own their
 *	      header + HTAB + LWLock, so the LMS worker pool never contends on
 *	      one lock and never shares dedup state across workers
 *	    - lookup_or_register / install_reply / remove APIs (worker_id-keyed)
 *	    - TTL sweep + node-dead + backend-exit GC (iterate every shard)
 *	    - before_shmem_exit local backend cleanup hook
 *	    - counter accessors that sum across shards + a misroute fail-closed
 *	      counter in the always-present ctl header (8.A)
 *
 *	  See cluster_gcs_block_dedup.h for the HC contracts and entry layout.
 *
 *	  spec-7.3 D5 承重 invariant: a block tag routes to exactly one worker
 *	  (worker[shard(tag)], D4), and every message for that tag — original +
 *	  retransmits — carries the same request_id, so the dedup entry for a
 *	  request lives in exactly one shard.  Accessing a shard other than the
 *	  routed worker is a mis-route (序破坏); the hot-path bounds guard fails
 *	  it closed (FULL + misroute_failclosed_count++) rather than serving
 *	  from the wrong shard.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_gcs_block_dedup.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-2.34-gcs-block-reliability-hardening.md (FROZEN v0.3)
 *	  Spec: spec-7.3-lms-worker-pool.md (D5 — per-worker shard)
 *	  Design: docs/cache-fusion-protocol-design.md
 *	  AD-005 (Cache Fusion full)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_conf.h" /* declared_node_count_early (spec-7.2a D4) */
#include "cluster/cluster_gcs_block_dedup.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_lms_shard.h" /* CLUSTER_LMS_MAX_WORKERS */
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "port/pg_crc32c.h"
#include "storage/bufpage.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"


/* ============================================================
 * Module-scope shmem state (spec-7.3 D5 — per-worker shards).
 *
 *	ClusterGcsBlockDedupShard is the old ClusterGcsBlockDedupShared renamed:
 *	one private lock + counter block + HTAB per LMS worker.  The ctl header
 *	holds cluster-wide state that must exist even when no worker touches its
 *	own shard (the misroute counter, the live shard count).
 * ============================================================ */

typedef struct ClusterGcsBlockDedupShard {
	LWLockPadded lock;				  /* guards this shard's HTAB + entry_count */
	LWLockPadded relation_lock;		  /* guards only relation lifecycle HTAB */
	pg_atomic_uint64 hit_count;		  /* CACHED_REPLY paths */
	pg_atomic_uint64 miss_count;	  /* MISS_REGISTERED paths */
	pg_atomic_uint64 collision_count; /* HC91 VALIDATION_FAIL */
	pg_atomic_uint64 full_count;	  /* HC92 FULL */
	pg_atomic_uint64 evict_count;	  /* spec-7.2a: eager reclaim + TTL sweep removed */
	/* GCS-race round-2 RC-F: completion-proof outcomes. */
	pg_atomic_uint64 done_marked_count;	  /* identity-verified DONE stamped */
	pg_atomic_uint64 done_mismatch_count; /* DONE dropped: miss / identity / in-flight */
	/* S3-P0-09: identity-valid DONE refused because the cell still carries a
	 * live forward marker.  Split by phase because the two windows have very
	 * different reachability: FORWARDED is the marker's natural resting state
	 * from the moment the send finishes, whereas CANCELLING only exists after
	 * a pending_x_deny_next() pass has flipped it.  Kept out of
	 * done_mismatch_count on purpose — the identity matched perfectly, so this
	 * is a phase refusal, not a mismatch. */
	pg_atomic_uint64 done_forwarded_refused_count;	/* DONE refused on FORWARDED */
	pg_atomic_uint64 done_cancelling_refused_count; /* DONE refused on CANCELLING */
	/* GCS-race round-2 review F5 (calibration 2): registration routing. */
	pg_atomic_uint64 hint_violation_count;	 /* capable peer, hint 0 / over-max: denied */
	pg_atomic_uint64 legacy_pin_count;		 /* no-capability peer: protocol-max pin */
	pg_atomic_uint64 pcm_x_stage_count;		 /* RESERVED -> immutable image */
	pg_atomic_uint64 pcm_x_replay_count;	 /* exact image replay */
	pg_atomic_uint64 pcm_x_release_count;	 /* exact terminal release */
	pg_atomic_uint64 pcm_x_failclosed_count; /* malformed, stale, or full image leg */
	pg_atomic_uint32 entry_count;			 /* live in-flight + completed entries */
} ClusterGcsBlockDedupShard;

typedef struct ClusterGcsBlockDedupCtl {
	pg_atomic_uint64 misroute_failclosed_count; /* spec-7.3 D5 — 8.A drops */
	LWLockPadded origin_boot_lock;				/* S3-P0-18 legacy boot fence */
	uint64 origin_boot_incarnation[CLUSTER_MAX_NODES];
	TimestampTz origin_legacy_quarantine_until[CLUSTER_MAX_NODES];
	int n_shards;								/* live shard count fixed at init */
	int max_entries_effective;					/* spec-7.2a D4: per-shard cap the
												 * HTABs were sized with (configured
												 * + auto-size floor); stamped once
												 * at init, read-only after */
} ClusterGcsBlockDedupCtl;

typedef struct ClusterGcsStaleXDurableSealSlot {
	slock_t lock;
	GcsStaleXDurableSeal seal;
} ClusterGcsStaleXDurableSealSlot;

static ClusterGcsBlockDedupCtl *cluster_gcs_block_dedup_ctl = NULL;
static ClusterGcsBlockDedupShard *cluster_gcs_block_dedup_shards = NULL;
static ClusterGcsStaleXDurableSealSlot *cluster_gcs_stale_x_durable_seal_slots = NULL;
static HTAB *cluster_gcs_block_dedup_htabs[CLUSTER_LMS_MAX_WORKERS];
static HTAB *cluster_gcs_stale_x_release_htabs[CLUSTER_LMS_MAX_WORKERS];
static HTAB *cluster_gcs_block_eviction_gate_htabs[CLUSTER_LMS_MAX_WORKERS];
static HTAB *cluster_gcs_stale_x_relation_htabs[CLUSTER_LMS_MAX_WORKERS];
static int cluster_gcs_block_dedup_n_shards = 0;
static bool dedup_backend_exit_hook_registered = false;
/* Process-local LMS cursors.  Each DATA worker owns exactly one shard in one
 * process, so this adds no shared-memory region or cross-worker authority. */
static GcsBlockDedupKey dedup_pcm_x_work_cursor[CLUSTER_LMS_MAX_WORKERS];
static bool dedup_pcm_x_work_cursor_valid[CLUSTER_LMS_MAX_WORKERS];
/* When both classes stay runnable, alternate the single-work LMS tick budget.
 * The initial false value preserves RESERVED-first admission while bounding a
 * READY replay behind at most one reservation tick. */
static bool dedup_pcm_x_prefer_ready_next[CLUSTER_LMS_MAX_WORKERS];
/* Process-local wake hint.  An empty scan clears it; exact reserve/rearm sets
 * it.  This keeps ordinary GCS traffic from rescanning every 8KB entry on
 * every LMS tick when no PCM-X image work exists. */
static bool dedup_pcm_x_work_pending[CLUSTER_LMS_MAX_WORKERS];

/*
 * Bind legacy (no REQUEST boot overlay) traffic to an authenticated peer
 * boot without letting late old-DATA frames poison the new boot's keyspace.
 * A wire-bound request may advance immediately, but every unbound request is
 * held for the complete protocol lifetime after a boot change.
 */
bool
cluster_gcs_block_dedup_origin_boot_admit(uint32 origin_node_id,
										 uint64 boot_incarnation,
										 bool wire_bound,
										 TimestampTz now,
										 int64 legacy_quarantine_us)
{
	uint64 previous;
	TimestampTz until;
	bool admit;

	if (cluster_gcs_block_dedup_ctl == NULL
		|| origin_node_id >= CLUSTER_MAX_NODES || boot_incarnation == 0
		|| now < 0 || legacy_quarantine_us <= 0)
		return false;

	LWLockAcquire(&cluster_gcs_block_dedup_ctl->origin_boot_lock.lock,
				  LW_EXCLUSIVE);
	previous
		= cluster_gcs_block_dedup_ctl->origin_boot_incarnation[origin_node_id];
	until = cluster_gcs_block_dedup_ctl
				->origin_legacy_quarantine_until[origin_node_id];
	if (previous == 0) {
		cluster_gcs_block_dedup_ctl
			->origin_boot_incarnation[origin_node_id] = boot_incarnation;
		cluster_gcs_block_dedup_ctl
			->origin_legacy_quarantine_until[origin_node_id] = 0;
		admit = true;
	} else if (previous != boot_incarnation) {
		cluster_gcs_block_dedup_ctl
			->origin_boot_incarnation[origin_node_id] = boot_incarnation;
		if (now > PG_INT64_MAX - legacy_quarantine_us)
			until = PG_INT64_MAX;
		else
			until = now + legacy_quarantine_us;
		cluster_gcs_block_dedup_ctl
			->origin_legacy_quarantine_until[origin_node_id] = until;
		admit = wire_bound;
	} else {
		admit = wire_bound || until == 0 || now >= until;
	}
	LWLockRelease(&cluster_gcs_block_dedup_ctl->origin_boot_lock.lock);
	return admit;
}

/*
 * Upper bound on entries examined per cap-full eager-reclaim probe.  We do
 * NOT full-scan the HTAB under the exclusive lock on every cap-full MISS
 * (spec-7.2a §6 "no full scan"): if no reclaim-safe entry is found within the
 * probe budget, fall back to the fail-closed HC92 path.
 */
#define GCS_BLOCK_DEDUP_RECLAIM_MAX_PROBE 256

/*
 * spec-7.2a D5:  emit a saturation LOG only after this many additional
 * DENIED_DEDUP_FULL events accrue since the last report, so a persistently
 * full table logs at most once per this many drops (rule 17: no hot-path
 * flood).  The LMON TTL sweep is the sole evaluation site.
 */
#define GCS_BLOCK_DEDUP_FULL_LOG_THRESHOLD 64

/* Forward declarations for GC helpers used by the MISS-path eager reclaim. */
static int64 dedup_expiry_threshold_us(void);
static int dedup_reclaim_reclaimable_locked(ClusterGcsBlockDedupShard *shard, HTAB *htab,
											TimestampTz now, int want);


static int
cluster_gcs_block_dedup_effective_entries(void)
{
	/*
	 * Heavy GCS block-dedup storage is only meaningful for configured
	 * cluster nodes.  initdb/bootstrap runs with cluster_node_id = -1 and
	 * tiny shared_buffers; allocating the 8KB-entry HTAB there can exceed
	 * PG's bootstrap shmem budget before any cluster path is usable.
	 */
	if (!cluster_enabled || cluster_node_id < 0)
		return 0;

	{
		int configured
			= cluster_gcs_block_dedup_max_entries > 0 ? cluster_gcs_block_dedup_max_entries : 1024;
		int64 auto_floor;

		/*
		 * spec-7.2a D4 (Q4) auto-size lower bound: every connected backend on
		 * every declared node can hold one block request in flight against
		 * this master, so a configured cap below MaxConnections × node_count
		 * is guaranteed to saturate under distinct-read pressure.  Raise such
		 * configs to that floor, clamped at the GUC ceiling — auto-sizing
		 * widens a foot-gun config, it never grows shmem past what the DBA
		 * could configure by hand.  The node count comes from the pre-shmem
		 * conf sniff (cluster_conf_load() runs only after every region is
		 * initialised, so cluster_conf_node_count() is still 0 when
		 * size_fn/init_fn call here); with no readable pgrac.conf the sniff
		 * reports 1 and the floor degenerates to MaxConnections.  The floor
		 * applies PER SHARD (spec-7.3 D5): tags route to exactly one worker's
		 * shard, so a single hot shard must alone absorb the floor's
		 * worst-case in-flight population.
		 */
		auto_floor = (int64)MaxConnections * cluster_conf_declared_node_count_early();
		if (auto_floor > CLUSTER_GCS_BLOCK_DEDUP_MAX_ENTRIES_CEILING)
			auto_floor = CLUSTER_GCS_BLOCK_DEDUP_MAX_ENTRIES_CEILING;
		if (configured < (int)auto_floor)
			configured = (int)auto_floor;

		return configured;
	}
}

/*
 * spec-7.3 D5 — live shard count = configured LMS worker count, clamped to
 * the compile-time cap.  POSTMASTER-level GUC, so this is stable across the
 * shmem_size / shmem_init pair and for the process lifetime.
 */
static int
cluster_gcs_block_dedup_live_shards(void)
{
	int n = cluster_lms_workers;

	if (n < 1)
		n = 1;
	if (n > CLUSTER_LMS_MAX_WORKERS)
		n = CLUSTER_LMS_MAX_WORKERS;
	return n;
}

/*
 * Bytes carved by ShmemInitStruct: ctl header + the per-worker shard array
 * (the HTABs are allocated separately by ShmemInitHash).
 */
static Size
cluster_gcs_block_dedup_struct_bytes(int n_shards)
{
	Size sz;

	sz = MAXALIGN(sizeof(ClusterGcsBlockDedupCtl));
	sz = add_size(sz, mul_size(n_shards, MAXALIGN(sizeof(ClusterGcsBlockDedupShard))));
	sz = add_size(
		sz, mul_size(NBuffers, MAXALIGN(sizeof(ClusterGcsStaleXDurableSealSlot))));
	return sz;
}


/* ============================================================
 * Shmem registry.
 * ============================================================ */

Size
cluster_gcs_block_dedup_shmem_size(void)
{
	Size sz;
	int cap;
	int n;

	cap = cluster_gcs_block_dedup_effective_entries();
	if (cap == 0)
		return 0;

	n = cluster_gcs_block_dedup_live_shards();
	sz = cluster_gcs_block_dedup_struct_bytes(n);
	sz = add_size(sz, mul_size(n, hash_estimate_size(cap, sizeof(GcsBlockDedupEntry))));
	sz = add_size(
		sz, mul_size(n, hash_estimate_size(cap, sizeof(GcsStaleXReleaseRecord))));
	sz = add_size(
		sz, mul_size(n, hash_estimate_size(cap,
										  sizeof(GcsBlockEvictionGateRecord))));
	sz = add_size(
		sz, mul_size(n, hash_estimate_size(
			cap, sizeof(GcsStaleXRelationGenerationEntry))));
	return sz;
}

void
cluster_gcs_block_dedup_shmem_init(void)
{
	bool found;
	HASHCTL info;
	int cap;
	int n;
	int i;
	char *base;

	cap = cluster_gcs_block_dedup_effective_entries();
	if (cap == 0)
		return;

	n = cluster_gcs_block_dedup_live_shards();

	base = (char *)ShmemInitStruct("pgrac cluster gcs block dedup",
								   cluster_gcs_block_dedup_struct_bytes(n), &found);
	cluster_gcs_block_dedup_ctl = (ClusterGcsBlockDedupCtl *)base;
	cluster_gcs_block_dedup_shards
		= (ClusterGcsBlockDedupShard *)(base + MAXALIGN(sizeof(ClusterGcsBlockDedupCtl)));
	cluster_gcs_stale_x_durable_seal_slots
		= (ClusterGcsStaleXDurableSealSlot
			   *)(base + MAXALIGN(sizeof(ClusterGcsBlockDedupCtl))
				  + mul_size(n, MAXALIGN(sizeof(ClusterGcsBlockDedupShard))));
	cluster_gcs_block_dedup_n_shards = n;
	memset(dedup_pcm_x_work_cursor, 0, sizeof(dedup_pcm_x_work_cursor));
	memset(dedup_pcm_x_work_cursor_valid, 0, sizeof(dedup_pcm_x_work_cursor_valid));
	memset(dedup_pcm_x_prefer_ready_next, 0, sizeof(dedup_pcm_x_prefer_ready_next));
	memset(dedup_pcm_x_work_pending, 0, sizeof(dedup_pcm_x_work_pending));
	for (i = 0; i < n; i++)
		dedup_pcm_x_work_pending[i] = true;

	if (!found) {
		pg_atomic_init_u64(&cluster_gcs_block_dedup_ctl->misroute_failclosed_count, 0);
		LWLockInitialize(&cluster_gcs_block_dedup_ctl->origin_boot_lock.lock,
						 LWTRANCHE_CLUSTER_GCS_BLOCK_DEDUP);
		memset(cluster_gcs_block_dedup_ctl->origin_boot_incarnation, 0,
			   sizeof(cluster_gcs_block_dedup_ctl->origin_boot_incarnation));
		memset(cluster_gcs_block_dedup_ctl->origin_legacy_quarantine_until, 0,
			   sizeof(cluster_gcs_block_dedup_ctl
						  ->origin_legacy_quarantine_until));
		cluster_gcs_block_dedup_ctl->n_shards = n;
		/* spec-7.2a D4: stamp the per-shard cap the HTABs below are sized
		 * with, so the observability accessor reports the capacity actually
		 * in force (identical in every process, EXEC_BACKEND included). */
		cluster_gcs_block_dedup_ctl->max_entries_effective = cap;

		for (i = 0; i < n; i++) {
			ClusterGcsBlockDedupShard *shard = &cluster_gcs_block_dedup_shards[i];

			memset(shard, 0, sizeof(*shard));
			LWLockInitialize(&shard->lock.lock, LWTRANCHE_CLUSTER_GCS_BLOCK_DEDUP);
			LWLockInitialize(&shard->relation_lock.lock,
							 LWTRANCHE_CLUSTER_GCS_BLOCK_DEDUP);
			pg_atomic_init_u64(&shard->hit_count, 0);
			pg_atomic_init_u64(&shard->miss_count, 0);
			pg_atomic_init_u64(&shard->collision_count, 0);
			pg_atomic_init_u64(&shard->full_count, 0);
			pg_atomic_init_u64(&shard->evict_count, 0);
			pg_atomic_init_u64(&shard->done_marked_count, 0);
			pg_atomic_init_u64(&shard->done_mismatch_count, 0);
			pg_atomic_init_u64(&shard->done_forwarded_refused_count, 0);
			pg_atomic_init_u64(&shard->done_cancelling_refused_count, 0);
			pg_atomic_init_u64(&shard->hint_violation_count, 0);
			pg_atomic_init_u64(&shard->legacy_pin_count, 0);
			pg_atomic_init_u64(&shard->pcm_x_stage_count, 0);
			pg_atomic_init_u64(&shard->pcm_x_replay_count, 0);
			pg_atomic_init_u64(&shard->pcm_x_release_count, 0);
			pg_atomic_init_u64(&shard->pcm_x_failclosed_count, 0);
			pg_atomic_init_u32(&shard->entry_count, 0);
		}
		for (i = 0; i < NBuffers; i++) {
			SpinLockInit(&cluster_gcs_stale_x_durable_seal_slots[i].lock);
			memset(&cluster_gcs_stale_x_durable_seal_slots[i].seal, 0,
				   sizeof(cluster_gcs_stale_x_durable_seal_slots[i].seal));
		}
	}

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(GcsBlockDedupKey);
	info.entrysize = sizeof(GcsBlockDedupEntry);

	for (i = 0; i < n; i++) {
		char hname[96];

		/* shard 0 keeps the spec-2.34 name so lms_workers=1 is a
		 * byte-identical topology (spec-7.3 §3.5). */
		if (i == 0)
			snprintf(hname, sizeof(hname), "pgrac cluster gcs block dedup htab");
		else
			snprintf(hname, sizeof(hname), "pgrac cluster gcs block dedup htab %d", i);

		cluster_gcs_block_dedup_htabs[i]
			= ShmemInitHash(hname, cap, cap, &info, HASH_ELEM | HASH_BLOBS);
	}

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(GcsStaleXReleaseKey);
	info.entrysize = sizeof(GcsStaleXReleaseRecord);
	for (i = 0; i < n; i++) {
		char hname[96];

		snprintf(hname, sizeof(hname), "pgrac stale x release journal %d", i);
		cluster_gcs_stale_x_release_htabs[i]
			= ShmemInitHash(hname, cap, cap, &info, HASH_ELEM | HASH_BLOBS);
	}

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(GcsStaleXReleaseKey);
	info.entrysize = sizeof(GcsBlockEvictionGateRecord);
	for (i = 0; i < n; i++) {
		char hname[96];

		snprintf(hname, sizeof(hname), "pgrac block eviction gate %d", i);
		cluster_gcs_block_eviction_gate_htabs[i]
			= ShmemInitHash(hname, cap, cap, &info, HASH_ELEM | HASH_BLOBS);
	}

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(RelFileLocator);
	info.entrysize = sizeof(GcsStaleXRelationGenerationEntry);
	for (i = 0; i < n; i++) {
		char hname[96];

		snprintf(hname, sizeof(hname),
				 "pgrac stale x relation generation %d", i);
		cluster_gcs_stale_x_relation_htabs[i]
			= ShmemInitHash(hname, cap, cap, &info, HASH_ELEM | HASH_BLOBS);
	}

}

static const ClusterShmemRegion cluster_gcs_block_dedup_region = {
	.name = "pgrac cluster gcs block dedup",
	.size_fn = cluster_gcs_block_dedup_shmem_size,
	.init_fn = cluster_gcs_block_dedup_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "cluster_gcs_block_dedup",
	.reserved_flags = 0,
};

void
cluster_gcs_block_dedup_module_init(void)
{
	cluster_shmem_register_region(&cluster_gcs_block_dedup_region);
}


/* ============================================================
 * spec-7.3 D5 — shard resolution.  Returns the shard for a hot-path
 * worker_id, or NULL (fail-closed) when the module is not initialized or
 * the worker_id is out of the live range (a mis-route, counted).
 * ============================================================ */
static ClusterGcsBlockDedupShard *
cluster_gcs_block_dedup_resolve_shard(int worker_id, HTAB **htab_out)
{
	if (cluster_gcs_block_dedup_ctl == NULL || cluster_gcs_block_dedup_shards == NULL)
		return NULL; /* module off — fail-closed, not a mis-route */

	if (worker_id < 0 || worker_id >= cluster_gcs_block_dedup_n_shards
		|| cluster_gcs_block_dedup_htabs[worker_id] == NULL) {
		/* A block tag reached the wrong worker: shard key = tag alone, and
		 * D3 negotiates a cluster-wide n_workers, so this is a code-path
		 * invariant break.  Fail closed (8.A), never serve wrong shard. */
		cluster_gcs_block_dedup_note_misroute();
		return NULL;
	}

	if (htab_out != NULL)
		*htab_out = cluster_gcs_block_dedup_htabs[worker_id];
	return &cluster_gcs_block_dedup_shards[worker_id];
}

static ClusterGcsBlockDedupShard *
cluster_gcs_block_stale_x_release_resolve_shard(int worker_id, HTAB **htab_out)
{
	if (cluster_gcs_block_dedup_ctl == NULL || cluster_gcs_block_dedup_shards == NULL)
		return NULL;
	if (worker_id < 0 || worker_id >= cluster_gcs_block_dedup_n_shards
		|| cluster_gcs_stale_x_release_htabs[worker_id] == NULL) {
		cluster_gcs_block_dedup_note_misroute();
		return NULL;
	}
	if (htab_out != NULL)
		*htab_out = cluster_gcs_stale_x_release_htabs[worker_id];
	return &cluster_gcs_block_dedup_shards[worker_id];
}

static ClusterGcsBlockDedupShard *
cluster_gcs_block_eviction_gate_resolve_shard(int worker_id, HTAB **htab_out)
{
	if (cluster_gcs_block_dedup_ctl == NULL || cluster_gcs_block_dedup_shards == NULL)
		return NULL;
	if (worker_id < 0 || worker_id >= cluster_gcs_block_dedup_n_shards
		|| cluster_gcs_block_eviction_gate_htabs[worker_id] == NULL) {
		cluster_gcs_block_dedup_note_misroute();
		return NULL;
	}
	if (htab_out != NULL)
		*htab_out = cluster_gcs_block_eviction_gate_htabs[worker_id];
	return &cluster_gcs_block_dedup_shards[worker_id];
}

static uint32
dedup_stale_x_relation_hash(RelFileLocator locator)
{
	uint32 h = UINT32_C(2166136261);

#define STALE_X_REL_MIX(v) (h = (h ^ (uint32)(v)) * UINT32_C(16777619))
	STALE_X_REL_MIX(locator.spcOid);
	STALE_X_REL_MIX(locator.dbOid);
	STALE_X_REL_MIX(locator.relNumber);
#undef STALE_X_REL_MIX
	return h;
}

static ClusterGcsBlockDedupShard *
cluster_gcs_block_stale_x_relation_resolve(
	RelFileLocator locator, HTAB **htab_out)
{
	int worker_id;

	if (cluster_gcs_block_dedup_ctl == NULL
		|| cluster_gcs_block_dedup_shards == NULL
		|| cluster_gcs_block_dedup_n_shards <= 0)
		return NULL;
	worker_id = (int)(
		dedup_stale_x_relation_hash(locator)
		% (uint32)cluster_gcs_block_dedup_n_shards);
	if (cluster_gcs_stale_x_relation_htabs[worker_id] == NULL)
		return NULL;
	if (htab_out != NULL)
		*htab_out = cluster_gcs_stale_x_relation_htabs[worker_id];
	return &cluster_gcs_block_dedup_shards[worker_id];
}

/* ============================================================
 * Backend-exit hook registration (idempotent; called by the sender/backend
 * path once per backend before it issues block requests).
 * ============================================================ */

static void
dedup_backend_exit_callback(int code pg_attribute_unused(), Datum arg pg_attribute_unused())
{
	if (cluster_node_id < 0 || MyBackendId <= 0)
		return;
	cluster_gcs_block_dedup_cleanup_on_backend_exit((uint32)cluster_node_id, (int32)MyBackendId);
}

void
cluster_gcs_block_dedup_register_backend_exit_hook(void)
{
	if (dedup_backend_exit_hook_registered)
		return;
	if (!IsUnderPostmaster)
		return; /* only meaningful in backends */
	if (MyBackendId <= 0)
		return; /* auxiliary processes do not own backend ids */
	before_shmem_exit(dedup_backend_exit_callback, (Datum)0);
	dedup_backend_exit_hook_registered = true;
}


/* ============================================================
 * PCM-X image-entry validation.
 * ============================================================ */

static void
dedup_pcm_x_note_failclosed(ClusterGcsBlockDedupShard *shard)
{
	if (shard != NULL)
		pg_atomic_fetch_add_u64(&shard->pcm_x_failclosed_count, 1);
}

/* The generic smart-fusion metadata cell is eight bytes and PCM-X entry
 * kinds never use it.  Keep the exact revoke token there without changing
 * the fixed shared-memory entry layout. */
static uint64
dedup_pcm_x_reservation_token_get(const GcsBlockDedupEntry *entry)
{
	uint64 token;

	memcpy(&token, &entry->has_sf_dep, sizeof(token));
	return token;
}

static void
dedup_pcm_x_reservation_token_set(GcsBlockDedupEntry *entry, uint64 token)
{
	memcpy(&entry->has_sf_dep, &token, sizeof(token));
}

static bool
dedup_pcm_x_source_state_valid(uint8 pcm_state)
{
	return pcm_state == (uint8)PCM_STATE_N || pcm_state == (uint8)PCM_STATE_S
		   || pcm_state == (uint8)PCM_STATE_X;
}

static uint32
dedup_pcm_x_block_checksum(const char *block_data)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, block_data, GCS_BLOCK_DATA_SIZE);
	FIN_CRC32C(crc);
	return (uint32)crc;
}

static bool
dedup_pcm_x_key_valid(const GcsBlockDedupKey *key)
{
	return key != NULL && key->origin_node_id < PCM_X_PROTOCOL_NODE_LIMIT
		   && key->requester_backend_id > 0
		   && cluster_pcm_x_image_id_decode(key->request_id, NULL, NULL);
}

static bool
dedup_pcm_x_binding_valid(const GcsBlockDedupKey *key, const BufferTag *tag,
						  const GcsBlockPcmXImageBinding *binding, bool reserved)
{
	const PcmXTicketRef *ref;
	const PcmXImageToken *image;
	int32 requester_node;
	int32 requester_backend_id;

	if (!dedup_pcm_x_key_valid(key) || tag == NULL || binding == NULL
		|| binding->master_session == 0)
		return false;

	ref = &binding->identity.ref;
	image = &binding->identity.image;
	if (memcmp(&ref->identity.tag, tag, sizeof(*tag)) != 0
		|| ref->identity.node_id != (int32)key->origin_node_id
		|| ref->identity.cluster_epoch != key->cluster_epoch || ref->identity.wait_seq == 0
		|| ref->handle.ticket_id == 0 || ref->handle.queue_generation == 0
		|| ref->grant_generation == 0 || image->image_id != key->request_id
		|| image->source_node >= PCM_X_PROTOCOL_NODE_LIMIT || cluster_node_id < 0
		|| cluster_node_id >= PCM_X_PROTOCOL_NODE_LIMIT
		|| image->source_node != (uint32)cluster_node_id)
		return false;

	if (!cluster_gcs_requester_id_decode(ref->identity.request_id, &requester_node,
										 &requester_backend_id, NULL)
		|| requester_node != ref->identity.node_id
		|| requester_backend_id != key->requester_backend_id)
		return false;

	if (reserved && (image->page_scn != 0 || image->page_lsn != 0 || image->page_checksum != 0))
		return false;
	return true;
}

/* Generic entries retain the established signed TTL meaning of
 * pinned_lifetime_us.  PCM-X entries are excluded from generic GC, so that
 * same fixed 8-byte cell carries their exact required source SCN without
 * changing the fixed-size entry ABI again. */
static uint64
dedup_pcm_x_required_page_scn_get(const GcsBlockDedupEntry *entry)
{
	return entry != NULL ? (uint64)entry->pinned_lifetime_us : 0;
}

static void
dedup_pcm_x_binding_from_entry(const GcsBlockDedupEntry *entry, GcsBlockPcmXImageBinding *binding)
{
	binding->identity = entry->payload_meta.pcm_x_identity;
	binding->master_session = entry->pcm_x_master_session;
	binding->required_page_scn = dedup_pcm_x_required_page_scn_get(entry);
}

static bool
dedup_pcm_x_reservation_equal(const GcsBlockDedupEntry *entry,
							  const GcsBlockPcmXImageBinding *binding)
{
	const GcsBlockPcmXImageIdentity *stored = &entry->payload_meta.pcm_x_identity;
	const GcsBlockPcmXImageIdentity *incoming = &binding->identity;

	return entry->pcm_x_master_session == binding->master_session
		   && dedup_pcm_x_required_page_scn_get(entry) == binding->required_page_scn
		   && memcmp(&stored->ref, &incoming->ref, sizeof(stored->ref)) == 0
		   && stored->image.image_id == incoming->image.image_id
		   && stored->image.source_own_generation == incoming->image.source_own_generation
		   && stored->image.source_node == incoming->image.source_node;
}

static bool
dedup_pcm_x_ready_payload_valid(const GcsBlockDedupKey *key, const BufferTag *tag,
								const GcsBlockPcmXImageBinding *binding,
								const GcsBlockReplyHeader *reply_header, const char *block_data)
{
	const PcmXImageToken *image;
	PageHeaderData page_header;
	static const uint8 zero_reserved[sizeof(reply_header->reserved_0)] = { 0 };

	if (!dedup_pcm_x_binding_valid(key, tag, binding, false) || reply_header == NULL
		|| block_data == NULL)
		return false;

	image = &binding->identity.image;
	if (reply_header->request_id != key->request_id || reply_header->page_lsn != image->page_lsn
		|| reply_header->epoch != key->cluster_epoch
		|| reply_header->checksum != image->page_checksum
		|| reply_header->sender_node != (int32)image->source_node
		|| reply_header->requester_backend_id != key->requester_backend_id
		|| reply_header->transition_id != (uint8)PCM_TRANS_N_TO_S
		|| reply_header->status != (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER
		|| GcsBlockReplyHeaderGetForwardingMasterNode(reply_header)
			   != GCS_BLOCK_REPLY_NO_FORWARDING_MASTER
		|| memcmp(reply_header->reserved_0, zero_reserved, sizeof(zero_reserved)) != 0)
		return false;

	memcpy(&page_header, block_data, sizeof(page_header));
	return image->page_lsn == (uint64)PageXLogRecPtrGet(page_header.pd_lsn)
		   && image->page_scn == (uint64)page_header.pd_block_scn
		   && image->page_checksum == dedup_pcm_x_block_checksum(block_data);
}

static bool
dedup_pcm_x_entry_payload_valid(const GcsBlockDedupKey *key, const BufferTag *tag,
								const GcsBlockDedupEntry *entry)
{
	GcsBlockPcmXImageBinding binding;

	dedup_pcm_x_binding_from_entry(entry, &binding);
	return entry->transition_id == (uint8)PCM_TRANS_N_TO_S
		   && entry->status == (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER
		   && dedup_pcm_x_ready_payload_valid(key, tag, &binding, &entry->reply_header,
											  entry->block_data);
}


static bool
dedup_pcm_x_entry_ready_valid(const GcsBlockDedupKey *key, const BufferTag *tag,
							  const GcsBlockDedupEntry *entry)
{
	return entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE
		   && dedup_pcm_x_entry_payload_valid(key, tag, entry);
}


static bool
dedup_pcm_x_entry_drained_valid(const GcsBlockDedupKey *key, const BufferTag *tag,
								const GcsBlockDedupEntry *entry)
{
	return entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_DRAINED && entry->request_flags > 0
		   && entry->request_flags <= (uint8)PCM_X_PROTOCOL_NODE_LIMIT
		   && dedup_pcm_x_reservation_token_get(entry) == 0
		   && dedup_pcm_x_entry_payload_valid(key, tag, entry);
}


/* ============================================================
 * Public API.
 * ============================================================ */

GcsBlockDedupResult
cluster_gcs_block_dedup_lookup_or_register(int worker_id, const GcsBlockDedupKey *key,
										   BufferTag tag, uint8 transition_id,
										   uint32 requester_lifetime_hint_ms,
										   bool requester_done_capable,
										   GcsBlockDedupEntry *cached_reply_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	bool found;
	GcsBlockDedupResult result;

	Assert(key != NULL);
	if (cached_reply_out != NULL)
		memset(cached_reply_out, 0, sizeof(*cached_reply_out));

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_DEDUP_FULL; /* not initialized / mis-route; fail closed */
	if (cluster_pcm_x_image_id_decode(key->request_id, NULL, NULL)) {
		dedup_pcm_x_note_failclosed(shard);
		return GCS_BLOCK_DEDUP_VALIDATION_FAIL;
	}

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);

	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);

	if (found) {
		if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_GENERIC) {
			dedup_pcm_x_note_failclosed(shard);
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_DEDUP_VALIDATION_FAIL;
		}

		/* HC91 — entry value collision check */
		if (memcmp(&entry->tag, &tag, sizeof(BufferTag)) != 0
			|| entry->transition_id != transition_id) {
			pg_atomic_fetch_add_u64(&shard->collision_count, 1);
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_DEDUP_VALIDATION_FAIL;
		}

		/*
		 * A forward marker has its own publish/claim lifecycle.  PREPARED is
		 * deliberately invisible and SEND_ARMED has one exact sender, so a
		 * duplicate must remain silent in either phase.  FORWARDED becomes
		 * replayable only after send_finish publishes the transport outcome.
		 * Exact DONE is terminal and must never trigger another forward.
		 */
		{
			GcsBlockForwardMarkerPhase marker_phase
				= GcsBlockDedupEntryForwardMarkerPhase(entry);

			if (GcsBlockDedupEntryHasForwardMarker(entry)
				&& marker_phase == GCS_BLOCK_FORWARD_MARK_NONE) {
				LWLockRelease(&shard->lock.lock);
				return GCS_BLOCK_DEDUP_VALIDATION_FAIL;
			}
			if (GcsBlockDedupEntryHasForwardMarker(entry)) {
				if (entry->done_at_ts != 0) {
					LWLockRelease(&shard->lock.lock);
					return GCS_BLOCK_DEDUP_DONE_DUPLICATE;
				}
				if (marker_phase == GCS_BLOCK_FORWARD_MARK_PREPARED
					|| marker_phase == GCS_BLOCK_FORWARD_MARK_SEND_ARMED
					|| marker_phase == GCS_BLOCK_FORWARD_MARK_HOLDER_MISS_PENDING
					|| marker_phase == GCS_BLOCK_FORWARD_MARK_HOLDER_FENCE_ACKED
					|| marker_phase == GCS_BLOCK_FORWARD_MARK_CANCELLING
					|| marker_phase == GCS_BLOCK_FORWARD_MARK_CANCEL_FENCED) {
					LWLockRelease(&shard->lock.lock);
					return GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE;
				}
				if (marker_phase == GCS_BLOCK_FORWARD_MARK_FORWARDED) {
					if (cached_reply_out != NULL)
						*cached_reply_out = *entry;
					LWLockRelease(&shard->lock.lock);
					return GCS_BLOCK_DEDUP_FORWARDED_DUPLICATE;
				}
				/* COMMITTED/KEEP_FENCED are terminal cached-reply phases.
				 * They are populated only by exact certificate/abort paths. */
				if (entry->completed_at_ts == 0) {
					LWLockRelease(&shard->lock.lock);
					return GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE;
				}
				if (cached_reply_out != NULL)
					*cached_reply_out = *entry;
				LWLockRelease(&shard->lock.lock);
				return GCS_BLOCK_DEDUP_CACHED_REPLY;
			}
		}

		/* PGRAC: spec-2.35 HC113/HC114 — forwarded entry path.  Master
		 * previously installed this entry with status=GRANTED_FROM_HOLDER
		 * to mark "forward in flight"; reply_header.sender_node carries
		 * the holder node id (not an 8KB cached block).  Caller must
		 * re-forward to the same holder rather than treat as silent
		 * duplicate.  This branch fires WHETHER OR NOT completed_at_ts
		 * is zero — the forward install_reply path stamps completed_at_ts
		 * so TTL sweep can age the entry; consumers distinguish FORWARDED
		 * from genuine CACHED_REPLY via the status field.
		 *
		 * A READ_IMAGE_FROM_XHOLDER entry is a forward marker TOO when it
		 * came from the xheld-read FORWARD install (no page payload;
		 * forwarding_master_node stamped).  Classifying that marker as
		 * CACHED_REPLY resends it payload-less: its header checksum was
		 * never computed (0), and the 31-hash of the all-zero page is also
		 * 0, so the resend VERIFIES at the requester and installs a zero
		 * page — a PageIsNew false-empty read (8.A).  The master-DIRECT
		 * xheld serve also installs READ_IMAGE but WITH the page and with
		 * NO_FORWARDING_MASTER — that one is a genuine cached reply, so the
		 * forwarding_master_node field is the discriminator. */
		if (entry->status == (uint8)GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER
			|| entry->status == (uint8)GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER
			|| (entry->status == (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER
				&& GcsBlockReplyHeaderGetForwardingMasterNode(&entry->reply_header)
					   != GCS_BLOCK_REPLY_NO_FORWARDING_MASTER)) {
			if (cached_reply_out != NULL)
				*cached_reply_out = *entry;
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_DEDUP_FORWARDED_DUPLICATE;
		}

		if (entry->completed_at_ts == 0) {
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE;
		}

		pg_atomic_fetch_add_u64(&shard->hit_count, 1);
		if (cached_reply_out != NULL)
			*cached_reply_out = *entry;
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_DEDUP_CACHED_REPLY;
	}

	/*
	 * MISS path — registration-time TTL routing (review F5 / calibration 2).
	 * Validate the wire hint BEFORE inserting: a violating request never
	 * claims a slot.  A GCS_DONE_V1-capable peer MUST carry its legal
	 * lifetime -- hint 0 (protocol violation) or a hint above what any
	 * legal configuration could produce (would pin the 8KB slot for days)
	 * is counted and DENIED, never served.
	 */
	if (requester_done_capable
		&& (requester_lifetime_hint_ms == 0
			|| (int64)requester_lifetime_hint_ms > GCS_BLOCK_DEDUP_MAX_PROTOCOL_LIFETIME_MS)) {
		pg_atomic_fetch_add_u64(&shard->hint_violation_count, 1);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_DEDUP_VALIDATION_FAIL;
	}

	/* MISS path — insert new in-flight slot.  HASH_ENTER_NULL → may fail
	 * with cap reached; convert to FULL fail-closed (HC92). */
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_ENTER_NULL, &found);
	if (entry == NULL) {
		/* PGRAC: spec-7.2a D1 — before failing closed, try to reclaim one
		 * reclaim-safe entry (aged past the 2x window, or a site-proven
		 * idempotent status) to make room.  Reclaim never removes an entry
		 * whose retransmitted duplicate could still be served incorrectly
		 * (§3.1); if nothing is safe to reclaim, keep the fail-closed HC92
		 * behavior below. */
		if (dedup_reclaim_reclaimable_locked(shard, htab, GetCurrentTimestamp(), 1) > 0)
			entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_ENTER_NULL, &found);
	}
	if (entry == NULL) {
		pg_atomic_fetch_add_u64(&shard->full_count, 1);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_DEDUP_FULL;
	}

	/* Reset entry to clean in-flight state. */
	memset(((char *)entry) + sizeof(GcsBlockDedupKey), 0,
		   sizeof(GcsBlockDedupEntry) - sizeof(GcsBlockDedupKey));
	entry->tag = tag;
	entry->transition_id = transition_id;
	entry->status = 0;
	entry->entry_kind = GCS_BLOCK_DEDUP_ENTRY_GENERIC;
	entry->completed_at_ts = 0;
	entry->registered_at_ts = GetCurrentTimestamp();

	/*
	 * GCS-race round-2 RC-F + review F5: pin the entry's whole TTL posture
	 * NOW, by capability.  A capable peer's validated wire hint is its own
	 * legal-request lifetime (backoff + reply-timeout budget); a legacy
	 * peer's window is unknowable (its GUCs may all be longer than this
	 * master's), so it pins the PROTOCOL-MAXIMUM lifetime -- an ~1 h
	 * availability cost under cap pressure (DENIED FULL), never an early
	 * reclaim (the master-formula fallback re-opened the re-execution P0).
	 * Sweep and reclaim consume only these pinned values -- a later SUSET
	 * change on the master can never re-shorten the window a live request
	 * registered under.
	 */
	entry->done_at_ts = 0;
	if (requester_done_capable)
		entry->pinned_lifetime_us = (int64)requester_lifetime_hint_ms * 1000 * 2;
	else {
		entry->pinned_lifetime_us = GCS_BLOCK_DEDUP_MAX_PROTOCOL_LIFETIME_MS * 1000 * 2;
		pg_atomic_fetch_add_u64(&shard->legacy_pin_count, 1);
	}
	entry->pinned_done_linger_us
		= (int64)(cluster_gcs_reply_timeout_ms > 0 ? cluster_gcs_reply_timeout_ms : 5000) * 1000
		  * 2;

	pg_atomic_fetch_add_u32(&shard->entry_count, 1);
	pg_atomic_fetch_add_u64(&shard->miss_count, 1);
	result = GCS_BLOCK_DEDUP_MISS_REGISTERED;

	LWLockRelease(&shard->lock.lock);
	return result;
}

static bool
dedup_forward_marker_authority_valid(const PcmAuthoritySnapshot *authority,
									 int32 expected_holder_node,
									 GcsBlockReplyStatus status, uint8 transition_id)
{
	uint32 holder_bit;

	if (authority == NULL || authority->authority_generation == 0 || expected_holder_node < 0
		|| expected_holder_node >= 32
		|| ((authority->pending_x_requester_node == -1)
			!= (authority->pending_x_since_lsn == 0)))
		return false;
	holder_bit = UINT32_C(1) << (uint32)expected_holder_node;

	switch (status) {
	case GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER:
		return transition_id == (uint8)PCM_TRANS_N_TO_S && authority->state == PCM_STATE_S
			   && authority->x_holder_node == -1
			   && (authority->s_holders_bitmap & holder_bit) != 0
			   && authority->master_holder.node_id == (uint32)expected_holder_node;
	case GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER:
		return transition_id == (uint8)PCM_TRANS_N_TO_S && authority->state == PCM_STATE_X
			   && authority->x_holder_node == expected_holder_node
			   && authority->s_holders_bitmap == 0
			   && authority->master_holder.node_id == (uint32)expected_holder_node;
	case GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER:
		return (transition_id == (uint8)PCM_TRANS_N_TO_X
				|| transition_id == (uint8)PCM_TRANS_S_TO_X_UPGRADE)
			   && authority->state == PCM_STATE_X
			   && authority->x_holder_node == expected_holder_node
			   && authority->s_holders_bitmap == 0
			   && authority->master_holder.node_id == (uint32)expected_holder_node;
	default:
		return false;
	}
}

static bool
dedup_forward_payload_valid(const GcsBlockDedupKey *key, const BufferTag *tag,
							uint8 transition_id, int32 expected_holder_node,
							int32 forwarding_master_node, GcsBlockReplyStatus status,
							const PcmAuthoritySnapshot *authority,
							const GcsBlockForwardPayload *forward)
{
	int i;

	if (key == NULL || tag == NULL || forward == NULL || key->origin_node_id >= 32
		|| key->requester_backend_id <= 0 || key->request_id == 0
		|| forwarding_master_node < 0 || forwarding_master_node >= 32
		|| forwarding_master_node == expected_holder_node
		|| !dedup_forward_marker_authority_valid(authority, expected_holder_node, status,
											 transition_id)
		|| forward->request_id != key->request_id || forward->epoch != key->cluster_epoch
		|| memcmp(&forward->tag, tag, sizeof(*tag)) != 0
		|| forward->original_requester_node != (int32)key->origin_node_id
		|| forward->requester_backend_id != key->requester_backend_id
		|| forward->master_node != forwarding_master_node
		|| forward->transition_id != transition_id)
		return false;

	for (i = 0; i < (int)lengthof(forward->reserved_0); i++) {
		uint8 expected = 0;

		if (status == GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER) {
			if (i == 0)
				expected = 1;
			else if (i == 3) {
				if (forward->reserved_0[i] > 1)
					return false;
				expected = forward->reserved_0[i];
			}
		}
		if (forward->reserved_0[i] != expected)
			return false;
	}
	return true;
}

static void
dedup_forward_reply_header_make(const GcsBlockDedupKey *key, uint8 transition_id,
								int32 expected_holder_node, int32 forwarding_master_node,
								GcsBlockReplyStatus status, GcsBlockReplyHeader *header)
{
	memset(header, 0, sizeof(*header));
	header->request_id = key->request_id;
	header->epoch = key->cluster_epoch;
	header->sender_node = expected_holder_node;
	header->requester_backend_id = key->requester_backend_id;
	header->transition_id = transition_id;
	header->status = (uint8)status;
	GcsBlockReplyHeaderSetForwardingMasterNode(header, forwarding_master_node);
}

static void
dedup_forward_phase_set(GcsBlockDedupEntry *entry, GcsBlockForwardMarkerPhase phase)
{
	Assert(entry != NULL);
	Assert(phase >= GCS_BLOCK_FORWARD_MARK_PREPARED
		   && phase <= GCS_BLOCK_FORWARD_MARK_CANCEL_FENCED);
	entry->has_sf_dep = false;
	entry->sf_flags
		= GCS_BLOCK_DEDUP_FORWARD_MARKER_FLAG
		  | ((uint8)phase & GCS_BLOCK_DEDUP_FORWARD_PHASE_MASK);
	entry->sf_dep_count = 0;
}

static bool
dedup_forward_boot_identity_valid(
	const GcsBlockForwardBootIdentity *identity)
{
	static const uint8 zero_reserved[4] = { 0 };
	bool all_zero;
	bool all_nonzero;

	if (identity == NULL
		|| memcmp(identity->reserved, zero_reserved,
				  sizeof(identity->reserved)) != 0)
		return false;
	all_zero = identity->requester_incarnation == 0
			   && identity->master_incarnation == 0
			   && identity->holder_incarnation == 0
			   && identity->relation_generation == 0
			   && identity->capability_generation == 0;
	all_nonzero = identity->requester_incarnation != 0
				  && identity->master_incarnation != 0
				  && identity->holder_incarnation != 0
				  && identity->relation_generation != 0
				  && identity->capability_generation != 0;
	return all_zero || all_nonzero;
}

static bool
dedup_forward_entry_exact(const GcsBlockDedupEntry *entry, const BufferTag *tag,
						  int32 expected_holder_node, GcsBlockReplyStatus status,
						  const GcsBlockForwardMarker *marker,
						  GcsBlockForwardMarkerPhase phase)
{
	GcsBlockReplyHeader header;

	if (entry == NULL || tag == NULL || marker == NULL
		|| !dedup_forward_boot_identity_valid(&marker->boot_identity)
		|| entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_GENERIC
		|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0
		|| entry->transition_id != marker->forward.transition_id
		|| entry->status != (uint8)status || entry->has_sf_dep
		|| entry->sf_dep_count != 0 || GcsBlockDedupEntryForwardMarkerPhase(entry) != phase
		|| memcmp(&entry->payload_meta.forward_marker, marker, sizeof(*marker)) != 0)
		return false;

	dedup_forward_reply_header_make(&entry->key, marker->forward.transition_id,
									expected_holder_node, marker->forward.master_node, status,
									&header);
	return memcmp(&entry->reply_header, &header, sizeof(header)) == 0;
}

GcsBlockForwardMarkResult
cluster_gcs_block_dedup_forward_prepare_identity_exact(
	int worker_id, const GcsBlockDedupKey *key, const BufferTag *tag, uint8 transition_id,
	int32 expected_holder_node, int32 forwarding_master_node, GcsBlockReplyStatus status,
	const PcmAuthoritySnapshot *authority, const GcsBlockForwardPayload *forward,
	const GcsBlockForwardBootIdentity *boot_identity,
	GcsBlockDedupEntry *marker_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsBlockForwardMarker desired;
	GcsBlockReplyHeader header;
	bool found;

	if (marker_out != NULL)
		memset(marker_out, 0, sizeof(*marker_out));
	if (!dedup_forward_payload_valid(key, tag, transition_id, expected_holder_node,
									forwarding_master_node, status, authority, forward)
		|| !dedup_forward_boot_identity_valid(boot_identity))
		return GCS_BLOCK_FORWARD_MARK_INVALID;

	memset(&desired, 0, sizeof(desired));
	desired.authority = *authority;
	desired.forward = *forward;
	desired.boot_identity = *boot_identity;
	dedup_forward_reply_header_make(key, transition_id, expected_holder_node,
									forwarding_master_node, status, &header);

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_FORWARD_MARK_INVALID;
	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found || entry == NULL) {
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_FORWARD_MARK_NOT_FOUND;
	}
	if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_GENERIC
		|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0
		|| entry->transition_id != transition_id) {
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_FORWARD_MARK_STALE;
	}
	if (GcsBlockDedupEntryHasForwardMarker(entry)
		|| entry->completed_at_ts != 0 || entry->done_at_ts != 0) {
		if (entry->done_at_ts == 0
			&& dedup_forward_entry_exact(entry, tag, expected_holder_node, status, &desired,
										 GCS_BLOCK_FORWARD_MARK_PREPARED)) {
			if (marker_out != NULL)
				*marker_out = *entry;
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_FORWARD_MARK_REPLAY;
		}
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_FORWARD_MARK_STALE;
	}

	entry->status = (uint8)status;
	entry->reply_header = header;
	dedup_forward_phase_set(entry, GCS_BLOCK_FORWARD_MARK_PREPARED);
	entry->payload_meta.forward_marker = desired;
	memset(entry->block_data, 0, sizeof(entry->block_data));
	entry->completed_at_ts = 0;
	entry->done_at_ts = 0;
	if (marker_out != NULL)
		*marker_out = *entry;
	LWLockRelease(&shard->lock.lock);
	return GCS_BLOCK_FORWARD_MARK_INSTALLED;
}

GcsBlockForwardMarkResult
cluster_gcs_block_dedup_forward_prepare_exact(
	int worker_id, const GcsBlockDedupKey *key, const BufferTag *tag,
	uint8 transition_id, int32 expected_holder_node,
	int32 forwarding_master_node, GcsBlockReplyStatus status,
	const PcmAuthoritySnapshot *authority,
	const GcsBlockForwardPayload *forward, GcsBlockDedupEntry *marker_out)
{
	GcsBlockForwardBootIdentity legacy_identity;

	memset(&legacy_identity, 0, sizeof(legacy_identity));
	return cluster_gcs_block_dedup_forward_prepare_identity_exact(
		worker_id, key, tag, transition_id, expected_holder_node,
		forwarding_master_node, status, authority, forward, &legacy_identity,
		marker_out);
}

GcsBlockForwardMarkResult
cluster_gcs_block_dedup_forward_send_claim_exact(
	int worker_id, const GcsBlockDedupKey *key, const BufferTag *tag,
	int32 expected_holder_node, GcsBlockReplyStatus status,
	const GcsBlockForwardMarker *marker, GcsBlockForwardMarkerPhase expected_phase,
	GcsBlockDedupEntry *marker_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsBlockForwardMarkerPhase phase;
	bool found;

	if (marker_out != NULL)
		memset(marker_out, 0, sizeof(*marker_out));
	if (key == NULL || tag == NULL || marker == NULL
		|| (expected_phase != GCS_BLOCK_FORWARD_MARK_PREPARED
			&& expected_phase != GCS_BLOCK_FORWARD_MARK_FORWARDED)
		|| !dedup_forward_payload_valid(
			key, tag, marker->forward.transition_id, expected_holder_node,
			marker->forward.master_node, status, &marker->authority, &marker->forward))
		return GCS_BLOCK_FORWARD_MARK_INVALID;

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_FORWARD_MARK_INVALID;
	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found || entry == NULL) {
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_FORWARD_MARK_NOT_FOUND;
	}
	phase = GcsBlockDedupEntryForwardMarkerPhase(entry);
	if (entry->done_at_ts != 0) {
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_FORWARD_MARK_DONE;
	}
	if (phase == GCS_BLOCK_FORWARD_MARK_SEND_ARMED
		&& dedup_forward_entry_exact(entry, tag, expected_holder_node, status, marker, phase)) {
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_FORWARD_MARK_BUSY;
	}
	if (!dedup_forward_entry_exact(entry, tag, expected_holder_node, status, marker,
								   expected_phase)) {
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_FORWARD_MARK_STALE;
	}

	dedup_forward_phase_set(entry, GCS_BLOCK_FORWARD_MARK_SEND_ARMED);
	entry->completed_at_ts = 0;
	if (marker_out != NULL)
		*marker_out = *entry;
	LWLockRelease(&shard->lock.lock);
	return GCS_BLOCK_FORWARD_MARK_INSTALLED;
}

GcsBlockForwardMarkResult
cluster_gcs_block_dedup_forward_send_finish_exact(
	int worker_id, const GcsBlockDedupKey *key, const BufferTag *tag,
	int32 expected_holder_node, GcsBlockReplyStatus status,
	const GcsBlockForwardMarker *marker, GcsBlockDedupEntry *marker_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	bool found;

	if (marker_out != NULL)
		memset(marker_out, 0, sizeof(*marker_out));
	if (key == NULL || tag == NULL || marker == NULL
		|| !dedup_forward_payload_valid(
			key, tag, marker->forward.transition_id, expected_holder_node,
			marker->forward.master_node, status, &marker->authority, &marker->forward))
		return GCS_BLOCK_FORWARD_MARK_INVALID;
	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_FORWARD_MARK_INVALID;
	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found || entry == NULL) {
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_FORWARD_MARK_NOT_FOUND;
	}
	if (entry->done_at_ts != 0) {
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_FORWARD_MARK_DONE;
	}
	/* The holder can observe the admitted frame and report its miss before
	 * this producer reacquires the shard lock to publish FORWARDED.  The
	 * REPORT transition is stronger and owns the same lock, so finish must
	 * acknowledge that serialization rather than overwrite it. */
	if (dedup_forward_entry_exact(entry, tag, expected_holder_node, status, marker,
								  GCS_BLOCK_FORWARD_MARK_HOLDER_MISS_PENDING)) {
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_FORWARD_MARK_REPLAY;
	}
	if (!dedup_forward_entry_exact(entry, tag, expected_holder_node, status, marker,
								   GCS_BLOCK_FORWARD_MARK_SEND_ARMED)) {
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_FORWARD_MARK_STALE;
	}

	dedup_forward_phase_set(entry, GCS_BLOCK_FORWARD_MARK_FORWARDED);
	entry->completed_at_ts = GetCurrentTimestamp();
	if (marker_out != NULL)
		*marker_out = *entry;
	LWLockRelease(&shard->lock.lock);
	return GCS_BLOCK_FORWARD_MARK_INSTALLED;
}

bool
cluster_gcs_block_dedup_forward_abort_prepared_exact(
	int worker_id, const GcsBlockDedupKey *key, const BufferTag *tag,
	int32 expected_holder_node, GcsBlockReplyStatus status,
	const GcsBlockForwardMarker *marker)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	bool found;
	bool removed = false;

	if (key == NULL || tag == NULL || marker == NULL
		|| !dedup_forward_payload_valid(
			key, tag, marker->forward.transition_id, expected_holder_node,
			marker->forward.master_node, status, &marker->authority, &marker->forward))
		return false;
	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return false;
	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (found
		&& dedup_forward_entry_exact(entry, tag, expected_holder_node, status, marker,
									 GCS_BLOCK_FORWARD_MARK_PREPARED)
		&& entry->completed_at_ts == 0 && entry->done_at_ts == 0) {
		(void)hash_search(htab, key, HASH_REMOVE, &found);
		Assert(found);
		pg_atomic_fetch_sub_u32(&shard->entry_count, 1);
		removed = true;
	}
	LWLockRelease(&shard->lock.lock);
	return removed;
}

static bool
dedup_stale_x_key_matches(const GcsBlockDedupKey *key, const GcsStaleXCertPayload *cert)
{
	return key != NULL && cert != NULL && key->request_id == cert->request_id
		   && key->cluster_epoch == cert->request_epoch
		   && key->origin_node_id == (uint32)cert->requester_node
		   && key->requester_backend_id == cert->requester_backend_id;
}

static bool
dedup_forward_cancel_key_matches(
	const GcsBlockDedupKey *key,
	const GcsBlockForwardCancelPayload *cancel)
{
	return key != NULL && cancel != NULL
		   && key->request_id == cancel->request_id
		   && key->cluster_epoch == cancel->request_epoch
		   && key->origin_node_id == (uint32)cancel->requester_node
		   && key->requester_backend_id
				  == cancel->requester_backend_id;
}

static bool
dedup_forward_cancel_master_shape_valid(
	const GcsBlockDedupKey *key,
	const GcsBlockForwardCancelPayload *cancel)
{
	return dedup_forward_cancel_key_matches(key, cancel)
		   && cluster_gcs_forward_cancel_master_ingress_valid(
			   cancel, sizeof(*cancel), cancel->master_node,
			   cancel->request_epoch, cancel->master_node,
			   cancel->holder_node, cancel->master_incarnation,
			   cancel->holder_incarnation);
}

static bool
dedup_forward_cancel_barrier_shape_valid(
	const GcsBlockDedupKey *key,
	const GcsBlockForwardCancelPayload *barrier)
{
	return dedup_forward_cancel_key_matches(key, barrier)
		   && cluster_gcs_forward_cancel_barrier_ingress_valid(
			   barrier, sizeof(*barrier), barrier->holder_node,
			   barrier->request_epoch, barrier->master_node,
			   barrier->requester_node, barrier->holder_incarnation,
			   barrier->requester_incarnation);
}

/* If the master chose CANCELLING before a holder's MISS_REPORT reached it,
 * the holder already has a provisional type65 fence under the same key.
 * Only that exact, still-unacknowledged report may be atomically replaced by
 * the ordered cancel barrier.  Any stronger stale-X phase keeps ownership. */
static bool
dedup_forward_cancel_matches_provisional_stale_x(
	const GcsBlockDedupEntry *entry,
	const GcsBlockForwardCancelPayload *cancel)
{
	const GcsStaleXCertPayload *report;

	if (entry == NULL || cancel == NULL
		|| entry->entry_kind
			   != GCS_BLOCK_DEDUP_ENTRY_STALE_X_HOLDER_FENCE
		|| entry->transition_id != (uint8)PCM_TRANS_N_TO_S)
		return false;
	report = &entry->payload_meta.stale_x_cert;
	return report->phase == (uint8)GCS_STALE_X_CERT_PHASE_MISS_REPORT
		   && report->proof == GCS_STALE_X_PROOF_REPORT_MASK
		   && report->pre_authority_generation == 0
		   && report->post_authority_generation == 0
		   && report->request_id == cancel->request_id
		   && report->request_epoch == cancel->request_epoch
		   && report->relation_generation == cancel->relation_generation
		   && (uint64)report->final_page_scn
				  == cancel->expected_pi_watermark_scn
		   && report->requester_incarnation
				  == cancel->requester_incarnation
		   && report->master_incarnation == cancel->master_incarnation
		   && report->holder_incarnation == cancel->holder_incarnation
		   && memcmp(&report->tag, &cancel->tag,
					 sizeof(cancel->tag)) == 0
		   && memcmp(&entry->tag, &cancel->tag,
					 sizeof(cancel->tag)) == 0
		   && report->requester_node == cancel->requester_node
		   && report->requester_backend_id
				  == cancel->requester_backend_id
		   && report->master_node == cancel->master_node
		   && report->holder_node == cancel->holder_node
		   && report->transition_id == cancel->transition_id;
}

GcsBlockForwardCancelResult
cluster_gcs_block_dedup_forward_cancel_holder_install(
	int worker_id, const GcsBlockDedupKey *key,
	const GcsBlockForwardCancelPayload *cancel,
	uint32 holder_requester_capability_generation,
	GcsBlockForwardCancelPayload *barrier_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsBlockForwardCancelPayload barrier;
	bool found = false;

	if (barrier_out != NULL)
		memset(barrier_out, 0, sizeof(*barrier_out));
	if (!dedup_forward_cancel_master_shape_valid(key, cancel)
		|| holder_requester_capability_generation == 0)
		return GCS_FORWARD_CANCEL_INVALID;
	barrier = *cancel;
	barrier.phase = (uint8)GCS_FORWARD_CANCEL_PHASE_HOLDER_BARRIER;
	barrier.proof = GCS_FORWARD_CANCEL_PROOF_BARRIER_MASK;
	barrier.holder_requester_capability_generation
		= holder_requester_capability_generation;
	if (!dedup_forward_cancel_barrier_shape_valid(key, &barrier))
		return GCS_FORWARD_CANCEL_INVALID;
	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_FORWARD_CANCEL_INVALID;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(
		htab, key, HASH_FIND, &found);
	if (found) {
		if (dedup_forward_cancel_matches_provisional_stale_x(
				entry, cancel)) {
			memset(((char *)entry) + sizeof(GcsBlockDedupKey), 0,
				   sizeof(*entry) - sizeof(GcsBlockDedupKey));
			entry->tag = barrier.tag;
			entry->transition_id = barrier.transition_id;
			entry->entry_kind
				= GCS_BLOCK_DEDUP_ENTRY_FORWARD_CANCEL_HOLDER;
			entry->payload_meta.forward_cancel = barrier;
			entry->registered_at_ts = GetCurrentTimestamp();
			if (barrier_out != NULL)
				*barrier_out = barrier;
			LWLockRelease(&shard->lock.lock);
			return GCS_FORWARD_CANCEL_INSTALLED;
		}
		if (entry->entry_kind
				== GCS_BLOCK_DEDUP_ENTRY_FORWARD_CANCEL_HOLDER
			&& memcmp(&entry->tag, &barrier.tag,
					  sizeof(barrier.tag)) == 0
			&& entry->transition_id == barrier.transition_id
			&& memcmp(&entry->payload_meta.forward_cancel,
					  &barrier, sizeof(barrier)) == 0) {
			if (barrier_out != NULL)
				*barrier_out
					= entry->payload_meta.forward_cancel;
			LWLockRelease(&shard->lock.lock);
			return GCS_FORWARD_CANCEL_REPLAY;
		}
		LWLockRelease(&shard->lock.lock);
		return GCS_FORWARD_CANCEL_STALE;
	}

	entry = (GcsBlockDedupEntry *)hash_search(
		htab, key, HASH_ENTER_NULL, &found);
	if (entry == NULL || found) {
		pg_atomic_fetch_add_u64(&shard->full_count, 1);
		LWLockRelease(&shard->lock.lock);
		return GCS_FORWARD_CANCEL_FULL;
	}
	memset(((char *)entry) + sizeof(GcsBlockDedupKey), 0,
		   sizeof(*entry) - sizeof(GcsBlockDedupKey));
	entry->tag = barrier.tag;
	entry->transition_id = barrier.transition_id;
	entry->entry_kind
		= GCS_BLOCK_DEDUP_ENTRY_FORWARD_CANCEL_HOLDER;
	entry->payload_meta.forward_cancel = barrier;
	entry->registered_at_ts = GetCurrentTimestamp();
	pg_atomic_fetch_add_u32(&shard->entry_count, 1);
	if (barrier_out != NULL)
		*barrier_out = barrier;
	LWLockRelease(&shard->lock.lock);
	return GCS_FORWARD_CANCEL_INSTALLED;
}

GcsBlockForwardCancelResult
cluster_gcs_block_dedup_forward_cancel_holder_lookup(
	int worker_id, const GcsBlockDedupKey *key,
	const BufferTag *tag,
	GcsBlockForwardCancelPayload *barrier_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	bool found = false;

	if (barrier_out != NULL)
		memset(barrier_out, 0, sizeof(*barrier_out));
	if (key == NULL || tag == NULL)
		return GCS_FORWARD_CANCEL_INVALID;
	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_FORWARD_CANCEL_INVALID;
	LWLockAcquire(&shard->lock.lock, LW_SHARED);
	entry = (GcsBlockDedupEntry *)hash_search(
		htab, key, HASH_FIND, &found);
	if (!found || entry == NULL) {
		LWLockRelease(&shard->lock.lock);
		return GCS_FORWARD_CANCEL_NOT_FOUND;
	}
	if (entry->entry_kind
			!= GCS_BLOCK_DEDUP_ENTRY_FORWARD_CANCEL_HOLDER
		|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0
		|| entry->transition_id != (uint8)PCM_TRANS_N_TO_S
		|| !dedup_forward_cancel_barrier_shape_valid(
			key, &entry->payload_meta.forward_cancel)) {
		LWLockRelease(&shard->lock.lock);
		return GCS_FORWARD_CANCEL_STALE;
	}
	if (barrier_out != NULL)
		*barrier_out = entry->payload_meta.forward_cancel;
	LWLockRelease(&shard->lock.lock);
	return GCS_FORWARD_CANCEL_REPLAY;
}

bool
cluster_gcs_block_dedup_forward_cancel_holder_admitted_exact(
	int worker_id, const GcsBlockDedupKey *key,
	const GcsBlockForwardCancelPayload *barrier)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	bool found = false;
	bool removed = false;

	if (!dedup_forward_cancel_barrier_shape_valid(key, barrier))
		return false;
	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return false;
	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(
		htab, key, HASH_FIND, &found);
	if (found && entry != NULL
		&& entry->entry_kind
			   == GCS_BLOCK_DEDUP_ENTRY_FORWARD_CANCEL_HOLDER
		&& memcmp(&entry->tag, &barrier->tag,
				  sizeof(barrier->tag)) == 0
		&& entry->transition_id == barrier->transition_id
		&& memcmp(&entry->payload_meta.forward_cancel, barrier,
				  sizeof(*barrier)) == 0) {
		(void)hash_search(htab, key, HASH_REMOVE, &found);
		Assert(found);
		pg_atomic_fetch_sub_u32(&shard->entry_count, 1);
		removed = true;
	}
	LWLockRelease(&shard->lock.lock);
	return removed;
}

static bool
dedup_stale_x_report_shape_valid(const GcsBlockDedupKey *key,
								 const GcsStaleXCertPayload *report)
{
	return dedup_stale_x_key_matches(key, report)
		   && cluster_gcs_stale_x_report_ingress_valid(
			   report, sizeof(*report), report->holder_node, report->request_epoch,
			   report->master_node, report->master_node, report->holder_incarnation,
			   report->master_incarnation);
}

static bool
dedup_stale_x_install_shape_valid(const GcsBlockDedupKey *key,
								  const GcsStaleXCertPayload *install)
{
	return dedup_stale_x_key_matches(key, install)
		   && cluster_gcs_stale_x_fence_install_ingress_valid(
			   install, sizeof(*install), install->master_node, install->request_epoch,
			   install->master_node, install->holder_node, install->master_incarnation,
			   install->holder_incarnation);
}

static bool
dedup_stale_x_ack_shape_valid(const GcsBlockDedupKey *key,
							  const GcsStaleXCertPayload *ack)
{
	return dedup_stale_x_key_matches(key, ack)
		   && cluster_gcs_stale_x_fence_ack_ingress_valid(
			   ack, sizeof(*ack), ack->holder_node, ack->request_epoch, ack->master_node,
			   ack->master_node, ack->holder_incarnation, ack->master_incarnation);
}

static bool
dedup_stale_x_commit_shape_valid(const GcsBlockDedupKey *key,
								 const GcsStaleXCertPayload *commit)
{
	return dedup_stale_x_key_matches(key, commit)
		   && cluster_gcs_stale_x_commit_ingress_valid(
			   commit, sizeof(*commit), commit->master_node, commit->request_epoch,
			   commit->master_node, commit->holder_node, commit->master_incarnation,
			   commit->holder_incarnation);
}

static bool
dedup_stale_x_retire_shape_valid(const GcsBlockDedupKey *key,
								 const GcsStaleXCertPayload *retire)
{
	return dedup_stale_x_key_matches(key, retire)
		   && cluster_gcs_stale_x_fence_retire_ingress_valid(
			   retire, sizeof(*retire), retire->master_node, retire->request_epoch,
			   retire->master_node, retire->holder_node, retire->master_incarnation,
			   retire->holder_incarnation);
}

static bool
dedup_stale_x_commit_ack_shape_valid(const GcsBlockDedupKey *key,
									 const GcsStaleXCertPayload *ack)
{
	return dedup_stale_x_key_matches(key, ack)
		   && cluster_gcs_stale_x_commit_ack_ingress_valid(
			   ack, sizeof(*ack), ack->holder_node, ack->request_epoch, ack->master_node,
			   ack->master_node, ack->holder_incarnation, ack->master_incarnation);
}

static bool
dedup_stale_x_retire_ack_shape_valid(const GcsBlockDedupKey *key,
									 const GcsStaleXCertPayload *ack)
{
	return dedup_stale_x_key_matches(key, ack)
		   && cluster_gcs_stale_x_fence_retire_ack_ingress_valid(
			   ack, sizeof(*ack), ack->holder_node, ack->request_epoch, ack->master_node,
			   ack->master_node, ack->holder_incarnation, ack->master_incarnation);
}

/*
 * Compare the immutable certificate identity while allowing only the phase,
 * proof, and authority generations to evolve across REPORT -> INSTALL ->
 * ACK -> COMMIT.  Both inputs have already passed their phase validator, so
 * byte comparison after canonical zeroing is padding-safe.
 */
static bool
dedup_stale_x_same_attempt(const GcsStaleXCertPayload *left,
						   const GcsStaleXCertPayload *right)
{
	GcsStaleXCertPayload a;
	GcsStaleXCertPayload b;

	if (left == NULL || right == NULL)
		return false;
	a = *left;
	b = *right;
	a.pre_authority_generation = 0;
	a.post_authority_generation = 0;
	a.phase = 0;
	a.proof = 0;
	b.pre_authority_generation = 0;
	b.post_authority_generation = 0;
	b.phase = 0;
	b.proof = 0;
	return memcmp(&a, &b, sizeof(a)) == 0;
}

static void
dedup_stale_x_ack_from_install(const GcsStaleXCertPayload *install,
							   GcsStaleXCertPayload *ack)
{
	*ack = *install;
	ack->phase = (uint8)GCS_STALE_X_CERT_PHASE_FENCE_ACK;
	ack->proof = GCS_STALE_X_PROOF_FENCED_MASK;
}

static void
dedup_stale_x_ack_from_retire(const GcsStaleXCertPayload *retire,
							  GcsStaleXCertPayload *ack)
{
	*ack = *retire;
	ack->phase = (uint8)GCS_STALE_X_CERT_PHASE_FENCE_RETIRE_ACK;
}

static void
dedup_stale_x_ack_from_commit(const GcsStaleXCertPayload *commit,
							  GcsStaleXCertPayload *ack)
{
	*ack = *commit;
	ack->phase = (uint8)GCS_STALE_X_CERT_PHASE_COMMIT_ACK;
}

GcsStaleXFenceResult
cluster_gcs_block_dedup_stale_x_holder_report_prepare(
	int worker_id, const GcsBlockDedupKey *key, const GcsStaleXCertPayload *report,
	GcsStaleXCertPayload *stored_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	bool found = false;

	if (stored_out != NULL)
		memset(stored_out, 0, sizeof(*stored_out));
	if (!dedup_stale_x_report_shape_valid(key, report))
		return GCS_STALE_X_FENCE_INVALID;
	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_STALE_X_FENCE_INVALID;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (found) {
		if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_STALE_X_HOLDER_FENCE
			&& memcmp(&entry->tag, &report->tag, sizeof(report->tag)) == 0
			&& entry->transition_id == report->transition_id
			&& entry->payload_meta.stale_x_cert.phase
				   == (uint8)GCS_STALE_X_CERT_PHASE_MISS_REPORT
			&& memcmp(&entry->payload_meta.stale_x_cert, report, sizeof(*report)) == 0) {
			if (stored_out != NULL)
				*stored_out = entry->payload_meta.stale_x_cert;
			LWLockRelease(&shard->lock.lock);
			return GCS_STALE_X_FENCE_REPLAY;
		}
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_STALE;
	}

	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_ENTER_NULL, &found);
	if (entry == NULL || found) {
		pg_atomic_fetch_add_u64(&shard->full_count, 1);
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_FULL;
	}
	memset(((char *)entry) + sizeof(GcsBlockDedupKey), 0,
		   sizeof(*entry) - sizeof(GcsBlockDedupKey));
	entry->tag = report->tag;
	entry->transition_id = report->transition_id;
	entry->entry_kind = GCS_BLOCK_DEDUP_ENTRY_STALE_X_HOLDER_FENCE;
	entry->payload_meta.stale_x_cert = *report;
	entry->registered_at_ts = GetCurrentTimestamp();
	pg_atomic_fetch_add_u32(&shard->entry_count, 1);
	if (stored_out != NULL)
		*stored_out = *report;
	LWLockRelease(&shard->lock.lock);
	return GCS_STALE_X_FENCE_INSTALLED;
}

GcsStaleXFenceResult
cluster_gcs_block_dedup_stale_x_holder_fence_install(
	int worker_id, const GcsBlockDedupKey *key, const GcsStaleXCertPayload *install,
	GcsStaleXCertPayload *ack_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsStaleXCertPayload ack;
	GcsStaleXCertPayload *stored;
	bool found = false;

	if (ack_out != NULL)
		memset(ack_out, 0, sizeof(*ack_out));
	if (!dedup_stale_x_install_shape_valid(key, install))
		return GCS_STALE_X_FENCE_INVALID;
	dedup_stale_x_ack_from_install(install, &ack);
	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_STALE_X_FENCE_INVALID;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found || entry == NULL) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_NOT_FOUND;
	}
	if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_STALE_X_HOLDER_FENCE
		|| memcmp(&entry->tag, &install->tag, sizeof(install->tag)) != 0
		|| entry->transition_id != install->transition_id) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_STALE;
	}
	stored = &entry->payload_meta.stale_x_cert;
	if (stored->phase == (uint8)GCS_STALE_X_CERT_PHASE_MISS_REPORT) {
		if (!dedup_stale_x_report_shape_valid(key, stored)
			|| !dedup_stale_x_same_attempt(stored, install)) {
			LWLockRelease(&shard->lock.lock);
			return GCS_STALE_X_FENCE_STALE;
		}
		*stored = ack;
		if (ack_out != NULL)
			*ack_out = ack;
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_INSTALLED;
	}
	if (stored->phase == (uint8)GCS_STALE_X_CERT_PHASE_FENCE_ACK) {
		if (!dedup_stale_x_ack_shape_valid(key, stored)
			|| memcmp(stored, &ack, sizeof(ack)) != 0) {
			LWLockRelease(&shard->lock.lock);
			return GCS_STALE_X_FENCE_STALE;
		}
		if (ack_out != NULL)
			*ack_out = *stored;
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_REPLAY;
	}
	if (stored->phase == (uint8)GCS_STALE_X_CERT_PHASE_COMMIT
		&& dedup_stale_x_commit_shape_valid(key, stored)
		&& dedup_stale_x_same_attempt(stored, install)
		&& stored->pre_authority_generation == install->pre_authority_generation) {
		if (ack_out != NULL)
			*ack_out = ack;
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_REPLAY;
	}
	LWLockRelease(&shard->lock.lock);
	return GCS_STALE_X_FENCE_STALE;
}

GcsStaleXFenceResult
cluster_gcs_block_dedup_stale_x_holder_commit(
	int worker_id, const GcsBlockDedupKey *key, const GcsStaleXCertPayload *commit,
	GcsStaleXCertPayload *stored_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsStaleXCertPayload *stored;
	bool found = false;

	if (stored_out != NULL)
		memset(stored_out, 0, sizeof(*stored_out));
	if (!dedup_stale_x_commit_shape_valid(key, commit))
		return GCS_STALE_X_FENCE_INVALID;
	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_STALE_X_FENCE_INVALID;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found || entry == NULL) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_NOT_FOUND;
	}
	if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_STALE_X_HOLDER_FENCE
		|| memcmp(&entry->tag, &commit->tag, sizeof(commit->tag)) != 0
		|| entry->transition_id != commit->transition_id) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_STALE;
	}
	stored = &entry->payload_meta.stale_x_cert;
	if (stored->phase == (uint8)GCS_STALE_X_CERT_PHASE_COMMIT) {
		if (dedup_stale_x_commit_shape_valid(key, stored)
			&& memcmp(stored, commit, sizeof(*commit)) == 0) {
			if (stored_out != NULL)
				*stored_out = *stored;
			LWLockRelease(&shard->lock.lock);
			return GCS_STALE_X_FENCE_REPLAY;
		}
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_STALE;
	}
	if (!dedup_stale_x_ack_shape_valid(key, stored)
		|| !dedup_stale_x_same_attempt(stored, commit)
		|| stored->pre_authority_generation != commit->pre_authority_generation) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_STALE;
	}
	*stored = *commit;
	if (stored_out != NULL)
		*stored_out = *stored;
	LWLockRelease(&shard->lock.lock);
	return GCS_STALE_X_FENCE_COMMITTED;
}

GcsStaleXFenceResult
cluster_gcs_block_dedup_stale_x_holder_retire(
	int worker_id, const GcsBlockDedupKey *key, const GcsStaleXCertPayload *retire,
	GcsStaleXCertPayload *ack_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsStaleXCertPayload ack;
	GcsStaleXCertPayload *stored;
	bool found = false;

	if (ack_out != NULL)
		memset(ack_out, 0, sizeof(*ack_out));
	if (!dedup_stale_x_retire_shape_valid(key, retire))
		return GCS_STALE_X_FENCE_INVALID;
	dedup_stale_x_ack_from_retire(retire, &ack);
	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_STALE_X_FENCE_INVALID;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found || entry == NULL) {
		/*
		 * An exact current-master RETIRE is replayable after the first
		 * delete.  The request key is not reusable within an epoch, and the
		 * master may only emit RETIRE after exact COMMIT_ACK plus requester
		 * quiescence, so absence means the prior RETIRE succeeded and only
		 * its ACK was lost.
		 */
		if (ack_out != NULL)
			*ack_out = ack;
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_REPLAY;
	}
	if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_STALE_X_HOLDER_FENCE
		|| memcmp(&entry->tag, &retire->tag, sizeof(retire->tag)) != 0
		|| entry->transition_id != retire->transition_id) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_STALE;
	}
	stored = &entry->payload_meta.stale_x_cert;
	if (stored->phase != (uint8)GCS_STALE_X_CERT_PHASE_COMMIT
		|| !dedup_stale_x_commit_shape_valid(key, stored)
		|| !dedup_stale_x_same_attempt(stored, retire)
		|| stored->pre_authority_generation != retire->pre_authority_generation
		|| stored->post_authority_generation != retire->post_authority_generation) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_STALE;
	}
	(void)hash_search(htab, key, HASH_REMOVE, &found);
	Assert(found);
	pg_atomic_fetch_sub_u32(&shard->entry_count, 1);
	if (ack_out != NULL)
		*ack_out = ack;
	LWLockRelease(&shard->lock.lock);
	return GCS_STALE_X_FENCE_RETIRED;
}

GcsStaleXFenceResult
cluster_gcs_block_dedup_stale_x_holder_lookup(
	int worker_id, const GcsBlockDedupKey *key, const BufferTag *tag,
	GcsStaleXCertPayload *stored_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsStaleXCertPayload *stored;
	GcsStaleXFenceResult result;
	bool found = false;

	if (stored_out != NULL)
		memset(stored_out, 0, sizeof(*stored_out));
	if (key == NULL || tag == NULL)
		return GCS_STALE_X_FENCE_INVALID;
	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_STALE_X_FENCE_INVALID;

	LWLockAcquire(&shard->lock.lock, LW_SHARED);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found || entry == NULL) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_NOT_FOUND;
	}
	if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_STALE_X_HOLDER_FENCE
		|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_STALE;
	}
	stored = &entry->payload_meta.stale_x_cert;
	if (stored->phase == (uint8)GCS_STALE_X_CERT_PHASE_MISS_REPORT
		&& dedup_stale_x_report_shape_valid(key, stored))
		result = GCS_STALE_X_FENCE_INSTALLED;
	else if (stored->phase == (uint8)GCS_STALE_X_CERT_PHASE_FENCE_ACK
			 && dedup_stale_x_ack_shape_valid(key, stored))
		result = GCS_STALE_X_FENCE_FULL;
	else if (stored->phase == (uint8)GCS_STALE_X_CERT_PHASE_COMMIT
			 && dedup_stale_x_commit_shape_valid(key, stored))
		result = GCS_STALE_X_FENCE_COMMITTED;
	else
		result = GCS_STALE_X_FENCE_STALE;
	if (result != GCS_STALE_X_FENCE_STALE && stored_out != NULL)
		*stored_out = *stored;
	LWLockRelease(&shard->lock.lock);
	return result;
}

static bool
dedup_stale_x_master_cert_matches_marker(const GcsBlockDedupEntry *entry,
										 const GcsStaleXCertPayload *cert,
										 GcsBlockForwardMarkerPhase phase,
										 uint32 capability_generation)
{
	const GcsBlockForwardMarker *marker;
	SCN expected_scn;

	if (entry == NULL || cert == NULL)
		return false;
	marker = &entry->payload_meta.forward_marker;
	expected_scn = GcsBlockForwardPayloadGetExpectedPiWatermarkScn(&marker->forward);
	return dedup_forward_entry_exact(
			   entry, &cert->tag, cert->holder_node,
			   GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER, marker, phase)
		   && marker->boot_identity.requester_incarnation
				  == cert->requester_incarnation
		   && marker->boot_identity.master_incarnation
				  == cert->master_incarnation
		   && marker->boot_identity.holder_incarnation
				  == cert->holder_incarnation
		   && marker->boot_identity.relation_generation
				  == cert->relation_generation
		   && marker->boot_identity.capability_generation
				  == capability_generation
		   && marker->forward.request_id == cert->request_id
		   && marker->forward.epoch == cert->request_epoch
		   && marker->forward.original_requester_node == cert->requester_node
		   && marker->forward.requester_backend_id == cert->requester_backend_id
		   && marker->forward.master_node == cert->master_node
		   && marker->forward.transition_id == cert->transition_id
		   && marker->authority.pending_x_requester_node == -1
		   && marker->authority.pending_x_since_lsn == 0
		   && SCN_VALID(expected_scn) && cert->final_page_scn == expected_scn
		   && scn_local(cert->durable_page_scn) >= scn_local(expected_scn);
}

static void
dedup_stale_x_install_from_report(const GcsStaleXCertPayload *report,
								  uint64 pre_authority_generation,
								  GcsStaleXCertPayload *install)
{
	*install = *report;
	install->phase = (uint8)GCS_STALE_X_CERT_PHASE_FENCE_INSTALL;
	install->pre_authority_generation = pre_authority_generation;
}

GcsStaleXFenceResult
cluster_gcs_block_dedup_stale_x_master_report_exact(
	int worker_id, const GcsBlockDedupKey *key, uint32 capability_generation,
	const GcsStaleXCertPayload *report,
	GcsStaleXCertPayload *install_out, GcsBlockForwardMarker *marker_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsBlockForwardMarkerPhase phase;
	GcsStaleXCertPayload stored;
	GcsStaleXCertPayload install;
	bool found = false;

	if (install_out != NULL)
		memset(install_out, 0, sizeof(*install_out));
	if (marker_out != NULL)
		memset(marker_out, 0, sizeof(*marker_out));
	if (capability_generation == 0
		|| !dedup_stale_x_report_shape_valid(key, report))
		return GCS_STALE_X_FENCE_INVALID;
	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_STALE_X_FENCE_INVALID;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found || entry == NULL) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_NOT_FOUND;
	}
	phase = GcsBlockDedupEntryForwardMarkerPhase(entry);
	if (!dedup_stale_x_master_cert_matches_marker(
			entry, report, phase, capability_generation)) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_STALE;
	}
	dedup_stale_x_install_from_report(
		report, entry->payload_meta.forward_marker.authority.authority_generation, &install);

	if (phase == GCS_BLOCK_FORWARD_MARK_SEND_ARMED
		|| phase == GCS_BLOCK_FORWARD_MARK_FORWARDED) {
		memset(entry->block_data, 0, sizeof(entry->block_data));
		memcpy(entry->block_data, report, sizeof(*report));
		dedup_forward_phase_set(entry, GCS_BLOCK_FORWARD_MARK_HOLDER_MISS_PENDING);
		entry->completed_at_ts = 0;
		if (install_out != NULL)
			*install_out = install;
		if (marker_out != NULL)
			*marker_out = entry->payload_meta.forward_marker;
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_INSTALLED;
	}

	memcpy(&stored, entry->block_data, sizeof(stored));
	if (phase == GCS_BLOCK_FORWARD_MARK_HOLDER_MISS_PENDING) {
		if (!dedup_stale_x_report_shape_valid(key, &stored)
			|| memcmp(&stored, report, sizeof(*report)) != 0) {
			LWLockRelease(&shard->lock.lock);
			return GCS_STALE_X_FENCE_STALE;
		}
	} else if (phase == GCS_BLOCK_FORWARD_MARK_HOLDER_FENCE_ACKED) {
		if (!dedup_stale_x_ack_shape_valid(key, &stored)
			|| !dedup_stale_x_same_attempt(&stored, report)
			|| stored.pre_authority_generation
				   != entry->payload_meta.forward_marker.authority.authority_generation) {
			LWLockRelease(&shard->lock.lock);
			return GCS_STALE_X_FENCE_STALE;
		}
	} else {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_STALE;
	}

	if (install_out != NULL)
		*install_out = install;
	if (marker_out != NULL)
		*marker_out = entry->payload_meta.forward_marker;
	LWLockRelease(&shard->lock.lock);
	return GCS_STALE_X_FENCE_REPLAY;
}

GcsStaleXFenceResult
cluster_gcs_block_dedup_stale_x_master_ack_exact(
	int worker_id, const GcsBlockDedupKey *key, uint32 capability_generation,
	const GcsStaleXCertPayload *ack,
	GcsBlockForwardMarker *marker_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsBlockForwardMarkerPhase phase;
	GcsStaleXCertPayload stored;
	bool found = false;

	if (marker_out != NULL)
		memset(marker_out, 0, sizeof(*marker_out));
	if (capability_generation == 0 || !dedup_stale_x_ack_shape_valid(key, ack))
		return GCS_STALE_X_FENCE_INVALID;
	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_STALE_X_FENCE_INVALID;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found || entry == NULL) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_NOT_FOUND;
	}
	phase = GcsBlockDedupEntryForwardMarkerPhase(entry);
	if (!dedup_stale_x_master_cert_matches_marker(
			entry, ack, phase, capability_generation)
		|| ack->pre_authority_generation
			   != entry->payload_meta.forward_marker.authority.authority_generation) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_STALE;
	}
	memcpy(&stored, entry->block_data, sizeof(stored));
	if (phase == GCS_BLOCK_FORWARD_MARK_HOLDER_MISS_PENDING) {
		if (!dedup_stale_x_report_shape_valid(key, &stored)
			|| !dedup_stale_x_same_attempt(&stored, ack)) {
			LWLockRelease(&shard->lock.lock);
			return GCS_STALE_X_FENCE_STALE;
		}
		memset(entry->block_data, 0, sizeof(entry->block_data));
		memcpy(entry->block_data, ack, sizeof(*ack));
		dedup_forward_phase_set(entry, GCS_BLOCK_FORWARD_MARK_HOLDER_FENCE_ACKED);
		if (marker_out != NULL)
			*marker_out = entry->payload_meta.forward_marker;
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_INSTALLED;
	}
	if (phase == GCS_BLOCK_FORWARD_MARK_HOLDER_FENCE_ACKED
		&& dedup_stale_x_ack_shape_valid(key, &stored)
		&& memcmp(&stored, ack, sizeof(*ack)) == 0) {
		if (marker_out != NULL)
			*marker_out = entry->payload_meta.forward_marker;
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_REPLAY;
	}
	if (phase == GCS_BLOCK_FORWARD_MARK_COMMITTED
		&& dedup_stale_x_commit_shape_valid(key, &stored)
		&& dedup_stale_x_same_attempt(&stored, ack)
		&& stored.pre_authority_generation == ack->pre_authority_generation
		&& stored.post_authority_generation == ack->pre_authority_generation + 1) {
		if (marker_out != NULL)
			*marker_out = entry->payload_meta.forward_marker;
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_REPLAY;
	}
	LWLockRelease(&shard->lock.lock);
	return GCS_STALE_X_FENCE_STALE;
}

GcsStaleXFenceResult
cluster_gcs_block_dedup_stale_x_master_commit_exact(
	int worker_id, const GcsBlockDedupKey *key, uint32 capability_generation,
	const GcsStaleXCertPayload *commit,
	GcsBlockForwardMarker *marker_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsBlockForwardMarkerPhase phase;
	GcsStaleXCertPayload stored;
	bool found = false;

	if (marker_out != NULL)
		memset(marker_out, 0, sizeof(*marker_out));
	if (capability_generation == 0 || !dedup_stale_x_commit_shape_valid(key, commit))
		return GCS_STALE_X_FENCE_INVALID;
	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_STALE_X_FENCE_INVALID;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found || entry == NULL) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_NOT_FOUND;
	}
	phase = GcsBlockDedupEntryForwardMarkerPhase(entry);
	if (!dedup_stale_x_master_cert_matches_marker(
			entry, commit, phase, capability_generation)
		|| commit->pre_authority_generation
			   != entry->payload_meta.forward_marker.authority.authority_generation) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_STALE;
	}
	memcpy(&stored, entry->block_data, sizeof(stored));
	if (phase == GCS_BLOCK_FORWARD_MARK_HOLDER_FENCE_ACKED) {
		if (!dedup_stale_x_ack_shape_valid(key, &stored)
			|| !dedup_stale_x_same_attempt(&stored, commit)
			|| stored.pre_authority_generation != commit->pre_authority_generation) {
			LWLockRelease(&shard->lock.lock);
			return GCS_STALE_X_FENCE_STALE;
		}
		memset(entry->block_data, 0, sizeof(entry->block_data));
		memcpy(entry->block_data, commit, sizeof(*commit));
		dedup_forward_phase_set(entry, GCS_BLOCK_FORWARD_MARK_COMMITTED);
		if (marker_out != NULL)
			*marker_out = entry->payload_meta.forward_marker;
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_COMMITTED;
	}
	if (phase == GCS_BLOCK_FORWARD_MARK_COMMITTED
		&& dedup_stale_x_commit_shape_valid(key, &stored)
		&& memcmp(&stored, commit, sizeof(*commit)) == 0) {
		if (marker_out != NULL)
			*marker_out = entry->payload_meta.forward_marker;
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_REPLAY;
	}
	LWLockRelease(&shard->lock.lock);
	return GCS_STALE_X_FENCE_STALE;
}

GcsStaleXFenceResult
cluster_gcs_block_dedup_stale_x_master_commit_ack_exact(
	int worker_id, const GcsBlockDedupKey *key, uint32 capability_generation,
	const GcsStaleXCertPayload *ack)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsBlockForwardMarkerPhase phase;
	GcsStaleXCertPayload stored;
	GcsStaleXCertPayload expected_ack;
	bool found = false;

	if (capability_generation == 0
		|| !dedup_stale_x_commit_ack_shape_valid(key, ack))
		return GCS_STALE_X_FENCE_INVALID;
	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_STALE_X_FENCE_INVALID;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found || entry == NULL) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_NOT_FOUND;
	}
	phase = GcsBlockDedupEntryForwardMarkerPhase(entry);
	if (!dedup_stale_x_master_cert_matches_marker(
			entry, ack, phase, capability_generation)
		|| ack->pre_authority_generation
			   != entry->payload_meta.forward_marker.authority.authority_generation) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_STALE;
	}
	memcpy(&stored, entry->block_data, sizeof(stored));
	if (phase == GCS_BLOCK_FORWARD_MARK_COMMITTED) {
		if (!dedup_stale_x_commit_shape_valid(key, &stored)) {
			LWLockRelease(&shard->lock.lock);
			return GCS_STALE_X_FENCE_STALE;
		}
		dedup_stale_x_ack_from_commit(&stored, &expected_ack);
		if (memcmp(&expected_ack, ack, sizeof(*ack)) != 0) {
			LWLockRelease(&shard->lock.lock);
			return GCS_STALE_X_FENCE_STALE;
		}
		memset(entry->block_data, 0, sizeof(entry->block_data));
		memcpy(entry->block_data, ack, sizeof(*ack));
		dedup_forward_phase_set(entry, GCS_BLOCK_FORWARD_MARK_COMMIT_ACKED);
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_INSTALLED;
	}
	if (phase == GCS_BLOCK_FORWARD_MARK_COMMIT_ACKED
		&& dedup_stale_x_commit_ack_shape_valid(key, &stored)
		&& memcmp(&stored, ack, sizeof(*ack)) == 0) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_REPLAY;
	}
	if (phase == GCS_BLOCK_FORWARD_MARK_RETIRE_ARMED
		&& dedup_stale_x_retire_shape_valid(key, &stored)
		&& dedup_stale_x_same_attempt(&stored, ack)
		&& stored.pre_authority_generation == ack->pre_authority_generation
		&& stored.post_authority_generation == ack->post_authority_generation) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_REPLAY;
	}
	LWLockRelease(&shard->lock.lock);
	return GCS_STALE_X_FENCE_STALE;
}

GcsStaleXFenceResult
cluster_gcs_block_dedup_stale_x_master_retire_arm_exact(
	int worker_id, const GcsBlockDedupKey *key, uint32 capability_generation,
	const GcsStaleXCertPayload *retire)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsBlockForwardMarkerPhase phase;
	GcsStaleXCertPayload stored;
	bool found = false;

	if (capability_generation == 0
		|| !dedup_stale_x_retire_shape_valid(key, retire))
		return GCS_STALE_X_FENCE_INVALID;
	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_STALE_X_FENCE_INVALID;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found || entry == NULL) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_NOT_FOUND;
	}
	phase = GcsBlockDedupEntryForwardMarkerPhase(entry);
	if (!dedup_stale_x_master_cert_matches_marker(
			entry, retire, phase, capability_generation)
		|| retire->pre_authority_generation
			   != entry->payload_meta.forward_marker.authority.authority_generation) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_STALE;
	}
	memcpy(&stored, entry->block_data, sizeof(stored));
	if (phase == GCS_BLOCK_FORWARD_MARK_COMMIT_ACKED) {
		if (!dedup_stale_x_commit_ack_shape_valid(key, &stored)
			|| !dedup_stale_x_same_attempt(&stored, retire)
			|| stored.pre_authority_generation != retire->pre_authority_generation
			|| stored.post_authority_generation != retire->post_authority_generation) {
			LWLockRelease(&shard->lock.lock);
			return GCS_STALE_X_FENCE_STALE;
		}
		memset(entry->block_data, 0, sizeof(entry->block_data));
		memcpy(entry->block_data, retire, sizeof(*retire));
		dedup_forward_phase_set(entry, GCS_BLOCK_FORWARD_MARK_RETIRE_ARMED);
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_INSTALLED;
	}
	if (phase == GCS_BLOCK_FORWARD_MARK_RETIRE_ARMED
		&& dedup_stale_x_retire_shape_valid(key, &stored)
		&& memcmp(&stored, retire, sizeof(*retire)) == 0) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_REPLAY;
	}
	LWLockRelease(&shard->lock.lock);
	return GCS_STALE_X_FENCE_STALE;
}

GcsStaleXFenceResult
cluster_gcs_block_dedup_stale_x_master_retire_ack_exact(
	int worker_id, const GcsBlockDedupKey *key, uint32 capability_generation,
	const GcsStaleXCertPayload *ack)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsStaleXCertPayload stored;
	GcsStaleXCertPayload expected_ack;
	bool found = false;

	if (capability_generation == 0
		|| !dedup_stale_x_retire_ack_shape_valid(key, ack))
		return GCS_STALE_X_FENCE_INVALID;
	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_STALE_X_FENCE_INVALID;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found || entry == NULL) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_NOT_FOUND;
	}
	if (!dedup_stale_x_master_cert_matches_marker(
			entry, ack, GCS_BLOCK_FORWARD_MARK_RETIRE_ARMED,
			capability_generation)) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_STALE;
	}
	memcpy(&stored, entry->block_data, sizeof(stored));
	if (!dedup_stale_x_retire_shape_valid(key, &stored)) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_STALE;
	}
	dedup_stale_x_ack_from_retire(&stored, &expected_ack);
	if (memcmp(&expected_ack, ack, sizeof(*ack)) != 0) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_FENCE_STALE;
	}
	(void)hash_search(htab, key, HASH_REMOVE, &found);
	Assert(found);
	pg_atomic_fetch_sub_u32(&shard->entry_count, 1);
	LWLockRelease(&shard->lock.lock);
	return GCS_STALE_X_FENCE_RETIRED;
}

static bool
dedup_stale_x_release_key_valid(const GcsStaleXReleaseKey *key)
{
	return key != NULL && key->source_buf_id >= 0
		   && key->durability_generation != 0
		   && key->durability_generation != UINT32_MAX
		   && key->holder_incarnation != 0 && key->release_cert_nonce != 0;
}

static bool
dedup_stale_x_durable_seal_key_valid(const GcsStaleXDurableSealKey *key)
{
	return key != NULL && key->source_buf_id >= 0
		   && key->durability_generation != 0
		   && key->durability_generation != UINT32_MAX
		   && key->holder_incarnation != 0;
}

static void
dedup_stale_x_durable_seal_key_from_note(
	const GcsPiWriteNote *note, GcsStaleXDurableSealKey *key)
{
	memset(key, 0, sizeof(*key));
	if (note == NULL)
		return;
	key->tag = note->tag;
	key->source_buf_id = note->source_buf_id;
	key->durability_generation = note->durability_generation;
	key->source_own_generation = note->source_own_generation;
	key->holder_incarnation = note->source_node_incarnation;
}

static void
dedup_stale_x_durable_seal_key_from_release(
	const GcsStaleXReleaseKey *release_key, GcsStaleXDurableSealKey *seal_key)
{
	memset(seal_key, 0, sizeof(*seal_key));
	if (release_key == NULL)
		return;
	seal_key->tag = release_key->tag;
	seal_key->source_buf_id = release_key->source_buf_id;
	seal_key->durability_generation = release_key->durability_generation;
	seal_key->source_own_generation = release_key->source_own_generation;
	seal_key->holder_incarnation = release_key->holder_incarnation;
}

bool
cluster_gcs_block_stale_x_durable_seal_publish(
	const GcsPiWriteNote *note, uint64 checkpoint_seal_id)
{
	GcsStaleXDurableSealKey key;
	ClusterGcsStaleXDurableSealSlot *slot;
	GcsStaleXDurableSeal *entry;

	if (note == NULL || checkpoint_seal_id == 0 || !SCN_VALID(note->page_scn))
		return false;
	dedup_stale_x_durable_seal_key_from_note(note, &key);
	if (!dedup_stale_x_durable_seal_key_valid(&key)
		|| cluster_gcs_stale_x_durable_seal_slots == NULL
		|| key.source_buf_id >= NBuffers)
		return false;
	slot = &cluster_gcs_stale_x_durable_seal_slots[key.source_buf_id];
	SpinLockAcquire(&slot->lock);
	entry = &slot->seal;
	if (memcmp(&entry->key, &key, sizeof(key)) != 0
		|| entry->checkpoint_seal_id == 0
		|| !SCN_VALID(entry->durable_page_scn)) {
		memset(entry, 0, sizeof(*entry));
		entry->key = key;
		entry->checkpoint_seal_id = checkpoint_seal_id;
		entry->durable_page_scn = note->page_scn;
	} else if (scn_local(note->page_scn) > scn_local(entry->durable_page_scn)) {
		entry->checkpoint_seal_id = checkpoint_seal_id;
		entry->durable_page_scn = note->page_scn;
	}
	SpinLockRelease(&slot->lock);
	return true;
}

bool
cluster_gcs_block_stale_x_durable_seal_lookup_exact(
	int worker_id, const GcsStaleXReleaseKey *release_key,
	SCN expected_final_page_scn, GcsStaleXDurableSeal *seal_out)
{
	GcsStaleXDurableSealKey key;
	ClusterGcsStaleXDurableSealSlot *slot;
	GcsStaleXDurableSeal *entry;
	bool exact;
	int expected_worker_id;

	if (seal_out != NULL)
		memset(seal_out, 0, sizeof(*seal_out));
	if (!dedup_stale_x_release_key_valid(release_key)
		|| !SCN_VALID(expected_final_page_scn))
		return false;
	dedup_stale_x_durable_seal_key_from_release(release_key, &key);
	expected_worker_id = cluster_lms_shard_for_tag(&key.tag, cluster_lms_workers);
	if (cluster_gcs_stale_x_durable_seal_slots == NULL
		|| key.source_buf_id >= NBuffers || worker_id != expected_worker_id
		|| worker_id < 0 || worker_id >= cluster_gcs_block_dedup_n_shards)
		return false;

	slot = &cluster_gcs_stale_x_durable_seal_slots[key.source_buf_id];
	SpinLockAcquire(&slot->lock);
	entry = &slot->seal;
	exact = dedup_stale_x_durable_seal_key_valid(&entry->key)
			&& memcmp(&entry->key, &key, sizeof(key)) == 0
			&& entry->checkpoint_seal_id != 0 && SCN_VALID(entry->durable_page_scn)
			&& scn_local(entry->durable_page_scn)
				   >= scn_local(expected_final_page_scn);
	if (exact && seal_out != NULL)
		*seal_out = *entry;
	SpinLockRelease(&slot->lock);
	return exact;
}

bool
cluster_gcs_block_stale_x_durable_seal_forget_exact(
	const GcsStaleXDurableSeal *seal)
{
	ClusterGcsStaleXDurableSealSlot *slot;
	GcsStaleXDurableSeal *entry;
	bool removed = false;

	if (seal == NULL || !dedup_stale_x_durable_seal_key_valid(&seal->key)
		|| seal->checkpoint_seal_id == 0 || !SCN_VALID(seal->durable_page_scn))
		return false;
	if (cluster_gcs_stale_x_durable_seal_slots == NULL
		|| seal->key.source_buf_id >= NBuffers)
		return false;

	slot = &cluster_gcs_stale_x_durable_seal_slots[seal->key.source_buf_id];
	SpinLockAcquire(&slot->lock);
	entry = &slot->seal;
	if (memcmp(entry, seal, sizeof(*seal)) == 0) {
		memset(entry, 0, sizeof(*entry));
		removed = true;
	}
	SpinLockRelease(&slot->lock);
	return removed;
}

static bool
dedup_stale_x_release_nodes_valid(const GcsStaleXReleaseRecord *record)
{
	return record != NULL && record->master_node >= 0 && record->master_node < 32
		   && record->holder_node >= 0 && record->holder_node < 32
		   && record->master_node != record->holder_node
		   && record->master_incarnation != 0;
}

static bool
dedup_stale_x_release_reserved_zero(const GcsStaleXReleaseRecord *record)
{
	static const uint8 zero[sizeof(record->reserved)] = { 0 };

	return record != NULL
		   && memcmp(record->reserved, zero, sizeof(record->reserved)) == 0;
}

static bool
dedup_stale_x_release_record_valid(const GcsStaleXReleaseRecord *record)
{
	if (!dedup_stale_x_release_key_valid(record != NULL ? &record->key : NULL)
		|| !dedup_stale_x_release_nodes_valid(record)
		|| !dedup_stale_x_release_reserved_zero(record)
		|| record->release_epoch == 0
		|| record->relation_generation == 0
		|| record->state < (uint8)GCS_STALE_X_RELEASE_RESERVED
		|| record->state > (uint8)GCS_STALE_X_RELEASE_RELEASED)
		return false;
	if (record->state == (uint8)GCS_STALE_X_RELEASE_RESERVED)
		return record->checkpoint_seal_id == 0 && record->result_own_generation == 0
			   && record->final_page_scn == InvalidScn
			   && record->durable_page_scn == InvalidScn;
	if (record->checkpoint_seal_id == 0 || !SCN_VALID(record->final_page_scn)
		|| !SCN_VALID(record->durable_page_scn)
		|| scn_local(record->durable_page_scn) < scn_local(record->final_page_scn))
		return false;
	if (record->state != (uint8)GCS_STALE_X_RELEASE_RELEASED)
		return record->result_own_generation == 0;
	return record->key.source_own_generation != UINT64_MAX
		   && record->result_own_generation == record->key.source_own_generation + 1;
}

GcsStaleXReleaseResult
cluster_gcs_block_stale_x_release_reserve(const GcsStaleXReleaseRecord *record)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsStaleXReleaseRecord *entry;
	bool found = false;
	int worker_id;

	if (!dedup_stale_x_release_record_valid(record)
		|| record->state != (uint8)GCS_STALE_X_RELEASE_RESERVED)
		return GCS_STALE_X_RELEASE_INVALID;
	worker_id = cluster_lms_shard_for_tag(&record->key.tag, cluster_lms_workers);
	shard = cluster_gcs_block_stale_x_release_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_STALE_X_RELEASE_INVALID;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsStaleXReleaseRecord *)hash_search(
		htab, &record->key, HASH_ENTER_NULL, &found);
	if (entry == NULL) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_RELEASE_FULL;
	}
	if (found) {
		GcsStaleXReleaseResult result
			= memcmp(entry, record, sizeof(*record)) == 0
				  ? GCS_STALE_X_RELEASE_REPLAY
				  : GCS_STALE_X_RELEASE_STALE;

		LWLockRelease(&shard->lock.lock);
		return result;
	}
	*entry = *record;
	LWLockRelease(&shard->lock.lock);
	return GCS_STALE_X_RELEASE_INSTALLED;
}

GcsStaleXReleaseResult
cluster_gcs_block_stale_x_release_seal_exact(
	const GcsStaleXReleaseKey *key, SCN final_page_scn, SCN durable_page_scn,
	uint64 checkpoint_seal_id, GcsStaleXReleaseRecord *record_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsStaleXReleaseRecord *entry;
	GcsStaleXReleaseRecord desired;
	bool found = false;
	int worker_id;

	if (record_out != NULL)
		memset(record_out, 0, sizeof(*record_out));
	if (!dedup_stale_x_release_key_valid(key) || checkpoint_seal_id == 0
		|| !SCN_VALID(final_page_scn) || !SCN_VALID(durable_page_scn)
		|| scn_local(durable_page_scn) < scn_local(final_page_scn))
		return GCS_STALE_X_RELEASE_INVALID;
	worker_id = cluster_lms_shard_for_tag(&key->tag, cluster_lms_workers);
	shard = cluster_gcs_block_stale_x_release_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_STALE_X_RELEASE_INVALID;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsStaleXReleaseRecord *)hash_search(htab, key, HASH_FIND, &found);
	if (!found || entry == NULL) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_RELEASE_NOT_FOUND;
	}
	desired = *entry;
	desired.checkpoint_seal_id = checkpoint_seal_id;
	desired.final_page_scn = final_page_scn;
	desired.durable_page_scn = durable_page_scn;
	desired.state = (uint8)GCS_STALE_X_RELEASE_SEALED;
	if (entry->state == (uint8)GCS_STALE_X_RELEASE_SEALED
		&& memcmp(entry, &desired, sizeof(desired)) == 0) {
		if (record_out != NULL)
			*record_out = *entry;
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_RELEASE_REPLAY;
	}
	if (entry->state != (uint8)GCS_STALE_X_RELEASE_RESERVED) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_RELEASE_STALE;
	}
	*entry = desired;
	if (record_out != NULL)
		*record_out = *entry;
	LWLockRelease(&shard->lock.lock);
	return GCS_STALE_X_RELEASE_INSTALLED;
}

GcsStaleXReleaseResult
cluster_gcs_block_stale_x_release_advance_exact(
	const GcsStaleXReleaseRecord *expected, GcsStaleXReleaseState target_state,
	uint64 result_own_generation, GcsStaleXReleaseRecord *record_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsStaleXReleaseRecord *entry;
	GcsStaleXReleaseRecord desired;
	bool found = false;
	int worker_id;

	if (record_out != NULL)
		memset(record_out, 0, sizeof(*record_out));
	if (!dedup_stale_x_release_record_valid(expected)
		|| !((expected->state == (uint8)GCS_STALE_X_RELEASE_SEALED
			  && target_state == GCS_STALE_X_RELEASE_COMMITTING
			  && result_own_generation == 0)
			 || (expected->state == (uint8)GCS_STALE_X_RELEASE_COMMITTING
				 && target_state == GCS_STALE_X_RELEASE_RELEASED
				 && expected->key.source_own_generation != UINT64_MAX
				 && result_own_generation == expected->key.source_own_generation + 1)))
		return GCS_STALE_X_RELEASE_INVALID;
	desired = *expected;
	desired.state = (uint8)target_state;
	desired.result_own_generation = result_own_generation;
	worker_id = cluster_lms_shard_for_tag(&expected->key.tag, cluster_lms_workers);
	shard = cluster_gcs_block_stale_x_release_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_STALE_X_RELEASE_INVALID;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsStaleXReleaseRecord *)hash_search(
		htab, &expected->key, HASH_FIND, &found);
	if (!found || entry == NULL) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_RELEASE_NOT_FOUND;
	}
	if (memcmp(entry, &desired, sizeof(desired)) == 0) {
		if (record_out != NULL)
			*record_out = *entry;
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_RELEASE_REPLAY;
	}
	if (memcmp(entry, expected, sizeof(*expected)) != 0) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_RELEASE_STALE;
	}
	*entry = desired;
	if (record_out != NULL)
		*record_out = *entry;
	LWLockRelease(&shard->lock.lock);
	return GCS_STALE_X_RELEASE_INSTALLED;
}

bool
cluster_gcs_block_stale_x_release_lookup_exact(
	int worker_id, const BufferTag *tag, uint64 release_epoch, int32 master_node,
	int32 holder_node, uint64 master_incarnation, uint64 holder_incarnation,
	SCN expected_final_page_scn, GcsStaleXReleaseRecord *record_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	HASH_SEQ_STATUS scan;
	GcsStaleXReleaseRecord *entry;
	GcsStaleXReleaseRecord found_record;
	int matches = 0;

	if (record_out != NULL)
		memset(record_out, 0, sizeof(*record_out));
	if (tag == NULL || master_node < 0 || master_node >= 32 || holder_node < 0
		|| holder_node >= 32 || master_incarnation == 0 || holder_incarnation == 0
		|| !SCN_VALID(expected_final_page_scn))
		return false;
	shard = cluster_gcs_block_stale_x_release_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return false;

	memset(&found_record, 0, sizeof(found_record));
	LWLockAcquire(&shard->lock.lock, LW_SHARED);
	hash_seq_init(&scan, htab);
	while ((entry = (GcsStaleXReleaseRecord *)hash_seq_search(&scan)) != NULL) {
		if (!dedup_stale_x_release_record_valid(entry)
			|| entry->state != (uint8)GCS_STALE_X_RELEASE_RELEASED
			|| !BufferTagsEqual(&entry->key.tag, tag)
			|| entry->release_epoch != release_epoch
			|| entry->master_node != master_node || entry->holder_node != holder_node
			|| entry->master_incarnation != master_incarnation
			|| entry->key.holder_incarnation != holder_incarnation
			|| entry->final_page_scn != expected_final_page_scn)
			continue;
		found_record = *entry;
		matches++;
	}
	LWLockRelease(&shard->lock.lock);
	if (matches != 1)
		return false;
	if (record_out != NULL)
		*record_out = found_record;
	return true;
}

bool
cluster_gcs_block_stale_x_release_forget_exact(const GcsStaleXReleaseRecord *record)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsStaleXReleaseRecord *entry;
	bool found = false;
	bool removed = false;
	int worker_id;

	if (!dedup_stale_x_release_record_valid(record))
		return false;
	worker_id = cluster_lms_shard_for_tag(&record->key.tag, cluster_lms_workers);
	shard = cluster_gcs_block_stale_x_release_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return false;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsStaleXReleaseRecord *)hash_search(
		htab, &record->key, HASH_FIND, &found);
	if (found && entry != NULL && memcmp(entry, record, sizeof(*record)) == 0) {
		(void)hash_search(htab, &record->key, HASH_REMOVE, &found);
		Assert(found);
		removed = true;
	}
	LWLockRelease(&shard->lock.lock);
	return removed;
}

bool
cluster_gcs_block_stale_x_release_lookup_key_exact(
	const GcsStaleXReleaseKey *key, GcsStaleXReleaseRecord *record_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsStaleXReleaseRecord *entry;
	bool found = false;
	int worker_id;

	if (record_out != NULL)
		memset(record_out, 0, sizeof(*record_out));
	if (!dedup_stale_x_release_key_valid(key))
		return false;
	worker_id = cluster_lms_shard_for_tag(&key->tag, cluster_lms_workers);
	shard = cluster_gcs_block_stale_x_release_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return false;

	LWLockAcquire(&shard->lock.lock, LW_SHARED);
	entry = (GcsStaleXReleaseRecord *)hash_search(htab, key, HASH_FIND, &found);
	if (!found || entry == NULL || !dedup_stale_x_release_record_valid(entry)) {
		LWLockRelease(&shard->lock.lock);
		return false;
	}
	if (record_out != NULL)
		*record_out = *entry;
	LWLockRelease(&shard->lock.lock);
	return true;
}

static bool
dedup_stale_x_relation_locator_valid(RelFileLocator locator)
{
	return OidIsValid(locator.spcOid) && locator.relNumber != 0;
}

bool
cluster_gcs_block_stale_x_relation_register(
	RelFileLocator locator, uint64 *generation_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsStaleXRelationGenerationEntry *entry;
	bool found = false;

	if (generation_out != NULL)
		*generation_out = 0;
	if (!dedup_stale_x_relation_locator_valid(locator))
		return false;
	shard = cluster_gcs_block_stale_x_relation_resolve(locator, &htab);
	if (shard == NULL)
		return false;

	LWLockAcquire(&shard->relation_lock.lock, LW_EXCLUSIVE);
	entry = (GcsStaleXRelationGenerationEntry *)hash_search(
		htab, &locator, HASH_ENTER_NULL, &found);
	if (entry == NULL || (found && entry->generation == 0)) {
		LWLockRelease(&shard->relation_lock.lock);
		return false;
	}
	if (!found) {
		memset(((char *)entry) + sizeof(entry->locator), 0,
			   sizeof(*entry) - sizeof(entry->locator));
		entry->locator = locator;
		entry->generation = 1;
	}
	if (generation_out != NULL)
		*generation_out = entry->generation;
	LWLockRelease(&shard->relation_lock.lock);
	return true;
}

bool
cluster_gcs_block_stale_x_relation_current(
	RelFileLocator locator, uint64 *generation_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsStaleXRelationGenerationEntry *entry;
	bool found = false;

	if (generation_out != NULL)
		*generation_out = 0;
	if (!dedup_stale_x_relation_locator_valid(locator))
		return false;
	shard = cluster_gcs_block_stale_x_relation_resolve(locator, &htab);
	if (shard == NULL)
		return false;

	LWLockAcquire(&shard->relation_lock.lock, LW_SHARED);
	entry = (GcsStaleXRelationGenerationEntry *)hash_search(
		htab, &locator, HASH_FIND, &found);
	if (!found || entry == NULL || entry->generation == 0) {
		LWLockRelease(&shard->relation_lock.lock);
		return false;
	}
	if (generation_out != NULL)
		*generation_out = entry->generation;
	LWLockRelease(&shard->relation_lock.lock);
	return true;
}

bool
cluster_gcs_block_stale_x_relation_guard_acquire(
	RelFileLocator locator, uint64 expected_generation,
	GcsStaleXRelationGenerationGuard *guard)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsStaleXRelationGenerationEntry *entry;
	bool found = false;

	if (guard != NULL) {
		memset(guard, 0, sizeof(*guard));
		guard->shard_index = -1;
	}
	if (guard == NULL || expected_generation == 0
		|| !dedup_stale_x_relation_locator_valid(locator))
		return false;
	shard = cluster_gcs_block_stale_x_relation_resolve(locator, &htab);
	if (shard == NULL)
		return false;

	LWLockAcquire(&shard->relation_lock.lock, LW_SHARED);
	entry = (GcsStaleXRelationGenerationEntry *)hash_search(
		htab, &locator, HASH_FIND, &found);
	if (!found || entry == NULL
		|| !GcsStaleXRelationGenerationExact(
			expected_generation, entry->generation)) {
		LWLockRelease(&shard->relation_lock.lock);
		return false;
	}
	guard->locator = locator;
	guard->shard_index
		= (int32)(shard - cluster_gcs_block_dedup_shards);
	guard->generation = expected_generation;
	return true;
}

void
cluster_gcs_block_stale_x_relation_guard_release(
	GcsStaleXRelationGenerationGuard *guard)
{
	int32 shard_index;

	if (guard == NULL)
		return;
	shard_index = guard->shard_index;
	if (shard_index < 0
		|| shard_index >= cluster_gcs_block_dedup_n_shards
		|| guard->generation == 0)
		return;
	LWLockRelease(
		&cluster_gcs_block_dedup_shards[shard_index].relation_lock.lock);
	memset(guard, 0, sizeof(*guard));
	guard->shard_index = -1;
}

bool
cluster_gcs_block_stale_x_relation_bump(RelFileLocator locator)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsStaleXRelationGenerationEntry *entry;
	bool found = false;

	if (!dedup_stale_x_relation_locator_valid(locator))
		return false;
	shard = cluster_gcs_block_stale_x_relation_resolve(locator, &htab);
	if (shard == NULL)
		return false;

	LWLockAcquire(&shard->relation_lock.lock, LW_EXCLUSIVE);
	entry = (GcsStaleXRelationGenerationEntry *)hash_search(
		htab, &locator, HASH_FIND, &found);
	if (!found || entry == NULL) {
		LWLockRelease(&shard->relation_lock.lock);
		return true; /* INV-G3: no registered locator has no type65 evidence. */
	}
	if (entry->generation == 0 || entry->generation == UINT64_MAX) {
		/* Poison on wrap.  Register/current now fail closed forever for this
		 * locator instead of letting a stale generation compare equal. */
		entry->generation = 0;
		LWLockRelease(&shard->relation_lock.lock);
		return false;
	}
	entry->generation++;
	LWLockRelease(&shard->relation_lock.lock);
	return true;
}

static bool
dedup_eviction_gate_reserved_zero(const GcsBlockEvictionGateRecord *record)
{
	static const uint8 zero[sizeof(record->reserved)] = { 0 };

	return record != NULL
		   && memcmp(record->reserved, zero, sizeof(record->reserved)) == 0;
}

static bool
dedup_eviction_gate_record_valid(const GcsBlockEvictionGateRecord *record)
{
	if (record == NULL || !dedup_stale_x_release_key_valid(&record->key)
		|| record->master_node < 0 || record->master_node >= 32
		|| record->holder_node < 0 || record->holder_node >= 32
		|| record->release_epoch == 0
		|| record->relation_generation == 0
		|| (record->old_pcm_mode != (uint8)PCM_LOCK_MODE_S
			&& record->old_pcm_mode != (uint8)PCM_LOCK_MODE_X)
		|| record->state < (uint8)GCS_BLOCK_EVICTION_GATE_COMMITTING
		|| record->state > (uint8)GCS_BLOCK_EVICTION_GATE_RELEASED
		|| record->has_stale_x_journal > 1
		|| !dedup_eviction_gate_reserved_zero(record))
		return false;
	if (record->has_stale_x_journal != 0) {
		if (record->old_pcm_mode != (uint8)PCM_LOCK_MODE_X
			|| record->master_node == record->holder_node
			|| record->master_incarnation == 0
			|| record->capability_generation == 0)
			return false;
	} else if (record->master_incarnation != 0
			   || record->capability_generation != 0)
		return false;
	if (record->state == (uint8)GCS_BLOCK_EVICTION_GATE_COMMITTING)
		return record->result_own_generation == 0;
	return record->key.source_own_generation != UINT64_MAX
		   && record->result_own_generation == record->key.source_own_generation + 1;
}

GcsStaleXReleaseResult
cluster_gcs_block_eviction_gate_reserve(const GcsBlockEvictionGateRecord *record)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockEvictionGateRecord *entry;
	HASH_SEQ_STATUS scan;
	bool found = false;
	int worker_id;

	if (!dedup_eviction_gate_record_valid(record)
		|| record->state != (uint8)GCS_BLOCK_EVICTION_GATE_COMMITTING)
		return GCS_STALE_X_RELEASE_INVALID;
	worker_id = cluster_lms_shard_for_tag(&record->key.tag, cluster_lms_workers);
	shard = cluster_gcs_block_eviction_gate_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_STALE_X_RELEASE_INVALID;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockEvictionGateRecord *)hash_search(
		htab, &record->key, HASH_FIND, &found);
	if (found && entry != NULL) {
		GcsStaleXReleaseResult result
			= memcmp(entry, record, sizeof(*record)) == 0
				  ? GCS_STALE_X_RELEASE_REPLAY
				  : GCS_STALE_X_RELEASE_STALE;

		LWLockRelease(&shard->lock.lock);
		return result;
	}

	/* One unresolved release owns both the logical tag and source descriptor.
	 * A second key must not bypass either ABA fence. */
	hash_seq_init(&scan, htab);
	while ((entry = (GcsBlockEvictionGateRecord *)hash_seq_search(&scan)) != NULL) {
		if (!dedup_eviction_gate_record_valid(entry)
			|| BufferTagsEqual(&entry->key.tag, &record->key.tag)
			|| entry->key.source_buf_id == record->key.source_buf_id) {
			hash_seq_term(&scan);
			LWLockRelease(&shard->lock.lock);
			return GCS_STALE_X_RELEASE_STALE;
		}
	}
	entry = (GcsBlockEvictionGateRecord *)hash_search(
		htab, &record->key, HASH_ENTER_NULL, &found);
	if (entry == NULL) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_RELEASE_FULL;
	}
	Assert(!found);
	*entry = *record;
	LWLockRelease(&shard->lock.lock);
	return GCS_STALE_X_RELEASE_INSTALLED;
}

GcsStaleXReleaseResult
cluster_gcs_block_eviction_gate_advance_exact(
	const GcsBlockEvictionGateRecord *expected,
	GcsBlockEvictionGateState target_state, uint64 result_own_generation,
	GcsBlockEvictionGateRecord *record_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockEvictionGateRecord *entry;
	GcsBlockEvictionGateRecord desired;
	bool found = false;
	int worker_id;

	if (record_out != NULL)
		memset(record_out, 0, sizeof(*record_out));
	if (!dedup_eviction_gate_record_valid(expected)
		|| expected->state != (uint8)GCS_BLOCK_EVICTION_GATE_COMMITTING
		|| target_state != GCS_BLOCK_EVICTION_GATE_RELEASED
		|| expected->key.source_own_generation == UINT64_MAX
		|| result_own_generation != expected->key.source_own_generation + 1)
		return GCS_STALE_X_RELEASE_INVALID;
	desired = *expected;
	desired.state = (uint8)target_state;
	desired.result_own_generation = result_own_generation;
	worker_id = cluster_lms_shard_for_tag(&expected->key.tag, cluster_lms_workers);
	shard = cluster_gcs_block_eviction_gate_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_STALE_X_RELEASE_INVALID;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockEvictionGateRecord *)hash_search(
		htab, &expected->key, HASH_FIND, &found);
	if (!found || entry == NULL) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_RELEASE_NOT_FOUND;
	}
	if (memcmp(entry, &desired, sizeof(desired)) == 0) {
		if (record_out != NULL)
			*record_out = *entry;
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_RELEASE_REPLAY;
	}
	if (memcmp(entry, expected, sizeof(*expected)) != 0) {
		LWLockRelease(&shard->lock.lock);
		return GCS_STALE_X_RELEASE_STALE;
	}
	*entry = desired;
	if (record_out != NULL)
		*record_out = *entry;
	LWLockRelease(&shard->lock.lock);
	return GCS_STALE_X_RELEASE_INSTALLED;
}

bool
cluster_gcs_block_eviction_gate_conflict(
	int worker_id, const BufferTag *tag, int32 source_buf_id,
	GcsBlockEvictionGateRecord *record_out)
{
	int first_shard;
	int last_shard;
	int s;

	if (record_out != NULL)
		memset(record_out, 0, sizeof(*record_out));
	if (tag == NULL || cluster_gcs_block_dedup_shards == NULL)
		return true;
	/* A source descriptor can be fenced by a tag in another shard. */
	first_shard = source_buf_id >= 0 ? 0 : worker_id;
	last_shard = source_buf_id >= 0 ? cluster_gcs_block_dedup_n_shards : worker_id + 1;
	if (worker_id < 0 || worker_id >= cluster_gcs_block_dedup_n_shards)
		return true;

	for (s = first_shard; s < last_shard; s++) {
		ClusterGcsBlockDedupShard *shard;
		HTAB *htab = NULL;
		HASH_SEQ_STATUS scan;
		GcsBlockEvictionGateRecord *entry;

		shard = cluster_gcs_block_eviction_gate_resolve_shard(s, &htab);
		if (shard == NULL)
			return true;
		LWLockAcquire(&shard->lock.lock, LW_SHARED);
		hash_seq_init(&scan, htab);
		while ((entry = (GcsBlockEvictionGateRecord *)hash_seq_search(&scan)) != NULL) {
			if (!dedup_eviction_gate_record_valid(entry)
				|| BufferTagsEqual(&entry->key.tag, tag)
				|| (source_buf_id >= 0
					&& entry->key.source_buf_id == source_buf_id)) {
				if (record_out != NULL && dedup_eviction_gate_record_valid(entry))
					*record_out = *entry;
				hash_seq_term(&scan);
				LWLockRelease(&shard->lock.lock);
				return true;
			}
		}
		LWLockRelease(&shard->lock.lock);
	}
	return false;
}

bool
cluster_gcs_block_eviction_gate_forget_exact(
	const GcsBlockEvictionGateRecord *record)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockEvictionGateRecord *entry;
	bool found = false;
	bool removed = false;
	int worker_id;

	if (!dedup_eviction_gate_record_valid(record))
		return false;
	worker_id = cluster_lms_shard_for_tag(&record->key.tag, cluster_lms_workers);
	shard = cluster_gcs_block_eviction_gate_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return false;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockEvictionGateRecord *)hash_search(
		htab, &record->key, HASH_FIND, &found);
	if (found && entry != NULL && memcmp(entry, record, sizeof(*record)) == 0) {
		(void)hash_search(htab, &record->key, HASH_REMOVE, &found);
		Assert(found);
		removed = true;
	}
	LWLockRelease(&shard->lock.lock);
	return removed;
}

static bool
dedup_pending_x_denial_is_exact(const GcsBlockDedupEntry *entry)
{
	const GcsBlockReplyHeader *header = &entry->reply_header;

	return entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_GENERIC
		   && entry->transition_id == (uint8)PCM_TRANS_N_TO_S
		   && entry->status == (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X
		   && entry->completed_at_ts != 0 && header->request_id == entry->key.request_id
		   && header->epoch == entry->key.cluster_epoch && header->sender_node == cluster_node_id
		   && header->requester_backend_id == entry->key.requester_backend_id
		   && header->transition_id == (uint8)PCM_TRANS_N_TO_S
		   && header->status == (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X
		   && GcsBlockReplyHeaderGetForwardingMasterNode(header)
				  == GCS_BLOCK_REPLY_NO_FORWARDING_MASTER;
}

static bool
dedup_pending_x_entry_has_legacy_s_right(const GcsBlockDedupEntry *entry)
{
	GcsBlockReplyStatus status;

	if (entry->completed_at_ts == 0)
		return true;
	status = (GcsBlockReplyStatus)entry->status;
	return status == GCS_BLOCK_REPLY_GRANTED || status == GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK
		   || status == GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER
		   || status == GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER
		   || status == GCS_BLOCK_REPLY_S_GRANTED_XHOLDER_DOWNGRADE;
}

/*
 * A forward marker is an independently serialized authority record, not a
 * generic cached grant that queue-kind X may overwrite with DENIED_PENDING_X.
 * Classify only a byte-coherent marker as a retryable blocker.  A stray marker
 * flag, an impossible phase/timestamp posture, or drift in the embedded
 * authority/FORWARD/header remains INVALID and therefore fail-closed.
 */
static bool
dedup_pending_x_forward_blocker_is_exact(const GcsBlockDedupEntry *entry)
{
	const GcsBlockForwardMarker *marker;
	GcsBlockForwardMarkerPhase phase;
	GcsBlockReplyStatus status;
	int32 expected_holder_node;
	int32 forwarding_master_node;

	if (entry == NULL || entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_GENERIC
		|| entry->done_at_ts != 0)
		return false;
	phase = GcsBlockDedupEntryForwardMarkerPhase(entry);
	if (phase == GCS_BLOCK_FORWARD_MARK_NONE
		|| phase == GCS_BLOCK_FORWARD_MARK_KEEP_FENCED)
		return false;
	if ((phase == GCS_BLOCK_FORWARD_MARK_FORWARDED
		 || phase == GCS_BLOCK_FORWARD_MARK_CANCELLING)
		!= (entry->completed_at_ts != 0))
		return false;

	marker = &entry->payload_meta.forward_marker;
	status = (GcsBlockReplyStatus)entry->status;
	expected_holder_node = entry->reply_header.sender_node;
	forwarding_master_node = GcsBlockReplyHeaderGetForwardingMasterNode(&entry->reply_header);
	return dedup_forward_payload_valid(
			   &entry->key, &entry->tag, entry->transition_id, expected_holder_node,
			   forwarding_master_node, status, &marker->authority, &marker->forward)
		   && dedup_forward_entry_exact(entry, &entry->tag, expected_holder_node, status, marker,
										phase);
}

static bool
dedup_forward_cancel_from_entry_exact(
	const GcsBlockDedupEntry *entry, GcsBlockForwardCancelPayload *cancel)
{
	const GcsBlockForwardMarker *marker;
	const GcsBlockForwardBootIdentity *boot;

	if (entry == NULL || cancel == NULL
		|| !GcsBlockDedupEntryHasForwardMarker(entry)
		|| entry->transition_id != (uint8)PCM_TRANS_N_TO_S)
		return false;
	marker = &entry->payload_meta.forward_marker;
	boot = &marker->boot_identity;
	if (!dedup_forward_boot_identity_valid(boot)
		|| boot->requester_incarnation == 0
		|| marker->authority.authority_generation == 0
		|| marker->forward.request_id != entry->key.request_id
		|| marker->forward.epoch != entry->key.cluster_epoch
		|| marker->forward.original_requester_node
			   != (int32)entry->key.origin_node_id
		|| marker->forward.requester_backend_id
			   != entry->key.requester_backend_id
		|| marker->forward.master_node != cluster_node_id
		|| memcmp(&marker->forward.tag, &entry->tag,
				  sizeof(entry->tag)) != 0)
		return false;

	memset(cancel, 0, sizeof(*cancel));
	cancel->request_id = entry->key.request_id;
	cancel->request_epoch = entry->key.cluster_epoch;
	cancel->pre_authority_generation
		= marker->authority.authority_generation;
	cancel->relation_generation = boot->relation_generation;
	cancel->expected_pi_watermark_scn
		= (uint64)GcsBlockForwardPayloadGetExpectedPiWatermarkScn(
			&marker->forward);
	cancel->requester_incarnation = boot->requester_incarnation;
	cancel->master_incarnation = boot->master_incarnation;
	cancel->holder_incarnation = boot->holder_incarnation;
	cancel->tag = entry->tag;
	cancel->requester_node = (int32)entry->key.origin_node_id;
	cancel->requester_backend_id = entry->key.requester_backend_id;
	cancel->master_node = marker->forward.master_node;
	cancel->holder_node = entry->reply_header.sender_node;
	cancel->phase
		= (uint8)GCS_FORWARD_CANCEL_PHASE_MASTER_TO_HOLDER;
	cancel->reason = (uint8)GCS_FORWARD_CANCEL_REASON_PENDING_X;
	cancel->proof = GCS_FORWARD_CANCEL_PROOF_MASTER_MASK;
	cancel->transition_id = (uint8)PCM_TRANS_N_TO_S;
	cancel->master_holder_capability_generation
		= boot->capability_generation;
	return cluster_gcs_forward_cancel_master_ingress_valid(
		cancel, sizeof(*cancel), cancel->master_node,
		cancel->request_epoch, cancel->master_node,
		cancel->holder_node, cancel->master_incarnation,
		cancel->holder_incarnation);
}

static bool
dedup_forward_cancel_entry_exact(
	const GcsBlockDedupEntry *entry,
	const GcsBlockForwardCancelPayload *cancel)
{
	GcsBlockForwardCancelPayload expected;

	return entry != NULL && cancel != NULL
		   && GcsBlockDedupEntryForwardMarkerPhase(entry)
				  == GCS_BLOCK_FORWARD_MARK_CANCELLING
		   && dedup_pending_x_forward_blocker_is_exact(entry)
		   && dedup_forward_cancel_from_entry_exact(entry, &expected)
		   && memcmp(&expected, cancel, sizeof(expected)) == 0;
}

static void
dedup_pending_x_install_denial(GcsBlockDedupEntry *entry)
{
	GcsBlockReplyHeader denial;

	memset(&denial, 0, sizeof(denial));
	denial.request_id = entry->key.request_id;
	denial.epoch = entry->key.cluster_epoch;
	denial.sender_node = cluster_node_id;
	denial.requester_backend_id = entry->key.requester_backend_id;
	denial.transition_id = (uint8)PCM_TRANS_N_TO_S;
	denial.status = (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X;
	GcsBlockReplyHeaderSetForwardingMasterNode(&denial, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);

	entry->status = (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X;
	entry->reply_header = denial;
	entry->has_sf_dep = false;
	entry->sf_flags = 0;
	entry->sf_dep_count = 0;
	cluster_sf_dep_vec_reset(&entry->payload_meta.sf_dep_vec);
	memset(entry->block_data, 0, sizeof(entry->block_data));
	entry->completed_at_ts = GetCurrentTimestamp();
	entry->done_at_ts = 0;
}

/* PCM-X queue arbitration must revoke the right of an older legacy reader
 * before type-49 can wait on that reader's GRANT_PENDING reservation.  The
 * scan returns at most one newly terminated identity per call so the caller
 * can send it without a bounded/sentinel array; after all live rights are
 * gone it returns one unacknowledged cached denial for loss recovery. */
GcsBlockPendingXDenyResult
cluster_gcs_block_dedup_pending_x_deny_next(int worker_id, const BufferTag *tag,
											GcsBlockDedupEntry *denied_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	HASH_SEQ_STATUS scan;
	GcsBlockDedupEntry *entry;
	GcsBlockDedupEntry replay;
	bool have_replay = false;

	Assert(tag != NULL);
	Assert(denied_out != NULL);
	if (tag == NULL || denied_out == NULL)
		return GCS_BLOCK_PENDING_X_DENY_INVALID;
	memset(denied_out, 0, sizeof(*denied_out));

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_PENDING_X_DENY_INVALID;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	hash_seq_init(&scan, htab);
	while ((entry = (GcsBlockDedupEntry *)hash_seq_search(&scan)) != NULL) {
		if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_GENERIC
			|| entry->transition_id != (uint8)PCM_TRANS_N_TO_S
			|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0 || entry->done_at_ts != 0)
			continue;

		/* A published/claimed forward is an authority record.  Queue-kind X
		 * arbitration may not silently overwrite it with a denial; the
		 * forward leg needs an explicit exact cancellation/certificate
		 * transition first. */
		if (GcsBlockDedupEntryHasForwardMarker(entry)) {
			GcsBlockForwardMarkerPhase phase
				= GcsBlockDedupEntryForwardMarkerPhase(entry);
			bool exact_blocker
				= dedup_pending_x_forward_blocker_is_exact(entry);
			GcsBlockForwardCancelPayload cancel;

			if (!exact_blocker) {
				hash_seq_term(&scan);
				LWLockRelease(&shard->lock.lock);
				return GCS_BLOCK_PENDING_X_DENY_INVALID;
			}
			/* PREPARED has no admitted transport frame, so the same shard
			 * lock may terminalize it without a holder barrier. */
			if (phase == GCS_BLOCK_FORWARD_MARK_PREPARED) {
				dedup_pending_x_install_denial(entry);
				*denied_out = *entry;
				hash_seq_term(&scan);
				LWLockRelease(&shard->lock.lock);
				return GCS_BLOCK_PENDING_X_DENY_NEW;
			}
			if (phase == GCS_BLOCK_FORWARD_MARK_CANCELLING) {
				memcpy(&cancel, entry->block_data, sizeof(cancel));
				if (!dedup_forward_cancel_entry_exact(entry, &cancel)) {
					hash_seq_term(&scan);
					LWLockRelease(&shard->lock.lock);
					return GCS_BLOCK_PENDING_X_DENY_INVALID;
				}
				*denied_out = *entry;
				hash_seq_term(&scan);
				LWLockRelease(&shard->lock.lock);
				return GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_REPLAY;
			}
			if (phase == GCS_BLOCK_FORWARD_MARK_SEND_ARMED
				|| phase == GCS_BLOCK_FORWARD_MARK_FORWARDED) {
				if (!dedup_forward_cancel_from_entry_exact(
						entry, &cancel)) {
					hash_seq_term(&scan);
					LWLockRelease(&shard->lock.lock);
					return GCS_BLOCK_PENDING_X_DENY_FORWARD_BLOCKED;
				}
				dedup_forward_phase_set(
					entry, GCS_BLOCK_FORWARD_MARK_CANCELLING);
				memset(entry->block_data, 0,
					   sizeof(entry->block_data));
				memcpy(entry->block_data, &cancel, sizeof(cancel));
				entry->completed_at_ts = GetCurrentTimestamp();
				*denied_out = *entry;
				hash_seq_term(&scan);
				LWLockRelease(&shard->lock.lock);
				return GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_NEW;
			}
			hash_seq_term(&scan);
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PENDING_X_DENY_FORWARD_BLOCKED;
		}
		if (dedup_pending_x_denial_is_exact(entry)) {
			if (!have_replay) {
				replay = *entry;
				have_replay = true;
			}
			continue;
		}
		if (entry->status == (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X
			&& entry->completed_at_ts != 0) {
			dedup_pcm_x_note_failclosed(shard);
			hash_seq_term(&scan);
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PENDING_X_DENY_INVALID;
		}
		if (!dedup_pending_x_entry_has_legacy_s_right(entry))
			continue;

		dedup_pending_x_install_denial(entry);
		*denied_out = *entry;
		hash_seq_term(&scan);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PENDING_X_DENY_NEW;
	}

	if (have_replay)
		*denied_out = replay;
	LWLockRelease(&shard->lock.lock);
	return have_replay ? GCS_BLOCK_PENDING_X_DENY_REPLAY : GCS_BLOCK_PENDING_X_DENY_NOT_FOUND;
}

GcsBlockForwardMarkResult
cluster_gcs_block_dedup_forward_cancel_ack_exact(
	int worker_id, const GcsBlockDedupKey *key,
	uint32 capability_generation,
	const GcsBlockForwardCancelPayload *ack,
	GcsBlockDedupEntry *denied_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsBlockForwardCancelPayload cancel;
	GcsBlockForwardCancelPayload expected_ack;
	bool found = false;

	if (denied_out != NULL)
		memset(denied_out, 0, sizeof(*denied_out));
	if (key == NULL || ack == NULL || capability_generation == 0
		|| !cluster_gcs_forward_cancel_ack_ingress_valid(
			ack, sizeof(*ack), ack->requester_node,
			ack->request_epoch, ack->master_node, ack->master_node,
			ack->requester_incarnation, ack->master_incarnation))
		return GCS_BLOCK_FORWARD_MARK_INVALID;
	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_FORWARD_MARK_INVALID;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(
		htab, key, HASH_FIND, &found);
	if (!found || entry == NULL) {
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_FORWARD_MARK_NOT_FOUND;
	}
	if (dedup_pending_x_denial_is_exact(entry)) {
		if (denied_out != NULL)
			*denied_out = *entry;
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_FORWARD_MARK_REPLAY;
	}
	memcpy(&cancel, entry->block_data, sizeof(cancel));
	if (!dedup_forward_cancel_entry_exact(entry, &cancel)) {
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_FORWARD_MARK_STALE;
	}
	expected_ack = cancel;
	expected_ack.phase
		= (uint8)GCS_FORWARD_CANCEL_PHASE_REQUESTER_FENCE_ACK;
	expected_ack.proof = GCS_FORWARD_CANCEL_PROOF_ACK_MASK;
	expected_ack.holder_requester_capability_generation
		= ack->holder_requester_capability_generation;
	expected_ack.requester_master_capability_generation
		= ack->requester_master_capability_generation;
	/* Each nonzero leg generation is a sender-local replay binding.  The
	 * current receiver-local generation proves live capability above but is
	 * not numerically comparable across independently reconnecting peers. */
	if (memcmp(&expected_ack, ack, sizeof(*ack)) != 0) {
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_FORWARD_MARK_STALE;
	}

	dedup_forward_phase_set(
		entry, GCS_BLOCK_FORWARD_MARK_CANCEL_FENCED);
	dedup_pending_x_install_denial(entry);
	if (denied_out != NULL)
		*denied_out = *entry;
	LWLockRelease(&shard->lock.lock);
	return GCS_BLOCK_FORWARD_MARK_INSTALLED;
}

GcsBlockPendingXDenyResult
cluster_gcs_block_dedup_pending_x_deny_exact(int worker_id, const GcsBlockDedupKey *key,
											 const BufferTag *tag, uint8 transition_id,
											 GcsBlockDedupEntry *denied_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	bool found = false;

	Assert(key != NULL);
	Assert(tag != NULL);
	Assert(denied_out != NULL);
	if (key == NULL || tag == NULL || denied_out == NULL
		|| transition_id != (uint8)PCM_TRANS_N_TO_S)
		return GCS_BLOCK_PENDING_X_DENY_INVALID;
	memset(denied_out, 0, sizeof(*denied_out));

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_PENDING_X_DENY_INVALID;
	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found || entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_GENERIC
		|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0 || entry->transition_id != transition_id) {
		if (found && entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_GENERIC)
			dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PENDING_X_DENY_INVALID;
	}
	if (dedup_pending_x_denial_is_exact(entry)) {
		*denied_out = *entry;
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PENDING_X_DENY_REPLAY;
	}
	if (GcsBlockDedupEntryHasForwardMarker(entry)) {
		GcsBlockForwardMarkerPhase phase
			= GcsBlockDedupEntryForwardMarkerPhase(entry);

		/*
		 * A live forward marker is an authority record, not corrupt generic
		 * dedup state.  The queue driver owns its exact cancellation/fence
		 * lifecycle; a racing same-identity N->S retry must wait without
		 * overwriting the marker or fabricating a terminal denial.
		 */
		if (entry->done_at_ts == 0
			&& phase == GCS_BLOCK_FORWARD_MARK_CANCELLING) {
			GcsBlockForwardCancelPayload cancel;

			memcpy(&cancel, entry->block_data, sizeof(cancel));
			if (!dedup_forward_cancel_entry_exact(entry, &cancel)) {
				LWLockRelease(&shard->lock.lock);
				return GCS_BLOCK_PENDING_X_DENY_INVALID;
			}
			*denied_out = *entry;
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PENDING_X_DENY_FORWARD_CANCEL_REPLAY;
		}
		if (phase == GCS_BLOCK_FORWARD_MARK_PREPARED
			|| phase == GCS_BLOCK_FORWARD_MARK_SEND_ARMED
			|| phase == GCS_BLOCK_FORWARD_MARK_FORWARDED
			|| phase == GCS_BLOCK_FORWARD_MARK_HOLDER_MISS_PENDING
			|| phase == GCS_BLOCK_FORWARD_MARK_HOLDER_FENCE_ACKED
			|| phase == GCS_BLOCK_FORWARD_MARK_CANCEL_FENCED
			|| entry->done_at_ts != 0) {
			*denied_out = *entry;
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PENDING_X_DENY_FORWARD_BLOCKED;
		}
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PENDING_X_DENY_INVALID;
	}
	if (entry->status == (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X && entry->completed_at_ts != 0) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PENDING_X_DENY_INVALID;
	}

	dedup_pending_x_install_denial(entry);
	*denied_out = *entry;
	LWLockRelease(&shard->lock.lock);
	return GCS_BLOCK_PENDING_X_DENY_NEW;
}

bool
cluster_gcs_block_dedup_set_request_flags_exact(int worker_id, const GcsBlockDedupKey *key,
												const BufferTag *tag, uint8 transition_id,
												uint8 request_flags)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	bool found = false;
	bool updated = false;

	Assert(key != NULL);
	Assert(tag != NULL);
	if (key == NULL || tag == NULL || (request_flags & ~GCS_BLOCK_DEDUP_REQUEST_F_VALID_MASK) != 0)
		return false;
	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return false;
	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (found && entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_GENERIC
		&& memcmp(&entry->tag, tag, sizeof(*tag)) == 0 && entry->transition_id == transition_id) {
		uint8 pinned_flags = GCS_BLOCK_DEDUP_REQUEST_F_PINNED | request_flags;

		if (entry->request_flags == 0)
			entry->request_flags = pinned_flags;
		else if (entry->request_flags
					 == (GCS_BLOCK_DEDUP_REQUEST_F_PINNED
						 | GCS_BLOCK_DEDUP_REQUEST_F_DIRECT_LAND)
				 && pinned_flags == GCS_BLOCK_DEDUP_REQUEST_F_PINNED)
			/*
			 * The requester deliberately suppresses direct-land after an
			 * authoritative direct denial while retaining the same request
			 * identity.  This is a one-way transport downgrade, not a grant
			 * identity change.  Clearing the bit also ensures a cached reply
			 * is resent on the generic lane; generic -> DIRECT remains
			 * rejected below.
			 */
			entry->request_flags = pinned_flags;
		updated = entry->request_flags == pinned_flags;
	} else if (found && entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_GENERIC)
		dedup_pcm_x_note_failclosed(shard);
	LWLockRelease(&shard->lock.lock);
	return updated;
}

/*
 * Read-only exact snapshot for rare fail-closed diagnostics.  No pointer or
 * lock escapes: the caller gets one byte-stable entry image, or false when
 * the full immutable generic identity is absent/mismatched.
 */
bool
cluster_gcs_block_dedup_snapshot_exact(int worker_id, const GcsBlockDedupKey *key,
									   const BufferTag *tag, uint8 transition_id,
									   GcsBlockDedupEntry *entry_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	bool found = false;
	bool exact = false;

	Assert(key != NULL);
	Assert(tag != NULL);
	Assert(entry_out != NULL);
	if (entry_out != NULL)
		memset(entry_out, 0, sizeof(*entry_out));
	if (key == NULL || tag == NULL || entry_out == NULL)
		return false;
	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return false;

	LWLockAcquire(&shard->lock.lock, LW_SHARED);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (found && entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_GENERIC
		&& memcmp(&entry->tag, tag, sizeof(*tag)) == 0
		&& entry->transition_id == transition_id) {
		*entry_out = *entry;
		exact = true;
	}
	LWLockRelease(&shard->lock.lock);
	return exact;
}

GcsBlockPcmXImageResult
cluster_gcs_block_dedup_pcm_x_reserve(int worker_id, const GcsBlockDedupKey *key,
									  const BufferTag *tag,
									  const GcsBlockPcmXImageBinding *reserved_binding)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	bool found = false;

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_PCM_X_IMAGE_FULL;
	if (!dedup_pcm_x_binding_valid(key, tag, reserved_binding, true)) {
		dedup_pcm_x_note_failclosed(shard);
		return GCS_BLOCK_PCM_X_IMAGE_INVALID;
	}

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (found) {
		if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_DRAINED
			&& !dedup_pcm_x_entry_drained_valid(key, tag, entry)) {
			dedup_pcm_x_note_failclosed(shard);
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PCM_X_IMAGE_STALE;
		}
		if ((entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_RESERVED
			 || entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE
			 || entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_MATERIALIZED_UNCOMMITTED
			 || entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_DRAINED)
			&& entry->transition_id == (uint8)PCM_TRANS_N_TO_S
			&& memcmp(&entry->tag, tag, sizeof(*tag)) == 0
			&& dedup_pcm_x_reservation_equal(entry, reserved_binding)) {
			if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_DRAINED)
				dedup_pcm_x_work_pending[worker_id] = true;
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PCM_X_IMAGE_DUPLICATE;
		}
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}

	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_ENTER_NULL, &found);
	if (entry == NULL
		&& dedup_reclaim_reclaimable_locked(shard, htab, GetCurrentTimestamp(), 1) > 0)
		entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_ENTER_NULL, &found);
	if (entry == NULL || found) {
		pg_atomic_fetch_add_u64(&shard->full_count, 1);
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_FULL;
	}

	memset(((char *)entry) + sizeof(GcsBlockDedupKey), 0,
		   sizeof(GcsBlockDedupEntry) - sizeof(GcsBlockDedupKey));
	entry->tag = *tag;
	entry->transition_id = (uint8)PCM_TRANS_N_TO_S;
	entry->entry_kind = GCS_BLOCK_DEDUP_ENTRY_PCM_X_RESERVED;
	entry->pcm_x_master_session = reserved_binding->master_session;
	entry->pinned_lifetime_us = (int64)reserved_binding->required_page_scn;
	entry->payload_meta.pcm_x_identity = reserved_binding->identity;
	entry->registered_at_ts = GetCurrentTimestamp();
	pg_atomic_fetch_add_u32(&shard->entry_count, 1);
	dedup_pcm_x_work_pending[worker_id] = true;
	LWLockRelease(&shard->lock.lock);
	return GCS_BLOCK_PCM_X_IMAGE_RESERVED;
}

GcsBlockPcmXImageResult
cluster_gcs_block_dedup_pcm_x_materialize(int worker_id, const GcsBlockDedupKey *key,
										  const BufferTag *tag,
										  const GcsBlockPcmXImageBinding *ready_binding,
										  uint64 reservation_token, uint8 source_pcm_state,
										  const GcsBlockReplyHeader *reply_header,
										  const char *block_data)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	bool found = false;

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_PCM_X_IMAGE_FULL;
	if (reservation_token == 0 || !dedup_pcm_x_source_state_valid(source_pcm_state)
		|| !dedup_pcm_x_ready_payload_valid(key, tag, ready_binding, reply_header, block_data)) {
		dedup_pcm_x_note_failclosed(shard);
		return GCS_BLOCK_PCM_X_IMAGE_INVALID;
	}

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND;
	}
	if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE
		|| entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_MATERIALIZED_UNCOMMITTED) {
		GcsBlockPcmXImageBinding stored_binding;

		dedup_pcm_x_binding_from_entry(entry, &stored_binding);
		if (entry->transition_id == (uint8)PCM_TRANS_N_TO_S
			&& memcmp(&entry->tag, tag, sizeof(*tag)) == 0
			&& GcsBlockPcmXImageBindingEqual(&stored_binding, ready_binding)
			&& dedup_pcm_x_entry_payload_valid(key, tag, entry)
			&& (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE
				|| (dedup_pcm_x_reservation_token_get(entry) == reservation_token
					&& entry->request_flags == source_pcm_state))
			&& memcmp(&entry->reply_header, reply_header, sizeof(*reply_header)) == 0
			&& memcmp(entry->block_data, block_data, GCS_BLOCK_DATA_SIZE) == 0) {
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PCM_X_IMAGE_DUPLICATE;
		}
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}
	if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_RESERVED
		|| entry->transition_id != (uint8)PCM_TRANS_N_TO_S
		|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0
		|| !dedup_pcm_x_reservation_equal(entry, ready_binding)) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}

	{
		GcsBlockPcmXImageIdentity reserved_identity = entry->payload_meta.pcm_x_identity;
		GcsBlockPcmXImageBinding stored_binding;

		entry->reply_header = *reply_header;
		entry->status = (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER;
		entry->request_flags = source_pcm_state;
		dedup_pcm_x_reservation_token_set(entry, reservation_token);
		entry->payload_meta.pcm_x_identity = ready_binding->identity;
		memcpy(entry->block_data, block_data, GCS_BLOCK_DATA_SIZE);
		entry->completed_at_ts = GetCurrentTimestamp();
		dedup_pcm_x_binding_from_entry(entry, &stored_binding);
		if (!dedup_pcm_x_ready_payload_valid(key, tag, &stored_binding, &entry->reply_header,
											 entry->block_data)) {
			memset(&entry->reply_header, 0, sizeof(entry->reply_header));
			memset(entry->block_data, 0, GCS_BLOCK_DATA_SIZE);
			entry->payload_meta.pcm_x_identity = reserved_identity;
			entry->status = 0;
			entry->request_flags = 0;
			dedup_pcm_x_reservation_token_set(entry, 0);
			entry->completed_at_ts = 0;
			dedup_pcm_x_note_failclosed(shard);
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PCM_X_IMAGE_INVALID;
		}
	}
	entry->entry_kind = GCS_BLOCK_DEDUP_ENTRY_PCM_X_MATERIALIZED_UNCOMMITTED;
	dedup_pcm_x_work_pending[worker_id] = true;
	pg_atomic_fetch_add_u64(&shard->pcm_x_stage_count, 1);
	LWLockRelease(&shard->lock.lock);
	return GCS_BLOCK_PCM_X_IMAGE_STORED;
}


GcsBlockPcmXImageResult
cluster_gcs_block_dedup_pcm_x_publish_ready_exact(int worker_id, const GcsBlockDedupKey *key,
												  const BufferTag *tag,
												  const GcsBlockPcmXImageBinding *ready_binding)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsBlockPcmXImageBinding stored_binding;
	bool found = false;

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_PCM_X_IMAGE_FULL;
	if (!dedup_pcm_x_binding_valid(key, tag, ready_binding, false)) {
		dedup_pcm_x_note_failclosed(shard);
		return GCS_BLOCK_PCM_X_IMAGE_INVALID;
	}

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND;
	}
	dedup_pcm_x_binding_from_entry(entry, &stored_binding);
	if (entry->transition_id != (uint8)PCM_TRANS_N_TO_S
		|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0
		|| !GcsBlockPcmXImageBindingEqual(&stored_binding, ready_binding)
		|| !dedup_pcm_x_entry_payload_valid(key, tag, entry)) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}
	if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE) {
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_DUPLICATE;
	}
	if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_MATERIALIZED_UNCOMMITTED) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}

	entry->entry_kind = GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE;
	entry->request_flags = 0;
	dedup_pcm_x_reservation_token_set(entry, 0);
	dedup_pcm_x_work_pending[worker_id] = true;
	LWLockRelease(&shard->lock.lock);
	return GCS_BLOCK_PCM_X_IMAGE_STORED;
}

GcsBlockPcmXImageResult
cluster_gcs_block_dedup_pcm_x_lookup(int worker_id, const GcsBlockDedupKey *key,
									 const BufferTag *tag,
									 const GcsBlockPcmXImageBinding *expected_binding,
									 GcsBlockDedupEntry *cached_reply_out)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsBlockPcmXImageBinding stored_binding;
	bool found = false;
	bool binding_is_reserved;

	if (cached_reply_out != NULL)
		memset(cached_reply_out, 0, sizeof(*cached_reply_out));
	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_PCM_X_IMAGE_FULL;
	binding_is_reserved = expected_binding != NULL && expected_binding->identity.image.page_scn == 0
						  && expected_binding->identity.image.page_lsn == 0
						  && expected_binding->identity.image.page_checksum == 0;
	if (!dedup_pcm_x_binding_valid(key, tag, expected_binding, binding_is_reserved)) {
		dedup_pcm_x_note_failclosed(shard);
		return GCS_BLOCK_PCM_X_IMAGE_INVALID;
	}

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND;
	}
	if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_DRAINED) {
		dedup_pcm_x_binding_from_entry(entry, &stored_binding);
		if (entry->transition_id != (uint8)PCM_TRANS_N_TO_S
			|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0
			|| !GcsBlockPcmXImageBindingEqual(&stored_binding, expected_binding)
			|| !dedup_pcm_x_entry_drained_valid(key, tag, entry)) {
			dedup_pcm_x_note_failclosed(shard);
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PCM_X_IMAGE_STALE;
		}
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND;
	}
	if (entry->transition_id != (uint8)PCM_TRANS_N_TO_S
		|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0
		|| (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_RESERVED
			&& entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE
			&& entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_MATERIALIZED_UNCOMMITTED)) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}
	dedup_pcm_x_binding_from_entry(entry, &stored_binding);
	if (!GcsBlockPcmXImageBindingEqual(&stored_binding, expected_binding)) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}
	if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_RESERVED) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_READY;
	}
	if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_MATERIALIZED_UNCOMMITTED) {
		if (!dedup_pcm_x_entry_payload_valid(key, tag, entry)) {
			dedup_pcm_x_note_failclosed(shard);
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PCM_X_IMAGE_STALE;
		}
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_READY;
	}
	if (!dedup_pcm_x_entry_ready_valid(key, tag, entry)) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}

	if (cached_reply_out != NULL)
		*cached_reply_out = *entry;
	pg_atomic_fetch_add_u64(&shard->pcm_x_replay_count, 1);
	LWLockRelease(&shard->lock.lock);
	return GCS_BLOCK_PCM_X_IMAGE_REPLAY;
}

GcsBlockPcmXImageResult
cluster_gcs_block_dedup_pcm_x_drain_status_exact(int worker_id, const GcsBlockDedupKey *key,
												 const BufferTag *tag,
												 const GcsBlockPcmXImageBinding *binding)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsBlockPcmXImageBinding stored_binding;
	bool found = false;

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_PCM_X_IMAGE_FULL;
	if (!dedup_pcm_x_binding_valid(key, tag, binding, false)) {
		dedup_pcm_x_note_failclosed(shard);
		return GCS_BLOCK_PCM_X_IMAGE_INVALID;
	}

	LWLockAcquire(&shard->lock.lock, LW_SHARED);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND;
	}
	dedup_pcm_x_binding_from_entry(entry, &stored_binding);
	if (entry->transition_id != (uint8)PCM_TRANS_N_TO_S
		|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0
		|| !GcsBlockPcmXImageBindingEqual(&stored_binding, binding)
		|| (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE
			&& entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_DRAINED)
		|| (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE
				? !dedup_pcm_x_entry_ready_valid(key, tag, entry)
				: !dedup_pcm_x_entry_drained_valid(key, tag, entry))) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}
	if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_DRAINED) {
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_DUPLICATE;
	}
	LWLockRelease(&shard->lock.lock);
	return GCS_BLOCK_PCM_X_IMAGE_NOT_READY;
}


GcsBlockPcmXImageResult
cluster_gcs_block_dedup_pcm_x_release_exact(int worker_id, const GcsBlockDedupKey *key,
											const BufferTag *tag,
											const GcsBlockPcmXImageBinding *binding,
											int32 drained_master_node)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsBlockPcmXImageBinding stored_binding;
	bool found = false;
	bool binding_is_reserved;

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_PCM_X_IMAGE_FULL;
	binding_is_reserved = binding != NULL && binding->identity.image.page_scn == 0
						  && binding->identity.image.page_lsn == 0
						  && binding->identity.image.page_checksum == 0;
	if (!dedup_pcm_x_binding_valid(key, tag, binding, binding_is_reserved)) {
		dedup_pcm_x_note_failclosed(shard);
		return GCS_BLOCK_PCM_X_IMAGE_INVALID;
	}
	if (drained_master_node < -1 || drained_master_node >= PCM_X_PROTOCOL_NODE_LIMIT) {
		dedup_pcm_x_note_failclosed(shard);
		return GCS_BLOCK_PCM_X_IMAGE_INVALID;
	}

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND;
	}
	dedup_pcm_x_binding_from_entry(entry, &stored_binding);
	if (entry->transition_id != (uint8)PCM_TRANS_N_TO_S
		|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0
		|| (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_RESERVED
			&& entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE
			&& entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_MATERIALIZED_UNCOMMITTED
			&& entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_DRAINED)
		|| !GcsBlockPcmXImageBindingEqual(&stored_binding, binding)) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}
	if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_DRAINED) {
		if (!dedup_pcm_x_entry_drained_valid(key, tag, entry)) {
			dedup_pcm_x_note_failclosed(shard);
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PCM_X_IMAGE_STALE;
		}
		if (drained_master_node < 0 || entry->request_flags != (uint8)(drained_master_node + 1)) {
			dedup_pcm_x_note_failclosed(shard);
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PCM_X_IMAGE_STALE;
		}
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_DUPLICATE;
	}
	if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE) {
		if (drained_master_node < 0) {
			dedup_pcm_x_note_failclosed(shard);
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PCM_X_IMAGE_INVALID;
		}
		if (!dedup_pcm_x_entry_ready_valid(key, tag, entry)) {
			dedup_pcm_x_note_failclosed(shard);
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PCM_X_IMAGE_STALE;
		}
		entry->entry_kind = GCS_BLOCK_DEDUP_ENTRY_PCM_X_DRAINED;
		entry->request_flags = (uint8)(drained_master_node + 1);
		entry->done_at_ts = 0;
		pg_atomic_fetch_add_u64(&shard->pcm_x_release_count, 1);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_RELEASED;
	}
	if (drained_master_node != -1) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_INVALID;
	}

	(void)hash_search(htab, key, HASH_REMOVE, &found);
	if (!found) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}
	pg_atomic_fetch_sub_u32(&shard->entry_count, 1);
	pg_atomic_fetch_add_u64(&shard->pcm_x_release_count, 1);
	LWLockRelease(&shard->lock.lock);
	return GCS_BLOCK_PCM_X_IMAGE_RELEASED;
}


bool
cluster_gcs_block_dedup_pcm_x_retire_up_to_observed(
	uint64 cluster_epoch, int32 authenticated_master_node, uint64 authenticated_master_session,
	uint64 retire_through_ticket_id, uint64 *removed_out)
{
	int s;
	uint64 removed_total = 0;

	if (removed_out != NULL)
		*removed_out = 0;
	if (authenticated_master_node < 0 || authenticated_master_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| authenticated_master_session == 0 || retire_through_ticket_id == 0
		|| cluster_gcs_block_dedup_shards == NULL)
		return false;

	for (s = 0; s < cluster_gcs_block_dedup_n_shards; s++) {
		ClusterGcsBlockDedupShard *shard = &cluster_gcs_block_dedup_shards[s];
		HTAB *htab = cluster_gcs_block_dedup_htabs[s];
		HASH_SEQ_STATUS scan;
		GcsBlockDedupEntry *entry;
		int removed = 0;

		if (htab == NULL)
			continue;
		LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
		hash_seq_init(&scan, htab);
		while ((entry = (GcsBlockDedupEntry *)hash_seq_search(&scan)) != NULL) {
			const PcmXTicketRef *ref;

			if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_DRAINED)
				continue;
			if (!dedup_pcm_x_entry_drained_valid(&entry->key, &entry->tag, entry)) {
				dedup_pcm_x_note_failclosed(shard);
				hash_seq_term(&scan);
				LWLockRelease(&shard->lock.lock);
				return false;
			}
			ref = &entry->payload_meta.pcm_x_identity.ref;
			if (ref->identity.cluster_epoch == cluster_epoch
				&& entry->request_flags == (uint8)(authenticated_master_node + 1)
				&& entry->pcm_x_master_session == authenticated_master_session
				&& ref->handle.ticket_id <= retire_through_ticket_id) {
				(void)hash_search(htab, &entry->key, HASH_REMOVE, NULL);
				removed++;
			}
		}
		if (removed > 0) {
			pg_atomic_fetch_sub_u32(&shard->entry_count, (uint32)removed);
			removed_total += (uint64)removed;
		}
		LWLockRelease(&shard->lock.lock);
	}
	if (removed_out != NULL)
		*removed_out = removed_total;
	return true;
}


bool
cluster_gcs_block_dedup_pcm_x_retire_up_to(uint64 cluster_epoch, int32 authenticated_master_node,
										   uint64 authenticated_master_session,
										   uint64 retire_through_ticket_id)
{
	return cluster_gcs_block_dedup_pcm_x_retire_up_to_observed(
		cluster_epoch, authenticated_master_node, authenticated_master_session,
		retire_through_ticket_id, NULL);
}


GcsBlockPcmXImageResult
cluster_gcs_block_dedup_pcm_x_preserve_finish_error_exact(
	int worker_id, const GcsBlockDedupKey *key, const BufferTag *tag,
	const GcsBlockPcmXImageBinding *binding, uint64 reservation_token, uint8 source_pcm_state)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsBlockPcmXImageBinding stored_binding;
	bool found = false;

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_PCM_X_IMAGE_FULL;
	if (reservation_token == 0 || !dedup_pcm_x_source_state_valid(source_pcm_state)
		|| !dedup_pcm_x_binding_valid(key, tag, binding, false)) {
		dedup_pcm_x_note_failclosed(shard);
		return GCS_BLOCK_PCM_X_IMAGE_INVALID;
	}

	LWLockAcquire(&shard->lock.lock, LW_SHARED);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND;
	}
	dedup_pcm_x_binding_from_entry(entry, &stored_binding);
	if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_MATERIALIZED_UNCOMMITTED
		|| entry->transition_id != (uint8)PCM_TRANS_N_TO_S
		|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0
		|| !GcsBlockPcmXImageBindingEqual(&stored_binding, binding)
		|| !dedup_pcm_x_entry_payload_valid(key, tag, entry)
		|| dedup_pcm_x_reservation_token_get(entry) != reservation_token
		|| entry->request_flags != source_pcm_state) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}
	LWLockRelease(&shard->lock.lock);
	return GCS_BLOCK_PCM_X_IMAGE_COMMIT_PENDING;
}


static void
dedup_pcm_x_copy_work(const GcsBlockDedupEntry *entry, GcsBlockPcmXImageWork *work)
{
	memset(work, 0, sizeof(*work));
	work->key = entry->key;
	dedup_pcm_x_binding_from_entry(entry, &work->binding);
	if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_MATERIALIZED_UNCOMMITTED) {
		work->reservation_token = dedup_pcm_x_reservation_token_get(entry);
		work->source_pcm_state = entry->request_flags;
	}
	work->tag = entry->tag;
	work->entry_kind = entry->entry_kind;
}


static int
dedup_pcm_x_key_compare(const GcsBlockDedupKey *left, const GcsBlockDedupKey *right)
{
	if (left->origin_node_id != right->origin_node_id)
		return left->origin_node_id < right->origin_node_id ? -1 : 1;
	if (left->requester_backend_id != right->requester_backend_id)
		return left->requester_backend_id < right->requester_backend_id ? -1 : 1;
	if (left->request_id != right->request_id)
		return left->request_id < right->request_id ? -1 : 1;
	if (left->cluster_epoch != right->cluster_epoch)
		return left->cluster_epoch < right->cluster_epoch ? -1 : 1;
	return 0;
}


static void
dedup_pcm_x_consider_work(const GcsBlockDedupEntry *entry, const GcsBlockDedupKey *cursor,
						  bool cursor_valid, GcsBlockPcmXImageWork *after, bool *have_after,
						  GcsBlockPcmXImageWork *wrap, bool *have_wrap)
{
	bool is_after = cursor_valid && dedup_pcm_x_key_compare(&entry->key, cursor) > 0;

	if (is_after && (!*have_after || dedup_pcm_x_key_compare(&entry->key, &after->key) < 0)) {
		dedup_pcm_x_copy_work(entry, after);
		*have_after = true;
	}
	if (!*have_wrap || dedup_pcm_x_key_compare(&entry->key, &wrap->key) < 0) {
		dedup_pcm_x_copy_work(entry, wrap);
		*have_wrap = true;
	}
}


/* Return at most one immutable work token per LMS tick.  Admission work
 * includes fresh RESERVED entries and commit-only retries for immutable
 * MATERIALIZED_UNCOMMITTED entries; the common exact-key cursor prevents a
 * contended commit from monopolizing the worker.  Admission wins the first
 * mixed-class tick, then selections alternate with READY replay. */
GcsBlockPcmXImageResult
cluster_gcs_block_dedup_pcm_x_next_work(int worker_id, GcsBlockPcmXImageWork *work_out)
{
	ClusterGcsBlockDedupShard *shard;
	GcsBlockPcmXImageWork ready_after;
	GcsBlockPcmXImageWork ready_wrap;
	GcsBlockPcmXImageWork reserved_after;
	GcsBlockPcmXImageWork reserved_wrap;
	HTAB *htab = NULL;
	HASH_SEQ_STATUS scan;
	GcsBlockDedupEntry *entry;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageResult result = GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND;
	bool have_ready_after = false;
	bool have_ready_wrap = false;
	bool have_reserved_after = false;
	bool have_reserved_wrap = false;

	if (work_out != NULL)
		memset(work_out, 0, sizeof(*work_out));
	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_PCM_X_IMAGE_FULL;
	if (work_out == NULL) {
		dedup_pcm_x_note_failclosed(shard);
		return GCS_BLOCK_PCM_X_IMAGE_INVALID;
	}
	if (!dedup_pcm_x_work_pending[worker_id])
		return GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	hash_seq_init(&scan, htab);
	while ((entry = (GcsBlockDedupEntry *)hash_seq_search(&scan)) != NULL) {
		if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_RESERVED
			&& entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE
			&& entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_MATERIALIZED_UNCOMMITTED)
			continue;
		/* A READY entry admitted to the outbound ring sleeps until an exact
		 * type-49 retransmit clears this marker. */
		if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE && entry->done_at_ts != 0)
			continue;
		dedup_pcm_x_binding_from_entry(entry, &binding);
		if (entry->transition_id != (uint8)PCM_TRANS_N_TO_S
			|| (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_RESERVED
				&& !dedup_pcm_x_binding_valid(&entry->key, &entry->tag, &binding, true))
			|| (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_MATERIALIZED_UNCOMMITTED
				&& (!dedup_pcm_x_entry_payload_valid(&entry->key, &entry->tag, entry)
					|| dedup_pcm_x_reservation_token_get(entry) == 0
					|| !dedup_pcm_x_source_state_valid(entry->request_flags)))
			|| (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE
				&& !dedup_pcm_x_entry_ready_valid(&entry->key, &entry->tag, entry))) {
			dedup_pcm_x_note_failclosed(shard);
			result = GCS_BLOCK_PCM_X_IMAGE_INVALID;
			break;
		}
		if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_RESERVED
			|| entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_MATERIALIZED_UNCOMMITTED) {
			dedup_pcm_x_consider_work(entry, &dedup_pcm_x_work_cursor[worker_id],
									  dedup_pcm_x_work_cursor_valid[worker_id], &reserved_after,
									  &have_reserved_after, &reserved_wrap, &have_reserved_wrap);
		} else
			dedup_pcm_x_consider_work(entry, &dedup_pcm_x_work_cursor[worker_id],
									  dedup_pcm_x_work_cursor_valid[worker_id], &ready_after,
									  &have_ready_after, &ready_wrap, &have_ready_wrap);
	}
	/* hash_seq_search() terminates a naturally exhausted scan itself.  Only
	 * the validation-failure break above leaves an open scan to close here. */
	if (result != GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND)
		hash_seq_term(&scan);
	if (result == GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND) {
		bool have_ready = have_ready_after || have_ready_wrap;
		bool have_reserved = have_reserved_after || have_reserved_wrap;
		bool choose_ready = have_ready && have_reserved && dedup_pcm_x_prefer_ready_next[worker_id];

		if (have_reserved && !choose_ready) {
			*work_out = have_reserved_after ? reserved_after : reserved_wrap;
			result = work_out->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_RESERVED
						 ? GCS_BLOCK_PCM_X_IMAGE_RESERVED
						 : GCS_BLOCK_PCM_X_IMAGE_COMMIT_PENDING;
		} else if (have_ready) {
			*work_out = have_ready_after ? ready_after : ready_wrap;
			result = GCS_BLOCK_PCM_X_IMAGE_REPLAY;
		}
		if (result != GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND) {
			dedup_pcm_x_work_cursor[worker_id] = work_out->key;
			dedup_pcm_x_work_cursor_valid[worker_id] = true;
			if (have_ready && have_reserved)
				dedup_pcm_x_prefer_ready_next[worker_id] = !choose_ready;
		}
	}
	if (result == GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND || result == GCS_BLOCK_PCM_X_IMAGE_INVALID)
		dedup_pcm_x_work_pending[worker_id] = false;
	LWLockRelease(&shard->lock.lock);
	return result;
}


/* Mark only outbound-ring admission, not application completion.  The exact
 * DRAIN_POLL consumer remains the sole owner of byte release. */
GcsBlockPcmXImageResult
cluster_gcs_block_dedup_pcm_x_mark_staged_exact(int worker_id, const GcsBlockDedupKey *key,
												const BufferTag *tag,
												const GcsBlockPcmXImageBinding *ready_binding)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsBlockPcmXImageBinding stored_binding;
	bool found = false;

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_PCM_X_IMAGE_FULL;
	if (!dedup_pcm_x_binding_valid(key, tag, ready_binding, false)) {
		dedup_pcm_x_note_failclosed(shard);
		return GCS_BLOCK_PCM_X_IMAGE_INVALID;
	}
	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND;
	}
	dedup_pcm_x_binding_from_entry(entry, &stored_binding);
	if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE
		|| entry->transition_id != (uint8)PCM_TRANS_N_TO_S
		|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0
		|| !GcsBlockPcmXImageBindingEqual(&stored_binding, ready_binding)
		|| !dedup_pcm_x_entry_ready_valid(key, tag, entry)) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}
	if (entry->done_at_ts != 0) {
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_DUPLICATE;
	}
	entry->done_at_ts = GetCurrentTimestamp();
	LWLockRelease(&shard->lock.lock);
	return GCS_BLOCK_PCM_X_IMAGE_STAGED;
}


/* Roll back only a failed outbound-ring admission.  The complete READY
 * binding is required because the reservation-shaped type-49 rearm API has a
 * different authority: it is remote retransmit evidence, not a local enqueue
 * transaction. */
GcsBlockPcmXImageResult
cluster_gcs_block_dedup_pcm_x_unmark_staged_exact(int worker_id, const GcsBlockDedupKey *key,
												  const BufferTag *tag,
												  const GcsBlockPcmXImageBinding *ready_binding)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	GcsBlockPcmXImageBinding stored_binding;
	bool found = false;

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_PCM_X_IMAGE_FULL;
	if (!dedup_pcm_x_binding_valid(key, tag, ready_binding, false)) {
		dedup_pcm_x_note_failclosed(shard);
		return GCS_BLOCK_PCM_X_IMAGE_INVALID;
	}
	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND;
	}
	dedup_pcm_x_binding_from_entry(entry, &stored_binding);
	if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE
		|| entry->transition_id != (uint8)PCM_TRANS_N_TO_S
		|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0
		|| !GcsBlockPcmXImageBindingEqual(&stored_binding, ready_binding)
		|| !dedup_pcm_x_entry_ready_valid(key, tag, entry)) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}
	if (entry->done_at_ts == 0) {
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_DUPLICATE;
	}
	entry->done_at_ts = 0;
	dedup_pcm_x_work_pending[worker_id] = true;
	LWLockRelease(&shard->lock.lock);
	return GCS_BLOCK_PCM_X_IMAGE_REARMED;
}


/* A byte-exact type-49 retransmit means the master has not applied type 50.
 * It may re-open only the matching READY outbound marker; a still-RESERVED
 * entry already appears in the normal work scan and needs no state change. */
GcsBlockPcmXImageResult
cluster_gcs_block_dedup_pcm_x_rearm_exact(int worker_id, const GcsBlockDedupKey *key,
										  const BufferTag *tag,
										  const GcsBlockPcmXImageBinding *reserved_binding)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	bool found = false;

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return GCS_BLOCK_PCM_X_IMAGE_FULL;
	if (!dedup_pcm_x_binding_valid(key, tag, reserved_binding, true)) {
		dedup_pcm_x_note_failclosed(shard);
		return GCS_BLOCK_PCM_X_IMAGE_INVALID;
	}
	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND;
	}
	if ((entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_RESERVED
		 && entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE
		 && entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_PCM_X_DRAINED)
		|| entry->transition_id != (uint8)PCM_TRANS_N_TO_S
		|| memcmp(&entry->tag, tag, sizeof(*tag)) != 0
		|| !dedup_pcm_x_reservation_equal(entry, reserved_binding)) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}
	if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_RESERVED) {
		dedup_pcm_x_work_pending[worker_id] = true;
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_READY;
	}
	if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_PCM_X_DRAINED) {
		if (!dedup_pcm_x_entry_drained_valid(key, tag, entry)) {
			dedup_pcm_x_note_failclosed(shard);
			LWLockRelease(&shard->lock.lock);
			return GCS_BLOCK_PCM_X_IMAGE_STALE;
		}
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_NOT_READY;
	}
	if (!dedup_pcm_x_entry_ready_valid(key, tag, entry)) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_STALE;
	}
	if (entry->done_at_ts == 0) {
		dedup_pcm_x_work_pending[worker_id] = true;
		LWLockRelease(&shard->lock.lock);
		return GCS_BLOCK_PCM_X_IMAGE_DUPLICATE;
	}
	entry->done_at_ts = 0;
	dedup_pcm_x_work_pending[worker_id] = true;
	LWLockRelease(&shard->lock.lock);
	return GCS_BLOCK_PCM_X_IMAGE_REARMED;
}


/* A newly forked LMS has no proof about which instruction the previous owner
 * completed.  Any dedicated entry is therefore retained recovery evidence;
 * the caller transitions the PCM-X runtime to RECOVERY_BLOCKED. */
bool
cluster_gcs_block_dedup_pcm_x_restart_audit(int worker_id)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	HASH_SEQ_STATUS scan;
	GcsBlockDedupEntry *entry;
	bool evidence_found = false;

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return true;

	LWLockAcquire(&shard->lock.lock, LW_SHARED);
	hash_seq_init(&scan, htab);
	while ((entry = (GcsBlockDedupEntry *)hash_seq_search(&scan)) != NULL) {
		if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_GENERIC) {
			evidence_found = true;
			hash_seq_term(&scan);
			break;
		}
	}
	if (evidence_found)
		dedup_pcm_x_note_failclosed(shard);
	LWLockRelease(&shard->lock.lock);
	return evidence_found;
}

/*
 * cluster_gcs_block_dedup_mark_done — GCS-race round-2 RC-F: consume a
 * requester completion proof.  Full identity verification + COMPLETED
 * check happen under the same exclusive shard lock as the stamp, so a
 * concurrent retransmit lookup can never observe a half-marked entry.
 *
 * PGRAC modifications by SqlRush (S3-P0-09)
 *	What changed:  the identity gate now also refuses a cell that still
 *				   carries a live forward marker (phase FORWARDED or
 *				   CANCELLING).  Such a DONE is counted and dropped instead
 *				   of stamping done_at_ts.
 *	Why:  the previous gate {GENERIC, tag, transition_id, completed_at_ts != 0}
 *		  had no phase leg, and BOTH live marker phases carry a nonzero
 *		  completed_at_ts (the consistency invariant in
 *		  dedup_pending_x_forward_blocker_is_exact() states exactly that;
 *		  forward_send_finish_exact() and the deny_next() FORWARDED ->
 *		  CANCELLING flip are the two producers).  A generic requester DONE
 *		  therefore satisfied every gate on a forward leg that was still in
 *		  flight and silently terminalized it:
 *
 *		    - pending_x_deny_next() skips a done_at_ts != 0 entry BEFORE it
 *		      looks at the phase, so it reported NOT_FOUND and the PCM-X
 *		      queue admitted the X conversion, while pending_x_deny_exact()
 *		      still answered FORWARD_BLOCKED for the very same cell — two
 *		      authority functions disagreeing about one cell;
 *		    - the backend-exit and node-dead sweeps retain a marker only
 *		      while done_at_ts == 0, so the next sweep HASH_REMOVEd a live
 *		      CANCELLING cell with no type-67 ACK and no trace;
 *		    - the forward-phase census never reads done_at_ts, so it kept
 *		      reporting cancelling_count = 1 for a cell the authority side
 *		      had already retired (CLAUDE.md rule 17: the observation face
 *		      must not contradict the authority face).
 *
 *		  DONE is advisory (the caller discards the result and the pinned TTL
 *		  is the backstop), and a forward marker owns a separate terminal
 *		  lifecycle — holder fence ACK / commit ACK / retire ACK, or the
 *		  cancel ACK which clears sf_flags — so refusing here retires
 *		  nothing early and leaks nothing: the marker is still reclaimed by
 *		  its own exact transitions.
 *
 *		  Scope note: FORWARDED and CANCELLING are not merely the phases we
 *		  chose to guard, they are provably the complete set that can reach
 *		  this gate.  Every other phase leaves completed_at_ts == 0
 *		  (PREPARED / SEND_ARMED / HOLDER_MISS_PENDING and the COMMITTED
 *		  chain that inherits from it) and is already refused by the
 *		  pre-existing completed_at_ts leg, while CANCEL_FENCED is
 *		  immediately followed by dedup_pending_x_install_denial(), which
 *		  zeroes sf_flags and therefore drops the marker altogether.
 */
bool
cluster_gcs_block_dedup_mark_done(int worker_id, const GcsBlockDedupKey *key, const BufferTag *tag,
								  uint8 transition_id)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	bool found = false;
	bool stamped = false;

	Assert(key != NULL);
	Assert(tag != NULL);

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return false;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (found && entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_GENERIC
		&& memcmp(&entry->tag, tag, sizeof(BufferTag)) == 0 && entry->transition_id == transition_id
		&& entry->completed_at_ts != 0) {
		GcsBlockForwardMarkerPhase phase = GcsBlockDedupEntryForwardMarkerPhase(entry);

		/* PGRAC: S3-P0-09 — a live forward leg outranks a generic completion
		 * proof.  Refuse without touching done_at_ts so the marker stays
		 * addressable by the cancel driver and reclaimable only by its own
		 * exact terminal transitions. */
		if (GcsBlockDedupEntryHasForwardMarker(entry)
			&& (phase == GCS_BLOCK_FORWARD_MARK_FORWARDED
				|| phase == GCS_BLOCK_FORWARD_MARK_CANCELLING)) {
			if (phase == GCS_BLOCK_FORWARD_MARK_FORWARDED)
				pg_atomic_fetch_add_u64(&shard->done_forwarded_refused_count, 1);
			else
				pg_atomic_fetch_add_u64(&shard->done_cancelling_refused_count, 1);
		} else {
			if (entry->done_at_ts == 0)
				entry->done_at_ts = GetCurrentTimestamp();
			stamped = true; /* duplicate DONE re-stamps nothing: idempotent */
			pg_atomic_fetch_add_u64(&shard->done_marked_count, 1);
		}
	} else {
		if (found && entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_GENERIC)
			dedup_pcm_x_note_failclosed(shard);
		pg_atomic_fetch_add_u64(&shard->done_mismatch_count, 1);
	}
	LWLockRelease(&shard->lock.lock);
	return stamped;
}

/*
 * cluster_gcs_block_dedup_note_done_mismatch — count a handler-level DONE
 * drop (transport identity binding / reserved-pad validation, review F6)
 * on the shard that would have consumed it.  Same counter as mark_done's
 * internal mismatch arm: operators read one "DONE dropped" surface.
 */
void
cluster_gcs_block_dedup_note_done_mismatch(int worker_id)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return;
	pg_atomic_fetch_add_u64(&shard->done_mismatch_count, 1);
}

/*
 * cluster_gcs_block_dedup_lifetime_ms — pure shared lifetime formula
 * (mirrors dedup_expiry_threshold_us WITHOUT the x2 margin or the us
 * conversion; the consumer applies its own margin).
 */
uint32
cluster_gcs_block_dedup_lifetime_ms(int initial_backoff_ms, int max_retries, int reply_timeout_ms)
{
	int64 lifetime_ms;

	if (initial_backoff_ms <= 0)
		initial_backoff_ms = 100;
	if (max_retries < 0)
		max_retries = 4;
	if (max_retries > 30)
		max_retries = 30;
	if (reply_timeout_ms <= 0)
		reply_timeout_ms = 5000;

	lifetime_ms = (int64)initial_backoff_ms * ((int64)((1u << max_retries) - 1))
				  + (int64)(max_retries + 1) * reply_timeout_ms;
	if (lifetime_ms > (int64)PG_UINT32_MAX)
		lifetime_ms = (int64)PG_UINT32_MAX;
	return (uint32)lifetime_ms;
}

void
cluster_gcs_block_dedup_install_reply_ex(int worker_id, const GcsBlockDedupKey *key,
										 GcsBlockReplyStatus status,
										 const GcsBlockReplyHeader *header, const char *block_data,
										 const ClusterSfDepVec *sf_dep_vec, bool has_sf_dep)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	bool found;
	bool has_block_payload;

	Assert(key != NULL);
	Assert(header != NULL);

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);

	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (!found) {
		/* Entry got swept between MISS_REGISTERED and install_reply
		 * (rare:  TTL sweep + reconfig race).  Drop silently — the
		 * sender will eventually time out and retry. */
		LWLockRelease(&shard->lock.lock);
		return;
	}
	if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_GENERIC) {
		dedup_pcm_x_note_failclosed(shard);
		LWLockRelease(&shard->lock.lock);
		return;
	}
	/* A queue-kind pending-X claim has already terminated this exact legacy
	 * reader.  Any asynchronous old producer has permanently lost the right
	 * to restore GRANTED/FORWARD/page bytes; preserve the cached denial for
	 * retransmit until exact DONE. */
	if (dedup_pending_x_denial_is_exact(entry)) {
		LWLockRelease(&shard->lock.lock);
		return;
	}
	/* PREPARED/SEND_ARMED/FORWARDED and certificate phases are exact
	 * authority records.  A late producer is never allowed to overlay their
	 * metadata or terminal posture. */
	if (GcsBlockDedupEntryHasForwardMarker(entry)) {
		LWLockRelease(&shard->lock.lock);
		return;
	}

	entry->status = (uint8)status;
	entry->reply_header = *header;
	entry->has_sf_dep = has_sf_dep;
	entry->sf_flags
		= has_sf_dep ? (GCS_BLOCK_REPLY_SF_HAS_DEP_VEC | GCS_BLOCK_REPLY_SF_EARLY_TRANSFER) : 0;
	entry->sf_dep_count = 0;
	cluster_sf_dep_vec_reset(&entry->payload_meta.sf_dep_vec);
	if (has_sf_dep && sf_dep_vec != NULL) {
		int i;

		for (i = 0; i < CLUSTER_SF_DEP_MAX_ORIGINS; i++) {
			if (XLogRecPtrIsInvalid(sf_dep_vec->required[i]))
				continue;
			entry->payload_meta.sf_dep_vec.required[i] = sf_dep_vec->required[i];
			entry->sf_dep_count++;
		}
	}
	has_block_payload = block_data != NULL
						&& (status == GCS_BLOCK_REPLY_GRANTED
							|| status == GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER);
	if (has_block_payload)
		memcpy(entry->block_data, block_data, GCS_BLOCK_DATA_SIZE);
	else
		memset(entry->block_data, 0, GCS_BLOCK_DATA_SIZE);
	entry->completed_at_ts = GetCurrentTimestamp();

	LWLockRelease(&shard->lock.lock);
}

void
cluster_gcs_block_dedup_install_reply(int worker_id, const GcsBlockDedupKey *key,
									  GcsBlockReplyStatus status, const GcsBlockReplyHeader *header,
									  const char *block_data)
{
	cluster_gcs_block_dedup_install_reply_ex(worker_id, key, status, header, block_data, NULL,
											 false);
}

void
cluster_gcs_block_dedup_remove(int worker_id, const GcsBlockDedupKey *key)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	bool found;

	Assert(key != NULL);

	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (found && entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_GENERIC
		&& !GcsBlockDedupEntryHasForwardMarker(entry)) {
		(void)hash_search(htab, key, HASH_REMOVE, &found);
		Assert(found);
		pg_atomic_fetch_sub_u32(&shard->entry_count, 1);
	} else if (found && entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_GENERIC)
		dedup_pcm_x_note_failclosed(shard);
	LWLockRelease(&shard->lock.lock);
}

bool
cluster_gcs_block_dedup_remove_inflight_exact(
	int worker_id, const GcsBlockDedupKey *key, const BufferTag *tag,
	uint8 transition_id)
{
	ClusterGcsBlockDedupShard *shard;
	HTAB *htab = NULL;
	GcsBlockDedupEntry *entry;
	bool found;
	bool removed = false;

	if (key == NULL || tag == NULL)
		return false;
	shard = cluster_gcs_block_dedup_resolve_shard(worker_id, &htab);
	if (shard == NULL)
		return false;

	LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
	entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
	if (found && entry != NULL
		&& entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_GENERIC
		&& BufferTagsEqual(&entry->tag, tag)
		&& entry->transition_id == transition_id
		&& !GcsBlockDedupEntryHasForwardMarker(entry)
		&& entry->completed_at_ts == 0 && entry->done_at_ts == 0) {
		(void)hash_search(htab, key, HASH_REMOVE, &found);
		Assert(found);
		pg_atomic_fetch_sub_u32(&shard->entry_count, 1);
		removed = true;
	}
	LWLockRelease(&shard->lock.lock);
	return removed;
}

/* ============================================================
 * GC paths — TTL sweep, backend exit, node DEAD.
 *
 * spec-7.3 D5 — these run in non-worker processes (LMON tick, requester
 * backend before_shmem_exit, CSSD DEAD hook) and a request's entries are
 * spread across shards by tag, so every GC path iterates all live shards.
 * ============================================================ */

/*
 * Compute the configured expiry threshold in microseconds.  The threshold
 * is 2 × the LEGAL REQUEST LIFETIME: every attempt may wait out a full
 * cluster.gcs_reply_timeout_ms before its retry fires, so the lifetime is
 * (max_retries + 1) reply-timeout windows PLUS the exponential backoff
 * total.  The pre-fix formula covered only the backoff component — with
 * the defaults that swept a still-live request's entry at 3s while its
 * attempts legally span ~26.5s (S3 rig: 25.5s vs 57.75s), so a late
 * retransmit re-registered as a MISS and re-executed a request whose
 * earlier attempt may already have granted.  Sweeping conservatively
 * biases toward retention.
 */
static int64
dedup_expiry_threshold_us(void)
{
	int64 initial_ms;
	int max_retries;
	int64 reply_timeout_ms;
	int64 lifetime_ms;

	initial_ms = cluster_gcs_block_retransmit_initial_backoff_ms > 0
					 ? cluster_gcs_block_retransmit_initial_backoff_ms
					 : 100;
	max_retries = cluster_gcs_block_retransmit_max_retries >= 0
					  ? cluster_gcs_block_retransmit_max_retries
					  : 4;
	reply_timeout_ms = cluster_gcs_reply_timeout_ms > 0 ? cluster_gcs_reply_timeout_ms : 5000;

	/* backoff total = initial × (2^max_retries - 1);  pin small max_retries
	 * to keep arithmetic in int64. */
	if (max_retries > 30)
		max_retries = 30;
	lifetime_ms = initial_ms * ((int64)((1u << max_retries) - 1))
				  + (int64)(max_retries + 1) * reply_timeout_ms;
	return lifetime_ms * 1000 * 2; /* × 2 safety margin (HC93) */
}

/*
 * dedup_reclaim_reclaimable_locked -- caller MUST hold this shard's dedup
 * lock exclusively.  Under cap pressure (HASH_ENTER_NULL about to fail),
 * scan the shard's HTAB and remove up to `want` reclaim-safe entries so the
 * MISS path can register instead of failing closed (spec-7.2a D1).
 *
 * Only entries GcsBlockDedupEntryIsReclaimSafe() approves are removed: an
 * entry aged past the 2x out-of-window threshold (safe for every status,
 * §3.1 theorem) or one whose status is site-proven in-window idempotent
 * (whitelist currently empty).  Reclaim NEVER removes an entry whose
 * retransmitted duplicate could be re-served incorrectly — it only ever
 * brings the FULL path forward in time, never sacrifices correctness.
 *
 * The scan is bounded to GCS_BLOCK_DEDUP_RECLAIM_MAX_PROBE entries so a
 * shard full of in-window entries does not turn every MISS into a full
 * O(cap) scan under the exclusive lock (spec-7.2a §6).  Returns the number
 * reclaimed.
 */
static int
dedup_reclaim_reclaimable_locked(ClusterGcsBlockDedupShard *shard, HTAB *htab, TimestampTz now,
								 int want)
{
	HASH_SEQ_STATUS seq;
	GcsBlockDedupEntry *entry;
	int64 out_of_window_us;
	int reclaimed = 0;
	int probed = 0;

	if (want <= 0)
		return 0;

	out_of_window_us = dedup_expiry_threshold_us();

	hash_seq_init(&seq, htab);
	while ((entry = (GcsBlockDedupEntry *)hash_seq_search(&seq)) != NULL) {
		if (GcsBlockDedupEntryIsReclaimSafe(entry, now, out_of_window_us)) {
			(void)hash_search(htab, &entry->key, HASH_REMOVE, NULL);
			reclaimed++;
		}

		if (reclaimed >= want || ++probed >= GCS_BLOCK_DEDUP_RECLAIM_MAX_PROBE) {
			hash_seq_term(&seq); /* early break must terminate the scan */
			break;
		}
	}

	if (reclaimed > 0) {
		pg_atomic_fetch_sub_u32(&shard->entry_count, (uint32)reclaimed);
		pg_atomic_fetch_add_u64(&shard->evict_count, (uint64)reclaimed);
	}
	return reclaimed;
}

void
cluster_gcs_block_dedup_sweep_expired(TimestampTz now)
{
	int64 threshold_us;
	int s;

	if (cluster_gcs_block_dedup_shards == NULL)
		return;

	threshold_us = dedup_expiry_threshold_us();

	/*
	 * spec-7.2a D5:  saturation LOG-once.  When DENIED_DEDUP_FULL keeps
	 * growing past a threshold across sweep cycles, emit one LOG so operators
	 * see sustained dedup saturation without flooding the hot request path
	 * (rule 17).  Lock-free (atomics only); the LMON sweep is the sole caller
	 * so the static high-water mark is process-local and race-free.  The
	 * counters aggregate over every shard (spec-7.3 D5).
	 */
	{
		static uint64 dedup_full_logged_hwm = 0;
		uint64 cur_full = cluster_gcs_block_dedup_get_full_count();

		if (cur_full - dedup_full_logged_hwm >= GCS_BLOCK_DEDUP_FULL_LOG_THRESHOLD) {
			elog(LOG,
				 "GCS block dedup saturating: %llu new DENIED_DEDUP_FULL since last report "
				 "(per-shard cap=%d, live entries=%llu); raise "
				 "cluster.gcs_block_dedup_max_entries if sustained",
				 (unsigned long long)(cur_full - dedup_full_logged_hwm),
				 cluster_gcs_block_dedup_ctl != NULL
					 ? cluster_gcs_block_dedup_ctl->max_entries_effective
					 : 0,
				 (unsigned long long)cluster_gcs_block_dedup_get_in_flight_count());
			dedup_full_logged_hwm = cur_full;
		}
	}

	for (s = 0; s < cluster_gcs_block_dedup_n_shards; s++) {
		ClusterGcsBlockDedupShard *shard = &cluster_gcs_block_dedup_shards[s];
		HTAB *htab = cluster_gcs_block_dedup_htabs[s];
		HASH_SEQ_STATUS seq;
		GcsBlockDedupEntry *entry;
		int removed = 0;

		if (htab == NULL)
			continue;

		LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);

		hash_seq_init(&seq, htab);
		while ((entry = (GcsBlockDedupEntry *)hash_seq_search(&seq)) != NULL) {
			TimestampTz anchor;
			int64 age_us;
			int64 deadline_us;

			if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_GENERIC)
				continue;
			if (GcsBlockDedupEntryHasForwardMarker(entry) && entry->done_at_ts == 0)
				continue;

			/*
			 * GCS-race round-2 RC-F: per-entry pinned deadlines.  A
			 * DONE-proven entry only lingers its pinned quarantine (reorder
			 * slop) from the proof; everything else ages its pinned
			 * registration-time lifetime from the reply/registration
			 * anchor.  Entries with no pinned value (0 — impossible after
			 * this build's registration path, but cheap to guard) fall
			 * back to the sweep-time threshold.
			 */
			if (entry->done_at_ts != 0) {
				anchor = entry->done_at_ts;
				deadline_us = entry->pinned_done_linger_us;
			} else {
				anchor = entry->completed_at_ts != 0 ? entry->completed_at_ts
													 : entry->registered_at_ts;
				deadline_us = entry->pinned_lifetime_us;
			}
			if (anchor == 0)
				continue;
			if (deadline_us <= 0)
				deadline_us = threshold_us;

			age_us = (int64)(now - anchor);
			if (age_us > deadline_us) {
				(void)hash_search(htab, &entry->key, HASH_REMOVE, NULL);
				removed++;
			}
		}

		if (removed > 0) {
			pg_atomic_fetch_sub_u32(&shard->entry_count, (uint32)removed);
			/* spec-7.2a D5: evict_count aggregates eager reclaim + TTL sweep. */
			pg_atomic_fetch_add_u64(&shard->evict_count, (uint64)removed);
		}

		LWLockRelease(&shard->lock.lock);
	}
}

void
cluster_gcs_block_dedup_cleanup_on_backend_exit(uint32 origin_node_id, int32 backend_id)
{
	int s;

	if (cluster_gcs_block_dedup_shards == NULL)
		return;

	for (s = 0; s < cluster_gcs_block_dedup_n_shards; s++) {
		ClusterGcsBlockDedupShard *shard = &cluster_gcs_block_dedup_shards[s];
		HTAB *htab = cluster_gcs_block_dedup_htabs[s];
		HASH_SEQ_STATUS seq;
		GcsBlockDedupEntry *entry;
		int removed = 0;

		if (htab == NULL)
			continue;

		LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
		hash_seq_init(&seq, htab);
		while ((entry = (GcsBlockDedupEntry *)hash_seq_search(&seq)) != NULL) {
			if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_GENERIC
				&& (!GcsBlockDedupEntryHasForwardMarker(entry) || entry->done_at_ts != 0)
				&& entry->key.origin_node_id == origin_node_id
				&& entry->key.requester_backend_id == backend_id) {
				(void)hash_search(htab, &entry->key, HASH_REMOVE, NULL);
				removed++;
			}
		}
		if (removed > 0)
			pg_atomic_fetch_sub_u32(&shard->entry_count, (uint32)removed);
		LWLockRelease(&shard->lock.lock);
	}
}

void
cluster_gcs_block_dedup_cleanup_on_node_dead(uint32 node_id)
{
	int s;

	if (cluster_gcs_block_dedup_shards == NULL)
		return;

	for (s = 0; s < cluster_gcs_block_dedup_n_shards; s++) {
		ClusterGcsBlockDedupShard *shard = &cluster_gcs_block_dedup_shards[s];
		HTAB *htab = cluster_gcs_block_dedup_htabs[s];
		HASH_SEQ_STATUS seq;
		GcsBlockDedupEntry *entry;
		int removed = 0;

		if (htab == NULL)
			continue;

		LWLockAcquire(&shard->lock.lock, LW_EXCLUSIVE);
		hash_seq_init(&seq, htab);
		while ((entry = (GcsBlockDedupEntry *)hash_seq_search(&seq)) != NULL) {
			if (entry->entry_kind == GCS_BLOCK_DEDUP_ENTRY_GENERIC
				&& (!GcsBlockDedupEntryHasForwardMarker(entry) || entry->done_at_ts != 0)
				&& entry->key.origin_node_id == node_id) {
				(void)hash_search(htab, &entry->key, HASH_REMOVE, NULL);
				removed++;
			}
		}
		if (removed > 0)
			pg_atomic_fetch_sub_u32(&shard->entry_count, (uint32)removed);
		LWLockRelease(&shard->lock.lock);
	}
}

/*
 * Copy a low-cardinality census of all live generic forward markers.
 *
 * Shard locks are acquired in ascending worker order and retained until every
 * HTAB has been scanned.  This gives the diagnostic reader one atomic
 * all-shard epoch without exposing entry identities or an iterator.
 */
bool
cluster_gcs_block_dedup_forward_phase_census(
	GcsBlockForwardPhaseCensus *out)
{
	int n_shards;
	int worker;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (cluster_gcs_block_dedup_ctl == NULL
		|| cluster_gcs_block_dedup_shards == NULL)
		return false;

	n_shards = cluster_gcs_block_dedup_ctl->n_shards;
	if (n_shards <= 0 || n_shards > CLUSTER_LMS_MAX_WORKERS
		|| n_shards != cluster_gcs_block_dedup_n_shards)
		return false;
	for (worker = 0; worker < n_shards; worker++) {
		if (cluster_gcs_block_dedup_htabs[worker] == NULL)
			return false;
	}

	for (worker = 0; worker < n_shards; worker++)
		LWLockAcquire(
			&cluster_gcs_block_dedup_shards[worker].lock.lock, LW_SHARED);

	for (worker = 0; worker < n_shards; worker++) {
		HASH_SEQ_STATUS scan;
		GcsBlockDedupEntry *entry;
		HTAB *htab = cluster_gcs_block_dedup_htabs[worker];

		hash_seq_init(&scan, htab);
		while ((entry = (GcsBlockDedupEntry *)hash_seq_search(&scan))
			   != NULL) {
			GcsBlockForwardMarkerPhase phase;
			bool status_valid;

			if (!GcsBlockDedupEntryHasForwardMarker(entry))
				continue;
			out->marker_count++;

			phase = GcsBlockDedupEntryForwardMarkerPhase(entry);
			status_valid
				= entry->status
					  == (uint8)GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER
				  || entry->status
						 == (uint8)GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER
				  || entry->status
						 == (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER;
			if (entry->entry_kind != GCS_BLOCK_DEDUP_ENTRY_GENERIC
				|| (entry->sf_flags
					& ~(GCS_BLOCK_DEDUP_FORWARD_MARKER_FLAG
						| GCS_BLOCK_DEDUP_FORWARD_PHASE_MASK))
					   != 0
				|| !status_valid) {
				out->invalid_count++;
				continue;
			}

			if (phase == GCS_BLOCK_FORWARD_MARK_FORWARDED)
				out->forwarded_count++;
			else if (phase == GCS_BLOCK_FORWARD_MARK_CANCELLING)
				out->cancelling_count++;
			else
				out->invalid_count++;
		}
	}

	for (worker = n_shards - 1; worker >= 0; worker--)
		LWLockRelease(
			&cluster_gcs_block_dedup_shards[worker].lock.lock);
	out->valid = true;
	return true;
}


/* ============================================================
 * Observability accessors — aggregate (sum) over every live shard.
 * ============================================================ */

static uint64
cluster_gcs_block_dedup_sum_u64(size_t member_offset)
{
	uint64 total = 0;
	int s;

	if (cluster_gcs_block_dedup_shards == NULL)
		return 0;

	for (s = 0; s < cluster_gcs_block_dedup_n_shards; s++) {
		pg_atomic_uint64 *ctr
			= (pg_atomic_uint64 *)((char *)&cluster_gcs_block_dedup_shards[s] + member_offset);

		total += pg_atomic_read_u64(ctr);
	}
	return total;
}

uint64
cluster_gcs_block_dedup_get_hit_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(offsetof(ClusterGcsBlockDedupShard, hit_count));
}

uint64
cluster_gcs_block_dedup_get_miss_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(offsetof(ClusterGcsBlockDedupShard, miss_count));
}

uint64
cluster_gcs_block_dedup_get_collision_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(offsetof(ClusterGcsBlockDedupShard, collision_count));
}

uint64
cluster_gcs_block_dedup_get_full_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(offsetof(ClusterGcsBlockDedupShard, full_count));
}

uint64
cluster_gcs_block_dedup_get_in_flight_count(void)
{
	uint64 total = 0;
	int s;

	if (cluster_gcs_block_dedup_shards == NULL)
		return 0;

	for (s = 0; s < cluster_gcs_block_dedup_n_shards; s++)
		total += (uint64)pg_atomic_read_u32(&cluster_gcs_block_dedup_shards[s].entry_count);
	return total;
}

uint64
cluster_gcs_block_dedup_get_evict_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(offsetof(ClusterGcsBlockDedupShard, evict_count));
}

uint64
cluster_gcs_block_dedup_get_done_marked_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(offsetof(ClusterGcsBlockDedupShard, done_marked_count));
}

uint64
cluster_gcs_block_dedup_get_done_mismatch_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(
		offsetof(ClusterGcsBlockDedupShard, done_mismatch_count));
}

/*
 * S3-P0-09 observability.  A nonzero value on either counter means a
 * requester DONE arrived with a fully valid identity while the master still
 * held a live forward leg for that cell, and was refused.  Before this
 * counter pair existed the same event was indistinguishable from an ordinary
 * accepted DONE, so a field run could not report whether the hole had been
 * exercised at all.
 */
uint64
cluster_gcs_block_dedup_get_done_forwarded_refused_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(
		offsetof(ClusterGcsBlockDedupShard, done_forwarded_refused_count));
}

uint64
cluster_gcs_block_dedup_get_done_cancelling_refused_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(
		offsetof(ClusterGcsBlockDedupShard, done_cancelling_refused_count));
}

#ifdef USE_ASSERT_CHECKING
/*
 * Exact, assertion-build-only runtime probe for S3-P0-18.  A key may
 * theoretically be present in more than one per-tag shard, so probe every
 * live shard once with HASH_FIND and report ambiguity instead of choosing a
 * row.  Never expose an iterator over the dedup registry.
 */
bool
cluster_gcs_block_dedup_debug_exact(const GcsBlockDedupKey *key,
									GcsBlockDedupDebugExactSnapshot *snapshot_out)
{
	int s;

	Assert(key != NULL);
	Assert(snapshot_out != NULL);
	memset(snapshot_out, 0, sizeof(*snapshot_out));
	snapshot_out->key = *key;
	snapshot_out->worker_id = -1;

	if (cluster_gcs_block_dedup_shards != NULL) {
		for (s = 0; s < cluster_gcs_block_dedup_n_shards; s++) {
			ClusterGcsBlockDedupShard *shard = &cluster_gcs_block_dedup_shards[s];
			HTAB *htab = cluster_gcs_block_dedup_htabs[s];
			GcsBlockDedupEntry *entry;
			bool found = false;

			if (htab == NULL)
				continue;
			LWLockAcquire(&shard->lock.lock, LW_SHARED);
			entry = (GcsBlockDedupEntry *)hash_search(htab, key, HASH_FIND, &found);
			if (found) {
				snapshot_out->match_count++;
				if (snapshot_out->match_count == 1) {
					snapshot_out->worker_id = s;
					snapshot_out->key = entry->key;
					snapshot_out->tag = entry->tag;
					snapshot_out->entry_kind = entry->entry_kind;
					snapshot_out->transition_id = entry->transition_id;
					snapshot_out->status = entry->status;
					snapshot_out->completed = entry->completed_at_ts != 0;
					snapshot_out->done = entry->done_at_ts != 0;
				}
			}
			LWLockRelease(&shard->lock.lock);
		}
	}

	snapshot_out->hit_count = cluster_gcs_block_dedup_get_hit_count();
	snapshot_out->miss_count = cluster_gcs_block_dedup_get_miss_count();
	snapshot_out->collision_count = cluster_gcs_block_dedup_get_collision_count();
	snapshot_out->done_marked_count = cluster_gcs_block_dedup_get_done_marked_count();
	snapshot_out->done_mismatch_count = cluster_gcs_block_dedup_get_done_mismatch_count();
	return snapshot_out->match_count == 1;
}
#endif

uint64
cluster_gcs_block_dedup_get_hint_violation_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(
		offsetof(ClusterGcsBlockDedupShard, hint_violation_count));
}

uint64
cluster_gcs_block_dedup_get_legacy_pin_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(offsetof(ClusterGcsBlockDedupShard, legacy_pin_count));
}

uint64
cluster_gcs_block_dedup_get_pcm_x_stage_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(offsetof(ClusterGcsBlockDedupShard, pcm_x_stage_count));
}

uint64
cluster_gcs_block_dedup_get_pcm_x_replay_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(offsetof(ClusterGcsBlockDedupShard, pcm_x_replay_count));
}

uint64
cluster_gcs_block_dedup_get_pcm_x_release_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(
		offsetof(ClusterGcsBlockDedupShard, pcm_x_release_count));
}

uint64
cluster_gcs_block_dedup_get_pcm_x_failclosed_count(void)
{
	return cluster_gcs_block_dedup_sum_u64(
		offsetof(ClusterGcsBlockDedupShard, pcm_x_failclosed_count));
}

/*
 * cluster_gcs_block_dedup_get_max_entries -- effective PER-SHARD dedup
 * capacity.
 *
 *	spec-7.2a D5:  reports the per-shard cap stamped at shmem init (the GUC
 *	value raised to the D4 auto-size floor — the size each shard HTAB was
 *	actually built with), or 0 when the HTABs were never allocated
 *	(initdb/bootstrap before a cluster node id is assigned, or vanilla mode).
 *	The occupancy ratio entry_count / max_entries is the saturation signal
 *	behind the DEDUP_FULL fail-closed path.
 */
uint64
cluster_gcs_block_dedup_get_max_entries(void)
{
	return cluster_gcs_block_dedup_ctl ? (uint64)cluster_gcs_block_dedup_ctl->max_entries_effective
									   : 0;
}

uint64
cluster_gcs_block_dedup_get_misroute_failclosed_count(void)
{
	return cluster_gcs_block_dedup_ctl
			   ? pg_atomic_read_u64(&cluster_gcs_block_dedup_ctl->misroute_failclosed_count)
			   : 0;
}

/*
 * spec-7.3 D5 — record a mis-routed dedup access (a block tag reaching the
 * wrong LMS worker).  Called both by the module's own bounds guard and by
 * the master-side handler when shard(tag) != its own DATA channel, so the
 * two fail-closed detectors share one observability face.
 */
void
cluster_gcs_block_dedup_note_misroute(void)
{
	if (cluster_gcs_block_dedup_ctl != NULL)
		pg_atomic_fetch_add_u64(&cluster_gcs_block_dedup_ctl->misroute_failclosed_count, 1);
}


#endif /* USE_PGRAC_CLUSTER */
