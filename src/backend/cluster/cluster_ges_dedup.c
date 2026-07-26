/*-------------------------------------------------------------------------
 *
 * cluster_ges_dedup.c
 *	  pgrac GES retransmit dedup HTAB — spec-2.27 D2 implementation.
 *
 *	  HTAB located in 'pgrac cluster ges dedup' shmem region; key is
 *	  the 6-tuple ClusterGesDedupKey (origin_node_id, opcode, request_id,
 *	  cluster_epoch, shard_master_generation, holder_procno).  Entry value is
 *	  the cached GES_REPLY blob (52B = GesReplyPayload spec-2.23) + processed timestamp
 *	  + status flag distinguishing in-flight vs cached.
 *
 *	  HC51 / HC52 invariants:  IN_FLIGHT_DUPLICATE handling (caller must
 *	  drop/defer not re-process), STALE_REPROCESS sweeping, FULL fail-
 *	  closed with no eviction of in-flight entries.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ges_dedup.c
 *
 * NOTES
 *	  pgrac-original file.  Compiled only in --enable-cluster builds.
 *	  Spec: spec-2.27-ges-reliability-hardening.md (FROZEN v0.2).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_ges_dedup.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"

#define CLUSTER_GES_DEDUP_REPLY_BLOB_LEN 52 /* GesReplyPayload spec-2.23 */

typedef struct ClusterGesDedupEntry {
	ClusterGesDedupKey key;	  /* 40B HASH_BLOBS key */
	TimestampTz processed_ts; /* set at register time */
	uint64 origin_boot_incarnation; /* verified HELLO boot; 0 = legacy */
	/* Length is payload size only: CACHED_REPLY with len=0 is the terminal
	 * no-reply outcome used by successful CANCEL_WAIT.  `status`, never this
	 * length, distinguishes an in-flight row from a terminal cached row. */
	uint16 cached_reply_len;
	uint16 status;			  /* ClusterGesDedupLookupStatus authority */
	bool retire_on_cache;	  /* DONE/HWM arrived while still in-flight */
	uint8 _pad0[3];
	uint8 cached_reply_blob[CLUSTER_GES_DEDUP_REPLY_BLOB_LEN];
} ClusterGesDedupEntry;

typedef struct ClusterGesDedupJournalKey {
	uint32 dest_node_id;
	uint8 kind; /* EXACT_DONE or PROC_EXIT_HWM */
	uint8 _pad0[3];
	uint32 origin_node_id;
	uint32 holder_procno;
	uint32 opcode;
	uint32 _pad1;
	uint64 request_id;
	uint64 cluster_epoch;
	uint64 shard_master_generation;
	uint64 origin_boot_incarnation;
	uint64 target_boot_incarnation;
} ClusterGesDedupJournalKey;

StaticAssertDecl(sizeof(ClusterGesDedupJournalKey) == 64,
				 "GES dedup requester journal key is fixed 64 bytes");

typedef struct ClusterGesDedupJournalEntry {
	ClusterGesDedupJournalKey key;
	TimestampTz next_retry_at;
	uint32 attempts;
	uint32 last_link_generation;
	int32 owner_pid;
	bool committed;
	uint8 _pad0[3];
	uint64 owner_generation;
} ClusterGesDedupJournalEntry;

typedef struct ClusterGesDedupFrontierKey {
	uint32 origin_node_id;
	uint32 holder_procno;
	uint64 origin_boot_incarnation;
} ClusterGesDedupFrontierKey;

typedef struct ClusterGesDedupFrontierEntry {
	ClusterGesDedupFrontierKey key;
	uint64 retired_hwm;
} ClusterGesDedupFrontierEntry;

typedef struct ClusterGesDedupShared {
	pg_atomic_uint64 hit_cached_count;
	pg_atomic_uint64 in_flight_dup_count;
	pg_atomic_uint64 stale_reprocess_count;
	pg_atomic_uint64 full_reject_count;
	pg_atomic_uint32 entry_count; /* approximate; HTAB authoritative */
	pg_atomic_uint32 journal_count;
	pg_atomic_uint64 journal_full_count;
	pg_atomic_uint64 journal_ack_count;
	uint64 origin_current_boot[CLUSTER_MAX_NODES];
} ClusterGesDedupShared;

static ClusterGesDedupShared *cluster_ges_dedup_shared = NULL;
static HTAB *cluster_ges_dedup_htab = NULL;
static HTAB *cluster_ges_dedup_journal_htab = NULL;
static HTAB *cluster_ges_dedup_frontier_htab = NULL;
static LWLock *cluster_ges_dedup_lock = NULL;

static void
ges_dedup_journal_key_from_done(uint32 dest_node_id,
							   const GesDedupLifecyclePayload *done,
							   ClusterGesDedupJournalKey *key)
{
	memset(key, 0, sizeof(*key));
	key->dest_node_id = dest_node_id;
	key->kind = done->kind;
	key->origin_node_id = done->origin_node_id;
	key->holder_procno = done->holder_procno;
	key->opcode = done->opcode;
	key->request_id = done->request_id;
	key->cluster_epoch = done->cluster_epoch;
	key->shard_master_generation = done->shard_master_generation;
	key->origin_boot_incarnation = done->origin_boot_incarnation;
	key->target_boot_incarnation = done->target_boot_incarnation;
}

static void
ges_dedup_journal_done_from_entry(const ClusterGesDedupJournalEntry *entry,
								  GesDedupLifecyclePayload *done)
{
	memset(done, 0, sizeof(*done));
	done->version = GES_DEDUP_LIFECYCLE_VERSION;
	done->kind = entry->key.kind;
	done->origin_node_id = entry->key.origin_node_id;
	done->holder_procno = entry->key.holder_procno;
	done->opcode = entry->key.opcode;
	done->request_id = entry->key.request_id;
	done->cluster_epoch = entry->key.cluster_epoch;
	done->shard_master_generation = entry->key.shard_master_generation;
	done->origin_boot_incarnation = entry->key.origin_boot_incarnation;
	done->target_boot_incarnation = entry->key.target_boot_incarnation;
	done->link_generation = entry->last_link_generation;
}

static bool
ges_dedup_frontier_advance_locked(uint32 origin_node_id, uint32 holder_procno,
								  uint64 origin_boot_incarnation,
								  uint64 request_id_hwm)
{
	ClusterGesDedupFrontierKey key;
	ClusterGesDedupFrontierEntry *entry;
	bool found;

	if (cluster_ges_dedup_frontier_htab == NULL
		|| origin_node_id >= CLUSTER_MAX_NODES || origin_boot_incarnation == 0
		|| request_id_hwm == 0)
		return false;
	memset(&key, 0, sizeof(key));
	key.origin_node_id = origin_node_id;
	key.holder_procno = holder_procno;
	key.origin_boot_incarnation = origin_boot_incarnation;
	entry = (ClusterGesDedupFrontierEntry *)hash_search(
		cluster_ges_dedup_frontier_htab, &key, HASH_ENTER_NULL, &found);
	if (entry == NULL)
		return false;
	if (!found) {
		entry->key = key;
		entry->retired_hwm = request_id_hwm;
	} else if (request_id_hwm > entry->retired_hwm)
		entry->retired_hwm = request_id_hwm;
	return true;
}

static bool
ges_dedup_frontier_covers_locked(const ClusterGesDedupKey *key,
								 uint64 origin_boot_incarnation)
{
	ClusterGesDedupFrontierKey fkey;
	ClusterGesDedupFrontierEntry *entry;
	bool found;

	if (cluster_ges_dedup_frontier_htab == NULL || key == NULL
		|| origin_boot_incarnation == 0)
		return false;
	memset(&fkey, 0, sizeof(fkey));
	fkey.origin_node_id = key->origin_node_id;
	fkey.holder_procno = key->holder_procno;
	fkey.origin_boot_incarnation = origin_boot_incarnation;
	entry = (ClusterGesDedupFrontierEntry *)hash_search(
		cluster_ges_dedup_frontier_htab, &fkey, HASH_FIND, &found);
	return found && entry != NULL
		   && cluster_ges_dedup_hwm_covers_request(
			   key->request_id, entry->retired_hwm);
}

/* Caller holds cluster_ges_dedup_lock EXCLUSIVE.  A verified new boot is a
 * hard transport/lifecycle boundary: old cached rows and completion
 * frontiers are atomically removed before this origin's first new-life
 * request is admitted. */
static uint64
ges_dedup_origin_boot_switch_locked(uint32 origin_node_id,
									uint64 origin_boot_incarnation)
{
	HASH_SEQ_STATUS scan;
	ClusterGesDedupEntry *entry;
	ClusterGesDedupFrontierEntry *frontier;
	uint64 previous;

	if (origin_node_id >= CLUSTER_MAX_NODES || origin_boot_incarnation == 0)
		return 0;
	previous = cluster_ges_dedup_shared->origin_current_boot[origin_node_id];
	if (previous == origin_boot_incarnation)
		return 0;
	if (previous != 0) {
		hash_seq_init(&scan, cluster_ges_dedup_htab);
		while ((entry = (ClusterGesDedupEntry *)hash_seq_search(&scan)) != NULL)
			if (entry->key.origin_node_id == origin_node_id
				&& hash_search(cluster_ges_dedup_htab, &entry->key,
							   HASH_REMOVE, NULL) != NULL)
				pg_atomic_fetch_sub_u32(
					&cluster_ges_dedup_shared->entry_count, 1);

		hash_seq_init(&scan, cluster_ges_dedup_frontier_htab);
		while ((frontier = (ClusterGesDedupFrontierEntry *)
					hash_seq_search(&scan)) != NULL)
			if (frontier->key.origin_node_id == origin_node_id)
				(void)hash_search(
					cluster_ges_dedup_frontier_htab, &frontier->key,
					HASH_REMOVE, NULL);
	}
	cluster_ges_dedup_shared->origin_current_boot[origin_node_id]
		= origin_boot_incarnation;
	return previous;
}

/* ============================================================
 * Shmem lifecycle.
 * ============================================================ */

Size
cluster_ges_dedup_shmem_size(void)
{
	Size sz;
	int cap;

	cap = cluster_ges_dedup_max_entries > 0 ? cluster_ges_dedup_max_entries : 8192;

	sz = MAXALIGN(sizeof(ClusterGesDedupShared));
	sz = add_size(sz, hash_estimate_size((Size)cap, sizeof(ClusterGesDedupEntry)));
	sz = add_size(sz, hash_estimate_size((Size)cap, sizeof(ClusterGesDedupJournalEntry)));
	sz = add_size(sz, hash_estimate_size((Size)cap, sizeof(ClusterGesDedupFrontierEntry)));
	return sz;
}

void
cluster_ges_dedup_shmem_request(void)
{
	/*
	 * cluster_request_shmem() already reserves bytes for every registered
	 * region by calling region.size_fn().  Keep this hook tranche-only so
	 * diagnostic size walks cannot accidentally double-request addin shmem.
	 */
	RequestNamedLWLockTranche("ClusterGesDedup", 1);
}

void
cluster_ges_dedup_shmem_init(void)
{
	bool found;
	HASHCTL info;
	int cap;

	cluster_ges_dedup_shared = (ClusterGesDedupShared *)ShmemInitStruct(
		"pgrac cluster ges dedup", MAXALIGN(sizeof(ClusterGesDedupShared)), &found);

	if (!found) {
		memset(cluster_ges_dedup_shared, 0, sizeof(*cluster_ges_dedup_shared));
		pg_atomic_init_u64(&cluster_ges_dedup_shared->hit_cached_count, 0);
		pg_atomic_init_u64(&cluster_ges_dedup_shared->in_flight_dup_count, 0);
		pg_atomic_init_u64(&cluster_ges_dedup_shared->stale_reprocess_count, 0);
		pg_atomic_init_u64(&cluster_ges_dedup_shared->full_reject_count, 0);
		pg_atomic_init_u32(&cluster_ges_dedup_shared->entry_count, 0);
		pg_atomic_init_u32(&cluster_ges_dedup_shared->journal_count, 0);
		pg_atomic_init_u64(&cluster_ges_dedup_shared->journal_full_count, 0);
		pg_atomic_init_u64(&cluster_ges_dedup_shared->journal_ack_count, 0);
	}

	cap = cluster_ges_dedup_max_entries > 0 ? cluster_ges_dedup_max_entries : 8192;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(ClusterGesDedupKey);
	info.entrysize = sizeof(ClusterGesDedupEntry);

	cluster_ges_dedup_htab
		= ShmemInitHash("pgrac cluster ges dedup htab", cap, cap, &info, HASH_ELEM | HASH_BLOBS);

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(ClusterGesDedupJournalKey);
	info.entrysize = sizeof(ClusterGesDedupJournalEntry);
	cluster_ges_dedup_journal_htab = ShmemInitHash(
		"pgrac cluster ges dedup completion journal", cap, cap, &info,
		HASH_ELEM | HASH_BLOBS);

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(ClusterGesDedupFrontierKey);
	info.entrysize = sizeof(ClusterGesDedupFrontierEntry);
	cluster_ges_dedup_frontier_htab = ShmemInitHash(
		"pgrac cluster ges dedup completion frontier", cap, cap, &info,
		HASH_ELEM | HASH_BLOBS);

	if (!IsBootstrapProcessingMode())
		cluster_ges_dedup_lock = &(GetNamedLWLockTranche("ClusterGesDedup"))[0].lock;
}

static const ClusterShmemRegion cluster_ges_dedup_region = {
	.name = "pgrac cluster ges dedup",
	.size_fn = cluster_ges_dedup_shmem_size,
	.init_fn = cluster_ges_dedup_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_ges_dedup",
	.reserved_flags = 0,
};

void
cluster_ges_dedup_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_ges_dedup_region);
}

/* ============================================================
 * Public API.
 * ============================================================ */

uint32
cluster_ges_dedup_capacity(void)
{
	return (uint32)(cluster_ges_dedup_max_entries > 0 ? cluster_ges_dedup_max_entries : 8192);
}

uint32
cluster_ges_dedup_entry_count(void)
{
	if (cluster_ges_dedup_shared == NULL)
		return 0;
	return pg_atomic_read_u32(&cluster_ges_dedup_shared->entry_count);
}

uint64
cluster_ges_dedup_hit_cached_count(void)
{
	if (cluster_ges_dedup_shared == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_ges_dedup_shared->hit_cached_count);
}

uint64
cluster_ges_dedup_in_flight_dup_count(void)
{
	if (cluster_ges_dedup_shared == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_ges_dedup_shared->in_flight_dup_count);
}

uint64
cluster_ges_dedup_stale_reprocess_count(void)
{
	if (cluster_ges_dedup_shared == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_ges_dedup_shared->stale_reprocess_count);
}

uint64
cluster_ges_dedup_full_reject_count(void)
{
	if (cluster_ges_dedup_shared == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_ges_dedup_shared->full_reject_count);
}

ClusterGesDedupLookupStatus
cluster_ges_dedup_lookup_or_register(const ClusterGesDedupKey *key, uint8 *reply_out,
									 uint16 reply_buf_len, uint16 *reply_len_out)
{
	return cluster_ges_dedup_lookup_or_register_identity(
		key, 0, reply_out, reply_buf_len, reply_len_out);
}

ClusterGesDedupLookupStatus
cluster_ges_dedup_lookup_or_register_identity(
	const ClusterGesDedupKey *key, uint64 origin_boot_incarnation,
	uint8 *reply_out, uint16 reply_buf_len, uint16 *reply_len_out)
{
	return cluster_ges_dedup_lookup_or_register_identity_ex(
		key, origin_boot_incarnation, NULL, reply_out, reply_buf_len,
		reply_len_out);
}

ClusterGesDedupLookupStatus
cluster_ges_dedup_lookup_or_register_identity_ex(
	const ClusterGesDedupKey *key, uint64 origin_boot_incarnation,
	uint64 *superseded_boot_out, uint8 *reply_out, uint16 reply_buf_len,
	uint16 *reply_len_out)
{
	ClusterGesDedupEntry *entry;
	bool found;
	ClusterGesDedupLookupStatus result;

	Assert(key != NULL);
	Assert(reply_len_out != NULL);
	if (cluster_ges_dedup_htab == NULL || cluster_ges_dedup_lock == NULL)
		return CLUSTER_GES_DEDUP_FULL; /* not ready → fail closed */

	*reply_len_out = 0;
	if (superseded_boot_out != NULL)
		*superseded_boot_out = 0;

	/* HC51 invalidation model:
	 *
	 *  Entries are keyed on the caller's shard_master_generation (part of
	 *  the 5-tuple ClusterGesDedupKey).  Stale entries from a prior LMS
	 *  generation are invalidated *exclusively* via
	 *  cluster_ges_dedup_drop_stale_entries() which LMS runs at restart
	 *  after bumping lms_restart_generation.  An inline stale check here
	 *  would create an infinite drop-and-reregister loop because the
	 *  caller would re-insert under the same (stale) caller-gen key.  The
	 *  STALE_REPROCESS status is reserved for the sweep path. */

	LWLockAcquire(cluster_ges_dedup_lock, LW_EXCLUSIVE);

	if (origin_boot_incarnation != 0) {
		uint64 superseded_boot
			= ges_dedup_origin_boot_switch_locked(
				key->origin_node_id, origin_boot_incarnation);

		if (superseded_boot_out != NULL)
			*superseded_boot_out = superseded_boot;
		if (ges_dedup_frontier_covers_locked(
				key, origin_boot_incarnation)) {
			LWLockRelease(cluster_ges_dedup_lock);
			return CLUSTER_GES_DEDUP_RETIRED_LATE;
		}
	}

	entry = (ClusterGesDedupEntry *)hash_search(cluster_ges_dedup_htab, key, HASH_FIND, &found);

	if (found && entry != NULL) {
		/*
		 * Capability can become usable after legacy traffic was already
		 * admitted under boot=0.  Never replay or overwrite that ambiguous
		 * row as if it belonged to the authenticated incarnation.  A known
		 * old->new boot transition is cleared above; unknown legacy overlap
		 * remains pinned/fail-closed until the normal generation sweep.
		 */
		if (entry->origin_boot_incarnation != origin_boot_incarnation) {
			LWLockRelease(cluster_ges_dedup_lock);
			pg_atomic_fetch_add_u64(
				&cluster_ges_dedup_shared->in_flight_dup_count, 1);
			return CLUSTER_GES_DEDUP_IN_FLIGHT_DUPLICATE;
		}
		if (entry->status
			== (uint16)CLUSTER_GES_DEDUP_MISS_REGISTERED) {
			/* HC52 IN_FLIGHT_DUPLICATE — caller MUST drop / defer, never
			 * re-process (double-grant risk). */
			LWLockRelease(cluster_ges_dedup_lock);
			pg_atomic_fetch_add_u64(&cluster_ges_dedup_shared->in_flight_dup_count, 1);
			return CLUSTER_GES_DEDUP_IN_FLIGHT_DUPLICATE;
		} else {
			uint16 to_copy = entry->cached_reply_len;
			if (to_copy > reply_buf_len)
				to_copy = reply_buf_len;
			if (reply_out != NULL && to_copy > 0)
				memcpy(reply_out, entry->cached_reply_blob, to_copy);
			*reply_len_out = to_copy;
			LWLockRelease(cluster_ges_dedup_lock);
			pg_atomic_fetch_add_u64(&cluster_ges_dedup_shared->hit_cached_count, 1);
			return CLUSTER_GES_DEDUP_CACHED_REPLY;
		}
	}

	/* MISS path:  register a fresh in-flight entry. */
	{
		uint32 count = pg_atomic_read_u32(&cluster_ges_dedup_shared->entry_count);
		uint32 cap = cluster_ges_dedup_capacity();

		if (count >= cap) {
			LWLockRelease(cluster_ges_dedup_lock);
			pg_atomic_fetch_add_u64(&cluster_ges_dedup_shared->full_reject_count, 1);
			return CLUSTER_GES_DEDUP_FULL;
		}

		entry = (ClusterGesDedupEntry *)hash_search(cluster_ges_dedup_htab, key, HASH_ENTER_NULL,
													&found);
		if (entry == NULL) {
			LWLockRelease(cluster_ges_dedup_lock);
			pg_atomic_fetch_add_u64(&cluster_ges_dedup_shared->full_reject_count, 1);
			return CLUSTER_GES_DEDUP_FULL;
		}

		entry->key = *key;
		entry->processed_ts = GetCurrentTimestamp();
		entry->origin_boot_incarnation = origin_boot_incarnation;
		entry->cached_reply_len = 0;
		entry->status = (uint16)CLUSTER_GES_DEDUP_MISS_REGISTERED;
		entry->retire_on_cache = false;
		memset(entry->_pad0, 0, sizeof(entry->_pad0));
		memset(entry->cached_reply_blob, 0, sizeof(entry->cached_reply_blob));
		pg_atomic_fetch_add_u32(&cluster_ges_dedup_shared->entry_count, 1);
		result = CLUSTER_GES_DEDUP_MISS_REGISTERED;
	}

	LWLockRelease(cluster_ges_dedup_lock);
	return result;
}

bool
cluster_ges_dedup_request_is_retired(
	const ClusterGesDedupKey *key, uint64 origin_boot_incarnation)
{
	bool retired;

	if (key == NULL || origin_boot_incarnation == 0
		|| cluster_ges_dedup_htab == NULL
		|| cluster_ges_dedup_lock == NULL)
		return false;
	LWLockAcquire(cluster_ges_dedup_lock, LW_SHARED);
	retired = ges_dedup_frontier_covers_locked(
		key, origin_boot_incarnation);
	LWLockRelease(cluster_ges_dedup_lock);
	return retired;
}

void
cluster_ges_dedup_record_reply(const ClusterGesDedupKey *key, const uint8 *reply, uint16 reply_len)
{
	cluster_ges_dedup_record_reply_identity(key, 0, reply, reply_len);
}

void
cluster_ges_dedup_record_reply_identity(const ClusterGesDedupKey *key,
										uint64 origin_boot_incarnation,
										const uint8 *reply, uint16 reply_len)
{
	ClusterGesDedupEntry *entry;
	bool found;

	Assert(key != NULL);
	if (cluster_ges_dedup_htab == NULL || cluster_ges_dedup_lock == NULL)
		return;
	if (reply_len > CLUSTER_GES_DEDUP_REPLY_BLOB_LEN)
		reply_len = CLUSTER_GES_DEDUP_REPLY_BLOB_LEN;

	LWLockAcquire(cluster_ges_dedup_lock, LW_EXCLUSIVE);

	entry = (ClusterGesDedupEntry *)hash_search(cluster_ges_dedup_htab, key, HASH_FIND, &found);
	if (found && entry != NULL) {
		/*
		 * A request admitted under an old peer boot can finish after the
		 * source reconnects and reuses the same epoch/procno/id.  Never let
		 * that stale work item populate the newly registered incarnation's
		 * cache row.
		 */
		if (entry->origin_boot_incarnation != origin_boot_incarnation) {
			LWLockRelease(cluster_ges_dedup_lock);
			return;
		}
		if (entry->retire_on_cache) {
			(void)hash_search(cluster_ges_dedup_htab, key, HASH_REMOVE, NULL);
			pg_atomic_fetch_sub_u32(&cluster_ges_dedup_shared->entry_count, 1);
			LWLockRelease(cluster_ges_dedup_lock);
			return;
		}
		entry->processed_ts = GetCurrentTimestamp();
		if (reply != NULL && reply_len > 0)
			memcpy(entry->cached_reply_blob, reply, reply_len);
		entry->cached_reply_len = reply_len;
		entry->status = (uint16)CLUSTER_GES_DEDUP_CACHED_REPLY;
	}

	LWLockRelease(cluster_ges_dedup_lock);
}

ClusterGesDedupRetireResult
cluster_ges_dedup_retire_exact(const ClusterGesDedupKey *key,
							   uint64 origin_boot_incarnation)
{
	ClusterGesDedupEntry *entry;
	bool found;

	if (key == NULL || origin_boot_incarnation == 0
		|| cluster_ges_dedup_htab == NULL || cluster_ges_dedup_lock == NULL)
		return CLUSTER_GES_DEDUP_RETIRE_IDENTITY_MISMATCH;

	LWLockAcquire(cluster_ges_dedup_lock, LW_EXCLUSIVE);
	ges_dedup_origin_boot_switch_locked(
		key->origin_node_id, origin_boot_incarnation);
	if (!ges_dedup_frontier_advance_locked(
			key->origin_node_id, key->holder_procno,
			origin_boot_incarnation, key->request_id)) {
		LWLockRelease(cluster_ges_dedup_lock);
		return CLUSTER_GES_DEDUP_RETIRE_IDENTITY_MISMATCH;
	}
	entry = (ClusterGesDedupEntry *)hash_search(
		cluster_ges_dedup_htab, key, HASH_FIND, &found);
	if (!found || entry == NULL) {
		LWLockRelease(cluster_ges_dedup_lock);
		return CLUSTER_GES_DEDUP_RETIRE_ALREADY_ABSENT;
	}
	/*
	 * boot=0 is a legacy/transient-unproven row.  An authenticated exact
	 * DONE carries the full key and may safely retire that one row; a
	 * nonzero different boot remains an identity mismatch.
	 */
	if (entry->origin_boot_incarnation != 0
		&& entry->origin_boot_incarnation != origin_boot_incarnation) {
		LWLockRelease(cluster_ges_dedup_lock);
		return CLUSTER_GES_DEDUP_RETIRE_IDENTITY_MISMATCH;
	}
	if (entry->status
		== (uint16)CLUSTER_GES_DEDUP_MISS_REGISTERED) {
		entry->retire_on_cache = true;
		LWLockRelease(cluster_ges_dedup_lock);
		return CLUSTER_GES_DEDUP_RETIRE_PENDING;
	}
	(void)hash_search(cluster_ges_dedup_htab, key, HASH_REMOVE, NULL);
	pg_atomic_fetch_sub_u32(&cluster_ges_dedup_shared->entry_count, 1);
	LWLockRelease(cluster_ges_dedup_lock);
	return CLUSTER_GES_DEDUP_RETIRE_REMOVED;
}

uint32
cluster_ges_dedup_retire_origin_proc_up_to(
	uint32 origin_node_id, uint32 holder_procno, uint64 request_id_hwm,
	uint64 origin_boot_incarnation, uint32 *pending_out, bool *applied_out)
{
	HASH_SEQ_STATUS scan;
	ClusterGesDedupEntry *entry;
	ClusterGesDedupKey remove_keys[64];
	uint32 nremove = 0;
	uint32 total_removed = 0;
	uint32 pending = 0;

	if (pending_out != NULL)
		*pending_out = 0;
	if (applied_out != NULL)
		*applied_out = false;
	if (request_id_hwm == 0 || origin_boot_incarnation == 0
		|| cluster_ges_dedup_htab == NULL || cluster_ges_dedup_lock == NULL)
		return 0;

	LWLockAcquire(cluster_ges_dedup_lock, LW_EXCLUSIVE);
	ges_dedup_origin_boot_switch_locked(
		origin_node_id, origin_boot_incarnation);
	if (!ges_dedup_frontier_advance_locked(
			origin_node_id, holder_procno, origin_boot_incarnation,
			request_id_hwm)) {
		LWLockRelease(cluster_ges_dedup_lock);
		return 0;
	}
	hash_seq_init(&scan, cluster_ges_dedup_htab);
	while ((entry = (ClusterGesDedupEntry *)hash_seq_search(&scan)) != NULL) {
		if (entry->key.origin_node_id != origin_node_id
			|| entry->key.holder_procno != holder_procno
			|| !cluster_ges_dedup_hwm_covers_request(
				entry->key.request_id, request_id_hwm))
			continue;
		/*
		 * HWM is accepted only from the currently authenticated origin boot.
		 * Rows stamped 0 are legacy rows from that procno; nonzero rows from
		 * an older boot are also safe to retire because the current boot
		 * fence proves that producer can no longer retransmit.
		 */
		if (entry->status
			== (uint16)CLUSTER_GES_DEDUP_MISS_REGISTERED) {
			entry->retire_on_cache = true;
			pending++;
			continue;
		}
		remove_keys[nremove++] = entry->key;
		if (nremove == lengthof(remove_keys)) {
			uint32 i;

			for (i = 0; i < nremove; i++)
				if (hash_search(cluster_ges_dedup_htab, &remove_keys[i],
								HASH_REMOVE, NULL) != NULL) {
					pg_atomic_fetch_sub_u32(
						&cluster_ges_dedup_shared->entry_count, 1);
					total_removed++;
				}
			nremove = 0;
		}
	}
	while (nremove > 0) {
		nremove--;
		if (hash_search(cluster_ges_dedup_htab, &remove_keys[nremove],
						HASH_REMOVE, NULL) != NULL) {
			pg_atomic_fetch_sub_u32(&cluster_ges_dedup_shared->entry_count, 1);
			total_removed++;
		}
	}
	LWLockRelease(cluster_ges_dedup_lock);
	if (pending_out != NULL)
		*pending_out = pending;
	if (applied_out != NULL)
		*applied_out = true;
	return total_removed;
}

static uint32
ges_dedup_drop_origin_if(uint32 origin_node_id, uint64 current_boot,
						 bool mismatch_only)
{
	HASH_SEQ_STATUS scan;
	ClusterGesDedupEntry *entry;
	ClusterGesDedupFrontierEntry *frontier;
	ClusterGesDedupKey keys[64];
	uint32 nkeys = 0;
	uint32 removed = 0;

	if (cluster_ges_dedup_htab == NULL || cluster_ges_dedup_lock == NULL)
		return 0;
	LWLockAcquire(cluster_ges_dedup_lock, LW_EXCLUSIVE);
	hash_seq_init(&scan, cluster_ges_dedup_htab);
	while ((entry = (ClusterGesDedupEntry *)hash_seq_search(&scan)) != NULL) {
		uint32 i;

		if (entry->key.origin_node_id != origin_node_id
			|| (mismatch_only
				&& entry->origin_boot_incarnation == current_boot))
			continue;
		keys[nkeys++] = entry->key;
		if (nkeys != lengthof(keys))
			continue;
		for (i = 0; i < nkeys; i++)
			if (hash_search(cluster_ges_dedup_htab, &keys[i],
							HASH_REMOVE, NULL) != NULL) {
				pg_atomic_fetch_sub_u32(
					&cluster_ges_dedup_shared->entry_count, 1);
				removed++;
			}
		nkeys = 0;
	}
	while (nkeys > 0) {
		nkeys--;
		if (hash_search(cluster_ges_dedup_htab, &keys[nkeys],
						HASH_REMOVE, NULL) != NULL) {
			pg_atomic_fetch_sub_u32(&cluster_ges_dedup_shared->entry_count, 1);
			removed++;
		}
	}
	hash_seq_init(&scan, cluster_ges_dedup_frontier_htab);
	while ((frontier = (ClusterGesDedupFrontierEntry *)
				hash_seq_search(&scan)) != NULL) {
		if (frontier->key.origin_node_id != origin_node_id
			|| (mismatch_only
				&& frontier->key.origin_boot_incarnation == current_boot))
			continue;
		(void)hash_search(
			cluster_ges_dedup_frontier_htab, &frontier->key,
			HASH_REMOVE, NULL);
	}
	if (!mismatch_only && origin_node_id < CLUSTER_MAX_NODES)
		cluster_ges_dedup_shared->origin_current_boot[origin_node_id] = 0;
	else if (mismatch_only && origin_node_id < CLUSTER_MAX_NODES)
		cluster_ges_dedup_shared->origin_current_boot[origin_node_id]
			= current_boot;
	LWLockRelease(cluster_ges_dedup_lock);
	return removed;
}

uint32
cluster_ges_dedup_drop_origin_node(uint32 origin_node_id)
{
	return ges_dedup_drop_origin_if(origin_node_id, 0, false);
}

uint32
cluster_ges_dedup_drop_origin_boot_mismatch(uint32 origin_node_id,
											uint64 current_origin_boot)
{
	if (current_origin_boot == 0)
		return 0;
	return ges_dedup_drop_origin_if(
		origin_node_id, current_origin_boot, true);
}

uint32
cluster_ges_dedup_drop_stale_entries(void)
{
	HASH_SEQ_STATUS scan;
	ClusterGesDedupEntry *entry;
	uint32 swept = 0;
	uint64 current_master_gen;

	if (cluster_ges_dedup_htab == NULL || cluster_ges_dedup_lock == NULL)
		return 0;

	current_master_gen = cluster_lms_get_shard_master_generation();

	LWLockAcquire(cluster_ges_dedup_lock, LW_EXCLUSIVE);

	hash_seq_init(&scan, cluster_ges_dedup_htab);
	while ((entry = (ClusterGesDedupEntry *)hash_seq_search(&scan)) != NULL) {
		if (entry->key.shard_master_generation != current_master_gen) {
			(void)hash_search(cluster_ges_dedup_htab, &entry->key, HASH_REMOVE, NULL);
			pg_atomic_fetch_sub_u32(&cluster_ges_dedup_shared->entry_count, 1);
			pg_atomic_fetch_add_u64(&cluster_ges_dedup_shared->stale_reprocess_count, 1);
			swept++;
		}
	}

	LWLockRelease(cluster_ges_dedup_lock);
	return swept;
}

bool
cluster_ges_dedup_journal_register(uint32 dest_node_id,
								   const GesDedupLifecyclePayload *done)
{
	ClusterGesDedupJournalKey key;
	ClusterGesDedupJournalEntry *entry;
	bool found;
	int32 owner_pid;
	uint64 owner_generation;

	if (done == NULL
		|| (done->kind != GES_DEDUP_LIFECYCLE_EXACT_DONE
			&& done->kind != GES_DEDUP_LIFECYCLE_PROC_EXIT_HWM)
		|| done->version != GES_DEDUP_LIFECYCLE_VERSION || done->status != 0
		|| done->flags != 0 || done->reserved != 0 || done->request_id == 0
		|| done->origin_boot_incarnation == 0
		|| done->target_boot_incarnation == 0
		|| cluster_ges_dedup_journal_htab == NULL
		|| cluster_ges_dedup_lock == NULL)
		return false;
	owner_pid = MyProc != NULL ? MyProc->pid : 0;
	owner_generation
		= MyProc != NULL ? MyProc->cluster_grd_generation : 0;
	if (owner_pid <= 0 || owner_generation == 0)
		return false;
	ges_dedup_journal_key_from_done(dest_node_id, done, &key);

	LWLockAcquire(cluster_ges_dedup_lock, LW_EXCLUSIVE);
	entry = (ClusterGesDedupJournalEntry *)hash_search(
		cluster_ges_dedup_journal_htab, &key, HASH_FIND, &found);
	if (found && entry != NULL) {
		if (entry->owner_pid != owner_pid
			|| entry->owner_generation != owner_generation) {
			LWLockRelease(cluster_ges_dedup_lock);
			return false;
		}
		entry->last_link_generation = done->link_generation;
		LWLockRelease(cluster_ges_dedup_lock);
		return true;
	}
	if (pg_atomic_read_u32(&cluster_ges_dedup_shared->journal_count)
		>= cluster_ges_dedup_capacity()) {
		LWLockRelease(cluster_ges_dedup_lock);
		pg_atomic_fetch_add_u64(
			&cluster_ges_dedup_shared->journal_full_count, 1);
		return false;
	}
	entry = (ClusterGesDedupJournalEntry *)hash_search(
		cluster_ges_dedup_journal_htab, &key, HASH_ENTER_NULL, &found);
	if (entry == NULL) {
		LWLockRelease(cluster_ges_dedup_lock);
		pg_atomic_fetch_add_u64(
			&cluster_ges_dedup_shared->journal_full_count, 1);
		return false;
	}
	entry->key = key;
	entry->next_retry_at = 0;
	entry->attempts = 0;
	entry->last_link_generation = done->link_generation;
	entry->owner_pid = owner_pid;
	entry->committed = false;
	memset(entry->_pad0, 0, sizeof(entry->_pad0));
	entry->owner_generation = owner_generation;
	pg_atomic_fetch_add_u32(&cluster_ges_dedup_shared->journal_count, 1);
	LWLockRelease(cluster_ges_dedup_lock);
	return true;
}

bool
cluster_ges_dedup_journal_commit(uint32 dest_node_id,
								 const GesDedupLifecyclePayload *done)
{
	ClusterGesDedupJournalKey key;
	ClusterGesDedupJournalEntry *entry;
	bool found;

	if (done == NULL || cluster_ges_dedup_journal_htab == NULL
		|| cluster_ges_dedup_lock == NULL)
		return false;
	ges_dedup_journal_key_from_done(dest_node_id, done, &key);
	LWLockAcquire(cluster_ges_dedup_lock, LW_EXCLUSIVE);
	entry = (ClusterGesDedupJournalEntry *)hash_search(
		cluster_ges_dedup_journal_htab, &key, HASH_FIND, &found);
	if (!found || entry == NULL) {
		LWLockRelease(cluster_ges_dedup_lock);
		return false;
	}
	entry->committed = true;
	entry->next_retry_at = 0;
	entry->last_link_generation = done->link_generation;
	LWLockRelease(cluster_ges_dedup_lock);
	return true;
}

bool
cluster_ges_dedup_journal_cancel(uint32 dest_node_id,
								 const GesDedupLifecyclePayload *done)
{
	ClusterGesDedupJournalKey key;
	ClusterGesDedupJournalEntry *entry;
	bool found;
	bool removed = false;

	if (done == NULL || cluster_ges_dedup_journal_htab == NULL
		|| cluster_ges_dedup_lock == NULL)
		return false;
	ges_dedup_journal_key_from_done(dest_node_id, done, &key);
	LWLockAcquire(cluster_ges_dedup_lock, LW_EXCLUSIVE);
	entry = (ClusterGesDedupJournalEntry *)hash_search(
		cluster_ges_dedup_journal_htab, &key, HASH_FIND, &found);
	if (found && entry != NULL && !entry->committed
		&& hash_search(cluster_ges_dedup_journal_htab, &key,
					   HASH_REMOVE, NULL) != NULL) {
		pg_atomic_fetch_sub_u32(&cluster_ges_dedup_shared->journal_count, 1);
		removed = true;
	}
	LWLockRelease(cluster_ges_dedup_lock);
	return removed;
}

static bool
ges_dedup_journal_owner_is_gone(
	const ClusterGesDedupJournalEntry *entry)
{
	PGPROC *proc;

	if (entry == NULL || entry->owner_pid <= 0
		|| entry->owner_generation == 0 || ProcGlobal == NULL
		|| entry->key.holder_procno >= ProcGlobal->allProcCount)
		return false;
	proc = GetPGProcByNumber(entry->key.holder_procno);
	return proc->pid != entry->owner_pid
		   || proc->cluster_grd_generation != entry->owner_generation;
}

/*
 * LMON crash backstop.  Find one dead backend/peer group containing an
 * uncommitted reservation, compact every exact row owned by that same old
 * backend incarnation into one committed PROC_EXIT_HWM, and let the normal
 * retry/ACK path deliver it.  The owner generation prevents a reused procno
 * (and even a recycled OS pid) from being folded into the old HWM.
 */
uint32
cluster_ges_dedup_journal_reap_dead_backend(void)
{
	HASH_SEQ_STATUS scan;
	ClusterGesDedupJournalEntry *entry;
	ClusterGesDedupJournalEntry group;
	ClusterGesDedupJournalKey hwm_key;
	ClusterGesDedupJournalEntry *hwm_entry;
	uint64 request_id_hwm = 0;
	uint32 link_generation = 0;
	uint32 removed = 0;
	bool found_group = false;
	bool found;

	if (cluster_ges_dedup_journal_htab == NULL
		|| cluster_ges_dedup_lock == NULL || ProcGlobal == NULL)
		return 0;

	LWLockAcquire(cluster_ges_dedup_lock, LW_EXCLUSIVE);
	hash_seq_init(&scan, cluster_ges_dedup_journal_htab);
	while ((entry = (ClusterGesDedupJournalEntry *)
				hash_seq_search(&scan)) != NULL) {
		if (entry->key.kind == GES_DEDUP_LIFECYCLE_EXACT_DONE
			&& !entry->committed
			&& ges_dedup_journal_owner_is_gone(entry)) {
			group = *entry;
			found_group = true;
			hash_seq_term(&scan);
			break;
		}
	}
	if (!found_group) {
		LWLockRelease(cluster_ges_dedup_lock);
		return 0;
	}

	/* First compute the inclusive HWM without mutating the scan. */
	hash_seq_init(&scan, cluster_ges_dedup_journal_htab);
	while ((entry = (ClusterGesDedupJournalEntry *)
				hash_seq_search(&scan)) != NULL) {
		if (entry->key.kind != GES_DEDUP_LIFECYCLE_EXACT_DONE
			|| entry->key.dest_node_id != group.key.dest_node_id
			|| entry->key.origin_node_id != group.key.origin_node_id
			|| entry->key.holder_procno != group.key.holder_procno
			|| entry->key.origin_boot_incarnation
				   != group.key.origin_boot_incarnation
			|| entry->key.target_boot_incarnation
				   != group.key.target_boot_incarnation
			|| entry->owner_pid != group.owner_pid
			|| entry->owner_generation != group.owner_generation)
			continue;
		if (entry->key.request_id > request_id_hwm)
			request_id_hwm = entry->key.request_id;
		if (entry->last_link_generation > link_generation)
			link_generation = entry->last_link_generation;
	}
	Assert(request_id_hwm != 0);

	/* Removing the exact group frees at least one slot before HWM insert. */
	hash_seq_init(&scan, cluster_ges_dedup_journal_htab);
	while ((entry = (ClusterGesDedupJournalEntry *)
				hash_seq_search(&scan)) != NULL) {
		ClusterGesDedupJournalKey remove_key;

		if (entry->key.kind != GES_DEDUP_LIFECYCLE_EXACT_DONE
			|| entry->key.dest_node_id != group.key.dest_node_id
			|| entry->key.origin_node_id != group.key.origin_node_id
			|| entry->key.holder_procno != group.key.holder_procno
			|| entry->key.origin_boot_incarnation
				   != group.key.origin_boot_incarnation
			|| entry->key.target_boot_incarnation
				   != group.key.target_boot_incarnation
			|| entry->owner_pid != group.owner_pid
			|| entry->owner_generation != group.owner_generation)
			continue;
		remove_key = entry->key;
		if (hash_search(cluster_ges_dedup_journal_htab, &remove_key,
						HASH_REMOVE, NULL) != NULL) {
			pg_atomic_fetch_sub_u32(
				&cluster_ges_dedup_shared->journal_count, 1);
			removed++;
		}
	}

	memset(&hwm_key, 0, sizeof(hwm_key));
	hwm_key.dest_node_id = group.key.dest_node_id;
	hwm_key.kind = GES_DEDUP_LIFECYCLE_PROC_EXIT_HWM;
	hwm_key.origin_node_id = group.key.origin_node_id;
	hwm_key.holder_procno = group.key.holder_procno;
	hwm_key.request_id = request_id_hwm;
	hwm_key.origin_boot_incarnation
		= group.key.origin_boot_incarnation;
	hwm_key.target_boot_incarnation
		= group.key.target_boot_incarnation;
	hwm_entry = (ClusterGesDedupJournalEntry *)hash_search(
		cluster_ges_dedup_journal_htab, &hwm_key, HASH_ENTER_NULL, &found);
	if (hwm_entry == NULL)
		ereport(PANIC,
				(errmsg_internal("GES dedup dead-backend HWM journal insert "
								 "failed after exact compaction")));
	if (!found) {
		memset(hwm_entry, 0, sizeof(*hwm_entry));
		hwm_entry->key = hwm_key;
		pg_atomic_fetch_add_u32(
			&cluster_ges_dedup_shared->journal_count, 1);
	}
	hwm_entry->next_retry_at = 0;
	hwm_entry->attempts = 0;
	hwm_entry->last_link_generation = link_generation;
	hwm_entry->owner_pid = group.owner_pid;
	hwm_entry->owner_generation = group.owner_generation;
	hwm_entry->committed = true;

	LWLockRelease(cluster_ges_dedup_lock);
	return removed;
}

bool
cluster_ges_dedup_journal_claim_due(TimestampTz now, uint32 *dest_node_id,
									GesDedupLifecyclePayload *done)
{
	HASH_SEQ_STATUS scan;
	ClusterGesDedupJournalEntry *entry;
	bool claimed = false;

	if (dest_node_id != NULL)
		*dest_node_id = 0;
	if (done != NULL)
		memset(done, 0, sizeof(*done));
	if (dest_node_id == NULL || done == NULL
		|| cluster_ges_dedup_journal_htab == NULL
		|| cluster_ges_dedup_lock == NULL)
		return false;

	LWLockAcquire(cluster_ges_dedup_lock, LW_EXCLUSIVE);
	hash_seq_init(&scan, cluster_ges_dedup_journal_htab);
	while ((entry = (ClusterGesDedupJournalEntry *)hash_seq_search(&scan))
		   != NULL) {
		int retry_ms;

		if (!entry->committed
			|| (entry->next_retry_at != 0 && now < entry->next_retry_at))
			continue;
		*dest_node_id = entry->key.dest_node_id;
		ges_dedup_journal_done_from_entry(entry, done);
		retry_ms = 100 << Min(entry->attempts, 4U);
		entry->attempts++;
		entry->next_retry_at = TimestampTzPlusMilliseconds(now, retry_ms);
		claimed = true;
		hash_seq_term(&scan);
		break;
	}
	LWLockRelease(cluster_ges_dedup_lock);
	return claimed;
}

bool
cluster_ges_dedup_journal_ack(uint32 authenticated_source,
							  const GesDedupLifecyclePayload *ack)
{
	ClusterGesDedupJournalKey key;
	GesDedupLifecyclePayload done;
	bool found;
	bool removed = false;

	if (ack == NULL || ack->version != GES_DEDUP_LIFECYCLE_VERSION
		|| ack->kind != GES_DEDUP_LIFECYCLE_ACK
		|| (ack->status != GES_DEDUP_ACK_REMOVED
			&& ack->status != GES_DEDUP_ACK_ALREADY_ABSENT
			&& ack->status != GES_DEDUP_ACK_HWM_APPLIED)
		|| ack->flags != 0 || ack->reserved != 0
		|| cluster_ges_dedup_journal_htab == NULL
		|| cluster_ges_dedup_lock == NULL)
		return false;
	if ((ack->opcode == 0
		 && ack->status != GES_DEDUP_ACK_HWM_APPLIED)
		|| (ack->opcode != 0
			&& ack->status != GES_DEDUP_ACK_REMOVED
			&& ack->status != GES_DEDUP_ACK_ALREADY_ABSENT))
		return false;
	done = *ack;
	done.kind = ack->opcode == 0 ? GES_DEDUP_LIFECYCLE_PROC_EXIT_HWM
								: GES_DEDUP_LIFECYCLE_EXACT_DONE;
	done.status = 0;
	ges_dedup_journal_key_from_done(authenticated_source, &done, &key);

	LWLockAcquire(cluster_ges_dedup_lock, LW_EXCLUSIVE);
	if (hash_search(cluster_ges_dedup_journal_htab, &key, HASH_REMOVE,
					&found) != NULL
		&& found) {
		pg_atomic_fetch_sub_u32(&cluster_ges_dedup_shared->journal_count, 1);
		pg_atomic_fetch_add_u64(
			&cluster_ges_dedup_shared->journal_ack_count, 1);
		removed = true;
	}
	LWLockRelease(cluster_ges_dedup_lock);
	return removed;
}

uint32
cluster_ges_dedup_journal_drop_target_boot_mismatch(
	uint32 dest_node_id, uint64 current_target_boot)
{
	HASH_SEQ_STATUS scan;
	ClusterGesDedupJournalEntry *entry;
	uint32 removed = 0;

	if (current_target_boot == 0 || cluster_ges_dedup_journal_htab == NULL
		|| cluster_ges_dedup_lock == NULL)
		return 0;
	LWLockAcquire(cluster_ges_dedup_lock, LW_EXCLUSIVE);
	hash_seq_init(&scan, cluster_ges_dedup_journal_htab);
	while ((entry = (ClusterGesDedupJournalEntry *)hash_seq_search(&scan))
		   != NULL) {
		if (entry->key.dest_node_id == dest_node_id
			&& entry->key.target_boot_incarnation != current_target_boot
			&& hash_search(cluster_ges_dedup_journal_htab, &entry->key,
						   HASH_REMOVE, NULL) != NULL) {
			pg_atomic_fetch_sub_u32(
				&cluster_ges_dedup_shared->journal_count, 1);
			removed++;
		}
	}
	LWLockRelease(cluster_ges_dedup_lock);
	return removed;
}

uint32
cluster_ges_dedup_journal_count(void)
{
	return cluster_ges_dedup_shared == NULL
			   ? 0
			   : pg_atomic_read_u32(&cluster_ges_dedup_shared->journal_count);
}

uint64
cluster_ges_dedup_journal_full_count(void)
{
	return cluster_ges_dedup_shared == NULL
			   ? 0
			   : pg_atomic_read_u64(
					 &cluster_ges_dedup_shared->journal_full_count);
}

uint64
cluster_ges_dedup_journal_ack_count(void)
{
	return cluster_ges_dedup_shared == NULL
			   ? 0
			   : pg_atomic_read_u64(
					 &cluster_ges_dedup_shared->journal_ack_count);
}
