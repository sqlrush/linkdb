/*-------------------------------------------------------------------------
 *
 * cluster_multixact.c
 *	  pgrac MULTIXACT reader/member-resolution foundation — implementation.
 *
 *	  spec-3.6 D2 (NEW;Stage 3 第 10 sub-spec).
 *
 *	  See cluster_multixact.h for the public contract.  This file
 *	  implements:
 *	    - cluster_multixact_member_overlay HTAB (shmem-resident,
 *	      bounded by cluster.multixact_member_overlay_max_entries)
 *	    - install / lookup / purge_epoch
 *	    - cluster_multixact_resolve_visibility (truth table per
 *	      OBS-1 MVCC-accurate semantics)
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.6-multixact-reader-member-resolution.md (v0.3 FROZEN 2026-05-27)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_multixact.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"

#include "cluster/cluster_cr.h"					/* vis53r97 multi_member_serve counters (D3-b) */
#include "cluster/cluster_cr_server.h"			/* undo_multi_verdict_fetch_and_wait (D3-b) */
#include "cluster/cluster_runtime_visibility.h" /* live-authority covers gate (D3-b review) */
#include "cluster/cluster_elog.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_multixact.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_subtrans.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_visibility_resolve.h" /* cluster_vis_cr_xmax_verdict (polarity SSOT) */

#ifdef USE_PGRAC_CLUSTER

/*
 * Overlay HTAB entry.  Key is ClusterMultiXactKey (16B blob, HASH_BLOBS);
 * value carries member_count + generation_ts + fixed-size members[] array
 * (CLUSTER_MULTIXACT_MAX_MEMBERS = 256 per HC208/HC209 default cap).
 */
#define CLUSTER_MULTIXACT_MAX_MEMBERS 256

typedef struct ClusterMultiXactOverlayEntry {
	ClusterMultiXactKey key;
	uint16 member_count;
	uint16 _pad16;
	TimestampTz generation_ts;
	ClusterMultiXactMember members[CLUSTER_MULTIXACT_MAX_MEMBERS];
} ClusterMultiXactOverlayEntry;

typedef struct ClusterMultiXactShmem {
	pg_atomic_uint64 overlay_install_count;
	pg_atomic_uint64 overlay_lookup_hit_count;
	pg_atomic_uint64 overlay_miss_count;
	pg_atomic_uint64 overlay_overflow_count;
	pg_atomic_uint64 resolve_visibility_count;
	/* spec-7.1 D3-a guardrail counters */
	pg_atomic_uint64 mxid_halfspace_refuse_count; /* allocator half-space refusals */
	pg_atomic_uint64 mxid_underivable_read_count; /* reader: foreign multi origin underivable */
} ClusterMultiXactShmem;

static HTAB *ClusterMultiXactHTAB = NULL;
static LWLock *ClusterMultiXactLock = NULL;
static ClusterMultiXactShmem *ClusterMultiXactState = NULL;

/* ------------------------------------------------------------ */
/* shmem layout                                                 */
/* ------------------------------------------------------------ */

Size
cluster_multixact_shmem_size(void)
{
	Size sz;

	if (IsBootstrapProcessingMode() || !cluster_enabled || cluster_node_id < 0)
		return 0;

	sz = MAXALIGN(sizeof(ClusterMultiXactShmem));
	sz = add_size(sz, MAXALIGN(sizeof(LWLockPadded)));
	sz = add_size(sz, hash_estimate_size(cluster_multixact_member_overlay_max_entries,
										 sizeof(ClusterMultiXactOverlayEntry)));
	return sz;
}

void
cluster_multixact_shmem_init(void)
{
	HASHCTL info;
	bool found;
	LWLockPadded *lockblock;

	if (IsBootstrapProcessingMode() || !cluster_enabled || cluster_node_id < 0)
		return;

	ClusterMultiXactState = (ClusterMultiXactShmem *)ShmemInitStruct(
		"ClusterMultiXactState", MAXALIGN(sizeof(ClusterMultiXactShmem)), &found);
	if (!found) {
		pg_atomic_init_u64(&ClusterMultiXactState->overlay_install_count, 0);
		pg_atomic_init_u64(&ClusterMultiXactState->overlay_lookup_hit_count, 0);
		pg_atomic_init_u64(&ClusterMultiXactState->overlay_miss_count, 0);
		pg_atomic_init_u64(&ClusterMultiXactState->overlay_overflow_count, 0);
		pg_atomic_init_u64(&ClusterMultiXactState->resolve_visibility_count, 0);
		pg_atomic_init_u64(&ClusterMultiXactState->mxid_halfspace_refuse_count, 0);
		pg_atomic_init_u64(&ClusterMultiXactState->mxid_underivable_read_count, 0);
	}

	lockblock = (LWLockPadded *)ShmemInitStruct("ClusterMultiXactLock",
												MAXALIGN(sizeof(LWLockPadded)), &found);
	if (!found)
		LWLockInitialize(&lockblock->lock, LWTRANCHE_CLUSTER_TT_STATUS);
	ClusterMultiXactLock = &lockblock->lock;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(ClusterMultiXactKey);
	info.entrysize = sizeof(ClusterMultiXactOverlayEntry);
	info.num_partitions = 1;
	ClusterMultiXactHTAB = ShmemInitHash(
		"ClusterMultiXactOverlay", cluster_multixact_member_overlay_max_entries,
		cluster_multixact_member_overlay_max_entries, &info, HASH_ELEM | HASH_BLOBS);
}

static const ClusterShmemRegion cluster_multixact_region = {
	.name = "pgrac cluster multixact overlay",
	.size_fn = cluster_multixact_shmem_size,
	.init_fn = cluster_multixact_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_multixact",
	.reserved_flags = 0,
};

void
cluster_multixact_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_multixact_region);
}

/* ------------------------------------------------------------ */
/* Public API                                                   */
/* ------------------------------------------------------------ */

static bool
cluster_multixact_member_overlay_install_raw(const ClusterMultiXactKey *key, uint16 member_count,
											 const ClusterMultiXactMember *members)
{
	ClusterMultiXactOverlayEntry *e;
	bool found;

	if (key == NULL || members == NULL || ClusterMultiXactHTAB == NULL)
		return false;

	if (member_count == 0 || member_count > cluster_multixact_member_overlay_max_members
		|| member_count > CLUSTER_MULTIXACT_MAX_MEMBERS) {
		if (ClusterMultiXactState != NULL)
			pg_atomic_fetch_add_u64(&ClusterMultiXactState->overlay_overflow_count, 1);
		return false;
	}

	LWLockAcquire(ClusterMultiXactLock, LW_EXCLUSIVE);
	e = (ClusterMultiXactOverlayEntry *)hash_search(ClusterMultiXactHTAB, key, HASH_ENTER_NULL,
													&found);
	if (e == NULL) {
		LWLockRelease(ClusterMultiXactLock);
		pg_atomic_fetch_add_u64(&ClusterMultiXactState->overlay_overflow_count, 1);
		ereport(WARNING, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
						  errmsg("cluster multixact overlay full; install dropped"),
						  errhint("Raise cluster.multixact_member_overlay_max_entries.")));
		return false;
	}

	e->member_count = member_count;
	e->_pad16 = 0;
	e->generation_ts = GetCurrentTimestamp();
	memcpy(e->members, members, member_count * sizeof(ClusterMultiXactMember));

	LWLockRelease(ClusterMultiXactLock);
	pg_atomic_fetch_add_u64(&ClusterMultiXactState->overlay_install_count, 1);
	return true;
}

static bool
cluster_multixact_member_overlay_lookup_raw(const ClusterMultiXactKey *key,
											ClusterMultiXactMemberOverlayResult *out,
											int max_members_buf)
{
	const ClusterMultiXactOverlayEntry *e;
	uint32 current_epoch;

	if (key == NULL || out == NULL || ClusterMultiXactHTAB == NULL)
		return false;

	/* fail-closed sentinel */
	out->authoritative = false;
	out->member_count = 0;
	out->_pad16 = 0;
	out->generation_ts = 0;

	current_epoch = (uint32)cluster_epoch_get_current();
	if (key->cluster_epoch != current_epoch) {
		pg_atomic_fetch_add_u64(&ClusterMultiXactState->overlay_miss_count, 1);
		return false;
	}

	LWLockAcquire(ClusterMultiXactLock, LW_SHARED);
	e = (const ClusterMultiXactOverlayEntry *)hash_search(ClusterMultiXactHTAB, key, HASH_FIND,
														  NULL);
	if (e == NULL) {
		LWLockRelease(ClusterMultiXactLock);
		pg_atomic_fetch_add_u64(&ClusterMultiXactState->overlay_miss_count, 1);
		return false;
	}

	if ((int)e->member_count > max_members_buf) {
		LWLockRelease(ClusterMultiXactLock);
		out->member_count = e->member_count; /* tell caller how big buf needed */
		pg_atomic_fetch_add_u64(&ClusterMultiXactState->overlay_overflow_count, 1);
		return false;
	}

	out->authoritative = true;
	out->member_count = e->member_count;
	out->generation_ts = e->generation_ts;
	memcpy(out->members, e->members, e->member_count * sizeof(ClusterMultiXactMember));

	LWLockRelease(ClusterMultiXactLock);
	pg_atomic_fetch_add_u64(&ClusterMultiXactState->overlay_lookup_hit_count, 1);
	return true;
}

/*
 * cluster_multixact_resolve_visibility (D2 core helper)
 *
 *   Per OBS-1 truth table:
 *     lock-only (status 0-3) ANY xid state              -> VISIBLE
 *     Update/NoKeyUpdate (4-5) ABORTED                  -> VISIBLE
 *     Update/NoKeyUpdate (4-5) IN_PROGRESS authoritative-> VISIBLE
 *     Update/NoKeyUpdate (4-5) COMMITTED scn<=read_scn  -> INVISIBLE
 *     Update/NoKeyUpdate (4-5) COMMITTED scn>read_scn   -> VISIBLE
 *     UNKNOWN / TT miss / overlay miss                  -> UNKNOWN
 *
 *   Helper resolves each member's per-xid status via
 *   cluster_tt_status_lookup_exact + builds ClusterTTStatusKey from the
 *   exact key fields carried in ClusterMultiXactMember;  miss -> UNKNOWN
 *   per L199.
 */
static ClusterVisibilityDecision
cluster_multixact_resolve_visibility_raw(const ClusterMultiXactMemberOverlayResult *overlay,
										 const Snapshot snap)
{
	uint16 i;

	if (overlay == NULL || !overlay->authoritative || snap == NULL)
		return CLUSTER_VISIBILITY_UNKNOWN;

	if (ClusterMultiXactState != NULL)
		pg_atomic_fetch_add_u64(&ClusterMultiXactState->resolve_visibility_count, 1);

	for (i = 0; i < overlay->member_count; i++) {
		const ClusterMultiXactMember *m = &overlay->members[i];
		uint8 status = m->status;

		/*
		 * Lock-only members (FOR_KEY_SHARE / FOR_SHARE / FOR_NOKEYUPDATE /
		 * FOR_UPDATE) cannot hide tuple data regardless of commit/abort
		 * state -- they only lock the row.
		 */
		if (status <= 3) /* MultiXactStatusForKeyShare..ForUpdate */
			continue;

		/*
		 * Update / NoKeyUpdate members:  consult per-member TT status to
		 * decide visibility against snapshot.
		 */
		{
			ClusterTTStatusKey ttkey;
			ClusterTTStatusResult ttres;
			ClusterTTStatusSourceRequest tt_request;
			ClusterTTStatusSourceResult tt_source_result;

			memset(&ttkey, 0, sizeof(ttkey));
			ttkey.origin_node_id = m->origin_node_id;
			ttkey.undo_segment_id = m->undo_segment_id;
			ttkey.tt_slot_id = m->tt_slot_id;
			ttkey.cluster_epoch = m->epoch;
			ttkey.local_xid = m->xid;

			memset(&tt_request, 0, sizeof(tt_request));
			tt_request.key = &ttkey;
			if (cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_LOOKUP, &tt_request,
										  &tt_source_result)
					!= CLUSTER_SEMANTIC_ADMISSION_OK
				|| !tt_source_result.bool_value || !tt_source_result.lookup.authoritative)
				return CLUSTER_VISIBILITY_UNKNOWN;
			ttres = tt_source_result.lookup;

			if (ttres.status == CLUSTER_TT_STATUS_SUBCOMMITTED && ttres.has_parent_key)
				ttres = cluster_subtrans_lookup_parent(&ttres, cluster_subtrans_max_chain_depth);

			if (ttres.status == CLUSTER_TT_STATUS_ABORTED)
				continue; /* aborted updater does not hide tuple */
			if (ttres.status == CLUSTER_TT_STATUS_IN_PROGRESS)
				continue; /* in-progress update not yet visible: tuple still visible */
			if (ttres.status == CLUSTER_TT_STATUS_COMMITTED
				|| ttres.status == CLUSTER_TT_STATUS_CLEANED_OUT) {
				/*
				 * spec-7.1 D3-b hotfix (P0): route the committed-updater
				 * decision through cluster_vis_cr_xmax_verdict -- the polarity
				 * SSOT shared with the single-xmax path
				 * (cluster_visibility_verdict.c).  A committed updater whose
				 * delete is VISIBLE at read_scn (commit_scn at/before read_scn) hides
				 * the tuple (CVV_INVISIBLE); one committed AFTER the snapshot
				 * leaves the row live (CVV_VISIBLE).  The prior inline compare
				 * had this INVERTED -- latent until D3-b's member serve first
				 * feeds this branch a real committed updater terminal.
				 */
				ClusterVisibilityDecision scn_decision
					= cluster_visibility_decide_by_scn(ttres.commit_scn, snap->read_scn);

				switch (cluster_vis_cr_xmax_verdict(ttres.status, scn_decision)) {
				case CVV_VISIBLE:
					continue; /* updater does not hide the tuple at this snapshot */
				case CVV_INVISIBLE:
					return CLUSTER_VISIBILITY_INVISIBLE; /* delete visible -> tuple gone */
				default:
					return CLUSTER_VISIBILITY_UNKNOWN; /* CVV_FAILCLOSED_* */
				}
			}
			/* SUBCOMMITTED / UNKNOWN / other -> caller fail-closed */
			return CLUSTER_VISIBILITY_UNKNOWN;
		}
	}

	/* No updater member hid the tuple -> visible. */
	return CLUSTER_VISIBILITY_VISIBLE;
}

/*
 * cluster_multixact_remote_xmax_ask_origin (spec-7.1 D3-b)
 *
 *	The member overlay missed a foreign multixact xmax.  For an updater-bearing
 *	multi this miss is STRUCTURAL, not incidental: the proactive overlay is
 *	installed at heap_update's OLD-xmax compose time, when the updater still has
 *	no TT binding (IN-12), so the overlay never covers it.  The only positive
 *	path is to ask the ORIGIN -- the sole owner of the multi's pg_multixact
 *	members -- for a batched per-member verdict, then feed the served terminals
 *	to the pure combination resolver.
 *
 *	Any fail-closed outcome (GUC off, DENIED / timeout / invalid page, or a
 *	SERVED page still UNKNOWN at this read_scn) keeps the pre-existing 53R97
 *	boundary (Rule 8.A).  ask/hit census: ask = every request; hit = a definite
 *	VISIBLE/INVISIBLE; unprovable = ask - hit (the feature #119 residue).
 */
static ClusterVisibilityDecision
cluster_multixact_remote_xmax_ask_origin(uint16 origin_slot, MultiXactId mxid, Snapshot snap)
{
	PGAlignedBlock page_buf;
	const ClusterGcsUndoMultiVerdictPage *page;
	ClusterLiveAuthority auth;
	ClusterMultiXactServedMember *served;
	ClusterVisibilityDecision decision;
	uint16 n;
	uint16 i;

	/* Only the GUC-armed cross-node runtime path asks; off = D3-a floor. */
	if (!cluster_crossnode_runtime_visibility || !cluster_multi_xmax_remote_resolve)
		return CLUSTER_VISIBILITY_UNKNOWN;

	cluster_vis53r97_note_multi_member_serve_ask();
	if (!cluster_gcs_block_undo_multi_verdict_fetch_and_wait((int32)origin_slot, mxid,
															 page_buf.data, &auth))
		return CLUSTER_VISIBILITY_UNKNOWN; /* DENIED / timeout / invalid -> 53R97 */

	/*
	 * PGRAC (spec-7.1 integration review, P0): the served member terminals are
	 * only conclusive for THIS snapshot when the co-sampled live authority
	 * covers the demand — same gate, same demand (the snapshot read_scn) as
	 * the single-xid verdict leg (cluster_runtime_visibility.c
	 * rtvis_try_origin_verdict).  Without it an epoch-stale reply (the origin
	 * may have been remastered/fenced since sampling, D-i3) or an origin clock
	 * behind read_scn (a member could still commit after the scan with a
	 * commit_scn at/below read_scn) would be consumed as conclusive.  The
	 * fetch already Lamport-observed every shipped SCN, so a refusal
	 * self-heals on the next snapshot (Rule 8.A: refuse -> UNKNOWN -> 53R97).
	 */
	if (!cluster_vis_live_authority_covers(snap->read_scn, auth)) {
		cluster_vis_bump_covers_scn_refuse_count(); /* spec-7.1a D6 */
		cluster_vis53r97_note_covers_refuse();		/* spec-7.1 D0 census */
		return CLUSTER_VISIBILITY_UNKNOWN;
	}

	/* The page is structurally validated (SERVED, nmembers in [2, MAX], every
	 * member consistent) by the fetch, which validates the STABLE local copy it
	 * returns in page_buf (not the volatile reply slot).  Map it to served
	 * terminals + resolve. */
	page = (const ClusterGcsUndoMultiVerdictPage *)page_buf.data;
	n = page->nmembers;
	/*
	 * Belt (spec-7.1 D3-b hardening, Rule 15): re-assert the [2, MAX] bound
	 * before the variable-length member loop so a future regression in the
	 * fetch validation can never turn an over-range wire count into an
	 * out-of-bounds read of page_buf.
	 */
	if (n < 2 || n > CLUSTER_GCS_UNDO_MULTI_VERDICT_MAX_MEMBERS)
		return CLUSTER_VISIBILITY_UNKNOWN;
	served
		= (ClusterMultiXactServedMember *)palloc0((Size)n * sizeof(ClusterMultiXactServedMember));
	for (i = 0; i < n; i++) {
		served[i].commit_scn = (SCN)page->members[i].commit_scn;
		served[i].horizon_scn = (SCN)page->members[i].horizon_scn;
		served[i].xid = page->members[i].xid;
		served[i].wrap = page->members[i].wrap;
		served[i].verdict = page->members[i].verdict;
		served[i].member_status = page->members[i].member_status;
	}
	decision = cluster_multixact_resolve_visibility_served(served, n, snap->read_scn);
	pfree(served);

	if (decision != CLUSTER_VISIBILITY_UNKNOWN)
		cluster_vis53r97_note_multi_member_serve_hit();
	return decision;
}

/*
 * cluster_multixact_remote_xmax_resolve (spec-7.1 D3-a + D3-b)
 *
 *	One-call reader helper: overlay key build (current epoch) + member
 *	overlay lookup + OBS-1 visibility resolution.  On overlay HIT the local
 *	OBS-1 resolver decides; on overlay MISS (structural for a foreign
 *	updater-multi, IN-12) the D3-b origin member-verdict ask decides.  See
 *	header; UNKNOWN always means fail closed at the caller.
 */
static ClusterVisibilityDecision
cluster_multixact_remote_xmax_resolve_raw(uint16 origin_slot, MultiXactId mxid, Snapshot snap,
										  bool *overlay_hit)
{
	ClusterMultiXactKey mxkey;
	ClusterMultiXactMemberOverlayResult *mxres;
	ClusterVisibilityDecision decision;
	Size resbuf_sz = offsetof(ClusterMultiXactMemberOverlayResult, members)
					 + CLUSTER_MULTIXACT_MAX_MEMBERS * sizeof(ClusterMultiXactMember);

	if (overlay_hit)
		*overlay_hit = false;

	memset(&mxkey, 0, sizeof(mxkey));
	mxkey.origin_node_id = origin_slot;
	mxkey.multixact_id = mxid;
	mxkey.cluster_epoch = (uint32)cluster_epoch_get_current();

	mxres = (ClusterMultiXactMemberOverlayResult *)palloc0(resbuf_sz);

	if (!cluster_multixact_member_overlay_lookup_raw(&mxkey, mxres,
											 CLUSTER_MULTIXACT_MAX_MEMBERS)) {
		pfree(mxres);
		/* spec-7.1 D3-b: overlay miss -> ask the origin (banner). */
		return cluster_multixact_remote_xmax_ask_origin(origin_slot, mxid, snap);
	}

	if (overlay_hit)
		*overlay_hit = true;
	decision = cluster_multixact_resolve_visibility_raw(mxres, snap);
	pfree(mxres);
	return decision;
}

static uint16
cluster_multixact_get_member_count_raw(const ClusterMultiXactKey *key)
{
	const ClusterMultiXactOverlayEntry *e;
	uint16 count = 0;

	if (key == NULL || ClusterMultiXactHTAB == NULL)
		return 0;

	LWLockAcquire(ClusterMultiXactLock, LW_SHARED);
	e = (const ClusterMultiXactOverlayEntry *)hash_search(ClusterMultiXactHTAB, key, HASH_FIND,
														  NULL);
	if (e != NULL)
		count = e->member_count;
	LWLockRelease(ClusterMultiXactLock);
	return count;
}

void
cluster_multixact_purge_epoch(uint32 obsolete_epoch)
{
	HASH_SEQ_STATUS hseq;
	ClusterMultiXactOverlayEntry *e;

	if (ClusterMultiXactHTAB == NULL)
		return;

	LWLockAcquire(ClusterMultiXactLock, LW_EXCLUSIVE);
	hash_seq_init(&hseq, ClusterMultiXactHTAB);
	while ((e = (ClusterMultiXactOverlayEntry *)hash_seq_search(&hseq)) != NULL) {
		if (e->key.cluster_epoch < obsolete_epoch)
			hash_search(ClusterMultiXactHTAB, &e->key, HASH_REMOVE, NULL);
	}
	LWLockRelease(ClusterMultiXactLock);
}

/* ------------------------------------------------------------ */
/* Counter getters                                              */
/* ------------------------------------------------------------ */

#define CLUSTER_MULTIXACT_GETTER(name)                                                             \
	uint64 cluster_multixact_get_##name(void)                                                      \
	{                                                                                              \
		if (ClusterMultiXactState == NULL)                                                         \
			return 0;                                                                              \
		return pg_atomic_read_u64(&ClusterMultiXactState->name);                                   \
	}

CLUSTER_MULTIXACT_GETTER(overlay_install_count)
CLUSTER_MULTIXACT_GETTER(overlay_lookup_hit_count)
CLUSTER_MULTIXACT_GETTER(overlay_miss_count)
CLUSTER_MULTIXACT_GETTER(overlay_overflow_count)
CLUSTER_MULTIXACT_GETTER(resolve_visibility_count)
CLUSTER_MULTIXACT_GETTER(mxid_halfspace_refuse_count)
CLUSTER_MULTIXACT_GETTER(mxid_underivable_read_count)

/*
 * spec-7.1 D3-a guardrail bumps.  Called from GetNewMultiXactId under
 * MultiXactGenLock (halfspace refuse) and from the reader fail-closed
 * legs in heapam_visibility.c (underivable read); atomics, no lock of
 * this module taken.
 */
static void
cluster_multixact_note_halfspace_refuse_raw(void)
{
	if (ClusterMultiXactState != NULL)
		pg_atomic_fetch_add_u64(&ClusterMultiXactState->mxid_halfspace_refuse_count, 1);
}

static void
cluster_multixact_note_underivable_read_raw(void)
{
	if (ClusterMultiXactState != NULL)
		pg_atomic_fetch_add_u64(&ClusterMultiXactState->mxid_underivable_read_count, 1);
}

#else /* !USE_PGRAC_CLUSTER */

Size
cluster_multixact_shmem_size(void)
{
	return 0;
}
void
cluster_multixact_shmem_init(void)
{}
void
cluster_multixact_shmem_register(void)
{}

static bool
cluster_multixact_member_overlay_install_raw(const ClusterMultiXactKey *key, uint16 member_count,
											 const ClusterMultiXactMember *members)
{
	(void)key;
	(void)member_count;
	(void)members;
	return false;
}

static bool
cluster_multixact_member_overlay_lookup_raw(const ClusterMultiXactKey *key,
											ClusterMultiXactMemberOverlayResult *out,
											int max_members_buf)
{
	(void)key;
	(void)max_members_buf;
	if (out != NULL) {
		out->authoritative = false;
		out->member_count = 0;
		out->generation_ts = 0;
	}
	return false;
}

static ClusterVisibilityDecision
cluster_multixact_resolve_visibility_raw(const ClusterMultiXactMemberOverlayResult *overlay,
										 const Snapshot snap)
{
	(void)overlay;
	(void)snap;
	return CLUSTER_VISIBILITY_UNKNOWN;
}

static ClusterVisibilityDecision
cluster_multixact_remote_xmax_resolve_raw(uint16 origin_slot, MultiXactId mxid, Snapshot snap,
										  bool *overlay_hit)
{
	(void)origin_slot;
	(void)mxid;
	(void)snap;
	if (overlay_hit)
		*overlay_hit = false;
	return CLUSTER_VISIBILITY_UNKNOWN;
}

static uint16
cluster_multixact_get_member_count_raw(const ClusterMultiXactKey *key)
{
	(void)key;
	return 0;
}

void
cluster_multixact_purge_epoch(uint32 obsolete_epoch)
{
	(void)obsolete_epoch;
}

#define CLUSTER_MULTIXACT_GETTER_STUB(name)                                                        \
	uint64 cluster_multixact_get_##name(void)                                                      \
	{                                                                                              \
		return 0;                                                                                  \
	}

CLUSTER_MULTIXACT_GETTER_STUB(overlay_install_count)
CLUSTER_MULTIXACT_GETTER_STUB(overlay_lookup_hit_count)
CLUSTER_MULTIXACT_GETTER_STUB(overlay_miss_count)
CLUSTER_MULTIXACT_GETTER_STUB(overlay_overflow_count)
CLUSTER_MULTIXACT_GETTER_STUB(resolve_visibility_count)
CLUSTER_MULTIXACT_GETTER_STUB(mxid_halfspace_refuse_count)
CLUSTER_MULTIXACT_GETTER_STUB(mxid_underivable_read_count)

static void
cluster_multixact_note_halfspace_refuse_raw(void)
{}

static void
cluster_multixact_note_underivable_read_raw(void)
{}

#endif /* USE_PGRAC_CLUSTER */

static ClusterSemanticAdmissionResult
cluster_multixact_source_dispatch_body(ClusterMultiXactSourceOp op,
									   const ClusterMultiXactSourceRequest *request,
									   ClusterMultiXactSourceResult *result)
{
	switch (op) {
	case CLUSTER_MULTI_SOURCE_OVERLAY_INSTALL:
		if (request == NULL || request->key == NULL || request->members == NULL)
			return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
		result->bool_value = cluster_multixact_member_overlay_install_raw(
			request->key, request->member_count, request->members);
		break;
	case CLUSTER_MULTI_SOURCE_OVERLAY_LOOKUP:
		if (request == NULL || request->key == NULL || request->overlay_out == NULL)
			return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
		result->bool_value = cluster_multixact_member_overlay_lookup_raw(
			request->key, request->overlay_out, request->max_members_buf);
		if (result->bool_value)
			result->member_count = request->overlay_out->member_count;
		break;
	case CLUSTER_MULTI_SOURCE_RESOLVE_VISIBILITY:
		if (request == NULL || request->overlay_in == NULL || request->snapshot == NULL)
			return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
		result->visibility
			= cluster_multixact_resolve_visibility_raw(request->overlay_in, request->snapshot);
		break;
	case CLUSTER_MULTI_SOURCE_GET_MEMBER_COUNT:
		if (request == NULL || request->key == NULL)
			return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
		result->member_count = cluster_multixact_get_member_count_raw(request->key);
		break;
	case CLUSTER_MULTI_SOURCE_REMOTE_XMAX_RESOLVE:
		if (request == NULL || request->snapshot == NULL)
			return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
		result->visibility = cluster_multixact_remote_xmax_resolve_raw(
			request->origin_slot, request->mxid, request->snapshot, &result->overlay_hit);
		break;
	case CLUSTER_MULTI_SOURCE_NOTE_HALFSPACE_REFUSE:
		cluster_multixact_note_halfspace_refuse_raw();
		break;
	case CLUSTER_MULTI_SOURCE_NOTE_UNDERIVABLE_READ:
		cluster_multixact_note_underivable_read_raw();
		break;
	default:
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	}

	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

/*
 * cluster_multixact_source_dispatch -- gate one legacy source operation.
 *
 * Inputs:
 *	op: typed operation selector.
 *	request: operation arguments; required pointers are checked after entry.
 *	result: fixed result storage, canonical-zeroed before admission.
 *
 * Returns:
 *	The semantic admission result.  OK means the fixed result is consumable.
 *
 * Side Effects:
 *	On OK admission, invokes exactly one module-private source operation and
 *	balances its admission token on normal and ERROR paths.
 */
ClusterSemanticAdmissionResult
cluster_multixact_source_dispatch(ClusterMultiXactSourceOp op,
								  const ClusterMultiXactSourceRequest *request,
								  ClusterMultiXactSourceResult *result)
{
	ClusterSemanticAdmissionToken token;
	ClusterMultiXactSourceResult local_result;
	ClusterSemanticAdmissionResult admission;
	volatile bool caught_error = false;

	memset(&token, 0, sizeof(token));
	memset(&local_result, 0, sizeof(local_result));
	if (result != NULL)
		memset(result, 0, sizeof(*result));

	admission = cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												  CLUSTER_SEMANTIC_SOURCE_SIDE, &token);
	if (admission != CLUSTER_SEMANTIC_ADMISSION_OK)
		return admission;

	PG_TRY();
	{
		if (result == NULL)
			admission = CLUSTER_SEMANTIC_ADMISSION_CLOSED;
		else
			admission = cluster_multixact_source_dispatch_body(op, request, &local_result);

		if (admission == CLUSTER_SEMANTIC_ADMISSION_OK) {
			if (!cluster_semantic_activation_recheck(&token))
				admission = CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED;
			else
				*result = local_result;
		}
	}
	PG_CATCH();
	{
		caught_error = true;
	}
	PG_END_TRY();

	cluster_semantic_activation_leave(&token);
	if (caught_error)
		PG_RE_THROW();
	return admission;
}
