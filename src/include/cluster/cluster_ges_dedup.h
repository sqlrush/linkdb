/*-------------------------------------------------------------------------
 *
 * cluster_ges_dedup.h
 *	  pgrac GES retransmit dedup HTAB — spec-2.27 D2.
 *
 *	  spec-2.27 reliability hardening introduces retransmit for GES
 *	  REQUEST / RELEASE messages (cluster_ges.c send helpers gain
 *	  exponential backoff loops).  Retransmit safety requires deduplication
 *	  at the receiver:  re-processing a REQUEST twice would double-grant
 *	  the holder count;  re-processing a RELEASE twice would double-pop
 *	  the wait queue.
 *
 *	  Design (HC51 / HC52):
 *	    - LMS-owned shmem region 'pgrac cluster ges dedup' (entries
 *	      persist across LMS process restart).
 *	    - 5-tuple key:  {origin_node_id, opcode, request_id, cluster_epoch,
 *	      shard_master_generation}.  shard_master_generation is supplied
 *	      by the caller in the wire payload (GesRequestPayload, spec-2.27
 *	      bump 48B→56B).
 *	    - Entry value:  {processed_ts, cached_reply_blob[GesReplyPayload
 *	      size = 52B], cached_reply_len, status}.
 *	    - lookup_or_register() returns an explicit enum (never a bool —
 *	      bool semantics were flagged in v0.1 codex review as silent
 *	      double-grant risk because IN_FLIGHT_DUPLICATE collapsed into
 *	      "miss" and the handler re-processed).
 *	    - Cap `cluster.ges_dedup_max_entries` PGC_POSTMASTER default 8192;
 *	      reaching cap returns FULL fail-closed (caller emits
 *	      GES_REJECT_REASON_WORK_QUEUE_FULL — never evicts in-flight
 *	      entries because eviction would re-introduce double-grant risk).
 *	    - LMS restart causes generation bump → LMS restart sweep
 *	      (cluster_ges_dedup_drop_stale_entries) removes entries whose
 *	      shard_master_generation < current LMS generation BEFORE the
 *	      caller's retransmit arrives.  Caller's retransmit then hits
 *	      the MISS_REGISTERED path with the fresh generation key.
 *	      STALE_REPROCESS is sweep-side bookkeeping only — it counts
 *	      entries swept, and is NEVER returned by
 *	      cluster_ges_dedup_lookup_or_register (an inline stale check
 *	      would loop because the caller would re-insert under the same
 *	      stale key; HC51 in cluster_ges_dedup.c §invalidation-model).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *	  src/include/cluster/cluster_ges_dedup.h
 *
 * NOTES
 *	  pgrac-original file.  Compiled only in --enable-cluster builds.
 *	  Spec: spec-2.27-ges-reliability-hardening.md (FROZEN v0.2).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_GES_DEDUP_H
#define CLUSTER_GES_DEDUP_H

#ifndef FRONTEND

#include "postgres.h"
#include "cluster/cluster_ges.h"
#include "datatype/timestamp.h"

/*
 * spec-2.27 D2 — dedup key.
 *
 * PGRAC modifications by SqlRush (spec-5.3 hardening H?):
 *	What changed:  added holder_procno (+ _pad0) — the key was a 5-tuple
 *	  that omitted procno, but a GES request's identity is the documented
 *	  4-tuple (node_id, procno, cluster_epoch, request_id) — see
 *	  cluster_lock_acquire.c assign_request_identity.  request_id is a
 *	  PER-BACKEND counter (each backend's first REQUEST is request_id=1),
 *	  so two backends on the same origin node issuing their first REQUEST
 *	  to the same master/opcode/epoch/generation produced the SAME 5-tuple
 *	  key.  The later request was deduped against the earlier backend's
 *	  cached reply (CACHED_REPLY) and never reached enqueue_or_grant, so no
 *	  GRD holder was recorded for it — a Rule 8.A false-grant (the requester
 *	  saw a "grant" reply that belonged to a different backend's request).
 *	  Latent since spec-2.27; surfaced by spec-5.3's cross-node CONVERT,
 *	  the first consumer that requires the prior REQUEST's holder to persist
 *	  at the master so the convert locator (node,procno,current_mode) can
 *	  find it.
 *	Why:  procno is already carried on the wire (GesRequestPayload.holder_
 *	  procno); folding it into the dedup key restores per-request uniqueness
 *	  without changing request_id's per-backend semantics.  Local shmem HTAB
 *	  key only — not a wire message; region is sized via sizeof() and grows
 *	  automatically.
 */
typedef struct ClusterGesDedupKey {
	uint32 origin_node_id;
	uint32 opcode;
	uint64 request_id;
	uint64 cluster_epoch;
	uint64 shard_master_generation;
	uint32 holder_procno;
	uint32 _pad0; /* HASH_BLOBS: keep padding deterministic (memset 0). */
} ClusterGesDedupKey;

StaticAssertDecl(sizeof(ClusterGesDedupKey) == 40,
				 "ClusterGesDedupKey 40-byte HASH_BLOBS key (6-tuple: +holder_procno)");

/* One authority for both receiver pre-lookup and reply caching. */
static inline bool
cluster_ges_dedup_opcode_uses_cache(uint32 opcode)
{
	return opcode == GES_REQ_OPCODE_REQUEST || opcode == GES_REQ_OPCODE_CONVERT
		   || opcode == GES_REQ_OPCODE_RELEASE || opcode == GES_REQ_OPCODE_REDECLARE
		   || opcode == GES_REQ_OPCODE_REQUEST_NOWAIT;
}

static inline bool
cluster_ges_dedup_request_uses_cache(const GesRequestPayload *req)
{
	if (req == NULL || !cluster_ges_dedup_opcode_uses_cache(req->opcode))
		return false;
	return !(req->opcode == GES_REQ_OPCODE_RELEASE
			 && req->current_mode
					== GES_RELEASE_CURRENT_MODE_CLEANUP_BYPASS);
}

static inline bool
cluster_ges_dedup_hwm_covers_request(uint64 request_id, uint64 inclusive_hwm)
{
	return request_id != 0 && inclusive_hwm != 0 && request_id <= inclusive_hwm;
}

static inline bool
cluster_ges_dedup_lifecycle_payload_valid(const GesDedupLifecyclePayload *p,
										  uint32 authenticated_source,
										  uint64 expected_origin_boot,
										  uint64 expected_target_boot)
{
	if (p == NULL || p->version != GES_DEDUP_LIFECYCLE_VERSION || p->flags != 0
		|| p->reserved != 0 || p->origin_node_id != authenticated_source
		|| p->origin_boot_incarnation == 0 || p->target_boot_incarnation == 0
		|| p->link_generation == 0
		|| p->origin_boot_incarnation != expected_origin_boot
		|| p->target_boot_incarnation != expected_target_boot || p->request_id == 0)
		return false;
	if (p->kind == GES_DEDUP_LIFECYCLE_EXACT_DONE)
		return p->status == 0 && p->opcode != 0
			   && p->shard_master_generation != 0;
	if (p->kind == GES_DEDUP_LIFECYCLE_PROC_EXIT_HWM)
		return p->status == 0 && p->opcode == 0 && p->cluster_epoch == 0
			   && p->shard_master_generation == 0;
	return false;
}

static inline bool
cluster_ges_dedup_ack_matches_done(const GesDedupLifecyclePayload *done,
								   const GesDedupLifecyclePayload *ack)
{
	GesDedupLifecyclePayload expected;

	if (done == NULL || ack == NULL || ack->kind != GES_DEDUP_LIFECYCLE_ACK
		|| ack->status < GES_DEDUP_ACK_REMOVED || ack->status > GES_DEDUP_ACK_REJECTED)
		return false;
	expected = *done;
	expected.kind = GES_DEDUP_LIFECYCLE_ACK;
	expected.status = ack->status;
	return memcmp(&expected, ack, sizeof(expected)) == 0;
}

/* Lookup status — explicit enum (HC51 / HC52);  never collapse states. */
typedef enum ClusterGesDedupLookupStatus {
	/* New key registered; caller MUST process + call record_reply. */
	CLUSTER_GES_DEDUP_MISS_REGISTERED = 0,
	/* Same key already in flight (registered but reply not yet cached).
	 * Caller MUST drop/defer — do NOT re-process (double-grant risk). */
	CLUSTER_GES_DEDUP_IN_FLIGHT_DUPLICATE = 1,
	/* Cached reply available; caller MUST resend (idempotent retransmit). */
	CLUSTER_GES_DEDUP_CACHED_REPLY = 2,
	/* Sweep-side bookkeeping only — counts entries removed by
	 * cluster_ges_dedup_drop_stale_entries (LMS restart) whose
	 * shard_master_generation < current LMS generation.  **Never returned
	 * by cluster_ges_dedup_lookup_or_register** (inline stale check would
	 * create drop-and-reregister loop; HC51 in cluster_ges_dedup.c
	 * §invalidation-model).  Reserved as enum value 3 for stat surface
	 * (cluster_ges_dedup_stale_reprocess_count accessor). */
	CLUSTER_GES_DEDUP_STALE_REPROCESS = 3,
	/* HTAB at cap; caller MUST fail-closed (REJECT_BUSY) — no eviction. */
	CLUSTER_GES_DEDUP_FULL = 4,
	/* A persistent completion/HWM frontier proves this is a delayed frame
	 * from an already-retired operation.  Caller MUST drop, never re-run. */
	CLUSTER_GES_DEDUP_RETIRED_LATE = 5
} ClusterGesDedupLookupStatus;

/*
 * Probe the dedup HTAB and optionally register a fresh entry.
 *
 *	On MISS_REGISTERED:  fresh entry inserted with status = in-flight;
 *	  caller MUST follow up with cluster_ges_dedup_record_reply().
 *	On CACHED_REPLY:  reply_out is filled with the cached
 *	  GesReplyPayload blob;  *reply_len_out set to the cached length.
 *	On IN_FLIGHT_DUPLICATE / STALE_REPROCESS / FULL:  reply_out untouched.
 */
extern ClusterGesDedupLookupStatus
cluster_ges_dedup_lookup_or_register(const ClusterGesDedupKey *key, uint8 *reply_out,
									 uint16 reply_buf_len, uint16 *reply_len_out);
extern ClusterGesDedupLookupStatus cluster_ges_dedup_lookup_or_register_identity(
	const ClusterGesDedupKey *key, uint64 origin_boot_incarnation, uint8 *reply_out,
	uint16 reply_buf_len, uint16 *reply_len_out);
extern ClusterGesDedupLookupStatus cluster_ges_dedup_lookup_or_register_identity_ex(
	const ClusterGesDedupKey *key, uint64 origin_boot_incarnation,
	uint64 *superseded_boot_out, uint8 *reply_out, uint16 reply_buf_len,
	uint16 *reply_len_out);

/*
 * Execution-time frontier fence.  A request can enter the GRD work queue just
 * before PROC_EXIT_HWM advances its persistent frontier.  LMON must recheck
 * immediately before mutation and skip a covered frame.
 */
extern bool cluster_ges_dedup_request_is_retired(
	const ClusterGesDedupKey *key, uint64 origin_boot_incarnation);

/*
 * Publish the terminal receiver outcome for an in-flight entry.
 *
 *	With a non-empty blob, caller invokes this after generating GES_REPLY.
 *	A NULL/zero blob is also terminal (successful CANCEL_WAIT has no reply).
 *	In both cases subsequent retransmits hit CACHED_REPLY; status, not payload
 *	length, distinguishes terminal from IN_FLIGHT.
 */
extern void cluster_ges_dedup_record_reply(const ClusterGesDedupKey *key, const uint8 *reply,
										   uint16 reply_len);
extern void cluster_ges_dedup_record_reply_identity(
	const ClusterGesDedupKey *key, uint64 origin_boot_incarnation,
	const uint8 *reply, uint16 reply_len);

typedef enum ClusterGesDedupRetireResult {
	CLUSTER_GES_DEDUP_RETIRE_REMOVED = 1,
	CLUSTER_GES_DEDUP_RETIRE_ALREADY_ABSENT = 2,
	CLUSTER_GES_DEDUP_RETIRE_PENDING = 3,
	CLUSTER_GES_DEDUP_RETIRE_IDENTITY_MISMATCH = 4
} ClusterGesDedupRetireResult;

/* Exact DONE first advances the serial backend's persistent completion
 * frontier, then removes the full-identity cached row.  A DONE racing an
 * in-flight row marks retire-on-cache; record_reply removes it instead of
 * publishing a replayable cache. */
extern ClusterGesDedupRetireResult cluster_ges_dedup_retire_exact(
	const ClusterGesDedupKey *key, uint64 origin_boot_incarnation);

/* Abrupt-backend cleanup.  request_id_hwm is inclusive and safe across
 * procno reuse because request ids are node-global monotonic. */
extern uint32 cluster_ges_dedup_retire_origin_proc_up_to(
	uint32 origin_node_id, uint32 holder_procno, uint64 request_id_hwm,
	uint64 origin_boot_incarnation, uint32 *pending_out, bool *applied_out);

/* Node death / authenticated boot replacement invalidates every request from
 * the old origin life. */
extern uint32 cluster_ges_dedup_drop_origin_node(uint32 origin_node_id);
extern uint32 cluster_ges_dedup_drop_origin_boot_mismatch(
	uint32 origin_node_id, uint64 current_origin_boot);

/*
 * Sweep entries whose shard_master_generation < current LMS generation.
 *
 *	Called by LMS at startup after bumping lms_restart_generation so
 *	the prior generation's cached replies are forcibly invalidated.
 *	Returns count swept.
 */
extern uint32 cluster_ges_dedup_drop_stale_entries(void);

/* Observability accessors. */
extern uint32 cluster_ges_dedup_entry_count(void);
extern uint32 cluster_ges_dedup_capacity(void);
extern uint64 cluster_ges_dedup_hit_cached_count(void);
extern uint64 cluster_ges_dedup_in_flight_dup_count(void);
extern uint64 cluster_ges_dedup_stale_reprocess_count(void);
extern uint64 cluster_ges_dedup_full_reject_count(void);

/*
 * Requester-side shared DONE/HWM journal.  It is postmaster shared, so an
 * ACK retry survives the originating backend's exit.  The caller reserves
 * before staging the first request and capability-generation-binds every
 * original/retransmit frame; these APIs own persistence/ACK correlation.
 */
extern bool cluster_ges_dedup_journal_register(
	uint32 dest_node_id, const GesDedupLifecyclePayload *done);
extern bool cluster_ges_dedup_journal_commit(
	uint32 dest_node_id, const GesDedupLifecyclePayload *done);
extern bool cluster_ges_dedup_journal_cancel(
	uint32 dest_node_id, const GesDedupLifecyclePayload *done);
extern bool cluster_ges_dedup_journal_claim_due(
	TimestampTz now, uint32 *dest_node_id, GesDedupLifecyclePayload *done);
extern bool cluster_ges_dedup_journal_ack(
	uint32 authenticated_source, const GesDedupLifecyclePayload *ack);
extern uint32 cluster_ges_dedup_journal_reap_dead_backend(void);
extern uint32 cluster_ges_dedup_journal_drop_target_boot_mismatch(
	uint32 dest_node_id, uint64 current_target_boot);
extern uint32 cluster_ges_dedup_journal_count(void);
extern uint64 cluster_ges_dedup_journal_full_count(void);
extern uint64 cluster_ges_dedup_journal_ack_count(void);

/* Shmem region lifecycle (registered via cluster_init_shmem_module). */
extern Size cluster_ges_dedup_shmem_size(void);
extern void cluster_ges_dedup_shmem_request(void);
extern void cluster_ges_dedup_shmem_init(void);
extern void cluster_ges_dedup_shmem_register(void);

#endif /* !FRONTEND */

#endif /* CLUSTER_GES_DEDUP_H */
