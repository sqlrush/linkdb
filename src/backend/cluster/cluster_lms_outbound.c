/*-------------------------------------------------------------------------
 *
 * cluster_lms_outbound.c
 *	  pgrac DATA-plane outbound ring — spec-7.2 D4 (Q6-B twin ring).
 *
 *	  Backend-context producers of DATA-plane messages (GCS block
 *	  REQUEST / INVALIDATE) stage frames here;  the LMS data-plane loop
 *	  drains and sends them over its own fds.  This is the DATA twin of
 *	  the CONTROL-plane cluster_grd_outbound ring, kept to the same
 *	  minimal single-tail-FIFO semantics (Q6-B: one consumer, one lock,
 *	  no claim/compaction machinery — the r1-F3 argument against a
 *	  shared dual-reader ring).  Enqueue marks no LMON duty and wakes
 *	  LMS, not LMON.
 *
 *	  Ordering note (INV-7.2-DATA-FIFO): per-peer DATA frames keep a
 *	  single ordered stream because (a) this ring is FIFO, (b) LMS is
 *	  its only consumer, and (c) tier1 owns per-peer ordering below us —
 *	  an ADMITTED frame (send result DONE or WOULD_BLOCK) is delivered
 *	  in submission order by the tier1 outbound FIFO, and a REFUSED
 *	  frame (NOT_ADMITTED) is retained here ahead of anything newer for
 *	  the same peer (GCS serve-stall round-5 ownership contract).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_lms_outbound.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.  Spec: spec-7.2-ic-data-plane-decoupling.md (D4).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_gcs_block.h" /* GcsBlockRequestPayload (pre-send hook) */
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_write_fence.h"
#include "miscadmin.h"
#include "portability/instr_time.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/elog.h"

#ifdef USE_PGRAC_CLUSTER

#define PGRAC_LMS_OUTBOUND_CAPACITY 256
#define PGRAC_LMS_OUTBOUND_PAYLOAD_MAX 128
#define PGRAC_LMS_RESOURCE_X_PROBE_BUDGET 4
#define PGRAC_LMS_RESOURCE_X_CALLS_PER_TICK 16

typedef enum ClusterLmsOutboundKind {
	CLUSTER_LMS_OUTBOUND_FRAME = 0,
	CLUSTER_LMS_OUTBOUND_ZERO_BLOCK_REPLY = 1,
	CLUSTER_LMS_OUTBOUND_DIRECT_ZERO_BLOCK_REPLY = 2,
	CLUSTER_LMS_OUTBOUND_RESOURCE_X_INTENT = 3
} ClusterLmsOutboundKind;

typedef struct ClusterLmsOutboundSlot {
	uint32 dest_node_id;
	uint8 msg_type;
	uint8 kind;
	uint16 payload_len;
	uint32 required_capability;
	uint32 connection_generation;
	uint64 deadline_us;
	uint8 payload[PGRAC_LMS_OUTBOUND_PAYLOAD_MAX];
} ClusterLmsOutboundSlot;

StaticAssertDecl(sizeof(ClusterLmsOutboundSlot) == 152,
				 "LMS outbound slot capability guard layout changed");

typedef struct ClusterLmsZeroBlockReplyWire {
	GcsBlockReplyHeader header;
	char block_data[GCS_BLOCK_DATA_SIZE];
} ClusterLmsZeroBlockReplyWire;

StaticAssertDecl(sizeof(ClusterLmsZeroBlockReplyWire) == GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE,
				 "staged zero-block reply must preserve the GCS reply wire size");

typedef struct ClusterLmsOutboundState {
	uint32 head; /* next slot to fill */
	uint32 tail; /* next slot to drain */
	uint32 count;
	ClusterLmsOutboundSlot ring[PGRAC_LMS_OUTBOUND_CAPACITY];
} ClusterLmsOutboundState;

/*
 * spec-7.3 D4 — one ring per DATA worker channel.  rings[0] is worker 0 (the
 * spec-7.2 ring; lms_workers=1 uses only it — byte-identical), rings[c] is
 * worker c.  Sizing is the compile-time cap (CLUSTER_LMS_MAX_WORKERS); the
 * live count follows cluster.lms_workers.  Each ring keeps the Q6-B single-
 * consumer single-tail FIFO semantics, and each has its own lock so worker c
 * drains rings[c] without contending on the other workers.
 */
static ClusterLmsOutboundState *cluster_lms_outbound_rings = NULL;
static LWLock *cluster_lms_outbound_locks[CLUSTER_LMS_MAX_WORKERS];

#define OB_RING(w) (&cluster_lms_outbound_rings[(w)])
#define OB_LOCK(w) (cluster_lms_outbound_locks[(w)])

static Size
cluster_lms_outbound_shmem_size(void)
{
	/* Contiguous array of CLUSTER_LMS_MAX_WORKERS rings; the C array stride is
	 * sizeof(ClusterLmsOutboundState), so size the whole block then MAXALIGN. */
	return MAXALIGN(mul_size(CLUSTER_LMS_MAX_WORKERS, sizeof(ClusterLmsOutboundState)));
}

static void
cluster_lms_outbound_shmem_init(void)
{
	bool found;
	int i;

	cluster_lms_outbound_rings = (ClusterLmsOutboundState *)ShmemInitStruct(
		"pgrac cluster lms data outbound", cluster_lms_outbound_shmem_size(), &found);
	if (!found)
		memset(cluster_lms_outbound_rings, 0,
			   CLUSTER_LMS_MAX_WORKERS * sizeof(ClusterLmsOutboundState));

	if (!IsBootstrapProcessingMode())
		for (i = 0; i < CLUSTER_LMS_MAX_WORKERS; i++)
			cluster_lms_outbound_locks[i]
				= &(GetNamedLWLockTranche("ClusterLmsDataOutbound"))[i].lock;
}

static const ClusterShmemRegion cluster_lms_outbound_region = {
	.name = "pgrac cluster lms data outbound",
	.size_fn = cluster_lms_outbound_shmem_size,
	.init_fn = cluster_lms_outbound_shmem_init,
	.lwlock_count = CLUSTER_LMS_MAX_WORKERS,
	.owner_subsys = "cluster_lms_outbound",
	.reserved_flags = 0,
};


/* Type 50 reports the holder's irreversible X->N handoff; types 51-56 drive
 * the resulting X grant to completion.  None may cross the final DATA
 * transport boundary while the protocol runtime or node write authority is
 * closed.  Admission/cancel/drain traffic remains independently routable. */
static bool
lms_outbound_pcm_x_grant_held(uint8 msg_type)
{
	PcmXRuntimeSnapshot runtime;

	if (msg_type < PGRAC_IC_MSG_PCM_X_IMAGE_READY || msg_type > PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM)
		return false;
	runtime = cluster_pcm_x_runtime_snapshot();
	if (runtime.state != PCM_X_RUNTIME_ACTIVE)
		return true;
	return cluster_write_fence_enforcing() && !cluster_write_fence_allowed();
}


static void
lms_outbound_pcm_x_image_ready_note(const ClusterLmsOutboundSlot *slot, const char *boundary,
									int result)
{
	const PcmXGrantPayload *ready;
	PcmXRuntimeSnapshot runtime;
	bool fence_enforcing;
	bool fence_allowed;

	if (slot == NULL || boundary == NULL
		|| (slot->msg_type != PGRAC_IC_MSG_PCM_X_IMAGE_READY
			&& slot->msg_type != PGRAC_IC_MSG_PCM_X_PREPARE_GRANT)
		|| slot->payload_len != sizeof(PcmXGrantPayload))
		return;
	ready = (const PcmXGrantPayload *)slot->payload;
	runtime = cluster_pcm_x_runtime_snapshot();
	fence_enforcing = cluster_write_fence_enforcing();
	fence_allowed = !fence_enforcing || cluster_write_fence_allowed();
	cluster_lms_note_pcm_x_image_ready_boundary(
		slot->msg_type, boundary, result, (int)runtime.state, fence_enforcing, fence_allowed,
		slot->dest_node_id, ready->ref.identity.request_id, ready->ref.handle.ticket_id,
		ready->ref.grant_generation, ready->image.image_id);
}

void
cluster_lms_outbound_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_lms_outbound_region);
}

/* Named-tranche request (process_shmem_requests window;  I15 pattern). */
void
cluster_lms_outbound_request_lwlocks(void)
{
	RequestNamedLWLockTranche("ClusterLmsDataOutbound", CLUSTER_LMS_MAX_WORKERS);
}

/*
 * cluster_lms_outbound_enqueue — stage one DATA-plane frame for LMS.
 *
 *	Returns false on full ring / oversized payload / pre-shmem call;
 *	callers treat false as WOULD_BLOCK (retry via the normal request
 *	retry machinery).  Publish-before-signal: the slot is visible
 *	before the LMS wakeup fires.
 */
static bool
lms_outbound_enqueue_internal(int worker_id, uint8 msg_type, uint32 dest_node_id,
							  const void *payload, uint16 payload_len, uint32 required_capability,
							  uint32 connection_generation)
{
	ClusterLmsOutboundState *ring;
	LWLock *lock;
	ClusterLmsOutboundSlot *slot;

	if (worker_id < 0 || worker_id >= CLUSTER_LMS_MAX_WORKERS)
		return false;
	if (cluster_lms_outbound_rings == NULL || OB_LOCK(worker_id) == NULL)
		return false;
	if (payload_len > PGRAC_LMS_OUTBOUND_PAYLOAD_MAX)
		return false;

	ring = OB_RING(worker_id);
	lock = OB_LOCK(worker_id);

	LWLockAcquire(lock, LW_EXCLUSIVE);
	if (ring->count >= PGRAC_LMS_OUTBOUND_CAPACITY) {
		LWLockRelease(lock);
		return false;
	}
	slot = &ring->ring[ring->head];
	slot->dest_node_id = dest_node_id;
	slot->msg_type = msg_type;
	slot->kind = (uint8)CLUSTER_LMS_OUTBOUND_FRAME;
	slot->payload_len = payload_len;
	slot->required_capability = required_capability;
	slot->connection_generation = connection_generation;
	if (payload_len > 0)
		memcpy(slot->payload, payload, payload_len);
	ring->head = (ring->head + 1) % PGRAC_LMS_OUTBOUND_CAPACITY;
	ring->count++;
	LWLockRelease(lock);

	cluster_lms_wakeup(worker_id);
	return true;
}

bool
cluster_lms_outbound_enqueue(int worker_id, uint8 msg_type, uint32 dest_node_id,
							 const void *payload, uint16 payload_len)
{
	return lms_outbound_enqueue_internal(worker_id, msg_type, dest_node_id, payload, payload_len, 0,
										 0);
}

/* Stage a wire-version-sensitive frame for one exact HELLO-authenticated
 * connection.  The reliable protocol leg remains outside this ring, so a
 * drain-side guard failure may consume this stale copy and let the periodic
 * producer reconstruct the correct wire version. */
bool
cluster_lms_outbound_enqueue_cap_bound(int worker_id, uint8 msg_type, uint32 dest_node_id,
									   const void *payload, uint16 payload_len,
									   uint32 required_capability, uint32 connection_generation)
{
	if (required_capability == 0 || dest_node_id >= CLUSTER_MAX_NODES)
		return false;
	return lms_outbound_enqueue_internal(worker_id, msg_type, dest_node_id, payload, payload_len,
										 required_capability, connection_generation);
}

static uint64
lms_outbound_monotonic_us(void)
{
	instr_time now;

	INSTR_TIME_SET_CURRENT(now);
	return (uint64)INSTR_TIME_GET_MICROSEC(now);
}

static bool
lms_outbound_resource_x_intent_valid(const ResourceXIntentSlot *intent)
{
	return intent != NULL
		&& intent->logical_generation != 0
		&& intent->logical_generation != UINT64_MAX
		&& intent->authority_generation != 0
		&& intent->authority_generation != UINT64_MAX
		&& intent->first_armed_us != 0
		&& intent->first_armed_us != UINT64_MAX
		&& intent->destination_node < RESOURCE_X_PROTOCOL_NODE_LIMIT
		&& intent->payload_bytes == RESOURCE_X_PROOF_V1_BYTES
		&& intent->kind == RESOURCE_X_WIRE_AUTHORITY_GRANT
		&& intent->state == RESOURCE_X_INTENT_SLOT_ARMED
		&& intent->body.assertion.requester_node
			   == (int32)intent->destination_node
		&& intent->body.owner_generation == intent->authority_generation
		&& intent->body.owner_node < RESOURCE_X_PROTOCOL_NODE_LIMIT
		&& intent->body.owner_kind
			   == RESOURCE_X_INTENT_OWNER_MASTER_GRANT
		&& intent->body.owner_index == 0
		&& intent->body.reserved == 0;
}

static bool
lms_outbound_resource_x_intent_identity_equal(
	const ResourceXIntentSlot *left, const ResourceXIntentSlot *right)
{
	return left != NULL && right != NULL
		&& left->logical_generation == right->logical_generation
		&& left->authority_generation == right->authority_generation
		&& left->first_armed_us == right->first_armed_us
		&& left->destination_node == right->destination_node
		&& left->payload_bytes == right->payload_bytes
		&& left->kind == right->kind
		&& memcmp(&left->body, &right->body, sizeof(left->body)) == 0;
}

bool
cluster_lms_outbound_enqueue_resource_x_intent(
	int worker_id, const ResourceXIntentSlot *intent,
	uint32 connection_generation, uint64 deadline_us)
{
	ClusterLmsOutboundState *ring;
	ClusterLmsOutboundSlot *slot;
	LWLock *lock;
	uint64 now_us;

	if (worker_id < 0 || worker_id >= CLUSTER_LMS_MAX_WORKERS
		|| connection_generation == 0 || deadline_us == 0
		|| !lms_outbound_resource_x_intent_valid(intent)
		|| cluster_lms_outbound_rings == NULL || OB_LOCK(worker_id) == NULL)
		return false;
	ring = OB_RING(worker_id);
	lock = OB_LOCK(worker_id);
	now_us = lms_outbound_monotonic_us();
	if (now_us == 0)
		return false;

	/* Publish the ring slot only after the exact semantic owner changes to
	 * STAGED.  Holding the ring lock across that owner mutation prevents a
	 * concurrently polling destination worker from consuming an ARMED slot
	 * before the transition becomes visible. */
	LWLockAcquire(lock, LW_EXCLUSIVE);
	if (ring->count >= PGRAC_LMS_OUTBOUND_CAPACITY) {
		LWLockRelease(lock);
		return false;
	}
	slot = &ring->ring[ring->head];
	memset(slot, 0, sizeof(*slot));
	slot->dest_node_id = intent->destination_node;
	slot->msg_type = RESOURCE_X_MSG_IMAGE_OR_GRANT;
	slot->kind = (uint8)CLUSTER_LMS_OUTBOUND_RESOURCE_X_INTENT;
	slot->payload_len = sizeof(*intent);
	slot->required_capability
		= PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1;
	slot->connection_generation = connection_generation;
	slot->deadline_us = deadline_us;
	memcpy(slot->payload, intent, sizeof(*intent));
	if (cluster_pcm_lock_resource_x_grant_intent_stage_exact(intent, now_us)
		!= RESOURCE_X_INTENT_STAGED) {
		memset(slot, 0, sizeof(*slot));
		LWLockRelease(lock);
		return false;
	}
	ring->head = (ring->head + 1) % PGRAC_LMS_OUTBOUND_CAPACITY;
	ring->count++;
	LWLockRelease(lock);
	cluster_lms_wakeup(worker_id);
	return true;
}

int
cluster_lms_outbound_resource_x_intent_pump(void)
{
	ResourceXIntentSlot intent;
	uint8 payload[RESOURCE_X_PROOF_V1_BYTES];
	int staged = 0;
	int call;

	Assert(MyBackendType == B_LMS);
	for (call = 0; call < PGRAC_LMS_RESOURCE_X_CALLS_PER_TICK; call++) {
		ResourceXIntentProbeResult probe_result;
		uint32 capability_word = 0;
		uint32 connection_generation = 0;
		uint32 examined = 0;
		uint64 deadline_us;
		uint64 now_us;
		uint64 timeout_us;
		int worker_id;

		probe_result = cluster_pcm_lock_resource_x_grant_intent_probe_exact(
			PGRAC_LMS_RESOURCE_X_PROBE_BUDGET, &intent, payload,
			sizeof(payload), &examined);
		if (probe_result == RESOURCE_X_INTENT_PROBE_IDLE
			|| probe_result == RESOURCE_X_INTENT_PROBE_COMPLETE)
			break;
		if (probe_result == RESOURCE_X_INTENT_PROBE_CORRUPT)
			break;
		if (probe_result == RESOURCE_X_INTENT_PROBE_MORE)
			continue;
		if (probe_result != RESOURCE_X_INTENT_PROBE_FOUND)
			break;

		now_us = lms_outbound_monotonic_us();
		if (now_us == 0)
			continue;
		if ((int32)intent.destination_node == cluster_node_id) {
			if ((cluster_ic_local_capability_word()
				 & PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1) == 0) {
				(void)cluster_pcm_lock_resource_x_grant_intent_not_admitted_exact(
					&intent, now_us);
				continue;
			}
			connection_generation = 1;
		} else if (!cluster_sf_peer_capability_word_sample(
				   (int32)intent.destination_node,
				   PGRAC_IC_HELLO_CAP_GCS_RESOURCE_X_CONVERT_V1,
				   &capability_word, &connection_generation)
				   || connection_generation == 0) {
			(void)cluster_pcm_lock_resource_x_grant_intent_not_admitted_exact(
				&intent, now_us);
			continue;
		}
		worker_id = cluster_lms_shard_for_tag(
			&intent.body.assertion.resource, cluster_lms_workers);
		/* The physical copy inherits the existing GCS DATA transport
		 * deadline.  Expiry returns only the ring ownership; it is not a
		 * new Resource-X operation timeout or runtime policy. */
		timeout_us
			= (uint64)Max(cluster_gcs_reply_timeout_ms, 1) * UINT64_C(1000);
		deadline_us
			= now_us > UINT64_MAX - timeout_us
			? UINT64_MAX
			: now_us + timeout_us;
		if (cluster_lms_outbound_enqueue_resource_x_intent(
				worker_id, &intent, connection_generation, deadline_us))
			staged++;
		else
			(void)cluster_pcm_lock_resource_x_grant_intent_not_admitted_exact(
				&intent, now_us);
	}
	return staged;
}

static bool
lms_outbound_r4_refusal_header_valid(const GcsBlockReplyHeader *header)
{
	int i;

	if (header == NULL || !GcsBlockReplyStatusIsR4Refusal((GcsBlockReplyStatus)header->status)
		|| header->request_id == 0 || header->checksum != 0 || header->sender_node < 0
		|| header->sender_node >= CLUSTER_MAX_NODES || header->requester_backend_id <= 0
		|| header->transition_id != (uint8)PCM_TRANS_N_TO_S
		|| GcsBlockReplyHeaderGetForwardingMasterNode(header)
			   != GCS_BLOCK_REPLY_NO_FORWARDING_MASTER)
		return false;
	for (i = 0; i < (int)sizeof(header->reserved_0); i++)
		if (header->reserved_0[i] != 0)
			return false;
	if (header->status == (uint8)GCS_BLOCK_REPLY_R4_DENIED)
		return header->page_lsn == 0;
	/* Status 25 optionally carries WRONG_MASTER as node+1.  The encoded
	 * value is therefore either zero or in [1, CLUSTER_MAX_NODES]. */
	return header->page_lsn <= (uint64)CLUSTER_MAX_NODES;
}

static bool
lms_outbound_enqueue_zero_block_reply_internal(int worker_id, uint32 dest_node_id,
											 const GcsBlockReplyHeader *header,
											 bool direct_land, uint32 required_capability,
											 uint32 connection_generation)
{
	ClusterLmsOutboundState *ring;
	LWLock *lock;
	ClusterLmsOutboundSlot *slot;
	bool r4_cap_bound = required_capability != 0;

	if (worker_id < 0 || worker_id >= CLUSTER_LMS_MAX_WORKERS || dest_node_id >= CLUSTER_MAX_NODES
		|| header == NULL || (direct_land && (int32)dest_node_id == cluster_node_id))
		return false;
	if (r4_cap_bound) {
		if (direct_land || !lms_outbound_r4_refusal_header_valid(header))
			return false;
	} else if (header->status != (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X)
		return false;
	if (cluster_lms_outbound_rings == NULL || OB_LOCK(worker_id) == NULL)
		return false;

	ring = OB_RING(worker_id);
	lock = OB_LOCK(worker_id);
	LWLockAcquire(lock, LW_EXCLUSIVE);
	if (ring->count >= PGRAC_LMS_OUTBOUND_CAPACITY) {
		LWLockRelease(lock);
		return false;
	}
	slot = &ring->ring[ring->head];
	slot->dest_node_id = dest_node_id;
	slot->msg_type = PGRAC_IC_MSG_GCS_BLOCK_REPLY;
	slot->kind = (uint8)(direct_land ? CLUSTER_LMS_OUTBOUND_DIRECT_ZERO_BLOCK_REPLY
									 : CLUSTER_LMS_OUTBOUND_ZERO_BLOCK_REPLY);
	slot->payload_len = sizeof(*header);
	slot->required_capability = required_capability;
	slot->connection_generation = connection_generation;
	memcpy(slot->payload, header, sizeof(*header));
	ring->head = (ring->head + 1) % PGRAC_LMS_OUTBOUND_CAPACITY;
	ring->count++;
	LWLockRelease(lock);

	cluster_lms_wakeup(worker_id);
	return true;
}

/* Stage the legacy Shape-B pending-X denial. */
bool
cluster_lms_outbound_enqueue_zero_block_reply(int worker_id, uint32 dest_node_id,
											  const GcsBlockReplyHeader *header, bool direct_land)
{
	return lms_outbound_enqueue_zero_block_reply_internal(
		worker_id, dest_node_id, header, direct_land, 0, 0);
}

/* PGRAC R4 adaptation: stage a typed 25/26 refusal on the requester's exact
 * HELLO-authenticated connection.  The existing DATA slot remains 144 bytes;
 * only the 48-byte header is retained until drain expands the zero page. */
bool
cluster_lms_outbound_enqueue_zero_block_reply_cap_bound(
	int worker_id, uint32 dest_node_id, const GcsBlockReplyHeader *header,
	uint32 required_capability, uint32 connection_generation)
{
	if (required_capability == 0)
		return false;
	return lms_outbound_enqueue_zero_block_reply_internal(
		worker_id, dest_node_id, header, false, required_capability, connection_generation);
}

/*
 * cluster_lms_outbound_drain_send — one worker drains + sends its own ring.
 *
 *	Bounded batch per call.  worker c only ever touches rings[c], so the
 *	single-consumer-single-tail guarantee holds per worker (spec-7.3 D4).
 *	The GCS block REQUEST pre-send hook (direct-land arm) rides along
 *	with the DATA consumer.
 *
 *	GCS serve-stall round-5 — the drain follows the four-state send
 *	ownership contract (see ClusterICSendResult):
 *
 *	  DONE / WOULD_BLOCK  frame is on the wire or ADMITTED into tier1's
 *	                      per-peer FIFO;  the slot is consumed and the
 *	                      frame is NEVER resubmitted.  (Pre-fix code
 *	                      head-requeued on WOULD_BLOCK — a duplicate
 *	                      frame on the per-peer stream, because tier1
 *	                      had usually retained the original.)
 *	  NOT_ADMITTED        transport refused;  the frame is RETAINED and
 *	                      the peer is marked blocked for the rest of the
 *	                      batch so its later frames keep per-peer order
 *	                      behind it.  Other peers keep flowing — one
 *	                      backpressured peer must not head-of-line block
 *	                      the worker ring (pre-fix code broke the batch).
 *	  HARD_ERROR          peer down — drop;  requesters retry
 *	                      fail-closed.
 *
 *	Retained frames go back at the HEAD in original order after the
 *	batch.  Producers may have refilled the ring meanwhile;  a retained
 *	frame that no longer fits is counted (requeue_drop, expected 0) —
 *	never silently discarded.
 */
int
cluster_lms_outbound_drain_send(int worker_id)
{
	ClusterLmsOutboundState *ring;
	LWLock *lock;
	int sent = 0;
	int scanned = 0;
	ClusterLmsOutboundSlot retained[64];
	int n_retained = 0;
	bool peer_blocked[CLUSTER_MAX_NODES] = { 0 };

	if (worker_id < 0 || worker_id >= CLUSTER_LMS_MAX_WORKERS)
		return 0;
	if (cluster_lms_outbound_rings == NULL || OB_LOCK(worker_id) == NULL)
		return 0;

	Assert(MyBackendType == B_LMS || MyBackendType == B_LMS_WORKER);

	ring = OB_RING(worker_id);
	lock = OB_LOCK(worker_id);

	while (scanned < 64) {
		ClusterLmsOutboundSlot slot;
		ClusterLmsZeroBlockReplyWire zero_reply;
		ResourceXIntentSlot resource_x_intent;
		ResourceXIntentSlot resource_x_current;
		uint8 resource_x_payload[RESOURCE_X_PROOF_V1_BYTES];
		const void *send_payload;
		uint32 send_payload_len;
		ClusterICSendResult rc;
		uint64 now_us;
		bool resource_x_slot = false;
		bool got = false;

		LWLockAcquire(lock, LW_EXCLUSIVE);
		if (ring->count > 0) {
			slot = ring->ring[ring->tail];
			ring->tail = (ring->tail + 1) % PGRAC_LMS_OUTBOUND_CAPACITY;
			ring->count--;
			got = true;
		}
		LWLockRelease(lock);
		if (!got)
			break;
		scanned++;
		memset(&resource_x_intent, 0, sizeof(resource_x_intent));
		resource_x_slot
			= slot.kind == (uint8)CLUSTER_LMS_OUTBOUND_RESOURCE_X_INTENT;
		if (resource_x_slot) {
			if (slot.msg_type != RESOURCE_X_MSG_IMAGE_OR_GRANT
				|| slot.payload_len != sizeof(resource_x_intent))
				continue;
			memcpy(&resource_x_intent, slot.payload,
				   sizeof(resource_x_intent));
		}
		/* Wire-version-sensitive slots are valid only for the exact
		 * connection generation whose HELLO advertised the required bit.
		 * A drift consumes this stale ring copy without transport admission;
		 * no protocol ACK is generated, so the armed reliable leg retries. */
		if (slot.required_capability != 0
			&& (((int32)slot.dest_node_id == cluster_node_id
				 && (cluster_ic_local_capability_word()
					 & slot.required_capability) != slot.required_capability)
				|| ((int32)slot.dest_node_id != cluster_node_id
					&& !cluster_sf_peer_capability_generation_matches(
						(int32)slot.dest_node_id, slot.required_capability,
						slot.connection_generation)))) {
			lms_outbound_pcm_x_image_ready_note(&slot, "capability-guard", -1);
			cluster_lms_obs_note_outbound_cap_guard_drop(worker_id);
			if (resource_x_slot)
				(void)cluster_pcm_lock_resource_x_grant_intent_hard_rearm_exact(
					&resource_x_intent, lms_outbound_monotonic_us());
			continue;
		}
		send_payload = slot.payload_len > 0 ? slot.payload : NULL;
		send_payload_len = slot.payload_len;
		if (resource_x_slot) {
			now_us = lms_outbound_monotonic_us();
			if (now_us == 0 || now_us >= slot.deadline_us) {
				(void)cluster_pcm_lock_resource_x_grant_intent_hard_rearm_exact(
					&resource_x_intent, now_us);
				continue;
			}
			if (cluster_pcm_lock_resource_x_grant_intent_snapshot_exact(
					&resource_x_intent.body.assertion, &resource_x_current,
					resource_x_payload, sizeof(resource_x_payload))
					!= RESOURCE_X_APPLY_APPLIED
				|| resource_x_current.state != RESOURCE_X_INTENT_SLOT_STAGED
				|| !lms_outbound_resource_x_intent_identity_equal(
					&resource_x_intent, &resource_x_current))
				continue;
			send_payload = resource_x_payload;
			send_payload_len = resource_x_current.payload_bytes;
		} else if (slot.kind == (uint8)CLUSTER_LMS_OUTBOUND_ZERO_BLOCK_REPLY
			|| slot.kind == (uint8)CLUSTER_LMS_OUTBOUND_DIRECT_ZERO_BLOCK_REPLY) {
			if (slot.msg_type != PGRAC_IC_MSG_GCS_BLOCK_REPLY
				|| slot.payload_len != sizeof(GcsBlockReplyHeader)) {
				rc = CLUSTER_IC_SEND_HARD_ERROR;
				goto handle_send_result;
			}
			memset(&zero_reply, 0, sizeof(zero_reply));
			memcpy(&zero_reply.header, slot.payload, sizeof(zero_reply.header));
			if (slot.required_capability == 0) {
				if (zero_reply.header.status != (uint8)GCS_BLOCK_REPLY_DENIED_PENDING_X) {
					rc = CLUSTER_IC_SEND_HARD_ERROR;
					goto handle_send_result;
				}
			} else if (slot.kind == (uint8)CLUSTER_LMS_OUTBOUND_DIRECT_ZERO_BLOCK_REPLY
					   || !lms_outbound_r4_refusal_header_valid(&zero_reply.header)) {
				rc = CLUSTER_IC_SEND_HARD_ERROR;
				goto handle_send_result;
			}
			zero_reply.header.checksum = cluster_gcs_block_compute_checksum(zero_reply.block_data);
			send_payload = &zero_reply;
			send_payload_len = sizeof(zero_reply);
		} else if (slot.kind != (uint8)CLUSTER_LMS_OUTBOUND_FRAME) {
			rc = CLUSTER_IC_SEND_HARD_ERROR;
			goto handle_send_result;
		}

		/* A peer that refused a frame this batch keeps its later frames
		 * queued BEHIND the refused one (per-peer order). */
		if (slot.dest_node_id < CLUSTER_MAX_NODES && peer_blocked[slot.dest_node_id]) {
			lms_outbound_pcm_x_image_ready_note(&slot, "peer-blocked", -2);
			if (resource_x_slot) {
				(void)cluster_pcm_lock_resource_x_grant_intent_hard_rearm_exact(
					&resource_x_intent, lms_outbound_monotonic_us());
				continue;
			}
			Assert(n_retained < (int)lengthof(retained));
			retained[n_retained++] = slot;
			continue;
		}

		/* Revalidate irreversible PCM-X grant authority immediately before
		 * transport admission.  Retaining also blocks later same-peer frames,
		 * preserving the DATA FIFO while unrelated peers keep flowing. */
		if (lms_outbound_pcm_x_grant_held(slot.msg_type)) {
			lms_outbound_pcm_x_image_ready_note(&slot, "grant-held", -3);
			if (slot.dest_node_id < CLUSTER_MAX_NODES)
				peer_blocked[slot.dest_node_id] = true;
			Assert(n_retained < (int)lengthof(retained));
			retained[n_retained++] = slot;
			continue;
		}

		if (slot.msg_type == PGRAC_IC_MSG_GCS_BLOCK_REQUEST
			&& slot.payload_len == sizeof(GcsBlockRequestPayload))
			cluster_gcs_block_lmon_prepare_outbound_request((GcsBlockRequestPayload *)slot.payload,
															(int32)slot.dest_node_id);

		/*
		 * The generic IC send path deliberately treats dest=self as a no-op
		 * DONE.  That is correct for transport diagnostics, but not for a
		 * staged DATA actor: PCM-X frequently maps a tag's resource master to
		 * the requesting node, and its ENQUEUE/ACK must still execute on the
		 * tag-owning LMS worker.  Build the same envelope and dispatch it here,
		 * after dequeue and without the ring lock.  A handler response is
		 * staged back onto this tag's ring, preserving the one-worker FIFO and
		 * avoiding recursive handler execution.
		 */
		if (slot.kind == (uint8)CLUSTER_LMS_OUTBOUND_DIRECT_ZERO_BLOCK_REPLY)
			rc = cluster_gcs_block_send_direct_zero_reply((int32)slot.dest_node_id,
														  &zero_reply.header);
		else if ((int32)slot.dest_node_id == cluster_node_id) {
			ClusterICEnvelope env;

			if (cluster_ic_envelope_build(&env, slot.msg_type, (uint32)cluster_node_id,
										  slot.dest_node_id, send_payload, send_payload_len)
				&& cluster_ic_dispatch_envelope(&env, send_payload, cluster_node_id))
				rc = CLUSTER_IC_SEND_DONE;
			else
				rc = CLUSTER_IC_SEND_HARD_ERROR;
		} else
			rc = cluster_ic_send_envelope(slot.msg_type, (int32)slot.dest_node_id, send_payload,
										  send_payload_len);

	handle_send_result:
		lms_outbound_pcm_x_image_ready_note(&slot, "send-result", (int)rc);
		if (slot.kind == (uint8)CLUSTER_LMS_OUTBOUND_ZERO_BLOCK_REPLY
			|| slot.kind == (uint8)CLUSTER_LMS_OUTBOUND_DIRECT_ZERO_BLOCK_REPLY)
			cluster_gcs_block_note_send_outcome(GCS_BLOCK_SEND_FAMILY_REPLY, rc);
		switch (rc) {
		case CLUSTER_IC_SEND_DONE:
		case CLUSTER_IC_SEND_WOULD_BLOCK:
			/* On the wire or admitted (transport owns a copy). */
			if (resource_x_slot)
				(void)cluster_pcm_lock_resource_x_grant_intent_complete_exact(
					&resource_x_intent);
			sent++;
			break;
		case CLUSTER_IC_SEND_NOT_ADMITTED:
			if (slot.dest_node_id < CLUSTER_MAX_NODES)
				peer_blocked[slot.dest_node_id] = true;
			if (resource_x_slot)
				(void)cluster_pcm_lock_resource_x_grant_intent_hard_rearm_exact(
					&resource_x_intent, lms_outbound_monotonic_us());
			else {
				Assert(n_retained < (int)lengthof(retained));
				retained[n_retained++] = slot;
			}
			cluster_lms_obs_note_outbound_not_admitted(worker_id);
			break;
		case CLUSTER_IC_SEND_HARD_ERROR:
			/* peer down — drop;  requesters retry fail-closed. */
			if (resource_x_slot)
				(void)cluster_pcm_lock_resource_x_grant_intent_hard_rearm_exact(
					&resource_x_intent, lms_outbound_monotonic_us());
			break;
		}
	}

	/* Put retained frames back at the head, original order preserved
	 * (reverse-order head pushes).  count can only have grown from
	 * producers since the dequeues above, so a full ring is possible:
	 * count the drop — the retransmit machinery self-heals, but the S3
	 * gate treats a nonzero delta as a capacity red flag. */
	if (n_retained > 0) {
		int i;

		LWLockAcquire(lock, LW_EXCLUSIVE);
		for (i = n_retained - 1; i >= 0; i--) {
			if (ring->count >= PGRAC_LMS_OUTBOUND_CAPACITY) {
				cluster_lms_obs_note_outbound_requeue_drop(worker_id);
				continue;
			}
			ring->tail
				= (ring->tail + PGRAC_LMS_OUTBOUND_CAPACITY - 1) % PGRAC_LMS_OUTBOUND_CAPACITY;
			ring->ring[ring->tail] = retained[i];
			ring->count++;
		}
		LWLockRelease(lock);
	}

	return sent;
}

uint32
cluster_lms_outbound_depth(int worker_id)
{
	ClusterLmsOutboundState *ring;
	LWLock *lock;
	uint32 depth;

	if (worker_id < 0 || worker_id >= CLUSTER_LMS_MAX_WORKERS)
		return 0;
	if (cluster_lms_outbound_rings == NULL || OB_LOCK(worker_id) == NULL)
		return 0;
	ring = OB_RING(worker_id);
	lock = OB_LOCK(worker_id);
	LWLockAcquire(lock, LW_SHARED);
	depth = ring->count;
	LWLockRelease(lock);
	return depth;
}

#endif /* USE_PGRAC_CLUSTER */
