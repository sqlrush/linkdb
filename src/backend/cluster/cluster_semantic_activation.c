/*-------------------------------------------------------------------------
 *
 * cluster_semantic_activation.c
 *	  Shared two-stage semantic activation framework.
 *
 * This first dependency-light body is deliberately fail-closed.  It gives
 * the exact frozen public types and the real R4A readiness adapter an
 * executable home before any durable, LMON, IC, parser, or positive
 * activation integration is admitted.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "miscadmin.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_epoch_ballot.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_gcs_block_dedup.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_replacement_wire.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_undo_smgr.h"
#include "port/atomics.h"
#include "port/pg_crc32c.h"
#include "storage/ipc.h"
#include "storage/shmem.h"

#define CLUSTER_SEMANTIC_RECORD_MAGIC UINT32_C(0x50475341)
#define CLUSTER_SEMANTIC_RECORD_VERSION 1
#define CLUSTER_SEMANTIC_RECORD_HEADER_LEN 104
#define CLUSTER_SEMANTIC_RECORD_CRC_OFFSET 96
#define CLUSTER_SEMANTIC_ADMISSION_SNAPSHOT_TRIES 3
#define CLUSTER_SEMANTIC_ADMISSION_COUNTER_TRIES 16
#define CLUSTER_SEMANTIC_UTILITY_WAIT_STEP_US 10000L
#define CLUSTER_SEMANTIC_REPLACEMENT_REQUIRED_CAPS                                  \
	(PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1 | PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1     \
	 | PGRAC_IC_HELLO_CAP_CANDIDATE2_CORRECTED_A1_V1                                 \
	 | PGRAC_IC_HELLO_CAP_UNDO_ROOT_DESCRIPTOR_V1)
#define CLUSTER_REPLACEMENT_PHASE3_REQUIRED_CAPS \
	PGRAC_IC_HELLO_CAP_CANDIDATE2_CORRECTED_A1_V1

typedef struct ClusterSemanticRecordSample {
	bool readable;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
} ClusterSemanticRecordSample;

typedef struct ClusterSemanticActivationShmem {
	pg_atomic_uint64 record_cas_request_seq;
	pg_atomic_uint64 record_cas_completion_seq;
	pg_atomic_uint32 record_cas_result;
	pg_atomic_uint32 record_cas_request_kind;
	uint64 record_cas_expected_generation;
	uint64 record_cas_expected_source_feature_bitmap;
	uint8 record_cas_desired_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	pg_atomic_uint64 admission_seq;
	pg_atomic_uint64 active_bits;
	pg_atomic_uint64 record_generation;
	pg_atomic_uint64 formation_epoch;
	pg_atomic_uint32 transition_closed;
	pg_atomic_uint32 inflight[2][64];
} ClusterSemanticActivationShmem;

typedef struct ClusterSemanticActivationUtilityMailboxShmem {
	pg_atomic_uint64 utility_request_seq;
	pg_atomic_uint64 utility_completion_seq;
	pg_atomic_uint32 utility_mailbox_state;
	uint32 utility_action;
	uint64 utility_source_feature_bitmap;
	uint64 utility_target_feature_bitmap;
	uint64 utility_rollback_feature_bitmap;
	uint64 utility_expected_record_generation;
	pg_atomic_uint32 utility_result;
	uint64 utility_result_feature_bit;
	uint64 utility_result_expected_generation;
} ClusterSemanticActivationUtilityMailboxShmem;

StaticAssertDecl(offsetof(ClusterSemanticActivationShmem, record_cas_request_kind) == 20,
				 "semantic authority request kind must occupy prior padding");
StaticAssertDecl(offsetof(ClusterSemanticActivationShmem, admission_seq) == 552,
				 "semantic admission sequence must follow the unchanged CAS mailbox");
StaticAssertDecl(offsetof(ClusterSemanticActivationShmem, active_bits) == 560,
				 "semantic admission active bitmap offset must remain stable");
StaticAssertDecl(offsetof(ClusterSemanticActivationShmem, record_generation) == 568,
				 "semantic admission record generation offset must remain stable");
StaticAssertDecl(offsetof(ClusterSemanticActivationShmem, formation_epoch) == 576,
				 "semantic admission formation offset must remain stable");
StaticAssertDecl(offsetof(ClusterSemanticActivationShmem, transition_closed) == 584,
				 "semantic admission closed flag offset must remain stable");
StaticAssertDecl(offsetof(ClusterSemanticActivationShmem, inflight) == 588,
				 "semantic admission inflight offset must remain stable");
StaticAssertDecl(sizeof(ClusterSemanticActivationShmem) == 1104,
				 "semantic activation shared gate must retain its frozen layout");
StaticAssertDecl(offsetof(ClusterSemanticActivationUtilityMailboxShmem,
					  utility_request_seq) == 0,
				 "semantic utility request sequence offset must remain stable");
StaticAssertDecl(offsetof(ClusterSemanticActivationUtilityMailboxShmem,
					  utility_mailbox_state) == 16,
				 "semantic utility mailbox state offset must remain stable");
StaticAssertDecl(offsetof(ClusterSemanticActivationUtilityMailboxShmem,
					  utility_expected_record_generation) == 48,
				 "semantic utility expected generation offset must remain stable");
StaticAssertDecl(offsetof(ClusterSemanticActivationUtilityMailboxShmem,
					  utility_result_feature_bit) == 64,
				 "semantic utility refusal feature offset must remain stable");
StaticAssertDecl(sizeof(ClusterSemanticActivationUtilityMailboxShmem) == 80,
				 "semantic utility mailbox must retain its natural layout");

static ClusterSemanticActivationShmem *SemanticActivationShmem = NULL;
static ClusterSemanticActivationUtilityMailboxShmem
	*SemanticActivationUtilityMailbox = NULL;
static uint32 semantic_activation_local_inflight[2][64];
static int semantic_activation_exit_hook_pid;
static uint64 semantic_activation_lmon_record_read_seq;
static uint64 semantic_activation_lmon_pgrd_request_seq;
static uint64 semantic_activation_lmon_pgrd_utility_request_seq;
static ClusterSemanticFormationBinding semantic_activation_lmon_pgrd_formation;
static uint64 semantic_activation_lmon_pgrd_read_request_seq;
static uint64 semantic_activation_lmon_pgrd_read_utility_request_seq;
static ClusterSemanticFormationBinding semantic_activation_lmon_pgrd_read_formation;

static bool semantic_activation_record_cas_mailbox_submit(
	uint64 expected_generation, uint64 expected_source_feature_bitmap,
	const uint8 desired_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES], uint64 *out_request_seq)
	pg_attribute_unused();
static bool semantic_activation_record_cas_mailbox_poll_completion(
	uint64 request_seq, ClusterSemanticActivationResult *out_result) pg_attribute_unused();
static bool semantic_activation_record_read_mailbox_submit(
	uint64 *out_request_seq);
static bool semantic_activation_record_read_mailbox_poll_completion(
	uint64 request_seq, ClusterSemanticActivationReadCompletion *out);
static bool semantic_activation_authority_mailbox_submit(
	ClusterSemanticAuthorityRequestKind request_kind, uint64 expected_generation,
	uint64 expected_source_feature_bitmap,
	const uint8 desired_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES],
	uint64 *out_request_seq);
static bool semantic_activation_authority_mailbox_complete(
	ClusterSemanticAuthorityRequestKind request_kind, uint64 request_seq,
	ClusterSemanticActivationResult result);
static bool semantic_activation_authority_mailbox_completion_matches(
	ClusterSemanticAuthorityRequestKind request_kind, uint64 request_seq);
static bool semantic_activation_authority_mailbox_poll_completion(
	ClusterSemanticAuthorityRequestKind request_kind, uint64 request_seq,
	ClusterSemanticActivationResult *out_result);
static bool semantic_activation_authority_request_formation_binding(
	ClusterSemanticAuthorityRequestKind request_kind,
	ClusterSemanticFormationBinding *out);
static bool semantic_activation_qvotec_formation_matches(
	const ClusterSemanticFormationBinding *formation);

typedef enum SemanticActivationUtilityMailboxState {
	SEMANTIC_ACTIVATION_UTILITY_MAILBOX_IDLE = 0,
	SEMANTIC_ACTIVATION_UTILITY_MAILBOX_WRITING = 1,
	SEMANTIC_ACTIVATION_UTILITY_MAILBOX_PENDING = 2,
	SEMANTIC_ACTIVATION_UTILITY_MAILBOX_COMPLETE = 3
} SemanticActivationUtilityMailboxState;

typedef struct SemanticActivationUtilityRequest {
	uint64 request_seq;
	uint64 source_feature_bitmap;
	uint64 target_feature_bitmap;
	uint64 rollback_feature_bitmap;
	uint64 expected_record_generation;
	ClusterSemanticActivationAction action;
} SemanticActivationUtilityRequest;

static bool semantic_activation_utility_mailbox_submit(
	ClusterSemanticActivationAction action, uint64 source_feature_bitmap,
	uint64 target_feature_bitmap, uint64 rollback_feature_bitmap,
	uint64 expected_record_generation, uint64 *out_request_seq);
static bool semantic_activation_utility_mailbox_poll(
	SemanticActivationUtilityRequest *out);
static bool semantic_activation_utility_mailbox_complete(
	uint64 request_seq, ClusterSemanticActivationResult result,
	uint64 feature_bit, uint64 expected_generation);
static bool semantic_activation_utility_mailbox_poll_completion(
	uint64 request_seq, ClusterSemanticActivationRefusal *out_refusal);
static ClusterSemanticActivationResult
semantic_activation_utility_mailbox_wait(
	uint64 request_seq, ClusterSemanticActivationRefusal *out_refusal);

typedef enum SemanticActivationState {
	SEMANTIC_ACTIVATION_STATE_INVALID = -1,
	SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN = 0,
	SEMANTIC_ACTIVATION_STATE_SOURCE_ADMISSION_STOPPED = 1,
	SEMANTIC_ACTIVATION_STATE_SOURCE_DRAIN_OR_RECOVERY = 2,
	SEMANTIC_ACTIVATION_STATE_SOURCE_LOGICAL_ZERO = 3,
	SEMANTIC_ACTIVATION_STATE_ORDERED_TRANSPORT_BARRIER = 4,
	SEMANTIC_ACTIVATION_STATE_TRANSPORT_BACKED_ZERO = 5,
	SEMANTIC_ACTIVATION_STATE_EPOCH_CAPABILITY_BARRIER = 6,
	SEMANTIC_ACTIVATION_STATE_TARGET_STAGED = 7,
	SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED = 8,
	SEMANTIC_ACTIVATION_STATE_TARGET_OPEN = 9
} SemanticActivationState;

typedef enum SemanticActivationCallbackKind {
	SEMANTIC_ACTIVATION_CALLBACK_NONE = 0,
	SEMANTIC_ACTIVATION_CALLBACK_CLOSE_SOURCE,
	SEMANTIC_ACTIVATION_CALLBACK_LOGICAL_ZERO,
	SEMANTIC_ACTIVATION_CALLBACK_TRANSPORT_BARRIER,
	SEMANTIC_ACTIVATION_CALLBACK_TRANSPORT_ZERO,
	SEMANTIC_ACTIVATION_CALLBACK_EPOCH_CAPABILITY_BARRIER,
	SEMANTIC_ACTIVATION_CALLBACK_PREPARE_TARGET,
	SEMANTIC_ACTIVATION_CALLBACK_APPLY_TARGET_CLOSED,
	SEMANTIC_ACTIVATION_CALLBACK_OPEN_TARGET
} SemanticActivationCallbackKind;

typedef struct SemanticActivationFailurePolicy {
	SemanticActivationState target;
	bool admission_closed_until_source_open;
	bool revert_source_closed;
} SemanticActivationFailurePolicy;

typedef struct SemanticActivationAckTuple {
	uint32 node_id;
	uint64 boot_id;
	uint64 admitted_incarnation;
	uint64 control_connection_generation;
	uint32 capability_word;
	uint64 capability_generation;
	uint64 transition_epoch;
	uint64 record_generation;
} SemanticActivationAckTuple;

typedef struct ClusterSemanticActivationAckTableV1 {
	pg_atomic_uint64 publication_seq;
	uint32 stage;
	uint32 flags;
	uint32 coordinator_node;
	uint32 reserved;
	uint64 round_nonce;
	uint64 expected_members_lo;
	uint64 expected_members_hi;
	uint64 observed_members_lo;
	uint64 observed_members_hi;
	uint64 transition_epoch;
	uint64 record_generation;
	uint64 source_feature_bitmap;
	uint64 target_feature_bitmap;
	uint64 rollback_feature_bitmap;
	uint64 capability_sample_digest;
	SemanticActivationAckTuple expected[CLUSTER_MAX_NODES];
	SemanticActivationAckTuple observed[CLUSTER_MAX_NODES];
} ClusterSemanticActivationAckTableV1;

typedef struct SemanticActivationAckIngressItem {
	ClusterSemanticActivationAckWireV1 message;
	int32 authenticated_source_node_id;
	int32 local_receiver_node_id;
	uint32 sampled_capability_word;
	uint32 sampled_capability_generation;
} SemanticActivationAckIngressItem;

typedef struct SemanticActivationAckIngress {
	uint64 producer_seq;
	uint64 consumer_seq;
	SemanticActivationAckIngressItem
		items[CLUSTER_SEMANTIC_ACTIVATION_ACK_INGRESS_CAPACITY];
} SemanticActivationAckIngress;

typedef enum SemanticActivationAckIngressResult {
	SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED = 0,
	SEMANTIC_ACTIVATION_ACK_INGRESS_ENQUEUED,
	SEMANTIC_ACTIVATION_ACK_INGRESS_FULL
} SemanticActivationAckIngressResult;

typedef enum SemanticActivationAckConsumeResult {
	SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED = 0,
	SEMANTIC_ACTIVATION_ACK_CONSUME_STALE,
	SEMANTIC_ACTIVATION_ACK_CONSUME_APPLIED,
	SEMANTIC_ACTIVATION_ACK_CONSUME_DUPLICATE,
	SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED
} SemanticActivationAckConsumeResult;

static SemanticActivationAckIngress semantic_activation_ack_local_ingress;
static uint64 semantic_activation_ack_ingress_result_count[3];

StaticAssertDecl(sizeof(SemanticActivationAckTuple)
				 == CLUSTER_SEMANTIC_ACTIVATION_ACK_TUPLE_BYTES,
				 "semantic activation ACK tuple must remain 64 bytes");
StaticAssertDecl(sizeof(ClusterSemanticActivationAckTableV1)
				 == CLUSTER_SEMANTIC_ACTIVATION_ACK_TABLE_BYTES,
				 "semantic activation ACK table must remain 16496 bytes");
StaticAssertDecl(sizeof(SemanticActivationAckIngressItem) == 136,
				 "semantic activation ACK ingress item must remain 136 bytes");
StaticAssertDecl(sizeof(SemanticActivationAckIngress) == 34832,
				 "semantic activation ACK ingress must remain 34832 bytes");

static ClusterSemanticActivationAckTableV1 *SemanticActivationAckTable = NULL;

static void semantic_activation_ack_ingress_init(
	SemanticActivationAckIngress *ingress) pg_attribute_unused();
static uint32 semantic_activation_ack_ingress_pending(
	const SemanticActivationAckIngress *ingress) pg_attribute_unused();
static bool semantic_activation_ack_ingress_push(
	SemanticActivationAckIngress *ingress,
	const SemanticActivationAckIngressItem *item) pg_attribute_unused();
static bool semantic_activation_ack_ingress_poll(
	SemanticActivationAckIngress *ingress,
	SemanticActivationAckIngressItem *out) pg_attribute_unused();
static SemanticActivationAckIngressResult
semantic_activation_ack_ingress_receive(
	SemanticActivationAckIngress *ingress, const ClusterICEnvelope *env,
	const void *payload, uint32 payload_length,
	int32 local_receiver_node_id, uint64 current_epoch) pg_attribute_unused();
static bool semantic_activation_ack_remote_tuple(
	const SemanticActivationAckIngressItem *item,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	SemanticActivationAckTuple *out) pg_attribute_unused();
static bool semantic_activation_ack_self_tuple(
	int32 local_node_id, uint32 local_capability_word,
	uint64 transition_epoch, uint64 record_generation,
	SemanticActivationAckTuple *out) pg_attribute_unused();
static bool semantic_activation_ack_current_authority(
	int32 local_node_id, uint64 *out_members_lo, uint64 *out_members_hi,
	uint64 *out_formation_epoch,
	int32 *out_coordinator_node) pg_attribute_unused();

static void
semantic_activation_ack_ingress_init(SemanticActivationAckIngress *ingress)
{
	if (ingress != NULL)
		memset(ingress, 0, sizeof(*ingress));
}

static uint32
semantic_activation_ack_ingress_pending(
	const SemanticActivationAckIngress *ingress)
{
	uint64 pending;

	if (ingress == NULL)
		return 0;
	pending = ingress->producer_seq - ingress->consumer_seq;
	if (pending > CLUSTER_SEMANTIC_ACTIVATION_ACK_INGRESS_CAPACITY)
		return 0;
	return (uint32)pending;
}

static bool
semantic_activation_ack_ingress_push(
	SemanticActivationAckIngress *ingress,
	const SemanticActivationAckIngressItem *item)
{
	uint64 pending;

	if (ingress == NULL || item == NULL)
		return false;
	pending = ingress->producer_seq - ingress->consumer_seq;
	if (pending >= CLUSTER_SEMANTIC_ACTIVATION_ACK_INGRESS_CAPACITY)
		return false;
	ingress->items[ingress->producer_seq
				   % CLUSTER_SEMANTIC_ACTIVATION_ACK_INGRESS_CAPACITY] = *item;
	ingress->producer_seq++;
	return true;
}

static bool
semantic_activation_ack_ingress_poll(
	SemanticActivationAckIngress *ingress,
	SemanticActivationAckIngressItem *out)
{
	uint64 pending;

	if (ingress == NULL || out == NULL)
		return false;
	pending = ingress->producer_seq - ingress->consumer_seq;
	if (pending == 0
		|| pending > CLUSTER_SEMANTIC_ACTIVATION_ACK_INGRESS_CAPACITY)
		return false;
	*out = ingress->items[ingress->consumer_seq
				   % CLUSTER_SEMANTIC_ACTIVATION_ACK_INGRESS_CAPACITY];
	ingress->consumer_seq++;
	return true;
}

static SemanticActivationAckIngressResult
semantic_activation_ack_ingress_receive(
	SemanticActivationAckIngress *ingress, const ClusterICEnvelope *env,
	const void *payload, uint32 payload_length,
	int32 local_receiver_node_id, uint64 current_epoch)
{
	ClusterSemanticActivationAckWireV1 message;
	SemanticActivationAckIngressItem item;
	uint32 capability_word;
	uint32 capability_generation;
	uint64 pending;
	bool local_is_admitted;
	int32 authenticated_source_node_id;

	if (ingress == NULL || env == NULL || payload == NULL
		|| payload_length != CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES
		|| env->payload_length != CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES
		|| env->msg_type != PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1
		|| local_receiver_node_id < 0
		|| local_receiver_node_id >= CLUSTER_MAX_NODES
		|| env->source_node_id >= CLUSTER_MAX_NODES
		|| env->source_node_id == (uint32)local_receiver_node_id
		|| env->dest_node_id != (uint32)local_receiver_node_id
		|| env->epoch != current_epoch
		|| !cluster_semantic_activation_ack_wire_decode(
			(const uint8 *)payload, &message)
		|| message.transition_epoch != env->epoch)
		return SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED;

	authenticated_source_node_id = (int32)env->source_node_id;
	local_is_admitted
		= local_receiver_node_id < 64
			  ? (message.admitted_members_lo
				 & (UINT64_C(1) << local_receiver_node_id)) != 0
			  : (message.admitted_members_hi
				 & (UINT64_C(1) << (local_receiver_node_id - 64))) != 0;
	if (!local_is_admitted)
		return SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED;

	if (message.kind == CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST) {
		if (message.coordinator_node != env->source_node_id
			|| message.member_node != (uint32)local_receiver_node_id)
			return SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED;
	} else if (message.kind == CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK) {
		if (message.member_node != env->source_node_id)
			return SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED;
		if (message.result == CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK) {
			if (message.boot_id != message.admitted_incarnation)
				return SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED;
		} else if (message.result
				   == CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REFUSED) {
			if (message.coordinator_node != (uint32)local_receiver_node_id)
				return SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED;
		} else
			return SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED;
	} else
		return SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED;

	if (!cluster_sf_peer_capability_word_sample(
			authenticated_source_node_id,
			CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS,
			&capability_word, &capability_generation)
		|| capability_generation == 0
		|| (message.kind == CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK
			&& message.result == CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK
			&& message.capability_word != capability_word))
		return SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED;

	pending = ingress->producer_seq - ingress->consumer_seq;
	if (pending > CLUSTER_SEMANTIC_ACTIVATION_ACK_INGRESS_CAPACITY)
		return SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED;
	if (pending == CLUSTER_SEMANTIC_ACTIVATION_ACK_INGRESS_CAPACITY)
		return SEMANTIC_ACTIVATION_ACK_INGRESS_FULL;

	memset(&item, 0, sizeof(item));
	item.message = message;
	item.authenticated_source_node_id = authenticated_source_node_id;
	item.local_receiver_node_id = local_receiver_node_id;
	item.sampled_capability_word = capability_word;
	item.sampled_capability_generation = capability_generation;
	if (!semantic_activation_ack_ingress_push(ingress, &item))
		return SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED;
	return SEMANTIC_ACTIVATION_ACK_INGRESS_ENQUEUED;
}

static bool
semantic_activation_ack_remote_tuple(
	const SemanticActivationAckIngressItem *item,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	SemanticActivationAckTuple *out)
{
	SemanticActivationAckTuple tuple;
	const ClusterSemanticActivationAckWireV1 *message;
	int32 source_node;
	bool source_is_admitted;
	bool coordinator_is_admitted;

	if (item == NULL || out == NULL
		|| current_coordinator_node < 0
		|| current_coordinator_node >= CLUSTER_MAX_NODES
		|| (current_members_lo == 0 && current_members_hi == 0))
		return false;
	message = &item->message;
	source_node = item->authenticated_source_node_id;
	if (source_node < 0 || source_node >= CLUSTER_MAX_NODES
		|| item->local_receiver_node_id != current_coordinator_node
		|| message->kind != CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK
		|| message->result != CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK
		|| message->coordinator_node != (uint32)current_coordinator_node
		|| message->member_node != (uint32)source_node
		|| message->admitted_members_lo != current_members_lo
		|| message->admitted_members_hi != current_members_hi
		|| message->transition_epoch != current_epoch
		|| message->boot_id == 0
		|| message->boot_id != message->admitted_incarnation
		|| item->sampled_capability_generation == 0
		|| item->sampled_capability_word != message->capability_word
		|| (item->sampled_capability_word
			& CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS)
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS)
		return false;

	source_is_admitted
		= source_node < 64
			  ? (current_members_lo & (UINT64_C(1) << source_node)) != 0
			  : (current_members_hi
				 & (UINT64_C(1) << (source_node - 64))) != 0;
	coordinator_is_admitted
		= current_coordinator_node < 64
			  ? (current_members_lo
				 & (UINT64_C(1) << current_coordinator_node)) != 0
			  : (current_members_hi
				 & (UINT64_C(1) << (current_coordinator_node - 64))) != 0;
	if (!source_is_admitted || !coordinator_is_admitted
		|| cluster_membership_get_state(source_node) != CLUSTER_MEMBER_MEMBER
		|| cluster_membership_get_last_admitted_incarnation(source_node)
		   != message->admitted_incarnation
		|| !cluster_sf_peer_capability_generation_matches(
			source_node, CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS,
			item->sampled_capability_generation))
		return false;

	memset(&tuple, 0, sizeof(tuple));
	tuple.node_id = (uint32)source_node;
	tuple.boot_id = message->boot_id;
	tuple.admitted_incarnation = message->admitted_incarnation;
	tuple.control_connection_generation
		= (uint64)item->sampled_capability_generation;
	tuple.capability_word = item->sampled_capability_word;
	tuple.capability_generation
		= (uint64)item->sampled_capability_generation;
	tuple.transition_epoch = message->transition_epoch;
	tuple.record_generation = message->record_generation;
	*out = tuple;
	return true;
}

static bool
semantic_activation_ack_self_tuple(
	int32 local_node_id, uint32 local_capability_word,
	uint64 transition_epoch, uint64 record_generation,
	SemanticActivationAckTuple *out)
{
	SemanticActivationAckTuple tuple;
	uint64 self_incarnation;

	if (out == NULL || local_node_id < 0
		|| local_node_id >= CLUSTER_MAX_NODES
		|| record_generation == 0
		|| (local_capability_word
			& CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS)
		   != CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS)
		return false;
	self_incarnation = cluster_qvotec_get_self_incarnation();
	if (self_incarnation == 0
		|| cluster_membership_get_state(local_node_id)
		   != CLUSTER_MEMBER_MEMBER
		|| cluster_membership_get_last_admitted_incarnation(local_node_id)
		   != self_incarnation)
		return false;

	memset(&tuple, 0, sizeof(tuple));
	tuple.node_id = (uint32)local_node_id;
	tuple.boot_id = self_incarnation;
	tuple.admitted_incarnation = self_incarnation;
	tuple.control_connection_generation = self_incarnation;
	tuple.capability_word = local_capability_word;
	tuple.capability_generation = self_incarnation;
	tuple.transition_epoch = transition_epoch;
	tuple.record_generation = record_generation;
	*out = tuple;
	return true;
}

static bool
semantic_activation_ack_current_authority(
	int32 local_node_id, uint64 *out_members_lo, uint64 *out_members_hi,
	uint64 *out_formation_epoch, int32 *out_coordinator_node)
{
	uint64 members_lo;
	uint64 members_hi;
	uint64 formation_epoch;
	uint64 current_epoch;
	int32 coordinator_node = -1;
	int32 node;
	bool local_is_member;

	if (out_members_lo == NULL || out_members_hi == NULL
		|| out_formation_epoch == NULL || out_coordinator_node == NULL)
		return false;
	*out_members_lo = 0;
	*out_members_hi = 0;
	*out_formation_epoch = 0;
	*out_coordinator_node = -1;
	if (local_node_id < 0 || local_node_id >= CLUSTER_MAX_NODES
		|| !cluster_qvotec_in_quorum()
		|| !cluster_reconfig_lmon_snapshot_admitted_membership(
			&members_lo, &members_hi, &formation_epoch)
		|| (members_lo == 0 && members_hi == 0))
		return false;

	current_epoch = cluster_epoch_get_current();
	local_is_member
		= local_node_id < 64
			  ? (members_lo & (UINT64_C(1) << local_node_id)) != 0
			  : (members_hi
				 & (UINT64_C(1) << (local_node_id - 64))) != 0;
	if (!local_is_member || formation_epoch != current_epoch
		|| cluster_membership_get_state(local_node_id)
		   != CLUSTER_MEMBER_MEMBER)
		return false;

	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		bool is_member
			= node < 64
				  ? (members_lo & (UINT64_C(1) << node)) != 0
				  : (members_hi & (UINT64_C(1) << (node - 64))) != 0;

		if (is_member) {
			coordinator_node = node;
			break;
		}
	}
	if (coordinator_node < 0
		|| cluster_membership_get_state(coordinator_node)
		   != CLUSTER_MEMBER_MEMBER
		|| !cluster_qvotec_in_quorum()
		|| cluster_epoch_get_current() != current_epoch)
		return false;

	*out_members_lo = members_lo;
	*out_members_hi = members_hi;
	*out_formation_epoch = formation_epoch;
	*out_coordinator_node = coordinator_node;
	return true;
}

void
cluster_semantic_activation_ack_handler(
	const ClusterICEnvelope *env, const void *payload)
{
	SemanticActivationAckIngressResult result;
	uint32 payload_length = env != NULL ? env->payload_length : 0;

	result = semantic_activation_ack_ingress_receive(
		&semantic_activation_ack_local_ingress, env, payload,
		payload_length, cluster_node_id, cluster_epoch_get_current());
	if (result < SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED
		|| result > SEMANTIC_ACTIVATION_ACK_INGRESS_FULL)
		result = SEMANTIC_ACTIVATION_ACK_INGRESS_REJECTED;
	if (semantic_activation_ack_ingress_result_count[result] != UINT64_MAX)
		semantic_activation_ack_ingress_result_count[result]++;
}

static bool semantic_activation_ack_table_snapshot(
	ClusterSemanticActivationAckTableV1 *out) pg_attribute_unused();

static bool
semantic_activation_ack_table_snapshot(ClusterSemanticActivationAckTableV1 *out)
{
	ClusterSemanticActivationAckTableV1 candidate;
	uint64 seq_before;
	uint64 seq_after;
	int attempt;

	if (SemanticActivationAckTable == NULL || out == NULL)
		return false;
	for (attempt = 0; attempt < 3; attempt++) {
		seq_before = pg_atomic_read_u64(
			&SemanticActivationAckTable->publication_seq);
		if ((seq_before & UINT64_C(1)) != 0)
			continue;
		pg_read_barrier();
		memcpy(&candidate, SemanticActivationAckTable, sizeof(candidate));
		pg_read_barrier();
		seq_after = pg_atomic_read_u64(
			&SemanticActivationAckTable->publication_seq);
		if (seq_before == seq_after
			&& (seq_after & UINT64_C(1)) == 0) {
			memcpy(out, &candidate, sizeof(candidate));
			return true;
		}
	}
	return false;
}

static bool semantic_activation_ack_table_publish(
	const ClusterSemanticActivationAckTableV1 *image) pg_attribute_unused();
static bool semantic_activation_ack_matches(
	const SemanticActivationAckTuple *observed,
	const SemanticActivationAckTuple *expected);
static bool semantic_activation_full_ack_table_matches(
	const SemanticActivationAckTuple observed[CLUSTER_MAX_NODES],
	uint64 observed_members_lo, uint64 observed_members_hi,
	const SemanticActivationAckTuple expected[CLUSTER_MAX_NODES],
	uint64 expected_members_lo, uint64 expected_members_hi);
static SemanticActivationAckConsumeResult
semantic_activation_ack_lmon_apply_item(
	const SemanticActivationAckIngressItem *item,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch,
	int32 current_coordinator_node) pg_attribute_unused();

static bool
semantic_activation_ack_table_publish(
	const ClusterSemanticActivationAckTableV1 *image)
{
	const Size payload_offset
		= offsetof(ClusterSemanticActivationAckTableV1, stage);
	uint64 seq;

	if (SemanticActivationAckTable == NULL || image == NULL)
		return false;
	seq = pg_atomic_read_u64(&SemanticActivationAckTable->publication_seq);
	if ((seq & UINT64_C(1)) != 0 || seq > UINT64_MAX - 2)
		return false;

	pg_atomic_write_u64(&SemanticActivationAckTable->publication_seq, seq + 1);
	pg_write_barrier();
	memcpy((uint8 *)SemanticActivationAckTable + payload_offset,
		   (const uint8 *)image + payload_offset,
		   sizeof(*SemanticActivationAckTable) - payload_offset);
	pg_write_barrier();
	pg_atomic_write_u64(&SemanticActivationAckTable->publication_seq, seq + 2);
	return true;
}

static bool
semantic_activation_ack_member_present(
	uint64 members_lo, uint64 members_hi, int32 node_id)
{
	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return false;
	return node_id < 64
			   ? (members_lo & (UINT64_C(1) << node_id)) != 0
			   : (members_hi & (UINT64_C(1) << (node_id - 64))) != 0;
}

static bool
semantic_activation_ack_tuple_structural(
	const SemanticActivationAckTuple *tuple, int32 node_id,
	uint64 transition_epoch, uint64 record_generation)
{
	return tuple != NULL && node_id >= 0 && node_id < CLUSTER_MAX_NODES
		   && tuple->node_id == (uint32)node_id
		   && tuple->boot_id != 0
		   && tuple->boot_id == tuple->admitted_incarnation
		   && tuple->control_connection_generation != 0
		   && (tuple->capability_word
			   & CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS)
			  == CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS
		   && tuple->capability_generation != 0
		   && tuple->transition_epoch == transition_epoch
		   && tuple->record_generation == record_generation;
}

static bool
semantic_activation_ack_image_structural(
	const ClusterSemanticActivationAckTableV1 *image)
{
	int node;

	if (image == NULL
		|| (image->expected_members_lo == 0
			&& image->expected_members_hi == 0)
		|| image->observed_members_lo != image->expected_members_lo
		|| image->observed_members_hi != image->expected_members_hi)
		return false;
	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		if (!semantic_activation_ack_member_present(
				image->expected_members_lo, image->expected_members_hi, node))
			continue;
		if (!semantic_activation_ack_tuple_structural(
				&image->observed[node], node, image->transition_epoch,
				image->record_generation))
			return false;
	}
	return true;
}

static bool
semantic_activation_ack_image_invalidate(
	ClusterSemanticActivationAckTableV1 *image)
{
	if (image == NULL)
		return false;
	image->flags = 0;
	image->expected_members_lo = 0;
	image->expected_members_hi = 0;
	image->observed_members_lo = 0;
	image->observed_members_hi = 0;
	memset(image->expected, 0, sizeof(image->expected));
	memset(image->observed, 0, sizeof(image->observed));
	return semantic_activation_ack_table_publish(image);
}

static SemanticActivationAckConsumeResult
semantic_activation_ack_lmon_apply_item(
	const SemanticActivationAckIngressItem *item,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node)
{
	ClusterSemanticActivationAckTableV1 image;
	SemanticActivationAckTuple tuple;
	const ClusterSemanticActivationAckWireV1 *message;
	uint64 member_bit;
	bool member_was_observed;
	int32 source_node;

	if (item == NULL
		|| !semantic_activation_ack_table_snapshot(&image))
		return SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
	if ((image.expected_members_lo == 0
		 && image.expected_members_hi == 0)
		|| image.stage < CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
		|| image.stage > CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED
		|| image.round_nonce == 0)
		return SEMANTIC_ACTIVATION_ACK_CONSUME_STALE;
	if (image.expected_members_lo != current_members_lo
		|| image.expected_members_hi != current_members_hi
		|| image.transition_epoch != current_epoch
		|| image.coordinator_node != (uint32)current_coordinator_node) {
		return semantic_activation_ack_image_invalidate(&image)
				   ? SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED
				   : SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
	}

	message = &item->message;
	if (message->kind != CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK
		|| message->stage != image.stage
		|| message->coordinator_node != image.coordinator_node
		|| message->round_nonce != image.round_nonce
		|| message->admitted_members_lo != image.expected_members_lo
		|| message->admitted_members_hi != image.expected_members_hi
		|| message->transition_epoch != image.transition_epoch
		|| message->record_generation != image.record_generation
		|| message->source_feature_bitmap != image.source_feature_bitmap
		|| message->target_feature_bitmap != image.target_feature_bitmap
		|| message->rollback_feature_bitmap != image.rollback_feature_bitmap
		|| message->capability_sample_digest
		   != image.capability_sample_digest)
		return SEMANTIC_ACTIVATION_ACK_CONSUME_STALE;
	if (message->result == CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REFUSED) {
		return semantic_activation_ack_image_invalidate(&image)
				   ? SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED
				   : SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
	}
	if (message->result != CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK
		|| !semantic_activation_ack_remote_tuple(
			item, current_members_lo, current_members_hi, current_epoch,
			current_coordinator_node, &tuple)) {
		return semantic_activation_ack_image_invalidate(&image)
				   ? SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED
				   : SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
	}

	source_node = item->authenticated_source_node_id;
	if (!semantic_activation_ack_member_present(
			image.expected_members_lo, image.expected_members_hi, source_node)) {
		return semantic_activation_ack_image_invalidate(&image)
				   ? SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED
				   : SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
	}
	member_bit = UINT64_C(1) << (source_node < 64 ? source_node
											: source_node - 64);
	member_was_observed
		= source_node < 64
			  ? (image.observed_members_lo & member_bit) != 0
			  : (image.observed_members_hi & member_bit) != 0;
	if (member_was_observed) {
		if (semantic_activation_ack_matches(
				&image.observed[source_node], &tuple))
			return SEMANTIC_ACTIVATION_ACK_CONSUME_DUPLICATE;
		return semantic_activation_ack_image_invalidate(&image)
				   ? SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED
				   : SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
	}
	if (image.stage != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
		&& ((image.flags
			  & CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID) == 0
			|| !semantic_activation_ack_matches(
				&image.expected[source_node], &tuple))) {
		return semantic_activation_ack_image_invalidate(&image)
				   ? SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED
				   : SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
	}

	image.observed[source_node] = tuple;
	if (source_node < 64)
		image.observed_members_lo |= member_bit;
	else
		image.observed_members_hi |= member_bit;
	if (image.observed_members_lo == image.expected_members_lo
		&& image.observed_members_hi == image.expected_members_hi) {
		if (image.stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE) {
			if (!semantic_activation_ack_image_structural(&image)) {
				return semantic_activation_ack_image_invalidate(&image)
						   ? SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED
						   : SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
			}
			memcpy(image.expected, image.observed, sizeof(image.expected));
			image.flags
				= CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
				  | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
		} else if (semantic_activation_full_ack_table_matches(
					   image.observed, image.observed_members_lo,
					   image.observed_members_hi, image.expected,
					   image.expected_members_lo,
					   image.expected_members_hi))
			image.flags |= CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;
		else {
			return semantic_activation_ack_image_invalidate(&image)
					   ? SEMANTIC_ACTIVATION_ACK_CONSUME_INVALIDATED
					   : SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
		}
	} else
		image.flags &= ~CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE;

	return semantic_activation_ack_table_publish(&image)
			   ? SEMANTIC_ACTIVATION_ACK_CONSUME_APPLIED
			   : SEMANTIC_ACTIVATION_ACK_CONSUME_REJECTED;
}

static void
semantic_activation_ack_lmon_invalidate_active(void)
{
	ClusterSemanticActivationAckTableV1 image;

	if (!semantic_activation_ack_table_snapshot(&image)
		|| (image.expected_members_lo == 0
			&& image.expected_members_hi == 0))
		return;
	(void)semantic_activation_ack_image_invalidate(&image);
}

static void
semantic_activation_ack_lmon_revalidate_active(
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node)
{
	ClusterSemanticActivationAckTableV1 image;

	if (!semantic_activation_ack_table_snapshot(&image)
		|| (image.expected_members_lo == 0
			&& image.expected_members_hi == 0))
		return;
	if (image.expected_members_lo != current_members_lo
		|| image.expected_members_hi != current_members_hi
		|| image.transition_epoch != current_epoch
		|| image.coordinator_node != (uint32)current_coordinator_node)
		(void)semantic_activation_ack_image_invalidate(&image);
}

static void
semantic_activation_ack_lmon_drain(void)
{
	SemanticActivationAckIngressItem item;
	uint64 current_members_lo;
	uint64 current_members_hi;
	uint64 current_epoch;
	uint64 publication_seq;
	int32 current_coordinator_node;
	uint32 consumed = 0;

	if (semantic_activation_ack_ingress_pending(
			&semantic_activation_ack_local_ingress) == 0) {
		if (SemanticActivationAckTable == NULL)
			return;
		publication_seq = pg_atomic_read_u64(
			&SemanticActivationAckTable->publication_seq);
		if ((publication_seq & UINT64_C(1)) == 0
			&& SemanticActivationAckTable->expected_members_lo == 0
			&& SemanticActivationAckTable->expected_members_hi == 0)
			return;
	}
	if (!semantic_activation_ack_current_authority(
			cluster_node_id, &current_members_lo, &current_members_hi,
			&current_epoch, &current_coordinator_node)) {
		semantic_activation_ack_lmon_invalidate_active();
		while (consumed < CLUSTER_SEMANTIC_ACTIVATION_ACK_INGRESS_CAPACITY
			   && semantic_activation_ack_ingress_poll(
				   &semantic_activation_ack_local_ingress, &item))
			consumed++;
		return;
	}
	semantic_activation_ack_lmon_revalidate_active(
		current_members_lo, current_members_hi, current_epoch,
		current_coordinator_node);

	while (consumed < CLUSTER_SEMANTIC_ACTIVATION_ACK_INGRESS_CAPACITY
		   && semantic_activation_ack_ingress_poll(
			   &semantic_activation_ack_local_ingress, &item)) {
		consumed++;
		if (!semantic_activation_ack_current_authority(
				cluster_node_id, &current_members_lo, &current_members_hi,
				&current_epoch, &current_coordinator_node)) {
			semantic_activation_ack_lmon_invalidate_active();
			continue;
		}
		if (item.message.kind == CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK)
			(void)semantic_activation_ack_lmon_apply_item(
				&item, current_members_lo, current_members_hi,
				current_epoch, current_coordinator_node);
	}
}

typedef enum SemanticActivationHeldLocks {
	SEMANTIC_ACTIVATION_HELD_NONE = 0,
	SEMANTIC_ACTIVATION_HELD_RESOURCE = UINT32_C(1),
	SEMANTIC_ACTIVATION_HELD_BUFFER = UINT32_C(2),
	SEMANTIC_ACTIVATION_HELD_SLRU = UINT32_C(4),
	SEMANTIC_ACTIVATION_HELD_UNDO_IO = UINT32_C(8),
	SEMANTIC_ACTIVATION_HELD_IC_DISPATCH = UINT32_C(16),
	SEMANTIC_ACTIVATION_HELD_ALL_FORBIDDEN = UINT32_C(31)
} SemanticActivationHeldLocks;

typedef enum SemanticActivationWaitEdge {
	SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON = 0,
	SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC = 1,
	SEMANTIC_ACTIVATION_WAIT_LMON_TO_PEER_ACK = 2,
	SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER = 3
} SemanticActivationWaitEdge;

typedef enum SemanticActivationActor {
	SEMANTIC_ACTIVATION_ACTOR_PROCESS_UTILITY = 0,
	SEMANTIC_ACTIVATION_ACTOR_LMON,
	SEMANTIC_ACTIVATION_ACTOR_QVOTEC,
	SEMANTIC_ACTIVATION_ACTOR_LMS,
	SEMANTIC_ACTIVATION_ACTOR_DATA
} SemanticActivationActor;

typedef enum SemanticActivationEffect {
	SEMANTIC_ACTIVATION_EFFECT_NONE = 0,
	SEMANTIC_ACTIVATION_EFFECT_REQUEST_PUBLICATION = UINT32_C(1),
	SEMANTIC_ACTIVATION_EFFECT_PGSA_WRITE = UINT32_C(2),
	SEMANTIC_ACTIVATION_EFFECT_SOURCE_CLOSE = UINT32_C(4),
	SEMANTIC_ACTIVATION_EFFECT_TARGET_OPEN = UINT32_C(8),
	SEMANTIC_ACTIVATION_EFFECT_ACK_MUTATION = UINT32_C(16),
	SEMANTIC_ACTIVATION_EFFECT_CONTROL_WIRE = UINT32_C(32),
	SEMANTIC_ACTIVATION_EFFECT_DATA_WIRE = UINT32_C(64)
} SemanticActivationEffect;

typedef struct SemanticActivationAdmissionSnapshot {
	uint64 seq;
	uint64 active_bits;
	uint64 record_generation;
	uint64 formation_epoch;
	bool transition_closed;
} SemanticActivationAdmissionSnapshot;

static bool semantic_activation_snapshot(SemanticActivationAdmissionSnapshot *snapshot);
static bool semantic_activation_lmon_publish_gate(
	const SemanticActivationAdmissionSnapshot *snapshot, uint64 active_bits,
	uint64 record_generation, uint64 formation_epoch,
	bool transition_closed);
static bool semantic_activation_counter_increment(pg_atomic_uint32 *counter);
static bool semantic_activation_ensure_exit_hook(void);
static void semantic_activation_release_debt(ClusterSemanticAdmissionSide side,
										 int feature_index);

static uint16
semantic_activation_read_u16_le(const uint8 *bytes)
{
	return (uint16)bytes[0] | ((uint16)bytes[1] << 8);
}

static uint32
semantic_activation_read_u32_le(const uint8 *bytes)
{
	return (uint32)bytes[0] | ((uint32)bytes[1] << 8) | ((uint32)bytes[2] << 16)
		   | ((uint32)bytes[3] << 24);
}

static uint64
semantic_activation_read_u64_le(const uint8 *bytes)
{
	uint64 value = 0;
	int i;

	for (i = 7; i >= 0; i--)
		value = (value << 8) | bytes[i];
	return value;
}

static void
semantic_activation_write_u16_le(uint8 *bytes, uint16 value)
{
	bytes[0] = (uint8)value;
	bytes[1] = (uint8)(value >> 8);
}

static void
semantic_activation_write_u32_le(uint8 *bytes, uint32 value)
{
	bytes[0] = (uint8)value;
	bytes[1] = (uint8)(value >> 8);
	bytes[2] = (uint8)(value >> 16);
	bytes[3] = (uint8)(value >> 24);
}

static void
semantic_activation_write_u64_le(uint8 *bytes, uint64 value)
{
	int i;

	for (i = 0; i < 8; i++) {
		bytes[i] = (uint8)value;
		value >>= 8;
	}
}

static bool
semantic_activation_bytes_are_zero(const uint8 *bytes, Size len)
{
	Size i;

	for (i = 0; i < len; i++) {
		if (bytes[i] != 0)
			return false;
	}
	return true;
}

static void
semantic_activation_set_refusal(ClusterSemanticActivationRefusal *refusal,
								ClusterSemanticActivationResult result, uint64 feature_bit,
								uint64 expected_generation)
{
	if (refusal == NULL)
		return;

	refusal->result = result;
	refusal->feature_bit = feature_bit;
	refusal->expected_generation = expected_generation;
}

static ClusterSemanticActivationResult
r4_pre_prepare_readiness(uint64 expected_generation, ClusterSemanticActivationRefusal *refusal);

static ClusterSemanticActivationResult
semantic_activation_select_majority(const ClusterSemanticRecordSample *samples, uint32 n_samples,
									uint8 selected[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES],
									bool *implicit_open)
{
	uint32 majority;
	uint32 zero_count = 0;
	uint32 i;

	if (selected != NULL)
		memset(selected, 0, CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES);
	if (implicit_open != NULL)
		*implicit_open = false;
	if (samples == NULL || n_samples == 0 || selected == NULL)
		return CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;

	majority = n_samples / 2 + 1;
	for (i = 0; i < n_samples; i++) {
		if (samples[i].readable
			&& semantic_activation_bytes_are_zero(samples[i].bytes,
												  CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES))
			zero_count++;
	}
	if (zero_count >= majority) {
		if (implicit_open != NULL)
			*implicit_open = true;
		return CLUSTER_SEMANTIC_ACTIVATION_OK;
	}

	/* An exact valid byte image, rather than merely a generation, must win. */
	for (i = 0; i < n_samples; i++) {
		ClusterSemanticActivationRecord record;
		uint32 identical = 0;
		uint32 j;

		if (!samples[i].readable
			|| semantic_activation_bytes_are_zero(samples[i].bytes,
												  CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES)
			|| !cluster_semantic_activation_record_decode(samples[i].bytes, &record, NULL))
			continue;

		for (j = 0; j < n_samples; j++) {
			if (samples[j].readable
				&& memcmp(samples[i].bytes, samples[j].bytes,
						  CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES)
					   == 0)
				identical++;
		}
		if (identical >= majority) {
			memcpy(selected, samples[i].bytes, CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES);
			return CLUSTER_SEMANTIC_ACTIVATION_OK;
		}
	}

	/* A split record at one generation is never reclassified as legacy zero. */
	for (i = 0; i < n_samples; i++) {
		ClusterSemanticActivationRecord left;
		uint32 j;

		if (!samples[i].readable
			|| semantic_activation_bytes_are_zero(samples[i].bytes,
												  CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES)
			|| !cluster_semantic_activation_record_decode(samples[i].bytes, &left, NULL))
			continue;
		for (j = i + 1; j < n_samples; j++) {
			ClusterSemanticActivationRecord right;

			if (!samples[j].readable
				|| semantic_activation_bytes_are_zero(samples[j].bytes,
													  CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES)
				|| !cluster_semantic_activation_record_decode(samples[j].bytes, &right, NULL))
				continue;
			if (left.record_generation == right.record_generation
				&& memcmp(samples[i].bytes, samples[j].bytes,
						  CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES)
					   != 0)
				return CLUSTER_SEMANTIC_ACTIVATION_RECORD_CONFLICT;
		}
	}

	return CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;
}

static bool
semantic_activation_fsm_next(SemanticActivationState current, bool reverse,
							 SemanticActivationState *next)
{
	(void)reverse;
	if (next == NULL || current < SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN
		|| current >= SEMANTIC_ACTIVATION_STATE_TARGET_OPEN)
		return false;

	*next = (SemanticActivationState)(current + 1);
	return true;
}

static SemanticActivationCallbackKind
semantic_activation_callback_for_state(SemanticActivationState state)
{
	switch (state) {
	case SEMANTIC_ACTIVATION_STATE_SOURCE_ADMISSION_STOPPED:
		return SEMANTIC_ACTIVATION_CALLBACK_CLOSE_SOURCE;
	case SEMANTIC_ACTIVATION_STATE_SOURCE_LOGICAL_ZERO:
		return SEMANTIC_ACTIVATION_CALLBACK_LOGICAL_ZERO;
	case SEMANTIC_ACTIVATION_STATE_ORDERED_TRANSPORT_BARRIER:
		return SEMANTIC_ACTIVATION_CALLBACK_TRANSPORT_BARRIER;
	case SEMANTIC_ACTIVATION_STATE_TRANSPORT_BACKED_ZERO:
		return SEMANTIC_ACTIVATION_CALLBACK_TRANSPORT_ZERO;
	case SEMANTIC_ACTIVATION_STATE_EPOCH_CAPABILITY_BARRIER:
		return SEMANTIC_ACTIVATION_CALLBACK_EPOCH_CAPABILITY_BARRIER;
	case SEMANTIC_ACTIVATION_STATE_TARGET_STAGED:
		return SEMANTIC_ACTIVATION_CALLBACK_PREPARE_TARGET;
	case SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED:
		return SEMANTIC_ACTIVATION_CALLBACK_APPLY_TARGET_CLOSED;
	case SEMANTIC_ACTIVATION_STATE_TARGET_OPEN:
		return SEMANTIC_ACTIVATION_CALLBACK_OPEN_TARGET;
	default:
		return SEMANTIC_ACTIVATION_CALLBACK_NONE;
	}
}

static bool
semantic_activation_source_target_exclusive(bool source_open, bool target_open)
{
	return !(source_open && target_open);
}

static bool
semantic_activation_failure_policy(SemanticActivationState state,
								   SemanticActivationFailurePolicy *policy)
{
	if (policy == NULL || state < SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN
		|| state > SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED)
		return false;

	policy->target = SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN;
	policy->admission_closed_until_source_open = state != SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN;
	policy->revert_source_closed = state == SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED;
	return true;
}

static bool semantic_activation_ack_tuple_encode(
	const SemanticActivationAckTuple *tuple,
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_ACK_TUPLE_BYTES]) pg_attribute_unused();

static bool
semantic_activation_ack_tuple_encode(
	const SemanticActivationAckTuple *tuple,
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_ACK_TUPLE_BYTES])
{
	uint8 encoded[CLUSTER_SEMANTIC_ACTIVATION_ACK_TUPLE_BYTES];

	if (tuple == NULL || bytes == NULL)
		return false;
	memset(encoded, 0, sizeof(encoded));
	semantic_activation_write_u32_le(encoded, tuple->node_id);
	semantic_activation_write_u64_le(encoded + 8, tuple->boot_id);
	semantic_activation_write_u64_le(encoded + 16, tuple->admitted_incarnation);
	semantic_activation_write_u64_le(encoded + 24,
								 tuple->control_connection_generation);
	semantic_activation_write_u32_le(encoded + 32, tuple->capability_word);
	semantic_activation_write_u64_le(encoded + 40, tuple->capability_generation);
	semantic_activation_write_u64_le(encoded + 48, tuple->transition_epoch);
	semantic_activation_write_u64_le(encoded + 56, tuple->record_generation);
	memcpy(bytes, encoded, sizeof(encoded));
	return true;
}

static bool
semantic_activation_ack_matches(const SemanticActivationAckTuple *observed,
								const SemanticActivationAckTuple *expected)
{
	return observed != NULL && expected != NULL && observed->node_id == expected->node_id
		   && observed->boot_id == expected->boot_id
		   && observed->admitted_incarnation == expected->admitted_incarnation
		   && observed->control_connection_generation == expected->control_connection_generation
		   && observed->capability_word == expected->capability_word
		   && observed->capability_generation == expected->capability_generation
		   && observed->transition_epoch == expected->transition_epoch
			   && observed->record_generation == expected->record_generation;
}

/* The ACK bitmap is part of the proof, not a cache hint: no missing or extra
 * node may be ignored, and every member must match its complete current tuple. */
static bool semantic_activation_full_ack_table_matches(
	const SemanticActivationAckTuple observed[CLUSTER_MAX_NODES],
	uint64 observed_members_lo, uint64 observed_members_hi,
	const SemanticActivationAckTuple expected[CLUSTER_MAX_NODES],
	uint64 expected_members_lo, uint64 expected_members_hi) pg_attribute_unused();

static bool
semantic_activation_full_ack_table_matches(
	const SemanticActivationAckTuple observed[CLUSTER_MAX_NODES],
	uint64 observed_members_lo, uint64 observed_members_hi,
	const SemanticActivationAckTuple expected[CLUSTER_MAX_NODES],
	uint64 expected_members_lo, uint64 expected_members_hi)
{
	int node;

	if (observed == NULL || expected == NULL
		|| (expected_members_lo == 0 && expected_members_hi == 0)
		|| observed_members_lo != expected_members_lo
		|| observed_members_hi != expected_members_hi)
		return false;

	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		bool member = node < 64
					  ? (expected_members_lo & (UINT64_C(1) << node)) != 0
					  : (expected_members_hi & (UINT64_C(1) << (node - 64))) != 0;

		if (!member)
			continue;
		if (expected[node].node_id != (uint32)node
			|| expected[node].boot_id == 0
			|| expected[node].admitted_incarnation == 0
			|| expected[node].control_connection_generation == 0
			|| (expected[node].capability_word
				& CLUSTER_SEMANTIC_REPLACEMENT_REQUIRED_CAPS)
				   != CLUSTER_SEMANTIC_REPLACEMENT_REQUIRED_CAPS
			|| expected[node].capability_generation == 0
			|| expected[node].record_generation == 0
			|| !semantic_activation_ack_matches(&observed[node],
											 &expected[node]))
			return false;
	}
	return true;
}

static bool semantic_activation_ack_complete_image_current(
	const ClusterSemanticActivationAckTableV1 *image,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	int32 local_node_id,
	uint32 local_capability_word) pg_attribute_unused();

static bool
semantic_activation_ack_complete_image_current(
	const ClusterSemanticActivationAckTableV1 *image,
	uint64 current_members_lo, uint64 current_members_hi,
	uint64 current_epoch, int32 current_coordinator_node,
	int32 local_node_id, uint32 local_capability_word)
{
	SemanticActivationAckTuple current;
	uint32 capability_word;
	uint32 capability_generation;
	uint64 admitted_incarnation;
	int node;

	if (image == NULL || local_node_id < 0
		|| local_node_id >= CLUSTER_MAX_NODES
		|| current_coordinator_node < 0
		|| current_coordinator_node >= CLUSTER_MAX_NODES
		|| image->stage < CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
		|| image->stage > CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED
		|| image->round_nonce == 0 || image->record_generation == 0
		|| image->coordinator_node != (uint32)current_coordinator_node
		|| image->expected_members_lo != current_members_lo
		|| image->expected_members_hi != current_members_hi
		|| image->transition_epoch != current_epoch
		|| !semantic_activation_ack_member_present(
			current_members_lo, current_members_hi, local_node_id)
		|| (image->flags
			& (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
			   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE))
		   != (CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_EXPECTED_VALID
			   | CLUSTER_SEMANTIC_ACTIVATION_ACK_FLAG_COMPLETE)
		|| !semantic_activation_full_ack_table_matches(
			image->observed, image->observed_members_lo,
			image->observed_members_hi, image->expected,
			image->expected_members_lo, image->expected_members_hi))
		return false;

	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		if (!semantic_activation_ack_member_present(
				current_members_lo, current_members_hi, node))
			continue;
		if (node == local_node_id) {
			if (!semantic_activation_ack_self_tuple(
					local_node_id, local_capability_word, current_epoch,
					image->record_generation, &current))
				return false;
		} else {
			admitted_incarnation
				= cluster_membership_get_last_admitted_incarnation(node);
			if (admitted_incarnation == 0
				|| cluster_membership_get_state(node)
				   != CLUSTER_MEMBER_MEMBER
				|| !cluster_sf_peer_capability_word_sample(
					node, CLUSTER_SEMANTIC_ACTIVATION_ACK_REQUIRED_CAPS,
					&capability_word, &capability_generation)
				|| capability_generation == 0)
				return false;
			memset(&current, 0, sizeof(current));
			current.node_id = (uint32)node;
			current.boot_id = admitted_incarnation;
			current.admitted_incarnation = admitted_incarnation;
			current.control_connection_generation
				= (uint64)capability_generation;
			current.capability_word = capability_word;
			current.capability_generation
				= (uint64)capability_generation;
			current.transition_epoch = current_epoch;
			current.record_generation = image->record_generation;
		}
		if (!semantic_activation_ack_matches(
				&image->expected[node], &current)
			|| !semantic_activation_ack_matches(
				&image->observed[node], &current))
			return false;
	}
	return true;
}

/*
 * Match the current replacement episode to a decoded JCMK-v3 image already
 * selected by the strict-majority reader.  The ADMITTED marker, rather than
 * the overwritten instantaneous phase-3 snapshot, is the historical
 * completion basis for PGSA PREPARE.  This pure match neither proves that the
 * caller ran the majority selector nor grants current block0 authority or a
 * later PGSA ACK.
 */
static bool semantic_activation_r4_prepare_basis_matches(
	const ClusterReplacementCommitMarkerV3 *majority_admitted,
	const ClusterReplacementEpisode *episode) pg_attribute_unused();

static bool
semantic_activation_r4_prepare_basis_matches(
	const ClusterReplacementCommitMarkerV3 *majority_admitted,
	const ClusterReplacementEpisode *episode)
{
	if (majority_admitted == NULL || episode == NULL)
		return false;
	if (majority_admitted->magic != CLUSTER_JCMK_MAGIC
		|| majority_admitted->version != CLUSTER_JCMK_REPLACEMENT_VERSION
		|| majority_admitted->target_node_id < 0
		|| majority_admitted->target_node_id >= CLUSTER_MAX_NODES
		|| majority_admitted->phase != CLUSTER_JCMK_REPLACEMENT_PHASE_ADMITTED
		|| majority_admitted->reserved0[0] != 0 || majority_admitted->reserved0[1] != 0
		|| majority_admitted->reserved0[2] != 0
		|| majority_admitted->ready_state_generation == 0
		|| majority_admitted->old_admitted_incarnation == 0
		|| majority_admitted->fresh_incarnation
			   <= majority_admitted->old_admitted_incarnation
		|| majority_admitted->request_nonce == 0
		|| majority_admitted->baseline_epoch == UINT64_MAX
		|| majority_admitted->reserved_or_committed_epoch
			   != majority_admitted->baseline_epoch + 1
		|| majority_admitted->grammar_fingerprint == 0)
		return false;
	if (!cluster_replacement_episode_is_valid(episode)
		|| episode->phase != CLUSTER_REPLACEMENT_EPISODE_ADMITTED
		|| episode->readiness_flags != CLUSTER_REPLACEMENT_EPISODE_READINESS_MASK)
		return false;

	return episode->target_node_id == majority_admitted->target_node_id
		   && episode->state_generation
			  == majority_admitted->ready_state_generation
		   && episode->request_nonce == majority_admitted->request_nonce
		   && episode->old_admitted_incarnation
			  == majority_admitted->old_admitted_incarnation
		   && episode->fresh_incarnation == majority_admitted->fresh_incarnation
		   && episode->baseline_epoch == majority_admitted->baseline_epoch
		   && episode->reserved_or_committed_epoch
			  == majority_admitted->reserved_or_committed_epoch
		   && memcmp(episode->expected_survivors,
					 majority_admitted->expected_purge_survivors,
					 sizeof(episode->expected_survivors)) == 0
		   && episode->grammar_fingerprint == majority_admitted->grammar_fingerprint;
}

/*
 * Consume the current-coordinator happy-path handoff without manufacturing a
 * durable majority result.  The reconfiguration owner publishes outputs only
 * after co-sampling its ACKed ADMITTED marker, episode, epoch and MEMBER set;
 * this adapter then rechecks their exact D13 lineage.  Formation takeover and
 * the remaining D13 predicates stay fail-closed elsewhere.
 */
static bool
semantic_activation_r4_current_admitted_basis(void)
{
	ClusterReplacementEpisode episode;
	ClusterReplacementCommitMarkerV3 marker;

	if (!cluster_reconfig_lmon_snapshot_replacement_admitted(&episode, &marker))
		return false;
	return semantic_activation_r4_prepare_basis_matches(&marker, &episode);
}

/*
 * Validate the read-only RECOVER_HEAD result used to repeat the mandatory
 * accepted-invalidator scan after ADMITTED.  CHOSEN means QVOTEC found no
 * strict-majority accepted next value; ADOPTED_OTHER is an invalidator and is
 * never reclassified as the old episode.  This helper starts no mailbox I/O
 * and grants no PGSA or admission authority.
 */
static bool semantic_activation_r4_invalidator_rescan_matches(
	const ClusterQvotecMailboxCompletion *completion,
	const ClusterReplacementCommitMarkerV3 *majority_admitted,
	const ClusterReplacementEpisode *episode) pg_attribute_unused();

static bool
semantic_activation_r4_invalidator_rescan_matches(
	const ClusterQvotecMailboxCompletion *completion,
	const ClusterReplacementCommitMarkerV3 *majority_admitted,
	const ClusterReplacementEpisode *episode)
{
	ClusterEpochAuthorityValue head;
	ClusterEpochBallotId ballot;
	uint8 expected_subject[CLUSTER_EPOCH_BALLOT_BITMAP_BYTES] = { 0 };

	if (completion == NULL
		|| !semantic_activation_r4_prepare_basis_matches(
			majority_admitted, episode)
		|| completion->request_seq == 0
		|| (completion->request_seq & UINT64_C(1)) != 0
		|| completion->result != CLUSTER_QVOTEC_MAILBOX_CHOSEN
		|| completion->actor_phase != CLUSTER_QVOTEC_ACTOR_RECOVER_SCAN_B
		|| completion->observed_disk_bitmap == 0 || completion->detail != 0
		|| !cluster_epoch_authority_value_decode(
			completion->completion_value,
			CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT, &head)
		|| !cluster_epoch_ballot_id_decode(
			completion->completion_ballot, &ballot))
		return false;
	(void)ballot;

	expected_subject[episode->target_node_id / 8]
		= (uint8)(1u << (episode->target_node_id % 8));
	return head.value_version == CLUSTER_EPOCH_AUTHORITY_VALUE_VERSION
		   && head.transition == CLUSTER_EPOCH_AUTHORITY_COMMIT_CLOSED
		   && head.event_kind == CLUSTER_EPOCH_EVENT_SAME_NODE_REPLACEMENT
		   && head.request_origin_node == episode->target_node_id
		   && head.target_node_id == episode->target_node_id
		   && head.authority_generation != 0
		   && head.authority_generation != UINT64_MAX
		   && head.baseline_epoch == episode->baseline_epoch
		   && head.reserved_epoch == episode->reserved_or_committed_epoch
		   && head.old_incarnation == episode->old_admitted_incarnation
		   && head.fresh_incarnation == episode->fresh_incarnation
		   && head.request_nonce == episode->request_nonce
		   && memcmp(head.authority_member_bitmap,
					 episode->expected_survivors,
					 sizeof(head.authority_member_bitmap)) == 0
		   && memcmp(head.event_subject_bitmap, expected_subject,
					 sizeof(head.event_subject_bitmap)) == 0
		   && head.grammar_fingerprint == episode->grammar_fingerprint;
}

static ClusterSemanticAdmissionResult
semantic_activation_admission_policy(uint64 feature_bit, uint64 active_bits, bool transition_closed,
									 ClusterSemanticAdmissionSide side, uint64 expected_generation,
									 uint64 current_generation)
{
	bool active;

	if (feature_bit == 0
		|| (side != CLUSTER_SEMANTIC_SOURCE_SIDE && side != CLUSTER_SEMANTIC_TARGET_SIDE))
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	if (expected_generation != current_generation)
		return CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED;
	if (transition_closed)
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;

	active = (active_bits & feature_bit) != 0;
	if (side == CLUSTER_SEMANTIC_SOURCE_SIDE)
		return active ? CLUSTER_SEMANTIC_ADMISSION_SOURCE_DORMANT : CLUSTER_SEMANTIC_ADMISSION_OK;
	return active ? CLUSTER_SEMANTIC_ADMISSION_OK : CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED;
}

static bool
semantic_activation_modifier_policy(uint64 active_bits, uint64 record_generation,
									bool transition_closed)
{
	if ((active_bits & ~CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1) != 0)
		return false;

	/* No durable semantic record means that no modifier side is established. */
	if (record_generation == 0)
		return false;
	if (transition_closed)
		return false;

	/* Both ordinary SOURCE and uniformly-open R4 TARGET admit modifiers. */
	return true;
}

static ClusterSemanticAdmissionResult
semantic_activation_modifier_enter_bootstrap(bool writable_admission,
										 ClusterSemanticAdmissionToken *token)
{
	SemanticActivationAdmissionSnapshot before;
	SemanticActivationAdmissionSnapshot after;
	uint64 epoch_before;
	uint64 epoch_after;
	bool incremented = false;

	if (token != NULL)
		memset(token, 0, sizeof(*token));
	if (!writable_admission || token == NULL || SemanticActivationShmem == NULL
		|| !semantic_activation_ensure_exit_hook())
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;

	epoch_before = cluster_epoch_get_current();
	if (!semantic_activation_snapshot(&before) || before.formation_epoch != epoch_before
		|| before.record_generation != 0 || before.active_bits != 0)
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;

	HOLD_INTERRUPTS();
	if (semantic_activation_local_inflight[CLUSTER_SEMANTIC_SOURCE_SIDE][0] != UINT32_MAX
		&& semantic_activation_counter_increment(
			&SemanticActivationShmem->inflight[CLUSTER_SEMANTIC_SOURCE_SIDE][0])) {
		semantic_activation_local_inflight[CLUSTER_SEMANTIC_SOURCE_SIDE][0]++;
		incremented = true;
	}
	pg_write_barrier();
	RESUME_INTERRUPTS();
	if (!incremented)
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;

	if (!semantic_activation_snapshot(&after))
		goto fail;
	epoch_after = cluster_epoch_get_current();
	if (before.seq != after.seq || after.record_generation != 0 || after.active_bits != 0
		|| before.formation_epoch != after.formation_epoch || epoch_before != epoch_after
		|| after.formation_epoch != epoch_after)
		goto fail;

	token->feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	token->record_generation = 0;
	token->formation_epoch = before.formation_epoch;
	token->side = CLUSTER_SEMANTIC_SOURCE_SIDE;
	token->entered = true;
	return CLUSTER_SEMANTIC_ADMISSION_OK;

fail:
	HOLD_INTERRUPTS();
	semantic_activation_release_debt(CLUSTER_SEMANTIC_SOURCE_SIDE, 0);
	RESUME_INTERRUPTS();
	return CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED;
}

static ClusterSemanticActivationResult
semantic_activation_preflight(ClusterSemanticActivationAction action, uint64 expected_generation,
							  ClusterSemanticActivationRefusal *refusal, uint32 *effects)
{
	ClusterSemanticActivationResult result;

	if (effects != NULL)
		*effects = SEMANTIC_ACTIVATION_EFFECT_NONE;
	if (action < CLUSTER_SEMANTIC_ENABLE_ALL || action > CLUSTER_SEMANTIC_ROLLBACK_ABORT) {
		if (refusal != NULL) {
			refusal->result = CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
			refusal->feature_bit = 0;
			refusal->expected_generation = expected_generation;
		}
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	}

	result = r4_pre_prepare_readiness(expected_generation, refusal);
	return result;
}

static bool
semantic_activation_control_wait_allowed(SemanticActivationWaitEdge edge, uint32 held_locks)
{
	return edge >= SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON
		   && edge <= SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER
		   && held_locks == SEMANTIC_ACTIVATION_HELD_NONE;
}

static bool
semantic_activation_actor_edge_allowed(SemanticActivationActor from, SemanticActivationActor to)
{
	return (from == SEMANTIC_ACTIVATION_ACTOR_PROCESS_UTILITY
			&& to == SEMANTIC_ACTIVATION_ACTOR_LMON)
		   || (from == SEMANTIC_ACTIVATION_ACTOR_LMON && to == SEMANTIC_ACTIVATION_ACTOR_QVOTEC);
}

/*
 * Keep durable and local effects on their frozen owners.  In particular,
 * ProcessUtility can only publish a request, formation LMON owns coordination,
 * and QVOTEC alone owns the PGSA voting-disk write.
 */
static bool semantic_activation_actor_effect_allowed(SemanticActivationActor actor,
											  SemanticActivationEffect effect)
	pg_attribute_unused();

static bool
semantic_activation_actor_effect_allowed(SemanticActivationActor actor,
									  SemanticActivationEffect effect)
{
	switch (actor) {
	case SEMANTIC_ACTIVATION_ACTOR_PROCESS_UTILITY:
		return effect == SEMANTIC_ACTIVATION_EFFECT_REQUEST_PUBLICATION;
	case SEMANTIC_ACTIVATION_ACTOR_LMON:
		return effect == SEMANTIC_ACTIVATION_EFFECT_SOURCE_CLOSE
			   || effect == SEMANTIC_ACTIVATION_EFFECT_TARGET_OPEN
			   || effect == SEMANTIC_ACTIVATION_EFFECT_ACK_MUTATION
			   || effect == SEMANTIC_ACTIVATION_EFFECT_CONTROL_WIRE;
	case SEMANTIC_ACTIVATION_ACTOR_QVOTEC:
		return effect == SEMANTIC_ACTIVATION_EFFECT_PGSA_WRITE;
	case SEMANTIC_ACTIVATION_ACTOR_LMS:
	case SEMANTIC_ACTIVATION_ACTOR_DATA:
	default:
		return false;
	}
}

static ClusterSemanticActivationResult
r4_pre_prepare_readiness(uint64 expected_generation, ClusterSemanticActivationRefusal *refusal)
{
	bool admitted_basis = semantic_activation_r4_current_admitted_basis();
	ClusterSemanticActivationResult result
		= admitted_basis ? CLUSTER_SEMANTIC_ACTIVATION_OK
						 : CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED;

	/* The instantaneous READY snapshot belongs exclusively to target LMON's
	 * phase-3 serializer.  The current-coordinator handoff becomes visible
	 * here only after the majority ADMITTED write and its post-write terminal-
	 * head rescan have both completed; this callback grants entry to PREPARE,
	 * not a PREPARED ACK, COMMIT ACK, target OPEN, or current write authority. */
	if (refusal != NULL) {
		refusal->result = result;
		refusal->feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		refusal->expected_generation = expected_generation;
	}
	return result;
}

static ClusterSemanticActivationResult
r4_stage_fail_closed(uint64 generation)
{
	(void)generation;
	return CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED;
}

static ClusterSemanticActivationResult
r4_close_source_admission(uint64 generation)
{
	SemanticActivationAdmissionSnapshot snapshot;
	uint32 debt;

	if (!semantic_activation_snapshot(&snapshot)
		|| generation == 0 || snapshot.record_generation != generation
		|| snapshot.formation_epoch != cluster_epoch_get_current()
		|| (snapshot.active_bits
			& CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1) != 0)
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;

	if (!snapshot.transition_closed
		&& !semantic_activation_lmon_publish_gate(
			&snapshot, snapshot.active_bits, snapshot.record_generation,
			snapshot.formation_epoch, true))
		return CLUSTER_SEMANTIC_ACTIVATION_MEMBERSHIP_CHANGED;

	pg_read_barrier();
	debt = pg_atomic_read_u32(
		&SemanticActivationShmem
			 ->inflight[CLUSTER_SEMANTIC_SOURCE_SIDE][0]);
	return debt == 0 ? CLUSTER_SEMANTIC_ACTIVATION_OK
					 : CLUSTER_SEMANTIC_ACTIVATION_DEBT_NONZERO;
}

static ClusterSemanticActivationResult
r4_source_logical_debt_zero(uint64 generation, ClusterSemanticZeroProof *proof)
{
	SemanticActivationAdmissionSnapshot snapshot;
	uint32 source_debt;

	if (proof == NULL)
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	memset(proof, 0, sizeof(*proof));

	if (generation == 0 || !semantic_activation_snapshot(&snapshot)
		|| snapshot.record_generation != generation
		|| snapshot.formation_epoch != cluster_epoch_get_current()
		|| !snapshot.transition_closed
		|| (snapshot.active_bits
			& CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1) != 0)
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;

	source_debt = pg_atomic_read_u32(
		&SemanticActivationShmem
			 ->inflight[CLUSTER_SEMANTIC_SOURCE_SIDE][0]);
	if (source_debt != 0)
		return CLUSTER_SEMANTIC_ACTIVATION_DEBT_NONZERO;

	/* Closed admission prevents a new source owner after the zero sample;
	 * re-sample the complete authority tuple before exposing the proof. */
	if (!semantic_activation_snapshot(&snapshot)
		|| snapshot.record_generation != generation
		|| snapshot.formation_epoch != cluster_epoch_get_current()
		|| !snapshot.transition_closed
		|| (snapshot.active_bits
			& CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1) != 0)
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	if (pg_atomic_read_u32(
			&SemanticActivationShmem
				 ->inflight[CLUSTER_SEMANTIC_SOURCE_SIDE][0])
		!= 0)
		return CLUSTER_SEMANTIC_ACTIVATION_DEBT_NONZERO;

	proof->record_generation = generation;
	proof->debt_count = 0;
	proof->sample_digest = 0;
	return CLUSTER_SEMANTIC_ACTIVATION_OK;
}

/*
 * D4's generation-bound transport proof is one fail-closed conjunction:
 * closed admission and zero TARGET debt, the exact worker-0 drain ACK plus
 * four-slot LMON reclaim, no R4 route record, and no live R4 requester slot.
 * The callback owns no new shared state; every sample comes from the existing
 * generation/incarnation-bound owners.
 */
static ClusterSemanticActivationResult
r4_source_transport_zero(uint64 generation, ClusterSemanticZeroProof *proof)
{
	SemanticActivationAdmissionSnapshot snapshot;
	ClusterLmsSharedState *lms_state;
	uint64 worker_incarnation = 0;
	uint32 target_debt;

	if (proof == NULL)
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	memset(proof, 0, sizeof(*proof));

	if (generation == 0 || !semantic_activation_snapshot(&snapshot)
		|| snapshot.record_generation != generation
		|| snapshot.formation_epoch != cluster_epoch_get_current()
		|| !snapshot.transition_closed)
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;

	target_debt = pg_atomic_read_u32(
		&SemanticActivationShmem
			 ->inflight[CLUSTER_SEMANTIC_TARGET_SIDE][0]);
	if (target_debt != 0)
		return CLUSTER_SEMANTIC_ACTIVATION_DEBT_NONZERO;

	lms_state = cluster_lms_shared_state();
	if (lms_state == NULL
		|| !cluster_lms_r4_drain_request(
			lms_state, generation, &worker_incarnation))
		return CLUSTER_SEMANTIC_ACTIVATION_TRANSPORT_NONZERO;
	cluster_lms_wakeup(0);
	if (!cluster_cr_server_r4_lmon_reclaim_closed(
			worker_incarnation, generation))
		return CLUSTER_SEMANTIC_ACTIVATION_TRANSPORT_NONZERO;

	(void)cluster_gcs_block_dedup_r4_route_purge_closed();
	if (cluster_gcs_block_dedup_r4_route_count() != 0
		|| cluster_gcs_block_r4_requester_count() != 0)
		return CLUSTER_SEMANTIC_ACTIVATION_TRANSPORT_NONZERO;

	/* Bind the proof to a final coherent gate sample; no partial proof escapes
	 * if the formation/generation/close edge moved during convergence. */
	if (!semantic_activation_snapshot(&snapshot)
		|| snapshot.record_generation != generation
		|| snapshot.formation_epoch != cluster_epoch_get_current()
		|| !snapshot.transition_closed)
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	target_debt = pg_atomic_read_u32(
		&SemanticActivationShmem
			 ->inflight[CLUSTER_SEMANTIC_TARGET_SIDE][0]);
	if (target_debt != 0)
		return CLUSTER_SEMANTIC_ACTIVATION_DEBT_NONZERO;

	proof->record_generation = generation;
	proof->debt_count = 0;
	/* The approved contract freezes the field but no cross-node digest
	 * formula; zero records the all-zero census without inventing authority. */
	proof->sample_digest = 0;
	return CLUSTER_SEMANTIC_ACTIVATION_OK;
}

static const ClusterSemanticActivationDescriptor r4_descriptor = {
	.name = "R4_SYNC_CR_V1",
	.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
	.required_hello_caps = CLUSTER_SEMANTIC_REPLACEMENT_REQUIRED_CAPS,
	.required_active_bits = 0,
	.source_available = true,
	.pre_prepare_readiness = r4_pre_prepare_readiness,
	.close_source_admission = r4_close_source_admission,
	.source_logical_debt_zero = r4_source_logical_debt_zero,
	.source_transport_zero = r4_source_transport_zero,
	.prepare_target = r4_stage_fail_closed,
	.apply_target_closed = r4_stage_fail_closed,
	.revert_source_closed = r4_stage_fail_closed,
	.open_target_admission = r4_stage_fail_closed,
};

static bool
semantic_activation_feature_index(uint64 feature_bit, int *feature_index)
{
	if (feature_bit != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1 || feature_index == NULL)
		return false;
	*feature_index = 0;
	return true;
}

static bool
semantic_activation_snapshot(SemanticActivationAdmissionSnapshot *snapshot)
{
	int attempt;

	if (SemanticActivationShmem == NULL || snapshot == NULL)
		return false;

	for (attempt = 0; attempt < CLUSTER_SEMANTIC_ADMISSION_SNAPSHOT_TRIES; attempt++) {
		uint64 seq_before;
		uint64 seq_after;

		seq_before = pg_atomic_read_u64(&SemanticActivationShmem->admission_seq);
		if ((seq_before & UINT64_C(1)) != 0)
			continue;
		pg_read_barrier();
		snapshot->active_bits = pg_atomic_read_u64(&SemanticActivationShmem->active_bits);
		snapshot->record_generation
			= pg_atomic_read_u64(&SemanticActivationShmem->record_generation);
		snapshot->formation_epoch = pg_atomic_read_u64(&SemanticActivationShmem->formation_epoch);
		snapshot->transition_closed
			= pg_atomic_read_u32(&SemanticActivationShmem->transition_closed) != 0;
		pg_read_barrier();
		seq_after = pg_atomic_read_u64(&SemanticActivationShmem->admission_seq);
		if (seq_before == seq_after && (seq_after & UINT64_C(1)) == 0) {
			snapshot->seq = seq_after;
			return true;
		}
	}
	return false;
}

static bool
semantic_activation_counter_increment(pg_atomic_uint32 *counter)
{
	int attempt;
	uint32 observed;

	observed = pg_atomic_read_u32(counter);
	for (attempt = 0; attempt < CLUSTER_SEMANTIC_ADMISSION_COUNTER_TRIES; attempt++) {
		uint32 expected = observed;

		if (observed == UINT32_MAX)
			return false;
		if (pg_atomic_compare_exchange_u32(counter, &expected, observed + 1))
			return true;
		observed = expected;
	}
	return false;
}

static void
semantic_activation_counter_subtract(pg_atomic_uint32 *counter, uint32 amount)
{
	uint32 observed = pg_atomic_read_u32(counter);

	for (;;) {
		uint32 expected = observed;

		if (amount == 0)
			return;
		if (observed < amount)
			ereport(PANIC, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("semantic activation admission debt underflow"),
							errhint("Restart the failed process and retain the shared-memory image "
									"for diagnosis.")));
		if (pg_atomic_compare_exchange_u32(counter, &expected, observed - amount))
			return;
		observed = expected;
	}
}

static void
semantic_activation_exit_cleanup(int code, Datum arg)
{
	int side;
	int feature_index;
	int registered_pid = DatumGetInt32(arg);

	(void)code;
	if (registered_pid != MyProcPid || semantic_activation_exit_hook_pid != MyProcPid
		|| SemanticActivationShmem == NULL)
		return;

	HOLD_INTERRUPTS();
	for (side = 0; side < 2; side++) {
		for (feature_index = 0; feature_index < 64; feature_index++) {
			uint32 local = semantic_activation_local_inflight[side][feature_index];
			uint32 shared;

			if (local == 0)
				continue;
			shared = pg_atomic_read_u32(&SemanticActivationShmem->inflight[side][feature_index]);
			if (shared < local)
				ereport(
					PANIC,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("semantic activation exit debt is inconsistent"),
					 errhint("Retain the shared-memory image and restart the failed process.")));
		}
	}
	for (side = 0; side < 2; side++) {
		for (feature_index = 0; feature_index < 64; feature_index++) {
			uint32 local = semantic_activation_local_inflight[side][feature_index];

			if (local == 0)
				continue;
			semantic_activation_counter_subtract(
				&SemanticActivationShmem->inflight[side][feature_index], local);
			semantic_activation_local_inflight[side][feature_index] = 0;
		}
	}
	semantic_activation_exit_hook_pid = 0;
	RESUME_INTERRUPTS();
}

static bool
semantic_activation_ensure_exit_hook(void)
{
	if (MyProcPid <= 0)
		return false;
	if (semantic_activation_exit_hook_pid == MyProcPid)
		return true;

	memset(semantic_activation_local_inflight, 0, sizeof(semantic_activation_local_inflight));
	on_shmem_exit(semantic_activation_exit_cleanup, Int32GetDatum(MyProcPid));
	semantic_activation_exit_hook_pid = MyProcPid;
	return true;
}

static void
semantic_activation_release_debt(ClusterSemanticAdmissionSide side, int feature_index)
{
	uint32 *local = &semantic_activation_local_inflight[side][feature_index];

	if (*local == 0)
		ereport(PANIC, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("semantic activation local debt is missing"),
						errhint("Retain the process and shared-memory state for diagnosis.")));
	semantic_activation_counter_subtract(&SemanticActivationShmem->inflight[side][feature_index],
										 1);
	(*local)--;
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter(uint64 feature_bit, ClusterSemanticAdmissionSide side,
								  ClusterSemanticAdmissionToken *token)
{
	SemanticActivationAdmissionSnapshot before;
	SemanticActivationAdmissionSnapshot after;
	ClusterSemanticAdmissionResult result;
	uint64 epoch_before;
	uint64 epoch_after;
	int feature_index;
	bool incremented = false;

	if (token != NULL)
		memset(token, 0, sizeof(*token));
	if (token == NULL || SemanticActivationShmem == NULL
		|| (side != CLUSTER_SEMANTIC_SOURCE_SIDE && side != CLUSTER_SEMANTIC_TARGET_SIDE)
		|| !semantic_activation_feature_index(feature_bit, &feature_index)
		|| !semantic_activation_ensure_exit_hook())
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;

	epoch_before = cluster_epoch_get_current();
	if (!semantic_activation_snapshot(&before))
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	if (before.formation_epoch != epoch_before)
		return CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED;
	result = semantic_activation_admission_policy(
		feature_bit, before.active_bits, before.transition_closed, side, before.record_generation,
		before.record_generation);
	if (result != CLUSTER_SEMANTIC_ADMISSION_OK)
		return result;

	HOLD_INTERRUPTS();
	if (semantic_activation_local_inflight[side][feature_index] != UINT32_MAX
		&& semantic_activation_counter_increment(
			&SemanticActivationShmem->inflight[side][feature_index])) {
		semantic_activation_local_inflight[side][feature_index]++;
		incremented = true;
	}
	pg_write_barrier();
	RESUME_INTERRUPTS();
	if (!incremented)
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;

	if (!semantic_activation_snapshot(&after))
		result = CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	else {
		epoch_after = cluster_epoch_get_current();
		if (before.seq != after.seq || before.record_generation != after.record_generation
			|| before.formation_epoch != after.formation_epoch || epoch_before != epoch_after
			|| after.formation_epoch != epoch_after)
			result = CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED;
		else
			result = semantic_activation_admission_policy(
				feature_bit, after.active_bits, after.transition_closed, side,
				before.record_generation, after.record_generation);
	}
	if (result != CLUSTER_SEMANTIC_ADMISSION_OK) {
		HOLD_INTERRUPTS();
		semantic_activation_release_debt(side, feature_index);
		RESUME_INTERRUPTS();
		return result;
	}

	token->feature_bit = feature_bit;
	token->record_generation = before.record_generation;
	token->formation_epoch = before.formation_epoch;
	token->side = (uint8)side;
	token->entered = true;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_modifier_enter(bool writable_admission,
									   ClusterSemanticAdmissionToken *token)
{
	SemanticActivationAdmissionSnapshot snapshot;
	ClusterSemanticAdmissionSide side;

	if (token != NULL)
		memset(token, 0, sizeof(*token));
	if (token == NULL || !semantic_activation_snapshot(&snapshot))
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;

	/*
	 * Before the first PGSA record, the ordinary-join write gate is the
	 * durable discriminator between a steady SOURCE member and a replacement
	 * MEMBER that is deliberately still closed.  We still take D10 debt so a
	 * later close can drain already-running ordinary modifiers.
	 */
	if (snapshot.record_generation == 0) {
		if (snapshot.active_bits != 0 || !writable_admission)
			return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
		return semantic_activation_modifier_enter_bootstrap(writable_admission, token);
	} else {
		if (!semantic_activation_modifier_policy(
				snapshot.active_bits, snapshot.record_generation, snapshot.transition_closed))
			return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
		side = (snapshot.active_bits & CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1) != 0
				   ? CLUSTER_SEMANTIC_TARGET_SIDE
				   : CLUSTER_SEMANTIC_SOURCE_SIDE;
	}

	return cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, side, token);
}

bool
cluster_semantic_activation_modifier_recheck(const ClusterSemanticAdmissionToken *token,
									 bool writable_admission)
{
	SemanticActivationAdmissionSnapshot snapshot;
	uint64 current_epoch;

	if (token == NULL || !token->entered || token->side > CLUSTER_SEMANTIC_TARGET_SIDE
		|| !semantic_activation_snapshot(&snapshot))
		return false;
	current_epoch = cluster_epoch_get_current();
	if (snapshot.record_generation != token->record_generation
		|| snapshot.formation_epoch != token->formation_epoch
		|| current_epoch != token->formation_epoch)
		return false;
	if (snapshot.record_generation == 0)
		return token->side == CLUSTER_SEMANTIC_SOURCE_SIDE && snapshot.active_bits == 0
			   && writable_admission;
	return semantic_activation_modifier_policy(
		snapshot.active_bits, snapshot.record_generation, snapshot.transition_closed);
}

bool
cluster_semantic_activation_recheck(const ClusterSemanticAdmissionToken *token)
{
	SemanticActivationAdmissionSnapshot snapshot;
	uint64 current_epoch;
	int feature_index;

	if (token == NULL || !token->entered)
		return false;
	if (!semantic_activation_feature_index(token->feature_bit, &feature_index)
		|| token->side > CLUSTER_SEMANTIC_TARGET_SIDE || SemanticActivationShmem == NULL)
		return false;
	(void)feature_index;
	if (!semantic_activation_snapshot(&snapshot))
		return false;
	current_epoch = cluster_epoch_get_current();
	if (snapshot.formation_epoch != current_epoch || token->formation_epoch != current_epoch)
		return false;

	return semantic_activation_admission_policy(
			   token->feature_bit, snapshot.active_bits, snapshot.transition_closed,
			   (ClusterSemanticAdmissionSide)token->side, token->record_generation,
			   snapshot.record_generation)
		   == CLUSTER_SEMANTIC_ADMISSION_OK;
}

bool
cluster_semantic_activation_peer_open_matches(
	const ClusterSemanticAdmissionToken *token, int32 authenticated_peer_node_id,
	uint32 required_hello_caps, uint32 sampled_capability_generation)
{
	if (token == NULL || !token->entered
		|| token->feature_bit != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		|| token->side != CLUSTER_SEMANTIC_TARGET_SIDE || authenticated_peer_node_id < 0
		|| authenticated_peer_node_id >= CLUSTER_MAX_NODES || required_hello_caps == 0)
		return false;
	if (!cluster_semantic_activation_recheck(token))
		return false;
	if (!cluster_sf_peer_capability_generation_matches(
			authenticated_peer_node_id, required_hello_caps, sampled_capability_generation))
		return false;

	/* D13 owns positive results after installing its frozen ACK table. */
	return false;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	int feature_index;

	if (token == NULL || !token->entered)
		return;
	if (SemanticActivationShmem == NULL || token->side > CLUSTER_SEMANTIC_TARGET_SIDE
		|| !semantic_activation_feature_index(token->feature_bit, &feature_index))
		ereport(PANIC,
				(errcode(ERRCODE_INTERNAL_ERROR), errmsg("semantic activation token is invalid"),
				 errhint("Retain the process and shared-memory state for diagnosis.")));

	HOLD_INTERRUPTS();
	semantic_activation_release_debt((ClusterSemanticAdmissionSide)token->side, feature_index);
	memset(token, 0, sizeof(*token));
	RESUME_INTERRUPTS();
}

Size
cluster_semantic_activation_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterSemanticActivationShmem))
		   + MAXALIGN(sizeof(ClusterSemanticActivationUtilityMailboxShmem))
		   + MAXALIGN(sizeof(ClusterSemanticActivationAckTableV1));
}

void
cluster_semantic_activation_shmem_init(void)
{
	bool gate_found;
	bool mailbox_found;
	bool ack_table_found;
	int side;
	int feature_index;

	SemanticActivationShmem = (ClusterSemanticActivationShmem *)ShmemInitStruct(
		"pgrac cluster semantic activation gate",
		MAXALIGN(sizeof(ClusterSemanticActivationShmem)), &gate_found);
	SemanticActivationUtilityMailbox
		= (ClusterSemanticActivationUtilityMailboxShmem *)ShmemInitStruct(
			"pgrac cluster semantic activation utility mailbox",
			MAXALIGN(sizeof(ClusterSemanticActivationUtilityMailboxShmem)),
			&mailbox_found);
	SemanticActivationAckTable
		= (ClusterSemanticActivationAckTableV1 *)ShmemInitStruct(
			"pgrac cluster semantic activation ACK table",
			MAXALIGN(sizeof(ClusterSemanticActivationAckTableV1)),
			&ack_table_found);
	if (SemanticActivationShmem == NULL
		|| SemanticActivationUtilityMailbox == NULL
		|| SemanticActivationAckTable == NULL)
		return;
	if (!ack_table_found) {
		memset(SemanticActivationAckTable, 0,
			   sizeof(*SemanticActivationAckTable));
		pg_atomic_init_u64(&SemanticActivationAckTable->publication_seq, 0);
	}

	if (!gate_found) {
		pg_atomic_init_u64(&SemanticActivationShmem->record_cas_request_seq, 0);
		pg_atomic_init_u64(&SemanticActivationShmem->record_cas_completion_seq, 0);
		pg_atomic_init_u32(&SemanticActivationShmem->record_cas_result,
						   CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
		pg_atomic_init_u32(&SemanticActivationShmem->record_cas_request_kind,
						   CLUSTER_SEMANTIC_AUTHORITY_REQUEST_NONE);
		SemanticActivationShmem->record_cas_expected_generation = 0;
		SemanticActivationShmem->record_cas_expected_source_feature_bitmap = 0;
		memset(SemanticActivationShmem->record_cas_desired_bytes, 0,
			   sizeof(SemanticActivationShmem->record_cas_desired_bytes));
		pg_atomic_init_u64(&SemanticActivationShmem->admission_seq, 0);
		pg_atomic_init_u64(&SemanticActivationShmem->active_bits, 0);
		pg_atomic_init_u64(&SemanticActivationShmem->record_generation, 0);
		pg_atomic_init_u64(&SemanticActivationShmem->formation_epoch, 0);
		pg_atomic_init_u32(&SemanticActivationShmem->transition_closed, 1);
		for (side = 0; side < 2; side++) {
			for (feature_index = 0; feature_index < 64; feature_index++)
				pg_atomic_init_u32(
					&SemanticActivationShmem->inflight[side][feature_index], 0);
		}
	}
	if (!mailbox_found) {
		pg_atomic_init_u64(
			&SemanticActivationUtilityMailbox->utility_request_seq, 0);
		pg_atomic_init_u64(
			&SemanticActivationUtilityMailbox->utility_completion_seq, 0);
		pg_atomic_init_u32(
			&SemanticActivationUtilityMailbox->utility_mailbox_state,
			SEMANTIC_ACTIVATION_UTILITY_MAILBOX_IDLE);
		SemanticActivationUtilityMailbox->utility_action
			= CLUSTER_SEMANTIC_ENABLE_ALL;
		SemanticActivationUtilityMailbox->utility_source_feature_bitmap = 0;
		SemanticActivationUtilityMailbox->utility_target_feature_bitmap = 0;
		SemanticActivationUtilityMailbox->utility_rollback_feature_bitmap = 0;
		SemanticActivationUtilityMailbox->utility_expected_record_generation = 0;
		pg_atomic_init_u32(&SemanticActivationUtilityMailbox->utility_result,
						   CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
		SemanticActivationUtilityMailbox->utility_result_feature_bit = 0;
		SemanticActivationUtilityMailbox->utility_result_expected_generation = 0;
	}
}

/*
 * One single-slot ProcessUtility -> formation-LMON request/result mailbox.  The
 * state word is the publication fence: writers own WRITING, LMON alone
 * consumes PENDING, and only the publishing backend consumes COMPLETE.
 * This mailbox carries no PGSA bytes and cannot bypass QVOTEC.
 */
static bool
semantic_activation_utility_mailbox_submit(
	ClusterSemanticActivationAction action, uint64 source_feature_bitmap,
	uint64 target_feature_bitmap, uint64 rollback_feature_bitmap,
	uint64 expected_record_generation, uint64 *out_request_seq)
{
	uint32 expected_state = SEMANTIC_ACTIVATION_UTILITY_MAILBOX_IDLE;
	uint64 request_seq;

	if (SemanticActivationUtilityMailbox == NULL || out_request_seq == NULL
		|| action < CLUSTER_SEMANTIC_ENABLE_ALL
		|| action > CLUSTER_SEMANTIC_ROLLBACK_ABORT
		|| !pg_atomic_compare_exchange_u32(
			&SemanticActivationUtilityMailbox->utility_mailbox_state,
			&expected_state, SEMANTIC_ACTIVATION_UTILITY_MAILBOX_WRITING))
		return false;

	request_seq = pg_atomic_read_u64(
		&SemanticActivationUtilityMailbox->utility_request_seq);
	if (request_seq == UINT64_MAX) {
		pg_atomic_write_u32(&SemanticActivationUtilityMailbox->utility_mailbox_state,
							SEMANTIC_ACTIVATION_UTILITY_MAILBOX_IDLE);
		return false;
	}

	request_seq++;
	SemanticActivationUtilityMailbox->utility_action = (uint32)action;
	SemanticActivationUtilityMailbox->utility_source_feature_bitmap
		= source_feature_bitmap;
	SemanticActivationUtilityMailbox->utility_target_feature_bitmap
		= target_feature_bitmap;
	SemanticActivationUtilityMailbox->utility_rollback_feature_bitmap
		= rollback_feature_bitmap;
	SemanticActivationUtilityMailbox->utility_expected_record_generation
		= expected_record_generation;
	pg_atomic_write_u64(&SemanticActivationUtilityMailbox->utility_request_seq,
						request_seq);
	pg_write_barrier();
	pg_atomic_write_u32(&SemanticActivationUtilityMailbox->utility_mailbox_state,
						SEMANTIC_ACTIVATION_UTILITY_MAILBOX_PENDING);
	*out_request_seq = request_seq;
	return true;
}

static bool
semantic_activation_utility_mailbox_poll(SemanticActivationUtilityRequest *out)
{
	uint64 request_seq;

	if (SemanticActivationUtilityMailbox == NULL || out == NULL
		|| pg_atomic_read_u32(
			   &SemanticActivationUtilityMailbox->utility_mailbox_state)
			   != SEMANTIC_ACTIVATION_UTILITY_MAILBOX_PENDING)
		return false;

	pg_read_barrier();
	request_seq = pg_atomic_read_u64(
		&SemanticActivationUtilityMailbox->utility_request_seq);
	if (request_seq == 0
		|| pg_atomic_read_u64(
			   &SemanticActivationUtilityMailbox->utility_completion_seq)
			   + 1 != request_seq
		|| SemanticActivationUtilityMailbox->utility_action
			   > CLUSTER_SEMANTIC_ROLLBACK_ABORT)
		return false;

	out->request_seq = request_seq;
	out->action = (ClusterSemanticActivationAction)
		SemanticActivationUtilityMailbox->utility_action;
	out->source_feature_bitmap
		= SemanticActivationUtilityMailbox->utility_source_feature_bitmap;
	out->target_feature_bitmap
		= SemanticActivationUtilityMailbox->utility_target_feature_bitmap;
	out->rollback_feature_bitmap
		= SemanticActivationUtilityMailbox->utility_rollback_feature_bitmap;
	out->expected_record_generation
		= SemanticActivationUtilityMailbox->utility_expected_record_generation;
	return true;
}

static bool
semantic_activation_utility_mailbox_complete(
	uint64 request_seq, ClusterSemanticActivationResult result,
	uint64 feature_bit, uint64 expected_generation)
{
	if (SemanticActivationUtilityMailbox == NULL || request_seq == 0
		|| result < CLUSTER_SEMANTIC_ACTIVATION_OK
		|| result > CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE
		|| pg_atomic_read_u32(
			   &SemanticActivationUtilityMailbox->utility_mailbox_state)
			   != SEMANTIC_ACTIVATION_UTILITY_MAILBOX_PENDING
		|| pg_atomic_read_u64(
			   &SemanticActivationUtilityMailbox->utility_request_seq)
			   != request_seq)
		return false;

	pg_atomic_write_u32(&SemanticActivationUtilityMailbox->utility_result,
						(uint32)result);
	SemanticActivationUtilityMailbox->utility_result_feature_bit = feature_bit;
	SemanticActivationUtilityMailbox->utility_result_expected_generation
		= expected_generation;
	pg_atomic_write_u64(&SemanticActivationUtilityMailbox->utility_completion_seq,
						request_seq);
	pg_write_barrier();
	pg_atomic_write_u32(&SemanticActivationUtilityMailbox->utility_mailbox_state,
						SEMANTIC_ACTIVATION_UTILITY_MAILBOX_COMPLETE);
	return true;
}

static bool
semantic_activation_utility_mailbox_poll_completion(
	uint64 request_seq, ClusterSemanticActivationRefusal *out_refusal)
{
	uint32 expected_state = SEMANTIC_ACTIVATION_UTILITY_MAILBOX_COMPLETE;

	if (SemanticActivationUtilityMailbox == NULL || request_seq == 0
		|| out_refusal == NULL
		|| pg_atomic_read_u32(
			   &SemanticActivationUtilityMailbox->utility_mailbox_state)
			   != SEMANTIC_ACTIVATION_UTILITY_MAILBOX_COMPLETE
		|| pg_atomic_read_u64(
			   &SemanticActivationUtilityMailbox->utility_completion_seq)
			   != request_seq)
		return false;

	pg_read_barrier();
	out_refusal->result = (ClusterSemanticActivationResult)
		pg_atomic_read_u32(&SemanticActivationUtilityMailbox->utility_result);
	out_refusal->feature_bit
		= SemanticActivationUtilityMailbox->utility_result_feature_bit;
	out_refusal->expected_generation
		= SemanticActivationUtilityMailbox->utility_result_expected_generation;
	return pg_atomic_compare_exchange_u32(
		&SemanticActivationUtilityMailbox->utility_mailbox_state,
		&expected_state,
		SEMANTIC_ACTIVATION_UTILITY_MAILBOX_IDLE);
}

static ClusterSemanticActivationResult
semantic_activation_utility_mailbox_wait(
	uint64 request_seq, ClusterSemanticActivationRefusal *out_refusal)
{
	if (out_refusal == NULL || request_seq == 0) {
		semantic_activation_set_refusal(
			out_refusal, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE, 0, 0);
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	}

	for (;;) {
		if (semantic_activation_utility_mailbox_poll_completion(
				request_seq, out_refusal))
			return out_refusal->result;
		CHECK_FOR_INTERRUPTS();
		pg_usleep(CLUSTER_SEMANTIC_UTILITY_WAIT_STEP_US);
	}
}

static bool
semantic_activation_record_cas_mailbox_submit(
	uint64 expected_generation, uint64 expected_source_feature_bitmap,
	const uint8 desired_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES], uint64 *out_request_seq)
{
	ClusterSemanticActivationRecord desired;
	ClusterSemanticFormationBinding formation;

	if (expected_generation == UINT64_MAX || desired_bytes == NULL
		|| SemanticActivationUtilityMailbox == NULL
		|| cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES
		|| !cluster_semantic_activation_record_decode(
			desired_bytes, &desired, NULL)
		|| desired.record_generation != expected_generation + 1
		|| desired.coordinator_node != (uint32)cluster_node_id)
		return false;
	formation = (ClusterSemanticFormationBinding){
		.utility_request_seq = pg_atomic_read_u64(
			&SemanticActivationUtilityMailbox->utility_request_seq),
		.formation_epoch = desired.transition_epoch,
		.coordinator_incarnation = desired.coordinator_incarnation,
		.expected_record_generation = expected_generation,
	};
	if (!semantic_activation_qvotec_formation_matches(&formation))
		return false;

	/* RECORD_CAS already carries epoch/incarnation in its durable desired
	 * image.  Reuse the utility result words for the pending utility sequence
	 * and a second incarnation copy, so utility-slot ABA and byte mutation are
	 * rejected without widening either shared mailbox. */
	SemanticActivationUtilityMailbox->utility_result_feature_bit
		= formation.utility_request_seq;
	SemanticActivationUtilityMailbox->utility_result_expected_generation
		= formation.coordinator_incarnation;
	return semantic_activation_authority_mailbox_submit(
		CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_CAS, expected_generation,
		expected_source_feature_bitmap, desired_bytes, out_request_seq);
}

static bool
semantic_activation_record_read_mailbox_submit(uint64 *out_request_seq)
{
	uint8 zero[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES] = { 0 };

	return semantic_activation_authority_mailbox_submit(
		CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_READ, 0, 0, zero,
		out_request_seq);
}

static bool
semantic_activation_authority_mailbox_submit(
	ClusterSemanticAuthorityRequestKind request_kind, uint64 expected_generation,
	uint64 expected_source_feature_bitmap,
	const uint8 desired_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES],
	uint64 *out_request_seq)
{
	uint64 request_seq;
	uint64 completion_seq;

	if (SemanticActivationShmem == NULL || desired_bytes == NULL
		|| out_request_seq == NULL
		|| (request_kind != CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_CAS
			&& request_kind
				   != CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR
			&& request_kind
				   != CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_READ
			&& request_kind
				   != CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR_READ))
		return false;

	request_seq = pg_atomic_read_u64(&SemanticActivationShmem->record_cas_request_seq);
	completion_seq = pg_atomic_read_u64(&SemanticActivationShmem->record_cas_completion_seq);
	if (request_seq != completion_seq || request_seq == UINT64_MAX)
		return false;

	SemanticActivationShmem->record_cas_expected_generation = expected_generation;
	SemanticActivationShmem->record_cas_expected_source_feature_bitmap
		= expected_source_feature_bitmap;
	memcpy(SemanticActivationShmem->record_cas_desired_bytes, desired_bytes,
		   sizeof(SemanticActivationShmem->record_cas_desired_bytes));
	pg_atomic_write_u32(&SemanticActivationShmem->record_cas_request_kind,
						request_kind);
	pg_write_barrier();
	request_seq++;
	pg_atomic_write_u64(&SemanticActivationShmem->record_cas_request_seq, request_seq);
	*out_request_seq = request_seq;
	return true;
}

static bool
semantic_activation_authority_request_formation_binding(
	ClusterSemanticAuthorityRequestKind request_kind,
	ClusterSemanticFormationBinding *out)
{
	ClusterSemanticActivationRecord desired;
	uint64 expected_generation;

	if (SemanticActivationShmem == NULL
		|| SemanticActivationUtilityMailbox == NULL || out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	pg_read_barrier();
	if (pg_atomic_read_u32(
			&SemanticActivationShmem->record_cas_request_kind)
		!= request_kind)
		return false;

	switch (request_kind) {
	case CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_CAS:
		expected_generation
			= SemanticActivationShmem->record_cas_expected_generation;
		if (expected_generation == UINT64_MAX || cluster_node_id < 0
			|| cluster_node_id >= CLUSTER_MAX_NODES
			|| !cluster_semantic_activation_record_decode(
				SemanticActivationShmem->record_cas_desired_bytes,
				&desired, NULL)
			|| desired.record_generation != expected_generation + 1
			|| desired.coordinator_node != (uint32)cluster_node_id
			|| desired.coordinator_incarnation
				   != SemanticActivationUtilityMailbox
					  ->utility_result_expected_generation)
			return false;
		out->utility_request_seq
			= SemanticActivationUtilityMailbox->utility_result_feature_bit;
		out->formation_epoch = desired.transition_epoch;
		out->coordinator_incarnation = desired.coordinator_incarnation;
		out->expected_record_generation = expected_generation;
		return true;
	case CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR:
	case CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR_READ:
		out->utility_request_seq
			= SemanticActivationShmem
			  ->record_cas_expected_source_feature_bitmap;
		out->formation_epoch
			= SemanticActivationUtilityMailbox->utility_result_feature_bit;
		out->coordinator_incarnation
			= SemanticActivationUtilityMailbox
			  ->utility_result_expected_generation;
		out->expected_record_generation
			= SemanticActivationUtilityMailbox
			  ->utility_expected_record_generation;
		return true;
	case CLUSTER_SEMANTIC_AUTHORITY_REQUEST_NONE:
	case CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_READ:
	default:
		return false;
	}
}

bool
cluster_semantic_activation_qvotec_poll_record_cas(ClusterSemanticActivationCasRequest *out)
{
	ClusterSemanticFormationBinding formation;
	uint64 request_seq;
	uint64 completion_seq;

	if (SemanticActivationShmem == NULL || out == NULL)
		return false;

	request_seq = pg_atomic_read_u64(&SemanticActivationShmem->record_cas_request_seq);
	completion_seq = pg_atomic_read_u64(&SemanticActivationShmem->record_cas_completion_seq);
	if (request_seq == completion_seq || completion_seq == UINT64_MAX
		|| request_seq != completion_seq + 1)
		return false;

	pg_read_barrier();
	if (pg_atomic_read_u32(&SemanticActivationShmem->record_cas_request_kind)
		!= CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_CAS)
		return false;
	if (!semantic_activation_authority_request_formation_binding(
			CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_CAS, &formation)
		|| !semantic_activation_qvotec_formation_matches(&formation)) {
		(void)cluster_semantic_activation_qvotec_complete_record_cas(
			request_seq, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
		memset(out, 0, sizeof(*out));
		return false;
	}
	out->request_seq = request_seq;
	out->expected_generation = SemanticActivationShmem->record_cas_expected_generation;
	out->expected_source_feature_bitmap
		= SemanticActivationShmem->record_cas_expected_source_feature_bitmap;
	memcpy(out->desired_bytes, SemanticActivationShmem->record_cas_desired_bytes,
		   sizeof(out->desired_bytes));
	return true;
}

bool
cluster_semantic_activation_qvotec_complete_record_cas(uint64 request_seq,
												   ClusterSemanticActivationResult result)
{
	return semantic_activation_authority_mailbox_complete(
		CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_CAS, request_seq, result);
}

bool
cluster_semantic_activation_qvotec_poll_record_read(
	ClusterSemanticActivationReadRequest *out)
{
	uint64 request_seq;
	uint64 completion_seq;

	if (SemanticActivationShmem == NULL || out == NULL)
		return false;
	request_seq = pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_request_seq);
	completion_seq = pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_completion_seq);
	if (request_seq == completion_seq || completion_seq == UINT64_MAX
		|| request_seq != completion_seq + 1)
		return false;
	pg_read_barrier();
	if (pg_atomic_read_u32(
			&SemanticActivationShmem->record_cas_request_kind)
		!= CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_READ)
		return false;
	out->request_seq = request_seq;
	return true;
}

bool
cluster_semantic_activation_qvotec_complete_record_read(
	uint64 request_seq, ClusterSemanticActivationResult result,
	bool implicit_open,
	const uint8 selected_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES])
{
	ClusterSemanticActivationRecord decoded;
	uint8 zero[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES] = { 0 };

	if (!semantic_activation_authority_mailbox_completion_matches(
			CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_READ, request_seq)
		|| result < CLUSTER_SEMANTIC_ACTIVATION_OK
		|| result > CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE
		|| (result == CLUSTER_SEMANTIC_ACTIVATION_OK
			&& (selected_bytes == NULL
				|| (implicit_open
					&& !semantic_activation_bytes_are_zero(
						selected_bytes, sizeof(zero)))
				|| (!implicit_open
					&& !cluster_semantic_activation_record_decode(
						selected_bytes, &decoded, NULL)))))
		return false;

	memcpy(SemanticActivationShmem->record_cas_desired_bytes,
		   result == CLUSTER_SEMANTIC_ACTIVATION_OK ? selected_bytes : zero,
		   sizeof(zero));
	SemanticActivationShmem->record_cas_expected_source_feature_bitmap
		= result == CLUSTER_SEMANTIC_ACTIVATION_OK && implicit_open ? 1 : 0;
	pg_write_barrier();
	return semantic_activation_authority_mailbox_complete(
		CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_READ, request_seq, result);
}

static bool
semantic_activation_authority_mailbox_completion_matches(
	ClusterSemanticAuthorityRequestKind request_kind, uint64 request_seq)
{
	uint64 current_request_seq;
	uint64 completion_seq;

	if (SemanticActivationShmem == NULL)
		return false;

	current_request_seq = pg_atomic_read_u64(&SemanticActivationShmem->record_cas_request_seq);
	completion_seq = pg_atomic_read_u64(&SemanticActivationShmem->record_cas_completion_seq);
	if (current_request_seq != request_seq || completion_seq == UINT64_MAX
		|| completion_seq + 1 != request_seq)
		return false;
	pg_read_barrier();
	if (pg_atomic_read_u32(&SemanticActivationShmem->record_cas_request_kind)
		!= request_kind)
		return false;
	return true;
}

static bool
semantic_activation_authority_mailbox_complete(
	ClusterSemanticAuthorityRequestKind request_kind, uint64 request_seq,
	ClusterSemanticActivationResult result)
{
	if (result < CLUSTER_SEMANTIC_ACTIVATION_OK
		|| result > CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE
		|| !semantic_activation_authority_mailbox_completion_matches(
			request_kind, request_seq))
		return false;

	pg_atomic_write_u32(&SemanticActivationShmem->record_cas_result, (uint32)result);
	pg_write_barrier();
	pg_atomic_write_u64(&SemanticActivationShmem->record_cas_completion_seq, request_seq);
	return true;
}

static bool
semantic_activation_record_cas_mailbox_poll_completion(uint64 request_seq,
												ClusterSemanticActivationResult *out_result)
{
	ClusterSemanticFormationBinding formation;
	ClusterSemanticActivationResult result;

	if (out_result == NULL
		|| !semantic_activation_authority_mailbox_poll_completion(
			CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_CAS, request_seq,
			&result))
		return false;
	if (result == CLUSTER_SEMANTIC_ACTIVATION_OK
		&& (!semantic_activation_authority_request_formation_binding(
				CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_CAS, &formation)
			|| !semantic_activation_qvotec_formation_matches(&formation)))
		result = CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;
	*out_result = result;
	return true;
}

static bool
semantic_activation_record_read_mailbox_poll_completion(
	uint64 request_seq, ClusterSemanticActivationReadCompletion *out)
{
	ClusterSemanticActivationResult result;

	if (out == NULL
		|| !semantic_activation_authority_mailbox_poll_completion(
			CLUSTER_SEMANTIC_AUTHORITY_REQUEST_RECORD_READ, request_seq,
			&result))
		return false;

	memset(out, 0, sizeof(*out));
	out->result = result;
	out->implicit_open
		= SemanticActivationShmem
		  ->record_cas_expected_source_feature_bitmap
		  != 0;
	memcpy(out->selected_bytes,
		   SemanticActivationShmem->record_cas_desired_bytes,
		   sizeof(out->selected_bytes));
	return true;
}

static bool
semantic_activation_authority_mailbox_poll_completion(
	ClusterSemanticAuthorityRequestKind request_kind, uint64 request_seq,
	ClusterSemanticActivationResult *out_result)
{
	if (SemanticActivationShmem == NULL || out_result == NULL
		|| pg_atomic_read_u64(&SemanticActivationShmem->record_cas_completion_seq)
			   != request_seq)
		return false;

	pg_read_barrier();
	if (pg_atomic_read_u32(&SemanticActivationShmem->record_cas_request_kind)
		!= request_kind)
		return false;
	*out_result = (ClusterSemanticActivationResult)pg_atomic_read_u32(
		&SemanticActivationShmem->record_cas_result);
	return true;
}

bool
cluster_semantic_activation_undo_root_descriptor_mailbox_submit(
	const ClusterSemanticFormationBinding *formation,
	uint64 system_identifier,
	const uint8 desired_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES],
	uint64 *out_request_seq)
{
	if (formation == NULL || formation->utility_request_seq == 0
		|| formation->coordinator_incarnation == 0
		|| system_identifier == 0
		|| SemanticActivationUtilityMailbox == NULL
		|| pg_atomic_read_u32(
			   &SemanticActivationUtilityMailbox->utility_mailbox_state)
			   != SEMANTIC_ACTIVATION_UTILITY_MAILBOX_PENDING
		|| pg_atomic_read_u64(
			   &SemanticActivationUtilityMailbox->utility_request_seq)
			   != formation->utility_request_seq
		|| SemanticActivationUtilityMailbox->utility_expected_record_generation
			   != formation->expected_record_generation
		|| !cluster_semantic_activation_qvotec_pgrd_formation_matches(
			formation))
		return false;

	SemanticActivationUtilityMailbox->utility_result_feature_bit
		= formation->formation_epoch;
	SemanticActivationUtilityMailbox->utility_result_expected_generation
		= formation->coordinator_incarnation;
	return semantic_activation_authority_mailbox_submit(
		CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR,
		system_identifier, formation->utility_request_seq, desired_bytes,
		out_request_seq);
}

bool
cluster_semantic_activation_qvotec_poll_undo_root_descriptor(
	ClusterUndoRootDescriptorRequest *out)
{
	uint64 request_seq;
	uint64 completion_seq;

	if (SemanticActivationShmem == NULL || out == NULL)
		return false;
	request_seq = pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_request_seq);
	completion_seq = pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_completion_seq);
	if (request_seq == completion_seq || completion_seq == UINT64_MAX
		|| request_seq != completion_seq + 1)
		return false;
	pg_read_barrier();
	if (pg_atomic_read_u32(&SemanticActivationShmem->record_cas_request_kind)
		!= CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR)
		return false;
	out->request_seq = request_seq;
	out->system_identifier
		= SemanticActivationShmem->record_cas_expected_generation;
	memcpy(out->desired_bytes,
		   SemanticActivationShmem->record_cas_desired_bytes,
		   sizeof(out->desired_bytes));
	if (out->system_identifier == 0
		|| !semantic_activation_authority_request_formation_binding(
			CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR,
			&out->formation)
		|| !semantic_activation_qvotec_formation_matches(
			&out->formation)) {
		(void)cluster_semantic_activation_qvotec_complete_undo_root_descriptor(
			request_seq, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
		memset(out, 0, sizeof(*out));
		return false;
	}
	return true;
}

bool
cluster_semantic_activation_qvotec_complete_undo_root_descriptor(
	uint64 request_seq, ClusterSemanticActivationResult result)
{
	return semantic_activation_authority_mailbox_complete(
		CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR,
		request_seq, result);
}

bool
cluster_semantic_activation_undo_root_descriptor_mailbox_poll_completion(
	uint64 request_seq, ClusterSemanticActivationResult *out_result)
{
	return semantic_activation_authority_mailbox_poll_completion(
		CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR,
		request_seq, out_result);
}

static bool
semantic_activation_undo_root_descriptor_read_mailbox_submit(
	const ClusterSemanticFormationBinding *formation,
	uint64 system_identifier, uint64 *out_request_seq)
{
	uint8 zero[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES] = { 0 };

	if (formation == NULL || formation->utility_request_seq == 0
		|| formation->coordinator_incarnation == 0
		|| system_identifier == 0
		|| SemanticActivationUtilityMailbox == NULL
		|| pg_atomic_read_u32(
			   &SemanticActivationUtilityMailbox->utility_mailbox_state)
			   != SEMANTIC_ACTIVATION_UTILITY_MAILBOX_PENDING
		|| pg_atomic_read_u64(
			   &SemanticActivationUtilityMailbox->utility_request_seq)
			   != formation->utility_request_seq
		|| SemanticActivationUtilityMailbox->utility_expected_record_generation
			   != formation->expected_record_generation
		|| !cluster_semantic_activation_qvotec_pgrd_formation_matches(
			formation))
		return false;
	SemanticActivationUtilityMailbox->utility_result_feature_bit
		= formation->formation_epoch;
	SemanticActivationUtilityMailbox->utility_result_expected_generation
		= formation->coordinator_incarnation;
	return semantic_activation_authority_mailbox_submit(
		CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR_READ,
		system_identifier, formation->utility_request_seq, zero,
		out_request_seq);
}

bool
cluster_semantic_activation_qvotec_poll_undo_root_descriptor_read(
	ClusterUndoRootDescriptorReadRequest *out)
{
	uint64 request_seq;
	uint64 completion_seq;
	uint8 zero[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES] = { 0 };

	if (SemanticActivationShmem == NULL || out == NULL)
		return false;
	request_seq = pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_request_seq);
	completion_seq = pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_completion_seq);
	if (request_seq == completion_seq || completion_seq == UINT64_MAX
		|| request_seq != completion_seq + 1)
		return false;
	pg_read_barrier();
	if (pg_atomic_read_u32(
			&SemanticActivationShmem->record_cas_request_kind)
		!= CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR_READ)
		return false;
	out->request_seq = request_seq;
	out->system_identifier
		= SemanticActivationShmem->record_cas_expected_generation;
	if (out->system_identifier == 0
		|| !semantic_activation_authority_request_formation_binding(
			CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR_READ,
			&out->formation)
		|| !semantic_activation_qvotec_formation_matches(
			&out->formation)) {
		(void)cluster_semantic_activation_qvotec_complete_undo_root_descriptor_read(
			request_seq, CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD, zero);
		memset(out, 0, sizeof(*out));
		return false;
	}
	return true;
}

static bool
semantic_activation_qvotec_formation_matches(
	const ClusterSemanticFormationBinding *formation)
{
	SemanticActivationAdmissionSnapshot snapshot;
	uint64 current_epoch;
	uint64 current_incarnation;

	if (formation == NULL || formation->utility_request_seq == 0
		|| formation->coordinator_incarnation == 0
		|| SemanticActivationUtilityMailbox == NULL
		|| SemanticActivationShmem == NULL
		|| cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES
		|| pg_atomic_read_u32(
			   &SemanticActivationUtilityMailbox->utility_mailbox_state)
			   != SEMANTIC_ACTIVATION_UTILITY_MAILBOX_PENDING
		|| pg_atomic_read_u64(
			   &SemanticActivationUtilityMailbox->utility_request_seq)
			   != formation->utility_request_seq
		|| SemanticActivationUtilityMailbox->utility_expected_record_generation
			   != formation->expected_record_generation
		|| !cluster_qvotec_in_quorum())
		return false;

	current_epoch = cluster_epoch_get_current();
	current_incarnation = cluster_qvotec_get_self_incarnation();
	if (current_epoch != formation->formation_epoch
		|| current_incarnation != formation->coordinator_incarnation
		|| !semantic_activation_snapshot(&snapshot)
		|| snapshot.formation_epoch != formation->formation_epoch
		|| snapshot.record_generation
			   != formation->expected_record_generation
		|| !semantic_activation_r4_current_admitted_basis())
		return false;

	return cluster_qvotec_in_quorum()
		   && cluster_epoch_get_current() == current_epoch
		   && cluster_qvotec_get_self_incarnation() == current_incarnation;
}

bool
cluster_semantic_activation_qvotec_pgrd_formation_matches(
	const ClusterSemanticFormationBinding *formation)
{
	return semantic_activation_qvotec_formation_matches(formation);
}

bool
cluster_semantic_activation_qvotec_complete_undo_root_descriptor_read(
	uint64 request_seq, ClusterUndoRootDescriptorState state,
	const uint8 selected_bytes[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES])
{
	ClusterUndoRootDescriptorV1 descriptor;
	uint8 zero[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES] = { 0 };
	uint64 system_identifier;

	if (!semantic_activation_authority_mailbox_completion_matches(
			CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR_READ,
			request_seq)
		|| state < CLUSTER_UNDO_ROOT_DESCRIPTOR_UNPROVISIONED
		|| state > CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD
		|| selected_bytes == NULL)
		return false;
	system_identifier
		= SemanticActivationShmem->record_cas_expected_generation;
	if ((state == CLUSTER_UNDO_ROOT_DESCRIPTOR_UNPROVISIONED
		 && !semantic_activation_bytes_are_zero(
			 selected_bytes, sizeof(zero)))
		|| (state == CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID
			&& (cluster_undo_root_descriptor_decode(
					selected_bytes, system_identifier, &descriptor)
					!= CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID
				|| descriptor.descriptor_incarnation != 1
				|| descriptor.root_kind != CLUSTER_UNDO_ROOT_KIND_SHARED
				|| descriptor.owner_node != -1)))
		return false;

	memcpy(SemanticActivationShmem->record_cas_desired_bytes,
		   state == CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID ? selected_bytes
													 : zero,
		   sizeof(zero));
	pg_write_barrier();
	return semantic_activation_authority_mailbox_complete(
		CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR_READ,
		request_seq, (ClusterSemanticActivationResult)state);
}

static bool
semantic_activation_undo_root_descriptor_read_mailbox_poll_completion(
	uint64 request_seq, ClusterUndoRootDescriptorReadCompletion *out)
{
	uint32 state;

	if (SemanticActivationShmem == NULL || out == NULL
		|| pg_atomic_read_u64(
			   &SemanticActivationShmem->record_cas_completion_seq)
			   != request_seq)
		return false;
	pg_read_barrier();
	if (pg_atomic_read_u32(
			&SemanticActivationShmem->record_cas_request_kind)
		!= CLUSTER_SEMANTIC_AUTHORITY_REQUEST_UNDO_ROOT_DESCRIPTOR_READ)
		return false;
	state = pg_atomic_read_u32(&SemanticActivationShmem->record_cas_result);
	if (state > CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD)
		return false;
	out->state = (ClusterUndoRootDescriptorState)state;
	memcpy(out->selected_bytes,
		   SemanticActivationShmem->record_cas_desired_bytes,
		   sizeof(out->selected_bytes));
	return true;
}

static bool
semantic_activation_lmon_submit_pgrd_candidate(
	const uint8 candidate[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES],
	const ClusterSemanticFormationBinding *formation,
	uint64 system_identifier,
	uint64 *out_request_seq)
{
	ClusterUndoRootDescriptorV1 descriptor;

	if (candidate == NULL || system_identifier == 0 || out_request_seq == NULL
		|| cluster_undo_root_descriptor_decode(
			   candidate, system_identifier, &descriptor)
			   != CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID
		|| descriptor.descriptor_incarnation != 1
		|| descriptor.root_kind != CLUSTER_UNDO_ROOT_KIND_SHARED
		|| descriptor.owner_node != -1)
		return false;

	return cluster_semantic_activation_undo_root_descriptor_mailbox_submit(
		formation, system_identifier, candidate, out_request_seq);
}

static bool
semantic_activation_lmon_submit_pgrd_exact_retry(
	const char *root_directory,
	const ClusterSemanticFormationBinding *formation,
	uint64 system_identifier,
	uint64 *out_request_seq)
{
	uint8 candidate[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];

	if (cluster_undo_smgr_root_descriptor_read_candidate(
			root_directory, candidate)
		!= CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT)
		return false;
	return semantic_activation_lmon_submit_pgrd_candidate(
		candidate, formation, system_identifier, out_request_seq);
}

static bool
semantic_activation_lmon_shared_pgrd_root_directory(
	char root_directory[MAXPGPATH])
{
	int path_len;

	if (root_directory == NULL || cluster_shared_data_dir == NULL
		|| cluster_shared_data_dir[0] == '\0')
		return false;
	path_len = snprintf(root_directory, MAXPGPATH, "%s/pg_undo",
					cluster_shared_data_dir);
	return path_len >= 0 && path_len < MAXPGPATH;
}

static bool
semantic_activation_lmon_publish_fresh_shared_pgrd(
	const char *root_directory,
	const ClusterSemanticFormationBinding *formation,
	uint64 system_identifier,
	uint64 *out_request_seq)
{
	ClusterUndoRootDescriptorV1 descriptor;
	ClusterUndoSmgrRootMirrorState publish_state;
	uint8 desired[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];

	if (root_directory == NULL || system_identifier == 0
		|| out_request_seq == NULL)
		return false;
	memset(&descriptor, 0, sizeof(descriptor));
	descriptor.descriptor_incarnation = 1;
	descriptor.root_kind = CLUSTER_UNDO_ROOT_KIND_SHARED;
	descriptor.owner_node = -1;
	descriptor.root_ordinal = 0;
	descriptor.system_identifier = system_identifier;
	if (!cluster_undo_root_namespace_id(
			descriptor.descriptor_incarnation, descriptor.root_ordinal,
			&descriptor.namespace_id)
		|| !pg_strong_random(descriptor.root_uuid,
						 sizeof(descriptor.root_uuid))
		|| !cluster_undo_root_descriptor_encode(&descriptor, desired))
		return false;
	publish_state = cluster_undo_smgr_root_descriptor_publish(
		root_directory, desired);
	if (publish_state != CLUSTER_UNDO_SMGR_ROOT_MIRROR_PUBLISHED
		&& publish_state != CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT)
		return false;

	return semantic_activation_lmon_submit_pgrd_candidate(
		desired, formation, system_identifier, out_request_seq);
}

void
cluster_semantic_activation_register(const ClusterSemanticActivationDescriptor *descriptor)
{}

static bool
semantic_activation_ack_wire_value_valid(
	const ClusterSemanticActivationAckWireV1 *message)
{
	if (message == NULL
		|| message->kind < CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST
		|| message->kind > CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_ACK
		|| message->stage < CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
		|| message->stage > CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_OPEN_APPLIED
		|| message->result > CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REFUSED
		|| message->coordinator_node >= CLUSTER_MAX_NODES
		|| message->member_node >= CLUSTER_MAX_NODES
		|| (message->coordinator_node < 64
			? (message->admitted_members_lo
				   & (UINT64_C(1) << message->coordinator_node)) == 0
			: (message->admitted_members_hi
				   & (UINT64_C(1) << (message->coordinator_node - 64))) == 0)
		|| (message->member_node < 64
			? (message->admitted_members_lo
				   & (UINT64_C(1) << message->member_node)) == 0
			: (message->admitted_members_hi
				   & (UINT64_C(1) << (message->member_node - 64))) == 0)
		|| message->record_generation == 0 || message->round_nonce == 0
		|| (message->stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
			&& message->capability_sample_digest != 0)
		|| (message->stage != CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE
			&& message->capability_sample_digest == 0))
		return false;
	if (message->kind == CLUSTER_SEMANTIC_ACTIVATION_ACK_KIND_REQUEST)
		return message->result
				   == CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REQUEST
			&& message->reason == 0
			&& message->boot_id == 0
			&& message->admitted_incarnation == 0
			&& message->capability_word == 0;
	if (message->result == CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_OK)
		return message->reason == 0
			&& message->boot_id != 0
			&& message->admitted_incarnation != 0
			&& message->capability_word != 0;
	if (message->result == CLUSTER_SEMANTIC_ACTIVATION_ACK_RESULT_REFUSED)
		return message->reason > CLUSTER_SEMANTIC_ACTIVATION_OK
			&& message->reason <= CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE
			&& message->boot_id == 0
			&& message->admitted_incarnation == 0
			&& message->capability_word == 0;
	return false;
}

bool
cluster_semantic_activation_ack_wire_encode(
	const ClusterSemanticActivationAckWireV1 *message,
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES])
{
	uint8 encoded[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES];

	if (bytes == NULL || !semantic_activation_ack_wire_value_valid(message))
		return false;

	memset(encoded, 0, sizeof(encoded));
	semantic_activation_write_u32_le(encoded,
								 CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_MAGIC);
	semantic_activation_write_u16_le(encoded + 4,
								 CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_VERSION);
	encoded[6] = message->kind;
	encoded[7] = message->stage;
	semantic_activation_write_u32_le(encoded + 8, message->result);
	semantic_activation_write_u32_le(encoded + 12, message->reason);
	semantic_activation_write_u32_le(encoded + 16, message->coordinator_node);
	semantic_activation_write_u32_le(encoded + 20, message->member_node);
	semantic_activation_write_u64_le(encoded + 24, message->transition_epoch);
	semantic_activation_write_u64_le(encoded + 32, message->record_generation);
	semantic_activation_write_u64_le(encoded + 40, message->round_nonce);
	semantic_activation_write_u64_le(encoded + 48, message->source_feature_bitmap);
	semantic_activation_write_u64_le(encoded + 56, message->target_feature_bitmap);
	semantic_activation_write_u64_le(encoded + 64, message->rollback_feature_bitmap);
	semantic_activation_write_u64_le(encoded + 72, message->admitted_members_lo);
	semantic_activation_write_u64_le(encoded + 80, message->admitted_members_hi);
	semantic_activation_write_u64_le(encoded + 88, message->capability_sample_digest);
	semantic_activation_write_u64_le(encoded + 96, message->boot_id);
	semantic_activation_write_u64_le(encoded + 104, message->admitted_incarnation);
	semantic_activation_write_u32_le(encoded + 112, message->capability_word);
	memcpy(bytes, encoded, sizeof(encoded));
	return true;
}

bool
cluster_semantic_activation_ack_wire_decode(
	const uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_BYTES],
	ClusterSemanticActivationAckWireV1 *message)
{
	ClusterSemanticActivationAckWireV1 decoded;

	if (bytes == NULL || message == NULL)
		return false;
	if (semantic_activation_read_u32_le(bytes)
			!= CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_MAGIC
		|| semantic_activation_read_u16_le(bytes + 4)
			!= CLUSTER_SEMANTIC_ACTIVATION_ACK_WIRE_VERSION
		|| !semantic_activation_bytes_are_zero(bytes + 116, 4))
		return false;

	memset(&decoded, 0, sizeof(decoded));
	decoded.kind = bytes[6];
	decoded.stage = bytes[7];
	decoded.result = semantic_activation_read_u32_le(bytes + 8);
	decoded.reason = semantic_activation_read_u32_le(bytes + 12);
	decoded.coordinator_node = semantic_activation_read_u32_le(bytes + 16);
	decoded.member_node = semantic_activation_read_u32_le(bytes + 20);
	decoded.transition_epoch = semantic_activation_read_u64_le(bytes + 24);
	decoded.record_generation = semantic_activation_read_u64_le(bytes + 32);
	decoded.round_nonce = semantic_activation_read_u64_le(bytes + 40);
	decoded.source_feature_bitmap = semantic_activation_read_u64_le(bytes + 48);
	decoded.target_feature_bitmap = semantic_activation_read_u64_le(bytes + 56);
	decoded.rollback_feature_bitmap = semantic_activation_read_u64_le(bytes + 64);
	decoded.admitted_members_lo = semantic_activation_read_u64_le(bytes + 72);
	decoded.admitted_members_hi = semantic_activation_read_u64_le(bytes + 80);
	decoded.capability_sample_digest = semantic_activation_read_u64_le(bytes + 88);
	decoded.boot_id = semantic_activation_read_u64_le(bytes + 96);
	decoded.admitted_incarnation = semantic_activation_read_u64_le(bytes + 104);
	decoded.capability_word = semantic_activation_read_u32_le(bytes + 112);
	if (!semantic_activation_ack_wire_value_valid(&decoded))
		return false;
	*message = decoded;
	return true;
}

bool
cluster_semantic_activation_record_encode(const ClusterSemanticActivationRecord *record,
										  uint8 bytes[512])
{
	uint8 encoded[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	pg_crc32c crc;

	if (record == NULL || bytes == NULL || record->record_generation == 0
		|| record->phase < CLUSTER_SEMANTIC_PHASE_PREPARE
		|| record->phase > CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE
		|| (record->phase != CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE
			&& record->rollback_feature_bitmap != 0))
		return false;

	memset(encoded, 0, sizeof(encoded));
	semantic_activation_write_u32_le(encoded, CLUSTER_SEMANTIC_RECORD_MAGIC);
	semantic_activation_write_u16_le(encoded + 4, CLUSTER_SEMANTIC_RECORD_VERSION);
	semantic_activation_write_u16_le(encoded + 6, CLUSTER_SEMANTIC_RECORD_HEADER_LEN);
	semantic_activation_write_u64_le(encoded + 8, record->record_generation);
	encoded[16] = (uint8)record->phase;
	semantic_activation_write_u64_le(encoded + 24, record->source_feature_bitmap);
	semantic_activation_write_u64_le(encoded + 32, record->target_feature_bitmap);
	semantic_activation_write_u64_le(encoded + 40, record->transition_epoch);
	semantic_activation_write_u32_le(encoded + 48, record->coordinator_node);
	semantic_activation_write_u64_le(encoded + 56, record->coordinator_incarnation);
	semantic_activation_write_u64_le(encoded + 64, record->admitted_members_lo);
	semantic_activation_write_u64_le(encoded + 72, record->admitted_members_hi);
	semantic_activation_write_u64_le(encoded + 80, record->capability_sample_digest);
	semantic_activation_write_u64_le(encoded + 88, record->rollback_feature_bitmap);

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, encoded, CLUSTER_SEMANTIC_RECORD_CRC_OFFSET);
	FIN_CRC32C(crc);
	semantic_activation_write_u32_le(encoded + CLUSTER_SEMANTIC_RECORD_CRC_OFFSET, (uint32)crc);

	memcpy(bytes, encoded, sizeof(encoded));
	return true;
}

bool
cluster_semantic_activation_record_decode(const uint8 bytes[512],
										  ClusterSemanticActivationRecord *record,
										  ClusterSemanticActivationRefusal *refusal)
{
	ClusterSemanticActivationRecord decoded;
	uint32 stored_crc;
	pg_crc32c crc;

	if (bytes == NULL || record == NULL) {
		semantic_activation_set_refusal(refusal, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE, 0, 0);
		return false;
	}

	memset(&decoded, 0, sizeof(decoded));
	if (semantic_activation_read_u32_le(bytes) != CLUSTER_SEMANTIC_RECORD_MAGIC
		|| semantic_activation_read_u16_le(bytes + 4) != CLUSTER_SEMANTIC_RECORD_VERSION
		|| semantic_activation_read_u16_le(bytes + 6) != CLUSTER_SEMANTIC_RECORD_HEADER_LEN
		|| semantic_activation_read_u64_le(bytes + 8) == 0
		|| bytes[16] < CLUSTER_SEMANTIC_PHASE_PREPARE
		|| bytes[16] > CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE
		|| !semantic_activation_bytes_are_zero(bytes + 17, 7)
		|| !semantic_activation_bytes_are_zero(bytes + 52, 4)
		|| !semantic_activation_bytes_are_zero(bytes + 100,
											   CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES - 100)) {
		semantic_activation_set_refusal(refusal, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE, 0,
										semantic_activation_read_u64_le(bytes + 8));
		return false;
	}

	stored_crc = semantic_activation_read_u32_le(bytes + CLUSTER_SEMANTIC_RECORD_CRC_OFFSET);
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, CLUSTER_SEMANTIC_RECORD_CRC_OFFSET);
	FIN_CRC32C(crc);
	if (stored_crc != (uint32)crc) {
		semantic_activation_set_refusal(refusal, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE, 0,
										semantic_activation_read_u64_le(bytes + 8));
		return false;
	}

	decoded.record_generation = semantic_activation_read_u64_le(bytes + 8);
	decoded.phase = (ClusterSemanticActivationPhase)bytes[16];
	decoded.source_feature_bitmap = semantic_activation_read_u64_le(bytes + 24);
	decoded.target_feature_bitmap = semantic_activation_read_u64_le(bytes + 32);
	decoded.transition_epoch = semantic_activation_read_u64_le(bytes + 40);
	decoded.coordinator_node = semantic_activation_read_u32_le(bytes + 48);
	decoded.coordinator_incarnation = semantic_activation_read_u64_le(bytes + 56);
	decoded.admitted_members_lo = semantic_activation_read_u64_le(bytes + 64);
	decoded.admitted_members_hi = semantic_activation_read_u64_le(bytes + 72);
	decoded.capability_sample_digest = semantic_activation_read_u64_le(bytes + 80);
	decoded.rollback_feature_bitmap = semantic_activation_read_u64_le(bytes + 88);
	if (decoded.phase != CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE
		&& decoded.rollback_feature_bitmap != 0) {
		semantic_activation_set_refusal(refusal, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE, 0,
										decoded.record_generation);
		return false;
	}

	*record = decoded;
	semantic_activation_set_refusal(refusal, CLUSTER_SEMANTIC_ACTIVATION_OK, 0,
									decoded.record_generation);
	return true;
}

static void
semantic_activation_lmon_consume_phase3(void)
{
	ClusterReplacementPhase3HandoffItem item;

	while (cluster_replacement_phase3_handoff_poll_local(&item)) {
		const ClusterReplacementWireMessage *message = &item.message;
		uint64 current_epoch = cluster_epoch_get_current();

		/* The GES ingress already checked these fields.  Formation LMON
		 * rechecks them after the process-local handoff so a reconnect or
		 * formation change between producer and consumer is zero-mutation. */
		if (message->phase
				!= CLUSTER_REPLACEMENT_WIRE_PHASE_TARGET_RECOVERY_READY
			|| message->target_node_id != item.authenticated_source_node_id
			|| item.local_receiver_node_id != cluster_node_id
			|| item.control_connection_generation == 0
			|| message->epoch == UINT64_MAX
			|| message->epoch + 1 != current_epoch
			|| message->body.phase3.jcmk_generation == 0
			|| message->body.phase3.episode_state_generation == 0
			|| message->body.phase3.reserved != 0
			|| message->grammar_fingerprint
				   != CANDIDATE2_CORRECTED_A1_GRAMMAR_FINGERPRINT)
			continue;
		if (!cluster_sf_peer_capability_generation_matches(
				item.authenticated_source_node_id,
				CLUSTER_REPLACEMENT_PHASE3_REQUIRED_CAPS,
				item.control_connection_generation))
			continue;
		(void)cluster_reconfig_lmon_observe_replacement_ready(&item);
	}
}

static void
semantic_activation_lmon_consume_utility(void)
{
	SemanticActivationUtilityRequest request;
	ClusterSemanticActivationRefusal refusal;
	ClusterSemanticActivationResult result;
	uint32 effects = SEMANTIC_ACTIVATION_EFFECT_NONE;

	if (!semantic_activation_utility_mailbox_poll(&request))
		return;

	memset(&refusal, 0, sizeof(refusal));
	if (request.action != CLUSTER_SEMANTIC_ENABLE_ALL
		|| request.source_feature_bitmap != 0
		|| request.target_feature_bitmap
			   != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		|| request.rollback_feature_bitmap != 0) {
		semantic_activation_set_refusal(
			&refusal, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE, 0,
			request.expected_record_generation);
	} else {
		result = semantic_activation_preflight(
			request.action, request.expected_record_generation, &refusal,
			&effects);
		if (result == CLUSTER_SEMANTIC_ACTIVATION_OK) {
			ClusterSemanticFormationBinding formation = {
				.utility_request_seq = request.request_seq,
				.formation_epoch = cluster_epoch_get_current(),
				.coordinator_incarnation
					= cluster_qvotec_get_self_incarnation(),
				.expected_record_generation
					= request.expected_record_generation,
			};
			ClusterUndoRootDescriptorReadCompletion pgrd_read_completion;
			ClusterSemanticActivationResult pgrd_result;
			ClusterUndoSmgrRootMirrorState mirror_state;
			uint8 candidate[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
			char root_directory[MAXPGPATH];
			uint64 system_identifier = GetSystemIdentifier();
			bool have_root_directory;

			if (semantic_activation_lmon_pgrd_request_seq != 0) {
				if (semantic_activation_lmon_pgrd_utility_request_seq
						!= request.request_seq) {
					semantic_activation_set_refusal(
						&refusal, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD,
						CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
						request.expected_record_generation);
				} else if (!cluster_semantic_activation_undo_root_descriptor_mailbox_poll_completion(
						   semantic_activation_lmon_pgrd_request_seq,
						   &pgrd_result)) {
					return;
				} else {
					semantic_activation_lmon_pgrd_request_seq = 0;
					semantic_activation_lmon_pgrd_utility_request_seq = 0;
					if (pgrd_result != CLUSTER_SEMANTIC_ACTIVATION_OK)
						semantic_activation_set_refusal(
							&refusal, pgrd_result,
							CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
							request.expected_record_generation);
					else if (!cluster_semantic_activation_qvotec_pgrd_formation_matches(
							 &semantic_activation_lmon_pgrd_formation))
						semantic_activation_set_refusal(
							&refusal,
							CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD,
							CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
							request.expected_record_generation);
				}
			} else if (semantic_activation_lmon_pgrd_read_request_seq != 0) {
				if (semantic_activation_lmon_pgrd_read_utility_request_seq
						!= request.request_seq) {
					semantic_activation_set_refusal(
						&refusal, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD,
						CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
						request.expected_record_generation);
				} else if (!semantic_activation_undo_root_descriptor_read_mailbox_poll_completion(
						   semantic_activation_lmon_pgrd_read_request_seq,
						   &pgrd_read_completion)) {
					return;
				} else {
					semantic_activation_lmon_pgrd_read_request_seq = 0;
					semantic_activation_lmon_pgrd_read_utility_request_seq = 0;
					semantic_activation_lmon_pgrd_formation
						= semantic_activation_lmon_pgrd_read_formation;
					have_root_directory
						= semantic_activation_lmon_shared_pgrd_root_directory(
							root_directory);
					if (!cluster_semantic_activation_qvotec_pgrd_formation_matches(
							&semantic_activation_lmon_pgrd_read_formation)
						|| pgrd_read_completion.state
							!= CLUSTER_UNDO_ROOT_DESCRIPTOR_UNPROVISIONED
						|| !have_root_directory
						|| !semantic_activation_lmon_publish_fresh_shared_pgrd(
							root_directory,
							&semantic_activation_lmon_pgrd_formation,
							system_identifier,
							&semantic_activation_lmon_pgrd_request_seq)) {
						semantic_activation_set_refusal(
							&refusal,
							CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD,
							CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
							request.expected_record_generation);
					} else {
						semantic_activation_lmon_pgrd_utility_request_seq
							= request.request_seq;
						return;
					}
				}
			} else {
				have_root_directory
					= semantic_activation_lmon_shared_pgrd_root_directory(
						root_directory);
				if (have_root_directory
					&& !cluster_semantic_activation_qvotec_pgrd_formation_matches(
						&formation)) {
					semantic_activation_set_refusal(
						&refusal, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD,
						CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
						request.expected_record_generation);
				} else if (have_root_directory) {
					mirror_state
						= cluster_undo_smgr_root_descriptor_read_candidate(
							root_directory, candidate);
					semantic_activation_lmon_pgrd_formation = formation;
					if (mirror_state == CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT
						&& semantic_activation_lmon_submit_pgrd_candidate(
							candidate,
							&semantic_activation_lmon_pgrd_formation,
							system_identifier,
							&semantic_activation_lmon_pgrd_request_seq)) {
						semantic_activation_lmon_pgrd_utility_request_seq
							= request.request_seq;
						return;
					}
					semantic_activation_lmon_pgrd_read_formation = formation;
					if (mirror_state == CLUSTER_UNDO_SMGR_ROOT_MIRROR_ABSENT
						&& semantic_activation_undo_root_descriptor_read_mailbox_submit(
							&semantic_activation_lmon_pgrd_read_formation,
							system_identifier,
							&semantic_activation_lmon_pgrd_read_request_seq)) {
						semantic_activation_lmon_pgrd_read_utility_request_seq
							= request.request_seq;
						return;
					}
					semantic_activation_set_refusal(
						&refusal, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD,
						CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
						request.expected_record_generation);
				}
			}

			/* ADMITTED grants entry to PREPARE only.  Source close is the
			 * next durable-FSM edge after the PREPARE CAS, so this carrier
			 * remains nonterminal until that state is installed. */
			if (refusal.result == CLUSTER_SEMANTIC_ACTIVATION_OK) {
				result = CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED;
				semantic_activation_set_refusal(
					&refusal, result,
					CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
					request.expected_record_generation);
			}
		}
	}

	(void)semantic_activation_utility_mailbox_complete(
		request.request_seq, refusal.result, refusal.feature_bit,
		refusal.expected_generation);
}

static bool
semantic_activation_lmon_publish_gate(
	const SemanticActivationAdmissionSnapshot *snapshot, uint64 active_bits,
	uint64 record_generation, uint64 formation_epoch, bool transition_closed)
{
	uint64 expected_seq;

	if (snapshot == NULL || SemanticActivationShmem == NULL
		|| snapshot->seq > UINT64_MAX - 2)
		return false;
	expected_seq = snapshot->seq;
	if (!pg_atomic_compare_exchange_u64(
			&SemanticActivationShmem->admission_seq, &expected_seq,
			snapshot->seq + 1))
		return false;
	pg_write_barrier();
	pg_atomic_write_u64(&SemanticActivationShmem->active_bits, active_bits);
	pg_atomic_write_u64(&SemanticActivationShmem->record_generation,
						record_generation);
	pg_atomic_write_u64(&SemanticActivationShmem->formation_epoch,
						formation_epoch);
	pg_atomic_write_u32(&SemanticActivationShmem->transition_closed,
						transition_closed ? 1 : 0);
	pg_write_barrier();
	pg_atomic_write_u64(&SemanticActivationShmem->admission_seq,
						snapshot->seq + 2);
	return true;
}

static void
semantic_activation_lmon_sync_durable_record(
	const SemanticActivationAdmissionSnapshot *snapshot, uint64 current_epoch)
{
	ClusterSemanticActivationReadCompletion completion;
	uint64 request_seq;
	uint64 members_lo;
	uint64 members_hi;
	uint64 membership_epoch;
	bool local_is_member;

	if (semantic_activation_lmon_record_read_seq == 0) {
		if (semantic_activation_record_read_mailbox_submit(&request_seq))
			semantic_activation_lmon_record_read_seq = request_seq;
		return;
	}
	if (!semantic_activation_record_read_mailbox_poll_completion(
			semantic_activation_lmon_record_read_seq, &completion))
		return;
	semantic_activation_lmon_record_read_seq = 0;

	if (completion.result != CLUSTER_SEMANTIC_ACTIVATION_OK
		|| !completion.implicit_open
		|| !semantic_activation_bytes_are_zero(
			completion.selected_bytes,
			sizeof(completion.selected_bytes)))
		return;

	/* Majority legacy zero has no member tuple of its own.  It may open only
	 * the legacy SOURCE gate after the current formation has a nonempty exact
	 * membership SSOT containing this coordinator and remains on one epoch
	 * across the sample.  Nonzero PGSA records stay closed until the full
	 * member/capability ACK table is installed. */
	if (!cluster_reconfig_lmon_snapshot_admitted_membership(
			&members_lo, &members_hi, &membership_epoch)
		|| membership_epoch != current_epoch
		|| cluster_epoch_get_current() != current_epoch
		|| cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES)
		return;
	local_is_member
		= cluster_node_id < 64
			  ? (members_lo & (UINT64_C(1) << cluster_node_id)) != 0
			  : (members_hi
				 & (UINT64_C(1) << (cluster_node_id - 64))) != 0;
	if (!local_is_member)
		return;

	(void)semantic_activation_lmon_publish_gate(
		snapshot, 0, 0, current_epoch, false);
}

void
cluster_semantic_activation_lmon_tick(void)
{
	SemanticActivationAdmissionSnapshot snapshot;
	uint64 current_epoch;

	semantic_activation_ack_lmon_drain();
	if (SemanticActivationShmem == NULL)
		return;
	semantic_activation_lmon_consume_phase3();
	semantic_activation_lmon_consume_utility();
	/*
	 * D13 owns validated majority-zero/durable-OPEN publication.  Until that
	 * proof is available, odd or unreadable state remains fail-closed.
	 */
	if (!semantic_activation_snapshot(&snapshot))
		return;

	current_epoch = cluster_epoch_get_current();
	if (snapshot.formation_epoch == current_epoch) {
		if (snapshot.transition_closed)
			semantic_activation_lmon_sync_durable_record(
				&snapshot, current_epoch);
		return;
	}
	if (snapshot.seq > UINT64_MAX - 2)
		ereport(PANIC, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("semantic activation admission sequence exhausted"),
						errhint("Retain the shared-memory image and restart the cluster.")));

	(void)semantic_activation_lmon_publish_gate(
		&snapshot, snapshot.active_bits, snapshot.record_generation,
		current_epoch, true);
}

ClusterSemanticActivationResult
cluster_semantic_activation_submit(ClusterSemanticActivationAction action,
								   ClusterSemanticActivationRefusal *refusal)
{
	SemanticActivationAdmissionSnapshot snapshot;
	ClusterSemanticActivationResult result;
	uint64 request_seq;
	uint32 effects;

	if (!semantic_activation_snapshot(&snapshot)) {
		semantic_activation_set_refusal(
			refusal, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE, 0, 0);
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	}

	result = semantic_activation_preflight(
		action, snapshot.record_generation, refusal, &effects);
	if (result != CLUSTER_SEMANTIC_ACTIVATION_OK)
		return result;

	/* The first positive carrier is the approved ENABLE happy path.  Later
	 * actions remain typed-closed until their durable phase/floor predicates
	 * are wired; they cannot be inferred from the local active bitmap. */
	if (action != CLUSTER_SEMANTIC_ENABLE_ALL || snapshot.active_bits != 0) {
		semantic_activation_set_refusal(
			refusal, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE, 0,
			snapshot.record_generation);
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	}

	if (!semantic_activation_utility_mailbox_submit(
			action, snapshot.active_bits,
			CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0,
			snapshot.record_generation, &request_seq)) {
		semantic_activation_set_refusal(
			refusal, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD, 0,
			snapshot.record_generation);
		return CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;
	}

	return semantic_activation_utility_mailbox_wait(request_seq, refusal);
}

const ClusterSemanticActivationDescriptor *
cluster_semantic_activation_r4_descriptor(void)
{
	return &r4_descriptor;
}
