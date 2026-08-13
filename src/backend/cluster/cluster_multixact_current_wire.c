/*-------------------------------------------------------------------------
 *
 * cluster_multixact_current_wire.c
 *	  Strict descriptor wire validation for current-DML MultiXact authority.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_multixact_current_wire.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-3.6b-multixact-current-dml.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_conf.h"
#include "cluster/cluster_multixact_current_wire.h"
#include "cluster/cluster_xid_stripe.h"


static bool
bytes_are_zero(const void *data, Size length)
{
	const uint8 *bytes = (const uint8 *)data;
	Size i;

	for (i = 0; i < length; i++)
		if (bytes[i] != 0)
			return false;
	return true;
}


static bool
wire_epoch_valid(uint64 current_epoch)
{
	return current_epoch != 0 && current_epoch <= UINT32_MAX;
}


static bool
wire_mxkey_valid(const ClusterCurrentMxKey *key, uint64 current_epoch)
{
	return key != NULL && wire_epoch_valid(current_epoch)
		   && key->origin_node_id < CLUSTER_MAX_NODES && key->reserved16 == 0
		   && key->reserved32 == 0 && MultiXactIdIsValid(key->multixact_id)
		   && key->cluster_epoch == (uint32)current_epoch;
}


static bool
wire_mxkey_equal(const ClusterCurrentMxKey *a, const ClusterCurrentMxKey *b)
{
	return a != NULL && b != NULL && memcmp(a, b, sizeof(*a)) == 0;
}


static bool
wire_tt_key_valid(const ClusterTTStatusKey *key, uint64 current_epoch, int32 expected_origin,
				  TransactionId expected_xid)
{
	return key != NULL && wire_epoch_valid(current_epoch) && expected_origin >= 0
		   && expected_origin < CLUSTER_MAX_NODES
		   && key->origin_node_id == (uint16)expected_origin && key->undo_segment_id != 0
		   && key->tt_slot_id != 0 && key->cluster_epoch == (uint32)current_epoch
		   && TransactionIdIsNormal(key->local_xid)
		   && (!TransactionIdIsValid(expected_xid) || key->local_xid == expected_xid)
		   && key->_reserved == 0 && key->_reserved2 == 0;
}


static bool
wire_proof_ask_valid(const ClusterCurrentMxProofAskWire *ask, uint32 total_count,
					 int32 local_node)
{
	return ask != NULL && TransactionIdIsNormal(ask->xid) && ask->member_ordinal < total_count
		   && ask->member_status <= MaxMultiXactStatus && ask->reserved8 == 0
		   && cluster_xid_origin_slot(ask->xid) == local_node;
}


bool
cluster_multixact_current_wire_validate_describe_forward(
	const void *payload, uint32 payload_length, int32 envelope_source, int32 local_node,
	uint64 current_epoch, ClusterCurrentMxDescribeForwardV2 *decoded)
{
	ClusterCurrentMxDescribeForwardV2 message;
	bool valid;

	if (decoded != NULL)
		memset(decoded, 0, sizeof(*decoded));
	if (payload == NULL || decoded == NULL || payload_length != sizeof(message)
		|| envelope_source < 0 || envelope_source >= CLUSTER_MAX_NODES || local_node < 0
		|| local_node >= CLUSTER_MAX_NODES || !wire_epoch_valid(current_epoch))
		return false;

	memcpy(&message, payload, sizeof(message));
	valid = message.prefix.request_id != 0 && message.prefix.epoch == current_epoch
			&& message.prefix.original_requester_node == envelope_source
			&& message.prefix.requester_backend_id > 0
			&& message.prefix.kind == CLUSTER_CURRENT_MX_DESCRIBE_KIND_FROZEN
			&& wire_mxkey_valid(&message.prefix.mxkey, current_epoch)
			&& message.prefix.mxkey.origin_node_id == (uint16)local_node
			&& bytes_are_zero(message.prefix.reserved_a, sizeof(message.prefix.reserved_a))
			&& bytes_are_zero(message.prefix.reserved_b, sizeof(message.prefix.reserved_b))
			&& message.trailer.magic == CLUSTER_CURRENT_MX_WIRE_MAGIC
			&& message.trailer.version == CLUSTER_CURRENT_MX_WIRE_VERSION
			&& message.trailer.flags == CLUSTER_CURRENT_MX_WIRE_FLAGS_NONE
			&& bytes_are_zero(message.trailer.reserved, sizeof(message.trailer.reserved));
	if (!valid)
		return false;

	*decoded = message;
	return true;
}


bool
cluster_multixact_current_wire_validate_proof_forward(
	const void *payload, uint32 payload_length, int32 envelope_source, int32 local_node,
	uint64 current_epoch, ClusterCurrentMxProofForwardV2 *decoded)
{
	ClusterCurrentMxProofForwardV2 message;
	uint16 semantic_chunk_count;
	uint8 i;

	if (decoded != NULL)
		memset(decoded, 0, sizeof(*decoded));
	if (payload == NULL || decoded == NULL || payload_length != sizeof(message)
		|| envelope_source < 0 || envelope_source >= CLUSTER_MAX_NODES || local_node < 0
		|| local_node >= CLUSTER_MAX_NODES || !wire_epoch_valid(current_epoch))
		return false;

	memcpy(&message, payload, sizeof(message));
	semantic_chunk_count = (uint16)message.prefix.chunk_count_minus_one + 1;
	if (message.prefix.request_id == 0 || message.prefix.epoch != current_epoch
		|| message.prefix.original_requester_node != envelope_source
		|| message.prefix.requester_backend_id <= 0
		|| message.prefix.kind != CLUSTER_CURRENT_MX_MEMBER_PROOF_KIND_FROZEN
		|| !wire_mxkey_valid(&message.prefix.mxkey, current_epoch)
		|| message.prefix.total_count < 2
		|| message.prefix.total_count > CLUSTER_CURRENT_MX_MAX_MEMBERS
		|| semantic_chunk_count > message.prefix.total_count
		|| semantic_chunk_count > CLUSTER_CURRENT_MX_MAX_CHUNKS
		|| message.prefix.chunk_ordinal >= semantic_chunk_count
		|| message.prefix.flags != CLUSTER_CURRENT_MX_WIRE_FLAGS_NONE
		|| !bytes_are_zero(message.prefix.reserved_a, sizeof(message.prefix.reserved_a))
		|| !bytes_are_zero(message.prefix.reserved_b, sizeof(message.prefix.reserved_b))
		|| message.trailer.magic != CLUSTER_CURRENT_MX_WIRE_MAGIC
		|| message.trailer.version != CLUSTER_CURRENT_MX_WIRE_VERSION
		|| message.trailer.reserved16 != 0)
		return false;

	switch ((ClusterCurrentMxProofBodyKind)message.prefix.body_kind) {
	case CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS:
		if (message.prefix.entry_count == 0
			|| message.prefix.entry_count > CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME)
			return false;
		for (i = 0; i < message.prefix.entry_count; i++) {
			uint8 j;

			if (!wire_proof_ask_valid(&message.trailer.body.asks[i],
									  message.prefix.total_count, local_node))
				return false;
			for (j = 0; j < i; j++)
				if (message.trailer.body.asks[j].member_ordinal
						== message.trailer.body.asks[i].member_ordinal
					|| message.trailer.body.asks[j].xid
						   == message.trailer.body.asks[i].xid)
					return false;
		}
		if (!bytes_are_zero(&message.trailer.body.asks[message.prefix.entry_count],
							 sizeof(message.trailer.body)
								 - message.prefix.entry_count
									   * sizeof(ClusterCurrentMxProofAskWire)))
			return false;
		break;

	case CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE: {
		const ClusterCurrentMxUpdaterChallengeWire *challenge
			= &message.trailer.body.updater.challenge;

		if (message.prefix.entry_count != 1 || !TransactionIdIsNormal(challenge->updater_xid)
			|| challenge->member_ordinal >= message.prefix.total_count
			|| challenge->member_status > MaxMultiXactStatus
			|| !ISUPDATE_from_mxstatus(challenge->member_status) || challenge->reserved8 != 0
			|| cluster_xid_origin_slot(challenge->updater_xid) != local_node
			|| !wire_tt_key_valid(&challenge->candidate_next_xmin_key, current_epoch,
								  local_node, challenge->updater_xid)
			|| !bytes_are_zero(message.trailer.body.updater.reserved,
							   sizeof(message.trailer.body.updater.reserved)))
			return false;
		break;
	}

	default:
		return false;
	}

	*decoded = message;
	return true;
}


static void
wire_init_proof_request(ClusterCurrentMxProofRequestPlan *plan, uint16 destination_node_id,
						const ClusterCurrentMxKey *key, uint64 descriptor_hash,
						uint32 total_count, uint64 request_id, uint64 current_epoch,
						int32 requester_node, int32 requester_backend_id, uint8 body_kind)
{
	memset(plan, 0, sizeof(*plan));
	plan->destination_node_id = destination_node_id;
	plan->request.prefix.request_id = request_id;
	plan->request.prefix.epoch = current_epoch;
	plan->request.prefix.mxkey = *key;
	plan->request.prefix.original_requester_node = requester_node;
	plan->request.prefix.requester_backend_id = requester_backend_id;
	plan->request.prefix.total_count = total_count;
	ClusterCurrentMxProofPrefixSetDescriptorHash(&plan->request.prefix, descriptor_hash);
	plan->request.prefix.body_kind = body_kind;
	plan->request.prefix.kind = CLUSTER_CURRENT_MX_MEMBER_PROOF_KIND_FROZEN;
	plan->request.trailer.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	plan->request.trailer.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
}


ClusterMxResolveResult
cluster_multixact_current_wire_build_proof_requests(
	const ClusterCurrentMxKey *key, const ClusterCurrentMxMemberDesc *members,
	const uint16 *member_origin_nodes, uint16 nmembers, uint64 descriptor_hash,
	const ClusterCurrentUpdaterChallenge *challenge, uint64 request_id, uint64 current_epoch,
	int32 requester_node, int32 requester_backend_id, ClusterCurrentMxProofRequestPlan *plans,
	uint16 plans_cap, uint16 *plan_count)
{
	uint16 origin_counts[CLUSTER_MAX_NODES];
	int updater_ordinal = -1;
	uint16 required_plans = 0;
	uint16 built = 0;
	uint16 origin;
	uint16 i;

	if (plan_count != NULL)
		*plan_count = 0;
	if (plans != NULL && plans_cap > 0)
		memset(plans, 0, sizeof(*plans) * plans_cap);
	if (nmembers > CLUSTER_CURRENT_MX_MAX_MEMBERS)
		return CMX_RESOLVE_SUPPORTED_LIMIT;
	if (key == NULL || members == NULL || member_origin_nodes == NULL || plans == NULL
		|| plan_count == NULL || nmembers < 2 || request_id == 0
		|| !wire_epoch_valid(current_epoch) || requester_node < 0
		|| requester_node >= CLUSTER_MAX_NODES || requester_backend_id <= 0
		|| !wire_mxkey_valid(key, current_epoch)
		|| cluster_multixact_current_validate_descriptor(
			   key, key->origin_node_id, (uint32)current_epoch, members, nmembers, nmembers)
			   != CMX_DESC_OK
		|| descriptor_hash
			   != cluster_multixact_current_descriptor_hash(key, members, nmembers))
		return CMX_RESOLVE_UNKNOWN;

	memset(origin_counts, 0, sizeof(origin_counts));
	for (i = 0; i < nmembers; i++) {
		int derived_origin = cluster_xid_origin_slot(members[i].xid);

		if (derived_origin < 0 || derived_origin >= CLUSTER_MAX_NODES
			|| member_origin_nodes[i] != (uint16)derived_origin)
			return CMX_RESOLVE_UNKNOWN;
		if (ISUPDATE_from_mxstatus(members[i].member_status))
			updater_ordinal = i;
		origin_counts[derived_origin]++;
	}

	if (challenge != NULL) {
		if (updater_ordinal < 0 || challenge->reserved16 != 0
			|| challenge->member_ordinal != (uint16)updater_ordinal
			|| challenge->updater_xid != members[updater_ordinal].xid
			|| !wire_tt_key_valid(&challenge->candidate_next_xmin_key, current_epoch,
								  member_origin_nodes[updater_ordinal],
								  members[updater_ordinal].xid))
			return CMX_RESOLVE_UNKNOWN;
		Assert(origin_counts[member_origin_nodes[updater_ordinal]] > 0);
		origin_counts[member_origin_nodes[updater_ordinal]]--;
		required_plans++;
	}

	for (origin = 0; origin < CLUSTER_MAX_NODES; origin++)
		if (origin_counts[origin] > 0)
			required_plans
				+= (origin_counts[origin] + CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME - 1)
				   / CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME;
	if (required_plans == 0 || required_plans > CLUSTER_CURRENT_MX_MAX_CHUNKS
		|| required_plans > plans_cap)
		return CMX_RESOLVE_UNKNOWN;

	for (origin = 0; origin < CLUSTER_MAX_NODES; origin++) {
		ClusterCurrentMxProofRequestPlan *plan = NULL;

		for (i = 0; i < nmembers; i++) {
			ClusterCurrentMxProofAskWire *ask;

			if (member_origin_nodes[i] != origin
				|| (challenge != NULL && i == (uint16)updater_ordinal))
				continue;
			if (plan == NULL
				|| plan->request.prefix.entry_count
					   == CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME) {
				plan = &plans[built++];
				wire_init_proof_request(plan, origin, key, descriptor_hash, nmembers, request_id,
								current_epoch, requester_node, requester_backend_id,
								CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS);
			}
			ask = &plan->request.trailer.body.asks[plan->request.prefix.entry_count++];
			ask->xid = members[i].xid;
			ask->member_ordinal = i;
			ask->member_status = members[i].member_status;
		}
	}

	if (challenge != NULL) {
		ClusterCurrentMxProofRequestPlan *plan = &plans[built++];
		ClusterCurrentMxUpdaterChallengeWire *wire_challenge;

		wire_init_proof_request(plan, member_origin_nodes[updater_ordinal], key, descriptor_hash,
							nmembers, request_id, current_epoch, requester_node,
							requester_backend_id,
							CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE);
		plan->request.prefix.entry_count = 1;
		wire_challenge = &plan->request.trailer.body.updater.challenge;
		wire_challenge->candidate_next_xmin_key = challenge->candidate_next_xmin_key;
		wire_challenge->updater_xid = challenge->updater_xid;
		wire_challenge->member_ordinal = challenge->member_ordinal;
		wire_challenge->member_status = members[updater_ordinal].member_status;
	}

	Assert(built == required_plans);
	for (i = 0; i < built; i++) {
		plans[i].request.prefix.chunk_ordinal = (uint8)i;
		plans[i].request.prefix.chunk_count_minus_one = (uint8)(built - 1);
	}
	*plan_count = built;
	return CMX_RESOLVE_OK;
}


ClusterMxDescribeResult
cluster_multixact_current_wire_validate_describe_reply(
	const void *payload, uint32 payload_length, int32 expected_source, uint64 current_epoch,
	uint64 expected_request_id, const ClusterCurrentMxKey *expected_key,
	ClusterCurrentMxMemberDesc *members, uint16 members_cap, uint16 *members_count,
	uint32 *reported_total_members)
{
	ClusterCurrentMxDescribeReplyPage page;
	const ClusterCurrentMxDescribeReplyHeader *header;
	ClusterMxDescribeResult result;
	Size expected_wire_length;

	if (members != NULL && members_cap > 0)
		memset(members, 0, sizeof(*members) * members_cap);
	if (members_count != NULL)
		*members_count = 0;
	if (reported_total_members != NULL)
		*reported_total_members = 0;

	if (payload == NULL || payload_length != sizeof(page) || expected_source < 0
		|| expected_source >= CLUSTER_MAX_NODES || expected_request_id == 0
		|| !wire_mxkey_valid(expected_key, current_epoch)
		|| expected_key->origin_node_id != (uint16)expected_source)
		return CMX_DESC_UNKNOWN;

	memcpy(&page, payload, sizeof(page));
	header = &page.header;
	if (header->magic != CLUSTER_CURRENT_MX_WIRE_MAGIC
		|| header->version != CLUSTER_CURRENT_MX_WIRE_VERSION
		|| header->kind != CLUSTER_CURRENT_MX_DESCRIBE_KIND_FROZEN
		|| header->flags != CLUSTER_CURRENT_MX_WIRE_FLAGS_NONE
		|| header->source_node_id != (uint32)expected_source
		|| header->request_id != expected_request_id
		|| !wire_mxkey_equal(&header->mxkey, expected_key)
		|| header->result > CMX_DESC_UNKNOWN || header->result == CMX_DESC_TIMEOUT
		|| header->chunk_ordinal != 0 || header->chunk_count_minus_one != 0
		|| header->reserved16 != 0 || header->reserved32 != 0
		|| header->wire_length < sizeof(*header) || header->wire_length > sizeof(page))
		return CMX_DESC_UNKNOWN;

	result = (ClusterMxDescribeResult)header->result;
	if (result != CMX_DESC_OK) {
		if (header->entry_count != 0 || header->descriptor_hash != 0
			|| header->wire_length != sizeof(*header)
			|| !bytes_are_zero((const uint8 *)&page + sizeof(*header),
							   sizeof(page) - sizeof(*header)))
			return CMX_DESC_UNKNOWN;
		if (result == CMX_DESC_SUPPORTED_LIMIT) {
			if (header->total_count <= CLUSTER_CURRENT_MX_MAX_MEMBERS)
				return CMX_DESC_UNKNOWN;
			if (reported_total_members != NULL)
				*reported_total_members = header->total_count;
		} else if (header->total_count != 0)
			return CMX_DESC_UNKNOWN;
		return result;
	}

	expected_wire_length
		= sizeof(*header) + sizeof(*page.members) * (Size)header->entry_count;
	if (header->total_count != header->entry_count || header->entry_count > members_cap
		|| header->entry_count > CLUSTER_CURRENT_MX_MAX_MEMBERS
		|| header->wire_length != expected_wire_length || members == NULL || members_count == NULL
		|| reported_total_members == NULL
		|| cluster_multixact_current_validate_descriptor(
			   expected_key, (uint16)expected_source, (uint32)current_epoch, page.members,
			   header->entry_count, header->total_count)
			   != CMX_DESC_OK
		|| header->descriptor_hash
			   != cluster_multixact_current_descriptor_hash(expected_key, page.members,
													header->entry_count)
		|| !bytes_are_zero((const uint8 *)&page + header->wire_length,
						   sizeof(page) - header->wire_length))
		return CMX_DESC_UNKNOWN;

	memcpy(members, page.members, sizeof(*members) * header->entry_count);
	*members_count = header->entry_count;
	*reported_total_members = header->total_count;
	return CMX_DESC_OK;
}
