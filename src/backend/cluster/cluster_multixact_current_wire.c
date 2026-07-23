/*-------------------------------------------------------------------------
 *
 * cluster_multixact_current_wire.c
 *	  Strict wire validation for current-DML MultiXact authority requests.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
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
			&& message.prefix.kind == GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE
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
		|| header->kind != GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE
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
