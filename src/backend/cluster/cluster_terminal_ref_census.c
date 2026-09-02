/*-------------------------------------------------------------------------
 *
 * cluster_terminal_ref_census.c
 *	  Bounded canonical terminal-reference census state.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_terminal_ref_census.c
 *
 * NOTES
 *	  This is a pgrac-original file.  CTRC is reference-release evidence;
 *	  it never replaces the physical transaction-table status authority.
 *	  The tables are activation-sized and never shrink or evict at runtime.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "../../common/sha2_int.h"

#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/storage/cluster_undo_alloc.h"
#include "port/atomics.h"
#include "storage/spin.h"

#ifndef CLUSTER_CTRC_UNIT_TEST
#include "miscadmin.h"

#include "cluster/cluster_conf.h"
#include "cluster/cluster_shmem.h"
#include "storage/shmem.h"
#endif

#define CLUSTER_CTRC_SHMEM_MAGIC UINT32_C(0x43545243)
#define CLUSTER_CTRC_SHMEM_VERSION UINT32_C(2)
const uint8 cluster_ctrc_empty_sha256[32] = {
	0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14,
	0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24,
	0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c,
	0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55,
};

bool
cluster_ctrc_sha256_exact(const void *bytes, Size length, uint8 digest[32])
{
	pg_sha256_ctx context;

	if (bytes == NULL || digest == NULL)
		return false;
	pg_sha256_init(&context);
	pg_sha256_update(&context, (const uint8 *)bytes, length);
	pg_sha256_final(&context, digest);
	return true;
}

static bool
ctrc_bytes_zero(const void *address, Size length)
{
	const uint8 *bytes = (const uint8 *)address;
	Size		i;

	if (address == NULL)
		return false;
	for (i = 0; i < length; i++)
		if (bytes[i] != 0)
			return false;
	return true;
}

static bool
ctrc_txn_key_valid(const ClusterCtrcTxnKeyV1 *key)
{
	return key != NULL
		&& key->format_version == CLUSTER_CTRC_FORMAT_VERSION
		&& key->origin_node_id < CLUSTER_CTRC_MAX_PARTICIPANTS
		&& key->owner_instance == key->origin_node_id + 1
		&& key->segment_generation != 0
		&& key->slot_offset < TT_SLOTS_PER_SEGMENT
		&& TransactionIdIsValid(key->xid)
		&& key->system_identifier != 0
		&& key->origin_boot_incarnation != 0
		&& key->formation_epoch != 0
		&& key->admission_record_generation != 0
		&& key->root_descriptor_incarnation != 0
		&& key->root_id != 0
		&& key->root_generation != 0
		&& ctrc_bytes_zero(key->reserved, sizeof(key->reserved));
}

static bool
ctrc_participant_identity_valid(const ClusterCtrcTxnKeyV1 *key,
								const ClusterCtrcParticipantIdentity *identity)
{
	return key != NULL && identity != NULL
		&& identity->node_id < CLUSTER_CTRC_MAX_PARTICIPANTS
		&& identity->reserved16 == 0
		&& identity->capability_record_generation != 0
		&& identity->boot_incarnation != 0
		&& identity->formation_epoch == key->formation_epoch
		&& identity->admission_record_generation
		   == key->admission_record_generation;
}

static bool
ctrc_bytes_nonzero(const void *address, Size length)
{
	return address != NULL && !ctrc_bytes_zero(address, length);
}

static bool
ctrc_publication_request_equal(const ClusterCtrcPublicationIdV1 *stored,
							   const ClusterCtrcPublicationIdV1 *request)
{
	ClusterCtrcPublicationIdV1 normalized;

	if (stored == NULL || request == NULL)
		return false;
	normalized = *stored;
	normalized.journal_sequence = 0;
	normalized.key_sequence = 0;
	normalized.journal_slot_generation = 0;
	return memcmp(&normalized, request, sizeof(normalized)) == 0;
}

static bool
ctrc_target_pending_itl_valid(const ClusterCtrcTargetV1 *target)
{
	return target != NULL
		&& target->kind == CTRC_TARGET_PAGE_PENDING_ITL_SLOT
		&& target->spc_oid != InvalidOid
		&& target->db_oid != InvalidOid
		&& target->rel_number != InvalidOid
		&& target->block_number != InvalidBlockNumber
		&& target->publication_own_generation != 0
		&& target->publication_acquisition_epoch != 0
		&& target->relation_persistence != 0
		&& target->page_operation_kind != 0
		&& target->itl_slot_index == 0
		&& target->itl_slot_wrap == 0
		&& !TransactionIdIsValid(target->itl_xid)
		&& target->itl_class == 0
		&& ctrc_bytes_zero(target->reserved8, sizeof(target->reserved8))
		&& ctrc_bytes_zero(target->uba, sizeof(target->uba))
		&& ctrc_bytes_zero(target->planned_predecessor_sha256,
						   sizeof(target->planned_predecessor_sha256))
		&& ctrc_bytes_zero(target->planned_successor_sha256,
						   sizeof(target->planned_successor_sha256))
		&& target->offset_number == 0
		&& target->itemid_flags == 0
		&& target->itemid_offset == 0
		&& target->itemid_length == 0
		&& ctrc_bytes_zero(target->tuple_header_sha256,
						   sizeof(target->tuple_header_sha256))
		&& target->mx_origin_node_id == 0
		&& target->reserved16 == 0
		&& target->multixact_id == 0
		&& target->mx_cluster_epoch == 0
		&& target->descriptor_hash == 0
		&& ctrc_bytes_zero(target->successor_topology_sha256,
						   sizeof(target->successor_topology_sha256))
		&& target->intended_descriptor_hash == 0;
}

static bool
ctrc_target_exact_itl_valid(const ClusterCtrcTxnKeyV1 *key,
							const ClusterCtrcTargetV1 *target)
{
	return key != NULL && target != NULL
		&& target->kind == CTRC_TARGET_EXACT_ITL_SLOT
		&& target->spc_oid != InvalidOid
		&& target->db_oid != InvalidOid
		&& target->rel_number != InvalidOid
		&& target->block_number != InvalidBlockNumber
		&& target->publication_own_generation != 0
		&& target->publication_acquisition_epoch != 0
		&& target->relation_persistence != 0
		&& target->page_operation_kind != 0
		&& TransactionIdIsValid(target->itl_xid)
		&& target->itl_xid == key->xid
		&& target->itl_class != 0
		&& ctrc_bytes_zero(target->reserved8, sizeof(target->reserved8))
		&& ctrc_bytes_nonzero(target->uba, sizeof(target->uba))
		&& ctrc_bytes_nonzero(target->planned_predecessor_sha256,
							  sizeof(target->planned_predecessor_sha256))
		&& ctrc_bytes_nonzero(target->planned_successor_sha256,
							  sizeof(target->planned_successor_sha256))
		&& target->offset_number == 0
		&& target->itemid_flags == 0
		&& target->itemid_offset == 0
		&& target->itemid_length == 0
		&& ctrc_bytes_zero(target->tuple_header_sha256,
						   sizeof(target->tuple_header_sha256))
		&& target->mx_origin_node_id == 0
		&& target->reserved16 == 0
		&& target->multixact_id == 0
		&& target->mx_cluster_epoch == 0
		&& target->descriptor_hash == 0
		&& ctrc_bytes_zero(target->successor_topology_sha256,
						   sizeof(target->successor_topology_sha256))
		&& target->intended_descriptor_hash == 0;
}

static bool
ctrc_target_itl_finalizes_exact(const ClusterCtrcTargetV1 *pending,
							const ClusterCtrcTargetV1 *final_target)
{
	return pending != NULL && final_target != NULL
		&& pending->kind == CTRC_TARGET_PAGE_PENDING_ITL_SLOT
		&& final_target->kind == CTRC_TARGET_EXACT_ITL_SLOT
		&& pending->spc_oid == final_target->spc_oid
		&& pending->db_oid == final_target->db_oid
		&& pending->rel_number == final_target->rel_number
		&& pending->fork_number == final_target->fork_number
		&& pending->block_number == final_target->block_number
		&& pending->predecessor_page_lsn
		   == final_target->predecessor_page_lsn
		&& pending->predecessor_page_scn
		   == final_target->predecessor_page_scn
		&& pending->publication_own_generation
		   == final_target->publication_own_generation
		&& pending->publication_acquisition_epoch
		   == final_target->publication_acquisition_epoch
		&& pending->relation_persistence == final_target->relation_persistence
		&& pending->needs_wal == final_target->needs_wal
		&& pending->page_operation_kind == final_target->page_operation_kind;
}

static bool
ctrc_target_pending_offnum_valid(const ClusterCtrcPublicationIdV1 *publication,
								 const ClusterCtrcTargetV1 *target)
{
	return publication != NULL && target != NULL
		&& target->kind == CTRC_TARGET_PAGE_PENDING_OFFNUM
		&& target->spc_oid != InvalidOid
		&& target->db_oid != InvalidOid
		&& target->rel_number != InvalidOid
		&& target->block_number != InvalidBlockNumber
		&& target->publication_own_generation != 0
		&& target->publication_acquisition_epoch != 0
		&& target->relation_persistence != 0
		&& target->page_operation_kind == 0
		&& target->itl_slot_index == 0
		&& target->itl_slot_wrap == 0
		&& !TransactionIdIsValid(target->itl_xid)
		&& target->itl_class == 0
		&& ctrc_bytes_zero(target->reserved8, sizeof(target->reserved8))
		&& ctrc_bytes_zero(target->uba, sizeof(target->uba))
		&& ctrc_bytes_zero(target->planned_predecessor_sha256,
						   sizeof(target->planned_predecessor_sha256))
		&& ctrc_bytes_zero(target->planned_successor_sha256,
						   sizeof(target->planned_successor_sha256))
		&& target->offset_number == 0
		&& target->itemid_flags == 0
		&& target->itemid_offset == 0
		&& target->itemid_length == 0
		&& ctrc_bytes_zero(target->tuple_header_sha256,
						   sizeof(target->tuple_header_sha256))
		&& target->mx_origin_node_id == 0
		&& target->reserved16 == 0
		&& target->multixact_id == 0
		&& target->mx_cluster_epoch == 0
		&& target->descriptor_hash == 0
		&& ctrc_bytes_zero(target->successor_topology_sha256,
						   sizeof(target->successor_topology_sha256))
		&& target->intended_descriptor_hash != 0
		&& target->intended_descriptor_hash == publication->descriptor_hash;
}

static bool
ctrc_target_exact_tid_valid(const ClusterCtrcPublicationIdV1 *publication,
							const ClusterCtrcTargetV1 *target)
{
	bool hot_edge;

	if (publication == NULL || target == NULL)
		return false;
	hot_edge = publication->reference_kind == CTRC_REF_HOT_FOLLOW_EDGE;
	return target->kind == CTRC_TARGET_EXACT_TID
		&& target->spc_oid != InvalidOid
		&& target->db_oid != InvalidOid
		&& target->rel_number != InvalidOid
		&& target->block_number != InvalidBlockNumber
		&& target->publication_own_generation != 0
		&& target->publication_acquisition_epoch != 0
		&& target->relation_persistence != 0
		&& target->page_operation_kind == 0
		&& target->itl_slot_index == 0
		&& target->itl_slot_wrap == 0
		&& !TransactionIdIsValid(target->itl_xid)
		&& target->itl_class == 0
		&& ctrc_bytes_zero(target->reserved8, sizeof(target->reserved8))
		&& ctrc_bytes_zero(target->uba, sizeof(target->uba))
		&& ctrc_bytes_zero(target->planned_predecessor_sha256,
						   sizeof(target->planned_predecessor_sha256))
		&& ctrc_bytes_zero(target->planned_successor_sha256,
						   sizeof(target->planned_successor_sha256))
		&& target->offset_number != 0
		&& target->itemid_flags != 0
		&& target->itemid_offset != 0
		&& target->itemid_length != 0
		&& ctrc_bytes_nonzero(target->tuple_header_sha256,
						  sizeof(target->tuple_header_sha256))
		&& target->mx_origin_node_id < CLUSTER_CTRC_MAX_PARTICIPANTS
		&& target->reserved16 == 0
		&& MultiXactIdIsValid((MultiXactId)target->multixact_id)
		&& target->mx_cluster_epoch != 0
		&& target->descriptor_hash != 0
		&& target->descriptor_hash == publication->descriptor_hash
		&& (hot_edge
			? ctrc_bytes_nonzero(target->successor_topology_sha256,
							 sizeof(target->successor_topology_sha256))
			: ctrc_bytes_zero(target->successor_topology_sha256,
							 sizeof(target->successor_topology_sha256)))
		&& target->intended_descriptor_hash == 0;
}

static bool
ctrc_target_offnum_finalizes_exact(
	const ClusterCtrcPublicationIdV1 *publication,
	const ClusterCtrcTargetV1 *pending,
	const ClusterCtrcTargetV1 *final_target)
{
	return ctrc_target_pending_offnum_valid(publication, pending)
		&& ctrc_target_exact_tid_valid(publication, final_target)
		&& pending->spc_oid == final_target->spc_oid
		&& pending->db_oid == final_target->db_oid
		&& pending->rel_number == final_target->rel_number
		&& pending->fork_number == final_target->fork_number
		&& pending->block_number == final_target->block_number
		&& pending->predecessor_page_lsn
		   == final_target->predecessor_page_lsn
		&& pending->predecessor_page_scn
		   == final_target->predecessor_page_scn
		&& pending->publication_own_generation
		   == final_target->publication_own_generation
		&& pending->publication_acquisition_epoch
		   == final_target->publication_acquisition_epoch
		&& pending->relation_persistence == final_target->relation_persistence
		&& pending->needs_wal == final_target->needs_wal
		&& pending->intended_descriptor_hash
		   == final_target->descriptor_hash;
}

static bool
ctrc_publication_prepare_valid(
	const ClusterCtrcParticipantEntry *participant,
	const ClusterCtrcPublicationIdV1 *publication,
	const ClusterCtrcTargetV1 *target)
{
	return participant != NULL && publication != NULL
		&& participant->state == CTRC_PARTICIPANT_OPEN
		&& participant->grant_generation != 0
		&& ctrc_txn_key_valid(&participant->key)
		&& ctrc_participant_identity_valid(&participant->key,
										 &participant->identity)
		&& publication->requester_node_id == participant->identity.node_id
		&& publication->requester_boot_incarnation
		   == participant->identity.boot_incarnation
		&& publication->capability_record_generation
		   == participant->identity.capability_record_generation
		&& publication->requester_backend_id > 0
		&& publication->wire_request_id != 0
		&& publication->operation_id != 0
		&& publication->attempt_generation != 0
		&& publication->reserved8 == 0
		&& publication->journal_sequence == 0
		&& publication->key_sequence == 0
		&& publication->journal_slot_generation == 0
		&& publication->grant_generation == participant->grant_generation
		&& ((publication->reference_kind == CTRC_REF_HEAP_ITL_UBA
				&& publication->descriptor_hash == 0
				&& publication->member_ordinal == UINT16_MAX
				&& publication->member_role == 0
				&& publication->target_kind
				   == CTRC_TARGET_PAGE_PENDING_ITL_SLOT
				&& ctrc_target_pending_itl_valid(target))
			|| (publication->reference_kind >= CTRC_REF_CURRENT_MX_LOCKER
				&& publication->reference_kind <= CTRC_REF_HOT_FOLLOW_EDGE
				&& publication->descriptor_hash != 0
				&& publication->member_ordinal
				   < CLUSTER_CURRENT_MX_MAX_MEMBERS
				&& publication->member_role != 0
				&& publication->target_kind
				   == CTRC_TARGET_PAGE_PENDING_OFFNUM
				&& ctrc_target_pending_offnum_valid(publication, target)));
}

/* Header bytes are part of sizing, not a wire or shared ABI promise. */
typedef struct ClusterCtrcSharedHeader
{
	slock_t origin_lock;
	slock_t participant_lock;
	slock_t receipt_lock;
	uint32 magic;
	uint32 version;
	uint32 global_grant_generation;
	uint32 reserved32;
	uint64 total_bytes;
	uint64 origin_key_entries;
	uint64 participant_key_entries;
	uint64 receipt_entries;
	uint64 ack_summary_entries;
	uint64 origin_offset;
	uint64 participant_offset;
	uint64 receipt_offset;
	uint64 ack_offset;
	uint64 global_reservation_generation;
	uint64 global_journal_generation;
	uint64 global_seal_generation;
	uint64 full_refusal_count;
	uint8 reserved[32];
} ClusterCtrcSharedHeader;

static bool
ctrc_size_add(Size left, Size right, Size *result)
{
	if (result == NULL || left > SIZE_MAX - right)
		return false;
	*result = left + right;
	return true;
}

static bool
ctrc_size_mul(Size left, Size right, Size *result)
{
	if (result == NULL || (right != 0 && left > SIZE_MAX / right))
		return false;
	*result = left * right;
	return true;
}

static bool
ctrc_capacity_add_array(Size count, Size element_bytes, Size *total)
{
	Size bytes;

	return ctrc_size_mul(count, element_bytes, &bytes)
		&& ctrc_size_add(*total, MAXALIGN(bytes), total);
}

bool
cluster_ctrc_capacity_compute(Size n_buffers, int max_backends,
							  int declared_nodes,
							  ClusterCtrcCapacity *capacity)
{
	Size origin_entries;
	Size participant_entries;
	Size receipt_entries;
	Size backend_receipts;
	Size total;
	int nodes;

	if (capacity == NULL || max_backends < 0)
		return false;
	MemSet(capacity, 0, sizeof(*capacity));
	nodes = declared_nodes < 1 ? 1 : declared_nodes;
	if (nodes > CLUSTER_CTRC_MAX_PARTICIPANTS)
		nodes = CLUSTER_CTRC_MAX_PARTICIPANTS;
	if (!ctrc_size_mul((Size)CLUSTER_UNDO_SEGS_PER_INSTANCE,
					   (Size)TT_SLOTS_PER_SEGMENT, &origin_entries)
		|| !ctrc_size_mul(origin_entries, (Size)nodes,
						&participant_entries)
		|| !ctrc_size_mul((Size)max_backends,
					   (Size)CLUSTER_CURRENT_MX_MAX_MEMBERS,
					   &backend_receipts)
		|| !ctrc_size_add(n_buffers, backend_receipts, &receipt_entries))
		return false;

	total = MAXALIGN(sizeof(ClusterCtrcSharedHeader));
	if (!ctrc_capacity_add_array(origin_entries,
							 sizeof(ClusterCtrcOriginEntry), &total)
		|| !ctrc_capacity_add_array(participant_entries,
							 sizeof(ClusterCtrcParticipantEntry), &total)
		|| !ctrc_capacity_add_array(receipt_entries,
							 sizeof(ClusterCtrcReceipt), &total)
		|| !ctrc_capacity_add_array(participant_entries,
							 sizeof(ClusterCtrcLocalReleaseAckV1), &total))
		return false;

	capacity->origin_key_entries = origin_entries;
	capacity->participant_key_entries = participant_entries;
	capacity->receipt_entries = receipt_entries;
	capacity->ack_summary_entries = participant_entries;
	capacity->total_bytes = total;
	return true;
}

ClusterCtrcCapacityDisposition
cluster_ctrc_runtime_full_disposition(void)
{
	return CLUSTER_CTRC_CAPACITY_REFUSE_BEFORE_MUTATION;
}

#ifndef CLUSTER_CTRC_UNIT_TEST

static ClusterCtrcSharedHeader *CtrcShared = NULL;

static ClusterCtrcOriginEntry *
ctrc_origin_entries(void)
{
	return (ClusterCtrcOriginEntry *)((char *)CtrcShared
		+ CtrcShared->origin_offset);
}

static ClusterCtrcParticipantEntry *
ctrc_participant_entries(void)
{
	return (ClusterCtrcParticipantEntry *)((char *)CtrcShared
		+ CtrcShared->participant_offset);
}

static ClusterCtrcReceipt *
ctrc_receipt_entries(void)
{
	return (ClusterCtrcReceipt *)((char *)CtrcShared
		+ CtrcShared->receipt_offset);
}

static bool
ctrc_origin_index(const ClusterCtrcTxnKeyV1 *key, uint64 *index_out)
{
	uint32 first_segment;
	uint32 segment_ordinal;
	uint64 index;

	if (!ctrc_txn_key_valid(key) || index_out == NULL
		|| key->owner_instance == 0
		|| key->origin_node_id + 1 != key->owner_instance)
		return false;
	first_segment = ((uint32)key->owner_instance - 1)
		* CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	if (key->segment_id < first_segment
		|| key->segment_id >= first_segment + CLUSTER_UNDO_SEGS_PER_INSTANCE)
		return false;
	segment_ordinal = key->segment_id - first_segment;
	index = (uint64)segment_ordinal * TT_SLOTS_PER_SEGMENT
		+ key->slot_offset;
	if (CtrcShared == NULL || index >= CtrcShared->origin_key_entries)
		return false;
	*index_out = index;
	return true;
}

static bool
ctrc_participant_index(const ClusterCtrcTxnKeyV1 *key, uint64 *index_out)
{
	uint64 origin_index;
	uint64 nodes;
	uint64 index;

	if (CtrcShared == NULL || index_out == NULL
		|| !ctrc_origin_index(key, &origin_index)
		|| CtrcShared->origin_key_entries == 0
		|| CtrcShared->participant_key_entries
		   % CtrcShared->origin_key_entries != 0)
		return false;
	nodes = CtrcShared->participant_key_entries
		/ CtrcShared->origin_key_entries;
	if (nodes == 0 || key->origin_node_id >= nodes)
		return false;
	index = origin_index * nodes + key->origin_node_id;
	if (index >= CtrcShared->participant_key_entries)
		return false;
	*index_out = index;
	return true;
}

static bool
ctrc_runtime_capacity(ClusterCtrcCapacity *capacity)
{
	return cluster_ctrc_capacity_compute((Size)NBuffers, MaxBackends,
		cluster_conf_declared_node_count_early(), capacity);
}

Size
cluster_ctrc_shmem_size(void)
{
	ClusterCtrcCapacity capacity;

	if (!ctrc_runtime_capacity(&capacity))
		ereport(FATAL,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("CTRC shared-memory capacity overflows Size")));
	return capacity.total_bytes;
}

static void
ctrc_shared_fill_layout(ClusterCtrcSharedHeader *state,
						const ClusterCtrcCapacity *capacity)
{
	Size offset = MAXALIGN(sizeof(*state));

	state->origin_offset = offset;
	offset += MAXALIGN(capacity->origin_key_entries
					   * sizeof(ClusterCtrcOriginEntry));
	state->participant_offset = offset;
	offset += MAXALIGN(capacity->participant_key_entries
					   * sizeof(ClusterCtrcParticipantEntry));
	state->receipt_offset = offset;
	offset += MAXALIGN(capacity->receipt_entries * sizeof(ClusterCtrcReceipt));
	state->ack_offset = offset;
}

static bool
ctrc_shared_header_exact(const ClusterCtrcSharedHeader *state,
						 const ClusterCtrcCapacity *capacity)
{
	ClusterCtrcSharedHeader expected;

	if (state == NULL || capacity == NULL)
		return false;
	MemSet(&expected, 0, sizeof(expected));
	ctrc_shared_fill_layout(&expected, capacity);
	return state->magic == CLUSTER_CTRC_SHMEM_MAGIC
		&& state->version == CLUSTER_CTRC_SHMEM_VERSION
		&& state->total_bytes == capacity->total_bytes
		&& state->origin_key_entries == capacity->origin_key_entries
		&& state->participant_key_entries
		   == capacity->participant_key_entries
		&& state->receipt_entries == capacity->receipt_entries
		&& state->ack_summary_entries == capacity->ack_summary_entries
		&& state->origin_offset == expected.origin_offset
		&& state->participant_offset == expected.participant_offset
		&& state->receipt_offset == expected.receipt_offset
		&& state->ack_offset == expected.ack_offset
		&& state->global_grant_generation != 0
		&& state->reserved32 == 0
		&& state->global_reservation_generation != 0
		&& state->global_journal_generation != 0
		&& state->global_seal_generation != 0
		&& ctrc_bytes_zero(state->reserved, sizeof(state->reserved));
}

void
cluster_ctrc_shmem_init(void)
{
	ClusterCtrcCapacity capacity;
	bool found;

	if (!ctrc_runtime_capacity(&capacity))
		ereport(FATAL,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("CTRC shared-memory capacity is invalid")));
	CtrcShared = (ClusterCtrcSharedHeader *)ShmemInitStruct(
		"pgrac cluster terminal reference census", capacity.total_bytes,
		&found);
	if (!found)
	{
		MemSet(CtrcShared, 0, capacity.total_bytes);
		CtrcShared->magic = CLUSTER_CTRC_SHMEM_MAGIC;
		CtrcShared->version = CLUSTER_CTRC_SHMEM_VERSION;
		CtrcShared->total_bytes = capacity.total_bytes;
		CtrcShared->origin_key_entries = capacity.origin_key_entries;
		CtrcShared->participant_key_entries
			= capacity.participant_key_entries;
		CtrcShared->receipt_entries = capacity.receipt_entries;
		CtrcShared->ack_summary_entries = capacity.ack_summary_entries;
		SpinLockInit(&CtrcShared->origin_lock);
		SpinLockInit(&CtrcShared->participant_lock);
		SpinLockInit(&CtrcShared->receipt_lock);
		CtrcShared->global_grant_generation = 1;
		CtrcShared->global_reservation_generation = 1;
		CtrcShared->global_journal_generation = 1;
		CtrcShared->global_seal_generation = 1;
		ctrc_shared_fill_layout(CtrcShared, &capacity);
	}
	if (!ctrc_shared_header_exact(CtrcShared, &capacity))
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("CTRC shared-memory header is invalid")));
}

bool
cluster_ctrc_shmem_ready(void)
{
	ClusterCtrcCapacity capacity;

	return CtrcShared != NULL && ctrc_runtime_capacity(&capacity)
		&& ctrc_shared_header_exact(CtrcShared, &capacity);
}

static const ClusterShmemRegion cluster_ctrc_region = {
	.name = "pgrac cluster terminal reference census",
	.size_fn = cluster_ctrc_shmem_size,
	.init_fn = cluster_ctrc_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "cluster_ctrc",
	.reserved_flags = 0,
};

void
cluster_ctrc_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_ctrc_region);
}

#else

Size
cluster_ctrc_shmem_size(void)
{
	return 0;
}

void
cluster_ctrc_shmem_init(void)
{
}

void
cluster_ctrc_shmem_register(void)
{
}

bool
cluster_ctrc_shmem_ready(void)
{
	return false;
}

#endif

static bool
ctrc_allocate_journal_sequence(uint64 *sequence_out)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	bool allocated = false;

	if (sequence_out == NULL || !cluster_ctrc_shmem_ready())
		return false;
	*sequence_out = 0;
	SpinLockAcquire(&CtrcShared->origin_lock);
	if (CtrcShared->global_journal_generation != 0
		&& CtrcShared->global_journal_generation != UINT64_MAX)
	{
		*sequence_out = CtrcShared->global_journal_generation++;
		allocated = true;
	}
	else
		CtrcShared->full_refusal_count++;
	SpinLockRelease(&CtrcShared->origin_lock);
	return allocated;
#else
	static uint64 unit_journal_sequence = 1;

	if (sequence_out == NULL || unit_journal_sequence == UINT64_MAX)
		return false;
	*sequence_out = unit_journal_sequence++;
	return true;
#endif
}

ClusterCtrcOriginReserveResult
cluster_ctrc_origin_reserve_entry(
	ClusterCtrcOriginEntry *origin, const ClusterCtrcTxnKeyV1 *key,
	uint64 origin_index, uint64 reservation_generation,
	ClusterCtrcOriginReservation *reservation)
{
	ClusterCtrcOriginReserveResult result
		= CLUSTER_CTRC_ORIGIN_RESERVE_REFUSED;

	if (reservation != NULL)
		MemSet(reservation, 0, sizeof(*reservation));
	if (origin == NULL || reservation == NULL || !ctrc_txn_key_valid(key)
		|| reservation_generation == 0
		|| reservation_generation == UINT64_MAX)
		return result;

	if (origin->state == CTRC_ORIGIN_OPEN
		&& origin->reservation_generation != 0
		&& origin->grant_generation != 0
		&& memcmp(&origin->key, key, sizeof(*key)) == 0)
	{
		reservation_generation = origin->reservation_generation;
		result = CLUSTER_CTRC_ORIGIN_RESERVED_EXISTING_OPEN;
		reservation->kind = CTRC_ORIGIN_RESERVATION_EXISTING_OPEN;
	}
	else if (ctrc_bytes_zero(origin, sizeof(*origin)))
	{
		origin->key = *key;
		origin->reservation_generation = reservation_generation;
		result = CLUSTER_CTRC_ORIGIN_RESERVED_PENDING;
		reservation->kind = CTRC_ORIGIN_RESERVATION_PENDING_OWNED;
	}
	else
	{
		/* A second owner for the same physical origin slot is an invariant
		 * conflict.  Retain it rather than allowing either claimant to reuse
		 * the entry. */
		origin->state = CTRC_ORIGIN_BLOCKED;
		return result;
	}

	reservation->key = *key;
	reservation->origin_index = origin_index;
	reservation->reservation_generation = reservation_generation;
	reservation->valid = true;
	return result;
}

static bool
ctrc_origin_reservation_exact(
	const ClusterCtrcOriginEntry *origin, uint64 origin_index,
	const ClusterCtrcOriginReservation *reservation)
{
	return origin != NULL && reservation != NULL && reservation->valid
		&& reservation->kind == CTRC_ORIGIN_RESERVATION_PENDING_OWNED
		&& reservation->origin_index == origin_index
		&& reservation->reservation_generation != 0
		&& reservation->reservation_generation != UINT64_MAX
		&& ctrc_bytes_zero(reservation->reserved8,
							  sizeof(reservation->reserved8))
		&& origin->reservation_generation
		   == reservation->reservation_generation
		&& memcmp(&origin->key, &reservation->key,
				  sizeof(reservation->key)) == 0;
}

bool
cluster_ctrc_origin_cancel_pre_bind_entry(
	ClusterCtrcOriginEntry *origin, uint64 origin_index,
	const ClusterCtrcOriginReservation *reservation)
{
	if (!ctrc_origin_reservation_exact(origin, origin_index, reservation)
		|| origin->state != CTRC_ORIGIN_EMPTY
		|| origin->grant_generation != 0
		|| origin->touched_bitmap != 0 || origin->touched_count != 0
		|| origin->seal_generation != 0
		|| !ctrc_bytes_zero(origin->reserved8, sizeof(origin->reserved8))
		|| !ctrc_bytes_zero(origin->touched, sizeof(origin->touched)))
		return false;
	MemSet(origin, 0, sizeof(*origin));
	return true;
}

bool
cluster_ctrc_origin_block_post_bind_entry(
	ClusterCtrcOriginEntry *origin, uint64 origin_index,
	const ClusterCtrcOriginReservation *reservation)
{
	if (!ctrc_origin_reservation_exact(origin, origin_index, reservation))
		return false;
	if (origin->state == CTRC_ORIGIN_BLOCKED)
		return true;
	if ((origin->state != CTRC_ORIGIN_EMPTY
		 && origin->state != CTRC_ORIGIN_OPEN)
		|| origin->touched_bitmap != 0 || origin->touched_count != 0
		|| origin->seal_generation != 0)
		return false;
	origin->state = CTRC_ORIGIN_BLOCKED;
	return true;
}

ClusterCtrcOriginReserveResult
cluster_ctrc_origin_reserve_active(const ClusterCtrcTxnKeyV1 *key,
								   ClusterCtrcOriginReservation *reservation)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	ClusterCtrcOriginEntry *origin;
	uint64 index;
	uint64 reservation_generation;
	ClusterCtrcOriginReserveResult result;

	if (reservation == NULL)
		return CLUSTER_CTRC_ORIGIN_RESERVE_REFUSED;
	MemSet(reservation, 0, sizeof(*reservation));
	if (!cluster_ctrc_shmem_ready() || !ctrc_origin_index(key, &index))
		return CLUSTER_CTRC_ORIGIN_RESERVE_REFUSED;

	SpinLockAcquire(&CtrcShared->origin_lock);
	origin = &ctrc_origin_entries()[index];
	if (origin->state == CTRC_ORIGIN_OPEN)
		reservation_generation = origin->reservation_generation;
	else if (CtrcShared->global_reservation_generation != 0
			 && CtrcShared->global_reservation_generation != UINT64_MAX)
		reservation_generation
			= CtrcShared->global_reservation_generation++;
	else
	{
		CtrcShared->full_refusal_count++;
		SpinLockRelease(&CtrcShared->origin_lock);
		return CLUSTER_CTRC_ORIGIN_RESERVE_REFUSED;
	}
	result = cluster_ctrc_origin_reserve_entry(origin, key, index,
		reservation_generation, reservation);
	if (result == CLUSTER_CTRC_ORIGIN_RESERVE_REFUSED)
		CtrcShared->full_refusal_count++;
	SpinLockRelease(&CtrcShared->origin_lock);
	return result;
#else
	(void)key;
	if (reservation != NULL)
		MemSet(reservation, 0, sizeof(*reservation));
	return CLUSTER_CTRC_ORIGIN_RESERVE_REFUSED;
#endif
}

bool
cluster_ctrc_origin_cancel_pre_bind(
	const ClusterCtrcOriginReservation *reservation)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	uint64 index;
	bool cancelled;

	if (reservation == NULL || !cluster_ctrc_shmem_ready()
		|| !ctrc_origin_index(&reservation->key, &index)
		|| index != reservation->origin_index)
		return false;
	SpinLockAcquire(&CtrcShared->origin_lock);
	cancelled = cluster_ctrc_origin_cancel_pre_bind_entry(
		&ctrc_origin_entries()[index], index, reservation);
	SpinLockRelease(&CtrcShared->origin_lock);
	return cancelled;
#else
	(void)reservation;
	return false;
#endif
}

bool
cluster_ctrc_origin_block_post_bind(
	const ClusterCtrcOriginReservation *reservation)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	uint64 index;
	bool blocked;

	if (reservation == NULL || !cluster_ctrc_shmem_ready()
		|| !ctrc_origin_index(&reservation->key, &index)
		|| index != reservation->origin_index)
		return false;
	SpinLockAcquire(&CtrcShared->origin_lock);
	blocked = cluster_ctrc_origin_block_post_bind_entry(
		&ctrc_origin_entries()[index], index, reservation);
	SpinLockRelease(&CtrcShared->origin_lock);
	return blocked;
#else
	(void)reservation;
	return false;
#endif
}

bool
cluster_ctrc_origin_open_reserved(
	const ClusterCtrcOriginReservation *reservation,
	uint32 *grant_generation)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcOriginOpenResult result;
	uint64 index;
	uint32 grant;
	bool opened = false;

	if (grant_generation != NULL)
		*grant_generation = 0;
	if (reservation == NULL || grant_generation == NULL
		|| !reservation->valid
		|| (reservation->kind != CTRC_ORIGIN_RESERVATION_PENDING_OWNED
			&& reservation->kind != CTRC_ORIGIN_RESERVATION_EXISTING_OPEN)
		|| reservation->reservation_generation == 0
		|| !ctrc_bytes_zero(reservation->reserved8,
						   sizeof(reservation->reserved8))
		|| !cluster_ctrc_shmem_ready()
		|| !ctrc_origin_index(&reservation->key, &index)
		|| index != reservation->origin_index)
		return false;

	SpinLockAcquire(&CtrcShared->origin_lock);
	origin = &ctrc_origin_entries()[index];
	if (origin->state == CTRC_ORIGIN_OPEN
		&& origin->reservation_generation
		   == reservation->reservation_generation
		&& origin->grant_generation != 0
		&& memcmp(&origin->key, &reservation->key,
				  sizeof(reservation->key)) == 0)
	{
		grant = origin->grant_generation;
		opened = true;
	}
	else if (origin->state == CTRC_ORIGIN_EMPTY
			 && reservation->kind
				== CTRC_ORIGIN_RESERVATION_PENDING_OWNED
			 && origin->reservation_generation
				== reservation->reservation_generation
			 && origin->grant_generation == 0
			 && memcmp(&origin->key, &reservation->key,
					   sizeof(reservation->key)) == 0
			 && CtrcShared->global_grant_generation != UINT32_MAX)
	{
		grant = CtrcShared->global_grant_generation++;
		result = cluster_ctrc_origin_open_active(
			origin, &reservation->key, grant);
		opened = result == CLUSTER_CTRC_ORIGIN_OPENED
			|| result == CLUSTER_CTRC_ORIGIN_DUPLICATE;
	}
	else
		CtrcShared->full_refusal_count++;
	if (opened)
		*grant_generation = origin->grant_generation;
	SpinLockRelease(&CtrcShared->origin_lock);
	return opened;
#else
	(void)reservation;
	if (grant_generation != NULL)
		*grant_generation = 0;
	return false;
#endif
}

ClusterCtrcTouchResult
cluster_ctrc_origin_touch_exact(const ClusterCtrcTxnKeyV1 *key,
								const ClusterCtrcParticipantIdentity *participant,
								ClusterCtrcProofClass proof_class,
								uint32 *grant_out)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	ClusterCtrcTouchResult result;
	ClusterCtrcOriginEntry *origin;
	uint64 index;

	if (grant_out != NULL)
		*grant_out = 0;
	if (proof_class == CTRC_PROOF_UNKNOWN
		|| proof_class == CTRC_PROOF_COMMITTED
		|| proof_class == CTRC_PROOF_ABORTED)
		return CLUSTER_CTRC_TOUCH_TERMINAL_NO_GRANT;
	if (grant_out == NULL || !cluster_ctrc_shmem_ready()
		|| !ctrc_origin_index(key, &index))
		return CLUSTER_CTRC_TOUCH_REFUSED;

	SpinLockAcquire(&CtrcShared->origin_lock);
	origin = &ctrc_origin_entries()[index];
	if (memcmp(&origin->key, key, sizeof(*key)) != 0)
	{
		origin->state = CTRC_ORIGIN_BLOCKED;
		CtrcShared->full_refusal_count++;
		result = CLUSTER_CTRC_TOUCH_REFUSED;
	}
	else
		result = cluster_ctrc_origin_record_touched(
			origin, participant, proof_class, grant_out);
	SpinLockRelease(&CtrcShared->origin_lock);
	return result;
#else
	(void)key;
	(void)participant;
	(void)proof_class;
	if (grant_out != NULL)
		*grant_out = 0;
	return CLUSTER_CTRC_TOUCH_REFUSED;
#endif
}

/* Tasks 3-8 replace these fail-closed transition bodies in protocol order.
 * Keeping every entry point closed now prevents partial Task-2 storage from
 * becoming a second transaction authority. */
ClusterCtrcOriginOpenResult
cluster_ctrc_origin_open_active(ClusterCtrcOriginEntry *origin,
								const ClusterCtrcTxnKeyV1 *key,
								uint32 grant_generation)
{
	if (origin == NULL || !ctrc_txn_key_valid(key) || grant_generation == 0)
		return CLUSTER_CTRC_ORIGIN_REFUSED;
	if (origin->state == CTRC_ORIGIN_OPEN
		&& memcmp(&origin->key, key, sizeof(*key)) == 0
		&& origin->grant_generation == grant_generation)
		return CLUSTER_CTRC_ORIGIN_DUPLICATE;
	if (origin->state != CTRC_ORIGIN_EMPTY
		|| (!ctrc_bytes_zero(&origin->key, sizeof(origin->key))
			&& memcmp(&origin->key, key, sizeof(*key)) != 0)
		|| origin->grant_generation != 0
		|| origin->touched_bitmap != 0 || origin->touched_count != 0
		|| origin->seal_generation != 0
		|| !ctrc_bytes_zero(origin->reserved8, sizeof(origin->reserved8))
		|| !ctrc_bytes_zero(origin->touched, sizeof(origin->touched)))
	{
		origin->state = CTRC_ORIGIN_BLOCKED;
		return CLUSTER_CTRC_ORIGIN_REFUSED;
	}

	origin->key = *key;
	origin->grant_generation = grant_generation;
	origin->state = CTRC_ORIGIN_OPEN;
	return CLUSTER_CTRC_ORIGIN_OPENED;
}

ClusterCtrcTouchResult
cluster_ctrc_origin_record_touched(ClusterCtrcOriginEntry *origin,
								   const ClusterCtrcParticipantIdentity *participant,
								   ClusterCtrcProofClass proof_class,
								   uint32 *grant_out)
{
	if (grant_out != NULL)
		*grant_out = 0;
	if (proof_class == CTRC_PROOF_UNKNOWN
		|| proof_class == CTRC_PROOF_COMMITTED
		|| proof_class == CTRC_PROOF_ABORTED)
		return CLUSTER_CTRC_TOUCH_TERMINAL_NO_GRANT;
	if (origin == NULL || grant_out == NULL
		|| (proof_class != CTRC_PROOF_SELF
			&& proof_class != CTRC_PROOF_ACTIVE)
		|| origin->state != CTRC_ORIGIN_OPEN
		|| origin->grant_generation == 0
		|| !ctrc_txn_key_valid(&origin->key)
		|| !ctrc_participant_identity_valid(&origin->key, participant))
		return CLUSTER_CTRC_TOUCH_REFUSED;

	if ((origin->touched_bitmap & (UINT32_C(1) << participant->node_id)) != 0)
	{
		if (memcmp(&origin->touched[participant->node_id], participant,
				   sizeof(*participant)) == 0)
		{
			*grant_out = origin->grant_generation;
			return CLUSTER_CTRC_TOUCH_DUPLICATE;
		}
		origin->state = CTRC_ORIGIN_BLOCKED;
		return CLUSTER_CTRC_TOUCH_REFUSED;
	}
	if (origin->touched_count >= CLUSTER_CTRC_MAX_PARTICIPANTS)
	{
		origin->state = CTRC_ORIGIN_BLOCKED;
		return CLUSTER_CTRC_TOUCH_REFUSED;
	}

	origin->touched[participant->node_id] = *participant;
	origin->touched_bitmap |= UINT32_C(1) << participant->node_id;
	origin->touched_count++;
	*grant_out = origin->grant_generation;
	return CLUSTER_CTRC_TOUCH_RECORDED;
}

bool
cluster_ctrc_origin_has_exact_touch(const ClusterCtrcOriginEntry *origin,
									const ClusterCtrcParticipantIdentity *participant)
{
	return origin != NULL && participant != NULL
		&& participant->node_id < CLUSTER_CTRC_MAX_PARTICIPANTS
		&& (origin->touched_bitmap
			& (UINT32_C(1) << participant->node_id)) != 0
		&& memcmp(&origin->touched[participant->node_id], participant,
				  sizeof(*participant)) == 0;
}

ClusterCtrcParticipantOpenResult
cluster_ctrc_participant_open(ClusterCtrcParticipantEntry *participant,
							  const ClusterCtrcTxnKeyV1 *key,
							  uint32 grant_generation,
							  const ClusterCtrcParticipantIdentity *identity)
{
	if (participant == NULL || !ctrc_txn_key_valid(key)
		|| grant_generation == 0
		|| !ctrc_participant_identity_valid(key, identity))
		return CLUSTER_CTRC_PARTICIPANT_REFUSED;
	if (participant->state == CTRC_PARTICIPANT_OPEN
		&& participant->grant_generation == grant_generation
		&& memcmp(&participant->key, key, sizeof(*key)) == 0
		&& memcmp(&participant->identity, identity, sizeof(*identity)) == 0)
		return CLUSTER_CTRC_PARTICIPANT_DUPLICATE;
	if (!ctrc_bytes_zero(participant, sizeof(*participant)))
	{
		participant->state = CTRC_PARTICIPANT_BLOCKED;
		return CLUSTER_CTRC_PARTICIPANT_REFUSED;
	}

	participant->key = *key;
	participant->identity = *identity;
	participant->grant_generation = grant_generation;
	participant->state = CTRC_PARTICIPANT_OPEN;
	participant->next_key_sequence = 1;
	return CLUSTER_CTRC_PARTICIPANT_OPENED;
}

static ClusterCtrcPrepareResult
ctrc_receipt_prepare_with_sequence(
	ClusterCtrcParticipantEntry *participant,
	const ClusterCtrcPublicationIdV1 *publication,
	const ClusterCtrcTargetV1 *target, ClusterCtrcReceipt *receipt,
	uint64 supplied_journal_sequence)
{
	ClusterCtrcPublicationIdV1 stored_publication;
	uint64 journal_sequence;
	uint64 key_sequence;

	if (receipt == NULL)
		return CLUSTER_CTRC_PREPARE_CAPACITY;
	if (!ctrc_publication_prepare_valid(participant, publication, target))
		return CLUSTER_CTRC_PREPARE_REFUSED;

	if (pg_atomic_read_u32((pg_atomic_uint32 *)&receipt->state)
		!= CTRC_RECEIPT_FREE)
	{
		stored_publication = receipt->publication;
		if (memcmp(&receipt->key, &participant->key,
				   sizeof(receipt->key)) == 0
			&& ctrc_publication_request_equal(&stored_publication, publication)
			&& memcmp(&receipt->target, target, sizeof(*target)) == 0
			&& stored_publication.journal_sequence != 0
			&& stored_publication.key_sequence != 0
			&& stored_publication.journal_slot_generation != 0)
			return CLUSTER_CTRC_PREPARE_DUPLICATE;
		participant->state = CTRC_PARTICIPANT_BLOCKED;
		return CLUSTER_CTRC_PREPARE_REFUSED;
	}
	if (!ctrc_bytes_zero(receipt, sizeof(*receipt))
		|| participant->next_key_sequence == 0
		|| participant->next_key_sequence == UINT64_MAX
		|| participant->last_key_sequence == UINT64_MAX
		|| participant->next_key_sequence
		   != participant->last_key_sequence + 1
		|| (supplied_journal_sequence == 0
			? !ctrc_allocate_journal_sequence(&journal_sequence)
			: (journal_sequence = supplied_journal_sequence) == 0))
	{
		participant->state = CTRC_PARTICIPANT_BLOCKED;
		return CLUSTER_CTRC_PREPARE_REFUSED;
	}

	key_sequence = participant->next_key_sequence;
	MemSet(receipt, 0, sizeof(*receipt));
	receipt->key = participant->key;
	receipt->publication = *publication;
	receipt->publication.journal_sequence = journal_sequence;
	receipt->publication.key_sequence = key_sequence;
	/* Until Task 8 reclaims slots, each allocation has a unique generation. */
	receipt->publication.journal_slot_generation = journal_sequence;
	receipt->target = *target;
	pg_atomic_init_u32((pg_atomic_uint32 *)&receipt->state,
					   CTRC_RECEIPT_PREPARED);
	participant->next_key_sequence++;
	participant->last_key_sequence = key_sequence;
	participant->receipt_count++;
	participant->prepared_count++;
	return CLUSTER_CTRC_PREPARE_READY;
}

ClusterCtrcPrepareResult
cluster_ctrc_receipt_prepare(ClusterCtrcParticipantEntry *participant,
							 const ClusterCtrcPublicationIdV1 *publication,
							 const ClusterCtrcTargetV1 *target,
							 ClusterCtrcReceipt *receipt)
{
	return ctrc_receipt_prepare_with_sequence(
		participant, publication, target, receipt, 0);
}

static bool
ctrc_receipt_publication_matches(const ClusterCtrcReceipt *receipt,
	const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcPublicationIdV1 *publication)
{
	return receipt != NULL
		&& memcmp(&receipt->key, key, sizeof(*key)) == 0
		&& ctrc_publication_request_equal(&receipt->publication, publication);
}

ClusterCtrcPrepareResult
cluster_ctrc_receipt_prepare_table_locked(
	ClusterCtrcParticipantEntry *participant,
	const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *identity,
	uint32 grant_generation,
	const ClusterCtrcPublicationIdV1 *publication,
	const ClusterCtrcTargetV1 *target,
	ClusterCtrcReceipt *receipts, Size receipt_count,
	uint64 journal_sequence, uint64 *receipt_index_out)
{
	ClusterCtrcReceipt *free_receipt = NULL;
	uint64 free_index = 0;
	Size i;

	if (receipt_index_out != NULL)
		*receipt_index_out = UINT64_MAX;
	if (participant == NULL || key == NULL || identity == NULL
		|| publication == NULL || target == NULL || receipts == NULL
		|| receipt_count == 0 || journal_sequence == 0
		|| receipt_index_out == NULL
		|| cluster_ctrc_participant_open(participant, key,
			grant_generation, identity) == CLUSTER_CTRC_PARTICIPANT_REFUSED
		|| !ctrc_publication_prepare_valid(participant, publication, target))
		return CLUSTER_CTRC_PREPARE_REFUSED;

	for (i = 0; i < receipt_count; i++)
	{
		uint32 state = pg_atomic_read_u32(
			(pg_atomic_uint32 *)&receipts[i].state);

		if (ctrc_receipt_publication_matches(&receipts[i], key,
				publication))
		{
			if (state != CTRC_RECEIPT_FREE
				&& memcmp(&receipts[i].target, target,
					sizeof(*target)) == 0
				&& receipts[i].publication.journal_sequence != 0
				&& receipts[i].publication.key_sequence != 0
				&& receipts[i].publication.journal_slot_generation != 0)
			{
				*receipt_index_out = (uint64)i;
				return CLUSTER_CTRC_PREPARE_DUPLICATE;
			}
			participant->state = CTRC_PARTICIPANT_BLOCKED;
			return CLUSTER_CTRC_PREPARE_REFUSED;
		}
		if (free_receipt == NULL && state == CTRC_RECEIPT_FREE
			&& ctrc_bytes_zero(&receipts[i], sizeof(receipts[i])))
		{
			free_receipt = &receipts[i];
			free_index = (uint64)i;
		}
	}
	if (free_receipt == NULL)
		return CLUSTER_CTRC_PREPARE_CAPACITY;
	if (ctrc_receipt_prepare_with_sequence(participant, publication, target,
			free_receipt, journal_sequence) != CLUSTER_CTRC_PREPARE_READY)
		return CLUSTER_CTRC_PREPARE_REFUSED;
	*receipt_index_out = free_index;
	return CLUSTER_CTRC_PREPARE_READY;
}

#ifndef CLUSTER_CTRC_UNIT_TEST
static void
ctrc_receipt_handle_fill(ClusterCtrcReceiptHandle *handle,
	ClusterCtrcParticipantEntry *participant, uint64 participant_index,
	ClusterCtrcReceipt *receipt, uint64 receipt_index)
{
	MemSet(handle, 0, sizeof(*handle));
	handle->participant = participant;
	handle->receipt = receipt;
	handle->key = receipt->key;
	handle->participant_index = participant_index;
	handle->receipt_index = receipt_index;
	handle->journal_slot_generation
		= receipt->publication.journal_slot_generation;
	handle->valid = true;
}

static bool
ctrc_receipt_handle_exact(const ClusterCtrcReceiptHandle *handle)
{
	return handle != NULL && handle->valid
		&& cluster_ctrc_shmem_ready()
		&& ctrc_bytes_zero(handle->reserved8, sizeof(handle->reserved8))
		&& handle->participant_index < CtrcShared->participant_key_entries
		&& handle->receipt_index < CtrcShared->receipt_entries
		&& handle->participant
		   == &ctrc_participant_entries()[handle->participant_index]
		&& handle->receipt == &ctrc_receipt_entries()[handle->receipt_index]
		&& handle->journal_slot_generation != 0
		&& handle->receipt->publication.journal_slot_generation
		   == handle->journal_slot_generation
		&& memcmp(&handle->participant->key, &handle->key,
				  sizeof(handle->key)) == 0
		&& memcmp(&handle->receipt->key, &handle->key,
				  sizeof(handle->key)) == 0;
}
#endif

ClusterCtrcPrepareResult
cluster_ctrc_receipt_prepare_shared(
	const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *identity,
	uint32 grant_generation,
	const ClusterCtrcPublicationIdV1 *publication,
	const ClusterCtrcTargetV1 *target,
	ClusterCtrcReceiptHandle *handle)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	ClusterCtrcParticipantEntry *participant;
	ClusterCtrcReceipt *receipts;
	ClusterCtrcPrepareResult result;
	uint64 participant_index;
	uint64 receipt_index = UINT64_MAX;
	uint64 journal_sequence;

	if (handle != NULL)
		MemSet(handle, 0, sizeof(*handle));
	if (handle == NULL || identity == NULL || publication == NULL
		|| target == NULL || cluster_node_id < 0
		|| identity->node_id != (uint16)cluster_node_id
		|| !cluster_ctrc_shmem_ready()
		|| !ctrc_participant_index(key, &participant_index)
		|| !ctrc_allocate_journal_sequence(&journal_sequence))
		return CLUSTER_CTRC_PREPARE_REFUSED;

	participant = &ctrc_participant_entries()[participant_index];
	receipts = ctrc_receipt_entries();
	SpinLockAcquire(&CtrcShared->participant_lock);
	SpinLockAcquire(&CtrcShared->receipt_lock);
	result = cluster_ctrc_receipt_prepare_table_locked(participant, key,
		identity, grant_generation, publication, target, receipts,
		CtrcShared->receipt_entries, journal_sequence, &receipt_index);
	if (result == CLUSTER_CTRC_PREPARE_CAPACITY)
	{
		(void)pg_atomic_fetch_add_u64(
			(pg_atomic_uint64 *)&CtrcShared->full_refusal_count, 1);
	}
	if (result == CLUSTER_CTRC_PREPARE_READY
		|| result == CLUSTER_CTRC_PREPARE_DUPLICATE)
		ctrc_receipt_handle_fill(handle, participant, participant_index,
			&receipts[receipt_index], receipt_index);

	SpinLockRelease(&CtrcShared->receipt_lock);
	SpinLockRelease(&CtrcShared->participant_lock);
	return result;
#else
	(void)key;
	(void)identity;
	(void)grant_generation;
	(void)publication;
	(void)target;
	if (handle != NULL)
		MemSet(handle, 0, sizeof(*handle));
	return CLUSTER_CTRC_PREPARE_REFUSED;
#endif
}

ClusterCtrcApplyResult
cluster_ctrc_receipt_apply_prepared(ClusterCtrcParticipantEntry *participant,
								ClusterCtrcReceipt *receipt,
								const ClusterCtrcTargetV1 *final_target,
								ClusterCtrcApplyToken *token)
{
	uint32 expected;
	bool target_valid;
	bool target_finalizes;

	if (token != NULL)
		MemSet(token, 0, sizeof(*token));
	if (participant == NULL || receipt == NULL || token == NULL
		|| memcmp(&receipt->key, &participant->key,
				  sizeof(receipt->key)) != 0
		|| receipt->publication.grant_generation
		   != participant->grant_generation
		|| receipt->publication.journal_sequence == 0
		|| receipt->publication.key_sequence == 0
		|| receipt->publication.journal_slot_generation == 0)
		return CLUSTER_CTRC_APPLY_FAIL_CLOSED;
	target_valid = receipt->publication.reference_kind == CTRC_REF_HEAP_ITL_UBA
		? ctrc_target_exact_itl_valid(&participant->key, final_target)
		: ctrc_target_exact_tid_valid(&receipt->publication, final_target);
	if (!target_valid)
		return CLUSTER_CTRC_APPLY_FAIL_CLOSED;

	expected = pg_atomic_read_u32((pg_atomic_uint32 *)&receipt->state);
	if (expected == CTRC_RECEIPT_APPLIED)
	{
		if (memcmp(&receipt->target, final_target,
				   sizeof(*final_target)) != 0)
			return CLUSTER_CTRC_APPLY_FAIL_CLOSED;
		token->valid = true;
		token->journal_sequence = receipt->publication.journal_sequence;
		token->key_sequence = receipt->publication.key_sequence;
		token->journal_slot_generation
			= receipt->publication.journal_slot_generation;
		return CLUSTER_CTRC_APPLY_APPLIED;
	}
	if (expected != CTRC_RECEIPT_PREPARED)
		return CLUSTER_CTRC_APPLY_FAIL_CLOSED;
	target_finalizes
		= receipt->publication.reference_kind == CTRC_REF_HEAP_ITL_UBA
		? ctrc_target_itl_finalizes_exact(&receipt->target, final_target)
		: ctrc_target_offnum_finalizes_exact(
			&receipt->publication, &receipt->target, final_target);
	if (participant->state != CTRC_PARTICIPANT_OPEN || !target_finalizes)
		return CLUSTER_CTRC_APPLY_RETRY_REQUIRED;

	receipt->target = *final_target;
	pg_write_barrier();
	expected = CTRC_RECEIPT_PREPARED;
	if (!pg_atomic_compare_exchange_u32((pg_atomic_uint32 *)&receipt->state,
									&expected, CTRC_RECEIPT_APPLIED))
		return expected == CTRC_RECEIPT_PREPARED
			? CLUSTER_CTRC_APPLY_RETRY_REQUIRED
			: CLUSTER_CTRC_APPLY_FAIL_CLOSED;
	(void)pg_atomic_fetch_sub_u64(
		(pg_atomic_uint64 *)&participant->prepared_count, 1);
	(void)pg_atomic_fetch_add_u64(
		(pg_atomic_uint64 *)&participant->applied_count, 1);
	token->valid = true;
	token->journal_sequence = receipt->publication.journal_sequence;
	token->key_sequence = receipt->publication.key_sequence;
	token->journal_slot_generation
		= receipt->publication.journal_slot_generation;
	return CLUSTER_CTRC_APPLY_APPLIED;
}

ClusterCtrcApplyResult
cluster_ctrc_receipt_apply_shared(
	const ClusterCtrcReceiptHandle *handle,
	const ClusterCtrcTargetV1 *final_target, ClusterCtrcApplyToken *token)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	if (!ctrc_receipt_handle_exact(handle))
	{
		if (token != NULL)
			MemSet(token, 0, sizeof(*token));
		return CLUSTER_CTRC_APPLY_FAIL_CLOSED;
	}
	return cluster_ctrc_receipt_apply_prepared(
		handle->participant, handle->receipt, final_target, token);
#else
	(void)handle;
	(void)final_target;
	if (token != NULL)
		MemSet(token, 0, sizeof(*token));
	return CLUSTER_CTRC_APPLY_FAIL_CLOSED;
#endif
}

bool
cluster_ctrc_receipt_cancel_prepared(ClusterCtrcParticipantEntry *participant,
								 ClusterCtrcReceipt *receipt)
{
	uint32 expected = CTRC_RECEIPT_PREPARED;

	if (participant == NULL || receipt == NULL
		|| memcmp(&receipt->key, &participant->key,
				  sizeof(receipt->key)) != 0
		|| receipt->publication.grant_generation
		   != participant->grant_generation
		|| participant->state != CTRC_PARTICIPANT_OPEN
		|| !pg_atomic_compare_exchange_u32(
			(pg_atomic_uint32 *)&receipt->state, &expected,
			CTRC_RECEIPT_CANCELLED))
		return false;
	(void)pg_atomic_fetch_sub_u64(
		(pg_atomic_uint64 *)&participant->prepared_count, 1);
	(void)pg_atomic_fetch_add_u64(
		(pg_atomic_uint64 *)&participant->cancelled_count, 1);
	receipt->disposition = CTRC_RELEASE_CANCELLED_PREMUTATION;
	return true;
}

bool
cluster_ctrc_receipt_cancel_shared(const ClusterCtrcReceiptHandle *handle)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	return ctrc_receipt_handle_exact(handle)
		&& cluster_ctrc_receipt_cancel_prepared(
			handle->participant, handle->receipt);
#else
	(void)handle;
	return false;
#endif
}

static bool
ctrc_itl_durability_covers(
	const ClusterCtrcTargetV1 *target,
	XLogRecPtr highest_local_lsn,
	const XLogRecPtr required_lsn[CLUSTER_SF_DEP_MAX_ORIGINS],
	const ClusterCtrcDurability *durability)
{
	int i;

	if (target == NULL || required_lsn == NULL || durability == NULL)
		return false;
	if (target->needs_wal)
	{
		if (XLogRecPtrIsInvalid(highest_local_lsn)
			|| highest_local_lsn < target->predecessor_page_lsn
			|| XLogRecPtrIsInvalid(durability->local_flush_lsn)
			|| durability->local_flush_lsn < highest_local_lsn)
			return false;
	}
	else if (!XLogRecPtrIsInvalid(highest_local_lsn)
			 && (XLogRecPtrIsInvalid(durability->local_flush_lsn)
				 || durability->local_flush_lsn < highest_local_lsn))
		return false;
	for (i = 0; i < CLUSTER_SF_DEP_MAX_ORIGINS; i++)
	{
		if (!XLogRecPtrIsInvalid(required_lsn[i])
			&& (XLogRecPtrIsInvalid(durability->durable_lsn[i])
				|| durability->durable_lsn[i] < required_lsn[i]))
			return false;
	}
	return true;
}

bool
cluster_ctrc_itl_target_identity_matches(
	const ClusterCtrcTargetV1 *target,
	const ClusterCtrcItlTargetIdentity *expected)
{
	return target != NULL && expected != NULL
		&& target->kind == CTRC_TARGET_EXACT_ITL_SLOT
		&& target->spc_oid == expected->spc_oid
		&& target->db_oid == expected->db_oid
		&& target->rel_number == expected->rel_number
		&& target->fork_number == expected->fork_number
		&& target->block_number == expected->block_number
		&& target->itl_slot_index == expected->itl_slot_index
		&& target->itl_slot_wrap == expected->itl_slot_wrap
		&& target->itl_xid == expected->itl_xid
		&& target->itl_class == expected->itl_class
		&& target->needs_wal == expected->needs_wal
		&& ctrc_bytes_zero(expected->reserved8, sizeof(expected->reserved8))
		&& memcmp(target->uba, expected->uba, sizeof(target->uba)) == 0;
}

ClusterCtrcDischargeResult
cluster_ctrc_receipt_discharge_itl(ClusterCtrcParticipantEntry *participant,
								   ClusterCtrcReceipt *receipt,
								   ClusterCtrcItlProjection projection,
								   const ClusterCtrcDurability *durability)
{
	ClusterCtrcReleaseDisposition disposition;
	uint32 expected;

	if (participant == NULL || receipt == NULL || durability == NULL
		|| memcmp(&receipt->key, &participant->key,
				  sizeof(receipt->key)) != 0
		|| receipt->publication.grant_generation
		   != participant->grant_generation
		|| (participant->state != CTRC_PARTICIPANT_OPEN
			&& participant->state != CTRC_PARTICIPANT_CLOSED_DRAINING)
		|| receipt->publication.reference_kind != CTRC_REF_HEAP_ITL_UBA
		|| receipt->publication.target_kind
		   != CTRC_TARGET_PAGE_PENDING_ITL_SLOT
		|| !ctrc_target_exact_itl_valid(&participant->key, &receipt->target))
		return CLUSTER_CTRC_DISCHARGE_RETAIN;
	if (projection == CTRC_ITL_TERMINAL_INDEPENDENT)
		disposition = CTRC_RELEASE_CLEANED_TERMINAL_REWRITE;
	else if (projection == CTRC_ITL_TARGET_ABSENT)
		disposition = CTRC_RELEASE_CLEANED_ABSENT;
	else
		return CLUSTER_CTRC_DISCHARGE_RETAIN;

	expected = pg_atomic_read_u32((pg_atomic_uint32 *)&receipt->state);
	if (expected == CTRC_RECEIPT_CLEANED)
	{
		return receipt->disposition == (uint8)disposition
			&& ctrc_itl_durability_covers(
				&receipt->target, receipt->highest_local_wal_lsn,
				receipt->required_lsn, durability)
				? CLUSTER_CTRC_DISCHARGE_CLEANED
				: CLUSTER_CTRC_DISCHARGE_RETAIN;
	}
	if (expected != CTRC_RECEIPT_APPLIED
		|| !ctrc_itl_durability_covers(
			&receipt->target, durability->highest_local_lsn,
			durability->required_lsn, durability))
		return CLUSTER_CTRC_DISCHARGE_RETAIN;

	receipt->highest_local_wal_lsn = durability->highest_local_lsn;
	memcpy(receipt->required_lsn, durability->required_lsn,
		   sizeof(receipt->required_lsn));
	receipt->disposition = (uint8)disposition;
	pg_write_barrier();
	expected = CTRC_RECEIPT_APPLIED;
	if (!pg_atomic_compare_exchange_u32(
			(pg_atomic_uint32 *)&receipt->state, &expected,
			CTRC_RECEIPT_CLEANED))
		return expected == CTRC_RECEIPT_CLEANED
			&& receipt->disposition == (uint8)disposition
				? CLUSTER_CTRC_DISCHARGE_CLEANED
				: CLUSTER_CTRC_DISCHARGE_RETAIN;
	(void)pg_atomic_fetch_sub_u64(
		(pg_atomic_uint64 *)&participant->applied_count, 1);
	(void)pg_atomic_fetch_add_u64(
		(pg_atomic_uint64 *)&participant->cleaned_count, 1);
	return CLUSTER_CTRC_DISCHARGE_CLEANED;
}

ClusterCtrcDischargeResult
cluster_ctrc_receipt_discharge_itl_shared(
	const ClusterCtrcReceiptHandle *handle,
	const ClusterCtrcItlTargetIdentity *expected_target,
	ClusterCtrcItlProjection projection,
	const ClusterCtrcDurability *durability)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	ClusterCtrcDischargeResult result;

	if (!ctrc_receipt_handle_exact(handle))
		return CLUSTER_CTRC_DISCHARGE_RETAIN;
	SpinLockAcquire(&CtrcShared->participant_lock);
	SpinLockAcquire(&CtrcShared->receipt_lock);
	result = ctrc_receipt_handle_exact(handle)
		&& cluster_ctrc_itl_target_identity_matches(
			&handle->receipt->target, expected_target)
		? cluster_ctrc_receipt_discharge_itl(
			handle->participant, handle->receipt, projection, durability)
		: CLUSTER_CTRC_DISCHARGE_RETAIN;
	SpinLockRelease(&CtrcShared->receipt_lock);
	SpinLockRelease(&CtrcShared->participant_lock);
	return result;
#else
	(void)handle;
	(void)expected_target;
	(void)projection;
	(void)durability;
	return CLUSTER_CTRC_DISCHARGE_RETAIN;
#endif
}

ClusterCtrcCloseResult
cluster_ctrc_participant_close(ClusterCtrcParticipantEntry *participant,
								   const ClusterCtrcParticipantIdentity *identity,
								   uint32 grant_generation,
								   uint64 seal_generation)
{
	uint64 terminal_count;

	if (participant == NULL)
		return CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN;
	if (participant->state == CTRC_PARTICIPANT_BLOCKED)
		return CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN;
	if (identity == NULL || grant_generation == 0 || seal_generation == 0
		|| !ctrc_txn_key_valid(&participant->key)
		|| !ctrc_participant_identity_valid(&participant->key, identity)
		|| memcmp(&participant->identity, identity, sizeof(*identity)) != 0
		|| participant->grant_generation != grant_generation
		|| !ctrc_bytes_zero(participant->reserved8,
								sizeof(participant->reserved8)))
	{
		participant->state = CTRC_PARTICIPANT_BLOCKED;
		return CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN;
	}

	if (participant->state == CTRC_PARTICIPANT_OPEN)
	{
		if (participant->seal_generation != 0)
		{
			participant->state = CTRC_PARTICIPANT_BLOCKED;
			return CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN;
		}
		participant->seal_generation = seal_generation;
		participant->state = CTRC_PARTICIPANT_CLOSED_DRAINING;
	}
	else if ((participant->state != CTRC_PARTICIPANT_CLOSED_DRAINING
			  && participant->state != CTRC_PARTICIPANT_ACK_READY
			  && participant->state != CTRC_PARTICIPANT_ACK_FROZEN)
			 || participant->seal_generation != seal_generation)
	{
		participant->state = CTRC_PARTICIPANT_BLOCKED;
		return CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN;
	}

	if (participant->next_key_sequence == 0
		|| participant->last_key_sequence == UINT64_MAX
		|| participant->next_key_sequence
		   != participant->last_key_sequence + 1
		|| participant->receipt_count != participant->last_key_sequence
		|| participant->prepared_count > participant->receipt_count
		|| participant->applied_count > participant->receipt_count
		|| participant->cancelled_count > participant->receipt_count
		|| participant->cleaned_count > participant->receipt_count)
	{
		participant->state = CTRC_PARTICIPANT_BLOCKED;
		return CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN;
	}
	if (participant->prepared_count != 0 || participant->applied_count != 0)
		return CLUSTER_CTRC_CLOSE_PENDING_DRAIN;

	terminal_count = participant->cancelled_count + participant->cleaned_count;
	if (terminal_count < participant->cancelled_count
		|| terminal_count != participant->receipt_count)
	{
		participant->state = CTRC_PARTICIPANT_BLOCKED;
		return CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN;
	}
	if (participant->state == CTRC_PARTICIPANT_CLOSED_DRAINING)
		participant->state = CTRC_PARTICIPANT_ACK_READY;
	return CLUSTER_CTRC_CLOSE_ACK_READY;
}

ClusterCtrcLossResult
cluster_ctrc_participant_note_owner_loss(ClusterCtrcParticipantEntry *participant,
									 ClusterCtrcReceipt *receipt)
{
	uint32 state;
	uint32 expected;

	if (participant == NULL)
		return CLUSTER_CTRC_LOSS_BLOCKED;
	participant->state = CTRC_PARTICIPANT_BLOCKED;
	if (receipt == NULL
		|| memcmp(&receipt->key, &participant->key,
				  sizeof(receipt->key)) != 0
		|| receipt->publication.grant_generation
		   != participant->grant_generation)
		return CLUSTER_CTRC_LOSS_BLOCKED;

	state = pg_atomic_read_u32((pg_atomic_uint32 *)&receipt->state);
	if (state == CTRC_RECEIPT_BLOCKED)
		return CLUSTER_CTRC_LOSS_BLOCKED;
	if (state != CTRC_RECEIPT_PREPARED && state != CTRC_RECEIPT_APPLIED)
		return CLUSTER_CTRC_LOSS_BLOCKED;
	expected = state;
	if (!pg_atomic_compare_exchange_u32((pg_atomic_uint32 *)&receipt->state,
									&expected, CTRC_RECEIPT_BLOCKED))
		return CLUSTER_CTRC_LOSS_BLOCKED;
	if (state == CTRC_RECEIPT_PREPARED)
		(void)pg_atomic_fetch_sub_u64(
			(pg_atomic_uint64 *)&participant->prepared_count, 1);
	else
		(void)pg_atomic_fetch_sub_u64(
			(pg_atomic_uint64 *)&participant->applied_count, 1);
	return CLUSTER_CTRC_LOSS_BLOCKED;
}

ClusterCtrcCleanResult
cluster_ctrc_clean_reference(const ClusterCtrcCleanReferenceInput *input)
{
	(void)input;
	return CTRC_CLEAN_RETAIN;
}

ClusterCtrcTransferResult
cluster_ctrc_transfer_note_successor_receipt(ClusterCtrcTransferState *transfer)
{
	(void)transfer;
	return CLUSTER_CTRC_TRANSFER_REFUSED;
}

ClusterCtrcTransferResult
cluster_ctrc_transfer_note_descriptor_durable(ClusterCtrcTransferState *transfer)
{
	(void)transfer;
	return CLUSTER_CTRC_TRANSFER_REFUSED;
}

ClusterCtrcTransferResult
cluster_ctrc_transfer_remove_predecessor(ClusterCtrcTransferState *transfer)
{
	(void)transfer;
	return CLUSTER_CTRC_TRANSFER_REFUSED;
}

ClusterCtrcAckResult
cluster_ctrc_participant_build_ack(ClusterCtrcParticipantEntry *participant,
								   const ClusterCtrcDurability *durability,
								   ClusterCtrcLocalReleaseAckV1 *ack)
{
	(void)participant;
	(void)durability;
	if (ack != NULL)
		MemSet(ack, 0, sizeof(*ack));
	return CLUSTER_CTRC_ACK_DENIED;
}

ClusterCtrcCertificateResult
cluster_ctrc_origin_certificate_validate(const ClusterCtrcCertificateInput *input)
{
	(void)input;
	return CLUSTER_CTRC_CERTIFICATE_RETAIN;
}

bool
cluster_ctrc_terminal_recyclable(const ClusterCtrcRecycleInput *input)
{
	(void)input;
	return false;
}

ClusterCtrcCrashDisposition
cluster_ctrc_crash_cut_disposition(ClusterCtrcCrashCut cut)
{
	(void)cut;
	return CLUSTER_CTRC_CRASH_RETAIN;
}
