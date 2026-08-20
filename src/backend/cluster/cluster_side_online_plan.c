/*-------------------------------------------------------------------------
 * cluster_side_online_plan.c
 *    RF-SIDE immutable online operation plan implementation.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_side_online_plan.h"

#ifdef RF_SIDE_ONLINE_TESTING
#include <stdlib.h>
#define side_alloc0(bytes_) calloc(1, (bytes_))
#define side_realloc(ptr_, bytes_) realloc((ptr_), (bytes_))
#define side_free(ptr_) free((ptr_))
#else
#define side_alloc0(bytes_) palloc0(bytes_)
#define side_realloc(ptr_, bytes_) repalloc((ptr_), (bytes_))
#define side_free(ptr_) pfree(ptr_)
#endif

#define RF_SIDE_ONLINE_PLAN_MAGIC UINT32_C(0x53495031)

struct RfSideOnlinePlanV1
{
	uint32		magic;
	bool		sealed;
	uint8		reserved5[3];
	uint64		system_identifier;
	uint8		storage_uuid[16];
	Size		memory_budget;
	Size		memory_used;
	uint32		participant_count;
	RfContributorStreamCutV1 *physical_cuts;
	XLogRecPtr *last_record_end;
	bool	   *participant_seen;
	RfSideOnlineOperationV1 *operations;
	uint32		operation_count;
	uint32		operation_capacity;
	uint8	   *owned_payload;
	uint32		owned_payload_bytes;
	uint32		owned_payload_capacity;
};

static bool
side_bytes_nonzero(const uint8 *bytes, Size length)
{
	uint8		seen = 0;
	Size		i;

	for (i = 0; i < length; i++)
		seen |= bytes[i];
	return seen != 0;
}

static bool
side_plan_reserve(RfSideOnlinePlanV1 *plan, Size bytes)
{
	if (plan->memory_used > plan->memory_budget - Min(plan->memory_budget, bytes))
		return false;
	plan->memory_used += bytes;
	return true;
}

static RfPageProofDetailV1
side_record_identity_validate(RfSideOnlinePlanV1 *plan,
	const RfDetachedRecordPlanV1 *record_plan,
	const RfPageOnlineRecordIdentityV1 *identity)
{
	const RfContributorStreamCutV1 *cut;
	const RfPageReplayRecordIdentityV1 *record;
	const XLogReaderState *reader;
	const DecodedXLogRecord *decoded;

	if (record_plan == NULL || !record_plan->preflight_complete ||
		record_plan->source_record == NULL ||
		record_plan->source_record->record == NULL || identity == NULL)
		return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
	reader = record_plan->source_record;
	decoded = reader->record;
	record = &identity->record;
	if (identity->reserved_zero != 0 || record->reserved_zero != 0 ||
		record->reserved_zero2 != 0 ||
		identity->participant_index >= plan->participant_count ||
		record->system_identifier != plan->system_identifier ||
		memcmp(record->storage_uuid, plan->storage_uuid, 16) != 0 ||
		record->origin_thread == 0 || record->timeline_id == 0 ||
		record->read_rec_ptr == InvalidXLogRecPtr ||
		record->end_rec_ptr <= record->read_rec_ptr ||
		reader->system_identifier != record->system_identifier ||
		reader->ReadRecPtr != record->read_rec_ptr ||
		reader->EndRecPtr != record->end_rec_ptr ||
		decoded->lsn != record->read_rec_ptr ||
		decoded->next_lsn != record->end_rec_ptr ||
		(uint32) decoded->header.xl_crc != record->record_crc ||
		decoded->header.xl_rmid != record->rmid ||
		decoded->header.xl_info != record->info)
		return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
	cut = &plan->physical_cuts[identity->participant_index];
	if (record->origin_thread != cut->failed_thread ||
		record->timeline_id != cut->timeline_id ||
		(cut->flags & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY) != 0 ||
		record->read_rec_ptr < cut->scan_begin_inclusive ||
		record->end_rec_ptr > cut->scan_end_exclusive ||
		(plan->participant_seen[identity->participant_index] &&
		 record->read_rec_ptr < plan->last_record_end[identity->participant_index]))
		return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
	return RF_PAGE_PROOF_DETAIL_OK;
}

static bool
side_ensure_operation_capacity(RfSideOnlinePlanV1 *plan)
{
	RfSideOnlineOperationV1 *operations;
	uint32		capacity;
	Size		delta;

	if (plan->operation_count < plan->operation_capacity)
		return true;
	capacity = plan->operation_capacity == 0 ? 16 :
		plan->operation_capacity * 2;
	if (capacity < plan->operation_capacity)
		return false;
	delta = (Size) (capacity - plan->operation_capacity) *
		sizeof(*operations);
	if (!side_plan_reserve(plan, delta))
		return false;
	if (plan->operations == NULL)
		operations = (RfSideOnlineOperationV1 *) side_alloc0(
			(Size) capacity * sizeof(*operations));
	else
	{
		operations = (RfSideOnlineOperationV1 *) side_realloc(plan->operations,
			(Size) capacity * sizeof(*operations));
		if (operations != NULL)
			memset(operations + plan->operation_capacity, 0, delta);
	}
	if (operations == NULL)
	{
		plan->memory_used -= delta;
		return false;
	}
	plan->operations = operations;
	plan->operation_capacity = capacity;
	return true;
}

static bool
side_ensure_payload_capacity(RfSideOnlinePlanV1 *plan, uint32 additional)
{
	uint8	   *payload;
	uint32		needed;
	uint32		capacity;
	Size		delta;

	if (additional == 0)
		return true;
	if (additional > UINT32_MAX - plan->owned_payload_bytes)
		return false;
	needed = plan->owned_payload_bytes + additional;
	if (needed <= plan->owned_payload_capacity)
		return true;
	capacity = plan->owned_payload_capacity == 0 ? BLCKSZ :
		plan->owned_payload_capacity;
	while (capacity < needed)
	{
		if (capacity > UINT32_MAX / 2)
		{
			capacity = needed;
			break;
		}
		capacity *= 2;
	}
	delta = (Size) capacity - plan->owned_payload_capacity;
	if (!side_plan_reserve(plan, delta))
		return false;
	if (plan->owned_payload == NULL)
		payload = (uint8 *) side_alloc0(capacity);
	else
		payload = (uint8 *) side_realloc(plan->owned_payload, capacity);
	if (payload == NULL)
	{
		plan->memory_used -= delta;
		return false;
	}
	plan->owned_payload = payload;
	plan->owned_payload_capacity = capacity;
	return true;
}

RfPageProofDetailV1
rf_side_online_plan_create_v1(const RfSideOnlinePlanRequestV1 *request,
						  RfSideOnlinePlanV1 **out_plan)
{
	RfSideOnlinePlanV1 *plan;
	Size		budget;
	Size		arrays_size;
	uint32		i;

	if (out_plan == NULL)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	*out_plan = NULL;
	if (request == NULL || request->system_identifier == 0 ||
		!side_bytes_nonzero(request->storage_uuid, 16) ||
		request->physical_cuts == NULL || request->participant_count == 0 ||
		request->participant_count > RF_PAGE_STABLE_MAX_PARTICIPANTS)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	budget = request->memory_budget == 0 ? RF_SIDE_ONLINE_PLAN_MAX_BYTES :
		request->memory_budget;
	if (budget > RF_SIDE_ONLINE_PLAN_MAX_BYTES || budget < sizeof(*plan))
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	for (i = 0; i < request->participant_count; i++)
	{
		const RfContributorStreamCutV1 *cut = &request->physical_cuts[i];
		bool empty = (cut->flags & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY) != 0;

		if (cut->failed_thread == 0 || cut->timeline_id == 0 ||
			(cut->flags & RF_CONTRIBUTOR_CUT_COMPLETE) == 0 ||
			(cut->flags & ~RF_CONTRIBUTOR_CUT_KNOWN_MASK) != 0 ||
			(empty && cut->scan_begin_inclusive != cut->scan_end_exclusive) ||
			(!empty && cut->scan_begin_inclusive >= cut->scan_end_exclusive) ||
			(i > 0 && request->physical_cuts[i - 1].failed_thread >=
			 cut->failed_thread))
			return RF_PAGE_PROOF_DETAIL_PARTICIPANT_MISSING;
	}
	arrays_size = (Size) request->participant_count *
		(sizeof(*plan->physical_cuts) + sizeof(*plan->last_record_end) +
		 sizeof(*plan->participant_seen));
	if (sizeof(*plan) > budget - Min(budget, arrays_size))
		return RF_PAGE_PROOF_DETAIL_CAPACITY;
	plan = (RfSideOnlinePlanV1 *) side_alloc0(sizeof(*plan));
	if (plan == NULL)
		return RF_PAGE_PROOF_DETAIL_OOM;
	plan->memory_budget = budget;
	plan->memory_used = sizeof(*plan) + arrays_size;
	plan->physical_cuts = (RfContributorStreamCutV1 *) side_alloc0(
		(Size) request->participant_count * sizeof(*plan->physical_cuts));
	plan->last_record_end = (XLogRecPtr *) side_alloc0(
		(Size) request->participant_count * sizeof(*plan->last_record_end));
	plan->participant_seen = (bool *) side_alloc0(
		(Size) request->participant_count * sizeof(*plan->participant_seen));
	if (plan->physical_cuts == NULL || plan->last_record_end == NULL ||
		plan->participant_seen == NULL)
	{
		rf_side_online_plan_destroy_v1(&plan);
		return RF_PAGE_PROOF_DETAIL_OOM;
	}
	memcpy(plan->physical_cuts, request->physical_cuts,
		(Size) request->participant_count * sizeof(*plan->physical_cuts));
	plan->magic = RF_SIDE_ONLINE_PLAN_MAGIC;
	plan->system_identifier = request->system_identifier;
	memcpy(plan->storage_uuid, request->storage_uuid, 16);
	plan->participant_count = request->participant_count;
	*out_plan = plan;
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
rf_side_online_plan_feed_record_v1(RfSideOnlinePlanV1 *plan,
	const RfDetachedRecordPlanV1 *record_plan,
	const RfPageOnlineRecordIdentityV1 *identity)
{
	RfSideOnlineOperationV1 candidate;
	RfPageProofDetailV1 detail;
	uint16		participant;

	if (plan == NULL || plan->magic != RF_SIDE_ONLINE_PLAN_MAGIC || plan->sealed)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	detail = side_record_identity_validate(plan, record_plan, identity);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	memset(&candidate, 0, sizeof(candidate));
	if (record_plan->route.record_owner == RF_ROUTE_OWNER_SIDE_TYPED)
	{
		if (record_plan->route.rmid == RM_XACT_ID &&
			record_plan->route.codec_id == RF_ROUTE_CODEC_SIDE_STANDARD)
		{
			if (!rf_side_xact_decode_v1(record_plan->source_record,
					plan->system_identifier, identity->record.origin_thread,
					&candidate.xact))
				return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
			candidate.kind = RF_SIDE_ONLINE_OPERATION_XACT;
			if (candidate.xact.kind == RF_SIDE_XACT_PREPARE)
			{
				uint32 record_length =
					XLogRecGetDataLen(record_plan->source_record);

				if (record_length == 0 || record_length !=
						candidate.xact.prepare_payload_length ||
					!side_ensure_operation_capacity(plan) ||
					!side_ensure_payload_capacity(plan, record_length))
					return RF_PAGE_PROOF_DETAIL_CAPACITY;
				candidate.owned_payload_offset = plan->owned_payload_bytes;
				candidate.owned_payload_length = record_length;
				memcpy(plan->owned_payload + plan->owned_payload_bytes,
					XLogRecGetData(record_plan->source_record), record_length);
				plan->owned_payload_bytes += record_length;
			}
		}
		else if (record_plan->route.rmid == RM_CLUSTER_UNDO_ID &&
				 record_plan->route.codec_id ==
					RF_ROUTE_CODEC_SIDE_CLUSTER_UNDO)
		{
			const char *record_data = XLogRecGetData(record_plan->source_record);
			uint32		record_length =
				XLogRecGetDataLen(record_plan->source_record);

			if (!cluster_undo_decode(record_plan->source_record,
					&candidate.undo) ||
				!cluster_undo_preflight(&candidate.undo) ||
				candidate.undo.instance != identity->record.origin_thread ||
				candidate.undo.payload_offset > record_length ||
				candidate.undo.payload_length >
					record_length - candidate.undo.payload_offset)
				return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
			candidate.kind = RF_SIDE_ONLINE_OPERATION_UNDO;
			candidate.owned_payload_length = candidate.undo.payload_length;
			if (!side_ensure_operation_capacity(plan))
				return RF_PAGE_PROOF_DETAIL_CAPACITY;
			if (candidate.owned_payload_length > 0)
			{
				if (!side_ensure_payload_capacity(plan,
						candidate.owned_payload_length))
					return RF_PAGE_PROOF_DETAIL_CAPACITY;
				candidate.owned_payload_offset = plan->owned_payload_bytes;
				memcpy(plan->owned_payload + plan->owned_payload_bytes,
					record_data + candidate.undo.payload_offset,
					candidate.owned_payload_length);
				plan->owned_payload_bytes += candidate.owned_payload_length;
			}
		}
		else
			return RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED;
		candidate.identity = *identity;
		candidate.route = record_plan->route;
		if (!side_ensure_operation_capacity(plan))
			return RF_PAGE_PROOF_DETAIL_CAPACITY;
		plan->operations[plan->operation_count++] = candidate;
	}
	else if (record_plan->route.record_owner != RF_ROUTE_OWNER_PAGE_CODEC &&
			 record_plan->route.record_owner != RF_ROUTE_OWNER_LOGICAL_NOOP)
		return RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED;
	participant = identity->participant_index;
	plan->participant_seen[participant] = true;
	plan->last_record_end[participant] = identity->record.end_rec_ptr;
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
rf_side_online_plan_seal_v1(RfSideOnlinePlanV1 *plan)
{
	uint32		i;

	if (plan == NULL || plan->magic != RF_SIDE_ONLINE_PLAN_MAGIC || plan->sealed)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	for (i = 0; i < plan->participant_count; i++)
	{
		const RfContributorStreamCutV1 *cut = &plan->physical_cuts[i];
		bool empty = (cut->flags & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY) != 0;

		if ((empty && plan->participant_seen[i]) ||
			(!empty && (!plan->participant_seen[i] ||
			 plan->last_record_end[i] != cut->scan_end_exclusive)))
			return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
	}
	plan->sealed = true;
	return RF_PAGE_PROOF_DETAIL_OK;
}

uint32
rf_side_online_plan_operation_count_v1(const RfSideOnlinePlanV1 *plan)
{
	return plan != NULL && plan->magic == RF_SIDE_ONLINE_PLAN_MAGIC &&
		plan->sealed ? plan->operation_count : 0;
}

bool
rf_side_online_plan_operation_v1(const RfSideOnlinePlanV1 *plan,
	uint32 index, RfSideOnlineOperationV1 *out_operation)
{
	if (plan == NULL || plan->magic != RF_SIDE_ONLINE_PLAN_MAGIC ||
		!plan->sealed || index >= plan->operation_count || out_operation == NULL)
		return false;
	*out_operation = plan->operations[index];
	if (out_operation->owned_payload_length > 0)
		out_operation->owned_payload = plan->owned_payload +
			out_operation->owned_payload_offset;
	return true;
}

RfPageProofDetailV1
rf_side_online_plan_apply_v1(const RfSideOnlinePlanV1 *plan,
						 const RfSideOnlineApplyOpsV1 *ops)
{
	uint32		i;

	if (plan == NULL || plan->magic != RF_SIDE_ONLINE_PLAN_MAGIC ||
		!plan->sealed || ops == NULL)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	if (plan->operation_count == 0)
		return RF_PAGE_PROOF_DETAIL_OK;
	if (ops->begin_protected_set == NULL || ops->end_protected_set == NULL)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	for (i = 0; i < plan->operation_count; i++)
		if ((plan->operations[i].kind == RF_SIDE_ONLINE_OPERATION_XACT &&
			 (ops->preflight_xact == NULL || ops->apply_xact == NULL)) ||
			(plan->operations[i].kind == RF_SIDE_ONLINE_OPERATION_UNDO &&
			 (ops->preflight_undo == NULL || ops->apply_undo == NULL)) ||
			plan->operations[i].kind == RF_SIDE_ONLINE_OPERATION_INVALID)
			return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	if (!ops->begin_protected_set(ops->arg))
		return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	/*
	 * STOP-06 section 9.2: classify every target before the first target
	 * byte changes.  The caller keeps its protected-set certification stable
	 * across this pass and the mutation pass below.
	 */
	for (i = 0; i < plan->operation_count; i++)
	{
		RfSideOnlineOperationV1 operation = plan->operations[i];
		bool		accepted;

		if (operation.owned_payload_length > 0)
			operation.owned_payload = plan->owned_payload +
				operation.owned_payload_offset;
		if (operation.kind == RF_SIDE_ONLINE_OPERATION_XACT)
			accepted = ops->preflight_xact(ops->arg, &operation);
		else
			accepted = ops->preflight_undo(ops->arg, &operation);
		if (!accepted)
		{
			ops->end_protected_set(ops->arg, false);
			return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		}
	}
	for (i = 0; i < plan->operation_count; i++)
	{
		RfSideOnlineOperationV1 operation = plan->operations[i];
		bool		applied;

		if (operation.owned_payload_length > 0)
			operation.owned_payload = plan->owned_payload +
				operation.owned_payload_offset;
		if (operation.kind == RF_SIDE_ONLINE_OPERATION_XACT)
			applied = ops->apply_xact(ops->arg, &operation);
		else
			applied = ops->apply_undo(ops->arg, &operation);
		if (!applied)
		{
			ops->end_protected_set(ops->arg, false);
			return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		}
	}
	ops->end_protected_set(ops->arg, true);
	return RF_PAGE_PROOF_DETAIL_OK;
}

void
rf_side_online_plan_destroy_v1(RfSideOnlinePlanV1 **plan_address)
{
	RfSideOnlinePlanV1 *plan;

	if (plan_address == NULL || *plan_address == NULL)
		return;
	plan = *plan_address;
	if (plan->physical_cuts != NULL)
		side_free(plan->physical_cuts);
	if (plan->last_record_end != NULL)
		side_free(plan->last_record_end);
	if (plan->participant_seen != NULL)
		side_free(plan->participant_seen);
	if (plan->operations != NULL)
		side_free(plan->operations);
	if (plan->owned_payload != NULL)
		side_free(plan->owned_payload);
	memset(plan, 0, sizeof(*plan));
	side_free(plan);
	*plan_address = NULL;
}

#endif
