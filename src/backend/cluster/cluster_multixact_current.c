/*-------------------------------------------------------------------------
 *
 * cluster_multixact_current.c
 *	  Pure validation and decision core for current-DML MultiXacts.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_multixact_current.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-3.6b-multixact-current-dml.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_multixact_current.h"
#include "cluster/cluster_multixact_current_wire.h"
#include "cluster/cluster_multixact_current_stats.h"
#include "cluster/cluster_mxid_stripe.h"
#include "cluster/cluster_runtime_visibility.h"
#include "storage/lock.h"

#define CLUSTER_CURRENT_MX_MAX_PARENT_CHAIN_DEPTH 1024


static bool
current_mx_key_equal(const ClusterCurrentMxKey *a, const ClusterCurrentMxKey *b)
{
	return memcmp(a, b, sizeof(*a)) == 0;
}


static bool
current_mx_key_valid(const ClusterCurrentMxKey *key)
{
	return key != NULL && key->origin_node_id < CLUSTER_MAX_NODES && key->reserved16 == 0
		   && key->reserved32 == 0 && MultiXactIdIsValid(key->multixact_id);
}


static bool
tt_key_is_zero(const ClusterTTStatusKey *key)
{
	static const ClusterTTStatusKey zero_key;

	return memcmp(key, &zero_key, sizeof(*key)) == 0;
}


static bool
tt_key_valid(const ClusterTTStatusKey *key, TransactionId xid, uint32 epoch, int expected_origin)
{
	if (key == NULL || key->origin_node_id >= CLUSTER_MAX_NODES || key->_reserved != 0
		|| key->_reserved2 != 0 || key->undo_segment_id == 0 || key->tt_slot_id == 0
		|| key->cluster_epoch != epoch || !TransactionIdIsNormal(key->local_xid)
		|| (TransactionIdIsValid(xid) && key->local_xid != xid))
		return false;
	if (expected_origin >= 0 && key->origin_node_id != (uint16)expected_origin)
		return false;
	return true;
}


static bool
tt_key_valid_holder(const ClusterTTStatusKey *key, TransactionId member_xid,
					uint32 epoch, int expected_origin)
{
	return tt_key_valid(key, InvalidTransactionId, epoch, expected_origin)
		   && (key->local_xid == member_xid
			   || TransactionIdPrecedes(key->local_xid, member_xid));
}


bool
cluster_multixact_current_resolve_origin_member_proof(
	TransactionId member_xid, uint8 member_status, uint16 member_ordinal,
	uint16 member_origin_node, uint32 current_epoch, bool requester_self,
	const ClusterTTStatusKey *initial_key, const ClusterTTStatusResult *initial_result,
	ClusterCurrentMxExactLookupFn exact_lookup, void *exact_lookup_arg,
	ClusterCurrentMemberProof *proof)
{
	ClusterTTStatusKey seen_keys[CLUSTER_CURRENT_MX_MAX_PARENT_CHAIN_DEPTH + 1];
	ClusterTTStatusKey resolved_key;
	ClusterTTStatusResult resolved_result;
	uint16 followed_count = 0;
	uint16 seen_count = 0;

	if (proof != NULL) {
		memset(proof, 0, sizeof(*proof));
		proof->state = CCM_UNKNOWN;
	}
	if (proof == NULL || !TransactionIdIsNormal(member_xid)
		|| member_status > MaxMultiXactStatus || member_origin_node >= CLUSTER_MAX_NODES
		|| initial_key == NULL || initial_result == NULL
		|| !initial_result->authoritative || initial_result->status_epoch != current_epoch
		|| !tt_key_valid(initial_key, member_xid, current_epoch, member_origin_node))
		return false;

	resolved_key = *initial_key;
	resolved_result = *initial_result;
	seen_keys[seen_count++] = resolved_key;

	while (resolved_result.status == CLUSTER_TT_STATUS_SUBCOMMITTED) {
		ClusterTTStatusKey parent_key;
		ClusterTTStatusResult parent_result;
		uint16 i;

		if (!resolved_result.has_parent_key || exact_lookup == NULL
			|| cluster_subtrans_max_chain_depth <= 0
			|| followed_count >= (uint16)Min(cluster_subtrans_max_chain_depth,
											 CLUSTER_CURRENT_MX_MAX_PARENT_CHAIN_DEPTH)
			|| seen_count >= lengthof(seen_keys))
			goto unknown;
		parent_key = resolved_result.parent_key;
		if (!tt_key_valid(&parent_key, InvalidTransactionId, current_epoch,
						  member_origin_node)
			|| !TransactionIdPrecedes(parent_key.local_xid, resolved_key.local_xid))
			goto unknown;
		for (i = 0; i < seen_count; i++)
			if (memcmp(&seen_keys[i], &parent_key, sizeof(parent_key)) == 0)
				goto unknown;
		memset(&parent_result, 0, sizeof(parent_result));
		if (!exact_lookup(&parent_key, &parent_result, exact_lookup_arg)
			|| !parent_result.authoritative || parent_result.status_epoch != current_epoch)
			goto unknown;
		seen_keys[seen_count++] = parent_key;
		followed_count++;
		resolved_key = parent_key;
		resolved_result = parent_result;
	}

	proof->member_xid = member_xid;
	proof->member_ordinal = member_ordinal;
	proof->member_status = member_status;
	switch (resolved_result.status) {
	case CLUSTER_TT_STATUS_IN_PROGRESS:
		if (resolved_result.has_parent_key || resolved_result.commit_scn != InvalidScn)
			goto unknown;
		proof->key = resolved_key;
		proof->state = requester_self ? CCM_SELF : CCM_ACTIVE;
		return true;
	case CLUSTER_TT_STATUS_COMMITTED:
	case CLUSTER_TT_STATUS_CLEANED_OUT:
		if (resolved_result.has_parent_key || !SCN_VALID(resolved_result.commit_scn))
			goto unknown;
		proof->commit_scn = resolved_result.commit_scn;
		proof->state = CCM_COMMITTED;
		return true;
	case CLUSTER_TT_STATUS_ABORTED:
		if (resolved_result.has_parent_key || resolved_result.commit_scn != InvalidScn)
			goto unknown;
		proof->state = CCM_ABORTED;
		return true;
	case CLUSTER_TT_STATUS_UNKNOWN:
	case CLUSTER_TT_STATUS_SUBCOMMITTED:
		break;
	}

unknown:
	memset(proof, 0, sizeof(*proof));
	proof->state = CCM_UNKNOWN;
	return false;
}


ClusterUpdaterCandidateVerdict
cluster_multixact_current_updater_candidate_verdict(
	const ClusterTTStatusKey *candidate, TransactionId updater_xid,
	uint16 updater_origin_node, uint32 current_epoch, ClusterTTStatusKey *current_binding,
	ClusterTTStatusResult *current_result)
{
	ClusterTTStatusKey sampled_binding;
	ClusterTTStatusResult sampled_result;

	if (current_binding != NULL)
		memset(current_binding, 0, sizeof(*current_binding));
	if (current_result != NULL) {
		memset(current_result, 0, sizeof(*current_result));
		current_result->status = CLUSTER_TT_STATUS_UNKNOWN;
		current_result->commit_scn = InvalidScn;
	}
	if (candidate == NULL || current_binding == NULL || current_result == NULL
		|| updater_origin_node >= CLUSTER_MAX_NODES
		|| cluster_node_id < 0 || updater_origin_node != (uint16)cluster_node_id
		|| !tt_key_valid(candidate, updater_xid, current_epoch, updater_origin_node))
		return CUCP_UNKNOWN;

	memset(&sampled_binding, 0, sizeof(sampled_binding));
	memset(&sampled_result, 0, sizeof(sampled_result));
	if (!cluster_runtime_visibility_current_owner_lookup_exact(
			updater_xid, &sampled_binding, &sampled_result)
		|| !sampled_result.authoritative
		|| sampled_result.status_epoch != current_epoch
		|| !tt_key_valid(&sampled_binding, updater_xid, current_epoch,
						  updater_origin_node))
		return CUCP_UNKNOWN;

	if (memcmp(candidate, &sampled_binding, sizeof(*candidate)) == 0) {
		*current_binding = sampled_binding;
		*current_result = sampled_result;
		return CUCP_MATCH;
	}
	return CUCP_MISMATCH;
}


static bool
descriptor_entries_valid(const ClusterCurrentMxMemberDesc *members, uint16 nmembers)
{
	int updater_count = 0;
	uint16 i;
	uint16 j;

	if (members == NULL || nmembers < 2 || nmembers > CLUSTER_CURRENT_MX_MAX_MEMBERS)
		return false;

	for (i = 0; i < nmembers; i++) {
		const ClusterCurrentMxMemberDesc *member = &members[i];

		if (!TransactionIdIsNormal(member->xid) || member->member_status > MaxMultiXactStatus
			|| member->reserved8[0] != 0 || member->reserved8[1] != 0 || member->reserved8[2] != 0)
			return false;
		if (ISUPDATE_from_mxstatus(member->member_status) && ++updater_count > 1)
			return false;

		for (j = 0; j < i; j++)
			if (members[j].xid == member->xid)
				return false;
	}

	return true;
}


static bool
descriptor_shape_matches(const ClusterCurrentMxMemberDesc *members, uint16 nmembers,
						 ClusterCurrentTupleShape shape)
{
	bool has_updater = false;
	uint16 i;

	for (i = 0; i < nmembers; i++)
		if (ISUPDATE_from_mxstatus(members[i].member_status)) {
			has_updater = true;
			break;
		}

	return shape == CCM_SHAPE_LOCK_ONLY ? !has_updater : has_updater;
}


ClusterMxDescribeResult
cluster_multixact_current_validate_descriptor(const ClusterCurrentMxKey *key, uint16 source_node_id,
											  uint32 current_epoch,
											  const ClusterCurrentMxMemberDesc *members,
											  uint16 nmembers, uint32 reported_total_members)
{
	if (!current_mx_key_valid(key) || key->origin_node_id != source_node_id
		|| key->cluster_epoch != current_epoch)
		return CMX_DESC_DENIED;

	if (reported_total_members > CLUSTER_CURRENT_MX_MAX_MEMBERS)
		return nmembers == 0 ? CMX_DESC_SUPPORTED_LIMIT : CMX_DESC_DENIED;

	if (reported_total_members != nmembers || !descriptor_entries_valid(members, nmembers))
		return CMX_DESC_DENIED;

	return CMX_DESC_OK;
}


static uint64
hash_byte(uint64 hash, uint8 value)
{
	hash ^= value;
	hash *= UINT64CONST(1099511628211);
	return hash;
}


static uint64
hash_u16(uint64 hash, uint16 value)
{
	hash = hash_byte(hash, (uint8)(value >> 8));
	hash = hash_byte(hash, (uint8)value);
	return hash;
}


static uint64
hash_u32(uint64 hash, uint32 value)
{
	hash = hash_byte(hash, (uint8)(value >> 24));
	hash = hash_byte(hash, (uint8)(value >> 16));
	hash = hash_byte(hash, (uint8)(value >> 8));
	hash = hash_byte(hash, (uint8)value);
	return hash;
}


uint64
cluster_multixact_current_descriptor_hash(const ClusterCurrentMxKey *key,
										  const ClusterCurrentMxMemberDesc *members,
										  uint16 nmembers)
{
	uint64 hash = UINT64CONST(14695981039346656037);
	uint16 i;

	if (!current_mx_key_valid(key) || members == NULL || nmembers == 0
		|| nmembers > CLUSTER_CURRENT_MX_MAX_MEMBERS)
		return 0;

	hash = hash_u32(hash, (uint32)0x434d5831); /* "CMX1" */
	hash = hash_u16(hash, key->origin_node_id);
	hash = hash_u32(hash, key->multixact_id);
	hash = hash_u32(hash, key->cluster_epoch);
	hash = hash_u16(hash, nmembers);

	for (i = 0; i < nmembers; i++) {
		hash = hash_u32(hash, members[i].xid);
		hash = hash_byte(hash, members[i].member_status);
	}

	return hash == 0 ? 1 : hash;
}


static bool
proof_entry_semantic_valid(const ClusterCurrentMemberProof *proof,
						   const ClusterCurrentMxMemberDesc *member, uint16 ordinal, uint32 epoch,
						   int expected_origin)
{
	if (proof == NULL || proof->member_ordinal != ordinal || proof->member_xid != member->xid
		|| proof->member_status != member->member_status || proof->state > CCM_UNKNOWN
		|| proof->reserved8[0] != 0 || proof->reserved8[1] != 0 || proof->reserved8[2] != 0
		|| proof->reserved8[3] != 0)
		return false;

	switch ((ClusterCurrentMemberState)proof->state) {
	case CCM_SELF:
	case CCM_ACTIVE:
		return proof->commit_scn == InvalidScn
			   && tt_key_valid(&proof->key, proof->member_xid, epoch, expected_origin);

	case CCM_COMMITTED:
		return tt_key_is_zero(&proof->key) && SCN_VALID(proof->commit_scn);

	case CCM_ABORTED:
	case CCM_UNKNOWN:
		return tt_key_is_zero(&proof->key) && proof->commit_scn == InvalidScn;
	}

	return false;
}


static void
proof_array_set_unknown(ClusterCurrentMemberProof *proofs, uint16 nmembers)
{
	uint16 i;

	if (proofs == NULL)
		return;

	memset(proofs, 0, sizeof(*proofs) * nmembers);
	for (i = 0; i < nmembers; i++)
		proofs[i].state = CCM_UNKNOWN;
}


ClusterMxResolveResult
cluster_multixact_current_validate_proof_set(const ClusterCurrentMxKey *key,
											 const ClusterCurrentMxMemberDesc *members,
											 const uint16 *member_origin_nodes, uint16 nmembers,
											 uint64 request_id, uint64 descriptor_hash,
											 const ClusterCurrentProofChunkView *chunks,
											 uint16 nchunks,
											 ClusterCurrentMemberProof *ordered_proofs)
{
	bool seen_members[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	bool seen_chunks[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	bool saw_unknown = false;
	uint16 i;
	uint16 j;

	if (nmembers > CLUSTER_CURRENT_MX_MAX_MEMBERS)
		return CMX_RESOLVE_SUPPORTED_LIMIT;
	proof_array_set_unknown(ordered_proofs, nmembers);
	if (!current_mx_key_valid(key) || !descriptor_entries_valid(members, nmembers)
		|| member_origin_nodes == NULL || request_id == 0 || descriptor_hash == 0
		|| descriptor_hash != cluster_multixact_current_descriptor_hash(key, members, nmembers)
		|| chunks == NULL || nchunks == 0 || nchunks > CLUSTER_CURRENT_MX_MAX_CHUNKS
		|| ordered_proofs == NULL)
		return CMX_RESOLVE_UNKNOWN;

	memset(seen_members, 0, sizeof(seen_members));
	memset(seen_chunks, 0, sizeof(seen_chunks));
	for (i = 0; i < nmembers; i++)
		if (member_origin_nodes[i] >= CLUSTER_MAX_NODES)
			goto invalid;

	for (i = 0; i < nchunks; i++) {
		const ClusterCurrentProofChunkView *chunk = &chunks[i];

		if (chunk->request_id != request_id || !current_mx_key_equal(&chunk->mxkey, key)
			|| chunk->descriptor_hash != descriptor_hash || chunk->total_count != nmembers
			|| chunk->chunk_count != nchunks || chunk->chunk_ordinal >= nchunks
			|| chunk->source_node_id >= CLUSTER_MAX_NODES || seen_chunks[chunk->chunk_ordinal]
			|| chunk->proof_count == 0
			|| chunk->proof_count > CLUSTER_CURRENT_MX_MAX_PROOFS_PER_CHUNK
			|| chunk->proof_count > nmembers || chunk->reserved16 != 0 || chunk->proofs == NULL)
			goto invalid;

		seen_chunks[chunk->chunk_ordinal] = true;

		for (j = 0; j < chunk->proof_count; j++) {
			const ClusterCurrentMemberProof *proof = &chunk->proofs[j];
			uint16 ordinal = proof->member_ordinal;

			if (ordinal >= nmembers || seen_members[ordinal]
				|| chunk->source_node_id != member_origin_nodes[ordinal]
				|| !proof_entry_semantic_valid(proof, &members[ordinal], ordinal,
											   key->cluster_epoch, member_origin_nodes[ordinal]))
				goto invalid;

			seen_members[ordinal] = true;
			ordered_proofs[ordinal] = *proof;
			if (proof->state == CCM_UNKNOWN)
				saw_unknown = true;
		}
	}

	for (i = 0; i < nchunks; i++)
		if (!seen_chunks[i])
			goto invalid;
	for (i = 0; i < nmembers; i++)
		if (!seen_members[i])
			goto invalid;

	if (saw_unknown)
		goto invalid;

	return CMX_RESOLVE_OK;

invalid:
	proof_array_set_unknown(ordered_proofs, nmembers);
	return CMX_RESOLVE_UNKNOWN;
}


static bool
validate_updater_proof_state(const ClusterCurrentMxKey *key,
							 const ClusterCurrentMxMemberDesc *members,
							 const ClusterCurrentMemberProof *proofs,
							 uint16 nmembers,
							 const ClusterCurrentUpdaterChallenge *challenge,
							 const ClusterCurrentUpdaterProof *updater_proof,
							 uint16 updater_origin_node_id,
							 ClusterCurrentMemberState expected_state)
{
	int updater_ordinal = -1;
	uint16 i;

	if (!current_mx_key_valid(key) || !descriptor_entries_valid(members, nmembers) || proofs == NULL
		|| challenge == NULL || updater_proof == NULL || updater_origin_node_id >= CLUSTER_MAX_NODES
		|| challenge->reserved16 != 0 || updater_proof->reserved8 != 0
		|| updater_proof->verdict != CUCP_MATCH
		|| !current_mx_key_equal(&updater_proof->mxkey, key))
		return false;

	for (i = 0; i < nmembers; i++) {
		if (!proof_entry_semantic_valid(&proofs[i], &members[i], i, key->cluster_epoch, -1)
			|| proofs[i].state == CCM_UNKNOWN)
			return false;
		if (ISUPDATE_from_mxstatus(members[i].member_status))
			updater_ordinal = i;
	}

	if (updater_ordinal < 0 || proofs[updater_ordinal].state != expected_state
		|| challenge->member_ordinal != (uint16)updater_ordinal
		|| updater_proof->member_ordinal != (uint16)updater_ordinal
		|| challenge->updater_xid != members[updater_ordinal].xid
		|| updater_proof->updater_xid != members[updater_ordinal].xid
		|| !tt_key_valid(&challenge->candidate_next_xmin_key, members[updater_ordinal].xid,
						 key->cluster_epoch, updater_origin_node_id)
		|| memcmp(&challenge->candidate_next_xmin_key, &updater_proof->candidate_next_xmin_key,
				  sizeof(ClusterTTStatusKey))
			   != 0)
		return false;

	return true;
}

bool
cluster_multixact_current_validate_updater_proof(const ClusterCurrentMxKey *key,
												 const ClusterCurrentMxMemberDesc *members,
												 const ClusterCurrentMemberProof *proofs,
												 uint16 nmembers,
												 const ClusterCurrentUpdaterChallenge *challenge,
												 const ClusterCurrentUpdaterProof *updater_proof,
												 uint16 updater_origin_node_id)
{
	return validate_updater_proof_state(
		key, members, proofs, nmembers, challenge, updater_proof,
		updater_origin_node_id, CCM_COMMITTED);
}


bool
cluster_multixact_current_status_conflicts(uint8 member_status, LockTupleMode wanted_mode,
										   bool *valid_out)
{
	LOCKMODE held;
	LOCKMODE wanted;

	if (valid_out != NULL)
		*valid_out = false;
	if (member_status > MaxMultiXactStatus || wanted_mode < LockTupleKeyShare
		|| wanted_mode > LockTupleExclusive)
		return false;

	/*
	 * Keep the translations byte-for-byte equivalent to heapam.c's file-local
	 * MultiXactStatusLock/tupleLockExtraInfo tables, then use PostgreSQL's
	 * canonical heavyweight lock conflict source.
	 */
	switch ((MultiXactStatus)member_status) {
	case MultiXactStatusForKeyShare:
		held = AccessShareLock;
		break;
	case MultiXactStatusForShare:
		held = RowShareLock;
		break;
	case MultiXactStatusForNoKeyUpdate:
	case MultiXactStatusNoKeyUpdate:
		held = ExclusiveLock;
		break;
	case MultiXactStatusForUpdate:
	case MultiXactStatusUpdate:
		held = AccessExclusiveLock;
		break;
	default:
		return false;
	}

	switch (wanted_mode) {
	case LockTupleKeyShare:
		wanted = AccessShareLock;
		break;
	case LockTupleShare:
		wanted = RowShareLock;
		break;
	case LockTupleNoKeyExclusive:
		wanted = ExclusiveLock;
		break;
	case LockTupleExclusive:
		wanted = AccessExclusiveLock;
		break;
	default:
		return false;
	}

	if (valid_out != NULL)
		*valid_out = true;
	return DoLockModesConflict(held, wanted);
}


static bool
desired_status_matches_mode(MultiXactStatus status, LockTupleMode mode)
{
	switch (status) {
	case MultiXactStatusForKeyShare:
		return mode == LockTupleKeyShare;
	case MultiXactStatusForShare:
		return mode == LockTupleShare;
	case MultiXactStatusForNoKeyUpdate:
	case MultiXactStatusNoKeyUpdate:
		return mode == LockTupleNoKeyExclusive;
	case MultiXactStatusForUpdate:
	case MultiXactStatusUpdate:
		return mode == LockTupleExclusive;
	}

	return false;
}


static bool
action_status_valid(const ClusterCurrentMxRequestContext *ctx)
{
	switch ((ClusterCurrentTupleAction)ctx->action) {
	case CCM_ACTION_UPDATE:
		return ctx->desired_status == MultiXactStatusNoKeyUpdate
			   || ctx->desired_status == MultiXactStatusUpdate;
	case CCM_ACTION_DELETE:
		return ctx->desired_status == MultiXactStatusUpdate && ctx->lock_mode == LockTupleExclusive;
	case CCM_ACTION_LOCK:
	case CCM_ACTION_HOT_FOLLOW:
		return ctx->desired_status <= MultiXactStatusForUpdate;
	}

	return false;
}


static bool
request_context_valid(const ClusterCurrentMxRequestContext *ctx)
{
	return ctx != NULL && current_mx_key_valid(&ctx->mxkey) && TransactionIdIsNormal(ctx->top_xid)
		   && TransactionIdIsNormal(ctx->current_member_xid)
		   && ctx->desired_status <= MaxMultiXactStatus && ctx->lock_mode >= LockTupleKeyShare
		   && ctx->lock_mode <= LockTupleExclusive && ctx->wait_policy >= LockWaitBlock
		   && ctx->wait_policy <= LockWaitError && ctx->action <= CCM_ACTION_HOT_FOLLOW
		   && ctx->tuple_shape <= CCM_SHAPE_DELETED && ctx->follow_updates <= 1
		   && ctx->wait_for_conflict <= 1 && ctx->updater_origin_node_id >= -1
		   && ctx->updater_origin_node_id < CLUSTER_MAX_NODES
		   && desired_status_matches_mode(ctx->desired_status, ctx->lock_mode)
		   && action_status_valid(ctx)
		   && (ctx->precheck_result == TM_Ok || ctx->precheck_result == TM_Invisible
			   || ctx->precheck_result == TM_SelfModified
			   || ctx->precheck_result == TM_BeingModified);
}


static bool
wait_key_precedes(const ClusterTTStatusKey *candidate, const ClusterTTStatusKey *current)
{
	if (candidate->origin_node_id != current->origin_node_id)
		return candidate->origin_node_id < current->origin_node_id;
	if (candidate->local_xid != current->local_xid)
		return candidate->local_xid < current->local_xid;
	if (candidate->tt_slot_id != current->tt_slot_id)
		return candidate->tt_slot_id < current->tt_slot_id;
	return candidate->undo_segment_id < current->undo_segment_id;
}


static ClusterCurrentMxDecision
active_conflict_decision(const ClusterCurrentMxRequestContext *ctx,
						 const ClusterTTStatusKey *holder_key, ClusterTTStatusKey *wait_key)
{
	if (ctx->action == CCM_ACTION_UPDATE || ctx->action == CCM_ACTION_DELETE) {
		if (!ctx->wait_for_conflict)
			return CMDL_BEING_MODIFIED;
	} else {
		if (ctx->wait_policy == LockWaitSkip)
			return CMDL_WOULD_BLOCK;
		if (ctx->wait_policy == LockWaitError)
			return CMDL_LOCK_NOT_AVAILABLE;
	}

	if (wait_key != NULL)
		*wait_key = *holder_key;
	return CMDL_WAIT_MEMBER;
}


ClusterCurrentMxDecision
cluster_multixact_current_decide(const ClusterCurrentMxMemberDesc *members,
								 const ClusterCurrentMemberProof *proofs, uint16 nmembers,
								 const ClusterCurrentMxRequestContext *ctx,
								 const ClusterCurrentUpdaterChallenge *challenge,
								 const ClusterCurrentUpdaterProof *updater_proof,
								 ClusterTTStatusKey *wait_key)
{
	ClusterTTStatusKey selected_wait_key;
	ClusterCurrentMxDecision self_result = CMDL_CONTINUE;
	bool have_active_conflict = false;
	bool have_unknown = false;
	int active_updater = -1;
	int committed_updater = -1;
	uint16 i;

	if (wait_key != NULL)
		memset(wait_key, 0, sizeof(*wait_key));
	memset(&selected_wait_key, 0, sizeof(selected_wait_key));

	if (!request_context_valid(ctx) || !descriptor_entries_valid(members, nmembers)
		|| !descriptor_shape_matches(members, nmembers, (ClusterCurrentTupleShape)ctx->tuple_shape)
		|| proofs == NULL)
		return CMDL_UNKNOWN;

	for (i = 0; i < nmembers; i++) {
		bool conflicts;
		bool valid;

		if (!proof_entry_semantic_valid(&proofs[i], &members[i], i, ctx->mxkey.cluster_epoch, -1))
			return CMDL_UNKNOWN;

		conflicts = cluster_multixact_current_status_conflicts(members[i].member_status,
															   ctx->lock_mode, &valid);
		if (!valid)
			return CMDL_UNKNOWN;

		switch ((ClusterCurrentMemberState)proofs[i].state) {
		case CCM_SELF:
			if (proofs[i].member_xid != ctx->current_member_xid
				&& proofs[i].member_xid != ctx->top_xid)
				return CMDL_UNKNOWN;
			if (ISUPDATE_from_mxstatus(members[i].member_status)
				&& ctx->action != CCM_ACTION_HOT_FOLLOW)
				self_result = ctx->tuple_cmax >= ctx->curcid ? CMDL_SELF_MODIFIED : CMDL_INVISIBLE;
			break;

		case CCM_ACTIVE:
			if (ISUPDATE_from_mxstatus(members[i].member_status))
				active_updater = i;
			if (conflicts
				&& (!have_active_conflict
					|| wait_key_precedes(&proofs[i].key, &selected_wait_key))) {
				selected_wait_key = proofs[i].key;
				have_active_conflict = true;
			}
			break;

		case CCM_COMMITTED:
			/*
			 * A compatible committed NoKeyUpdate can be ignored only when the
			 * caller explicitly does not follow update chains.  With
			 * follow_updates=true, authenticate the successor exactly as for a
			 * conflicting updater before allowing the outer heap path to
			 * advance.
			 */
			if (ISUPDATE_from_mxstatus(members[i].member_status)
				&& (conflicts || ctx->follow_updates))
				committed_updater = i;
			break;

		case CCM_ABORTED:
			break;

		case CCM_UNKNOWN:
			have_unknown = true;
			break;
		}
	}

	if (have_unknown)
		return CMDL_UNKNOWN;

	if (ctx->precheck_result == TM_Invisible)
		return CMDL_INVISIBLE;
	if (ctx->precheck_result == TM_SelfModified)
		return CMDL_SELF_MODIFIED;

	if (self_result != CMDL_CONTINUE)
		return self_result;

	if (committed_updater >= 0) {
		if (ctx->tuple_shape == CCM_SHAPE_DELETED)
			return CMDL_DELETED;
		if (ctx->tuple_shape != CCM_SHAPE_UPDATED
			|| !cluster_multixact_current_validate_updater_proof(
				&ctx->mxkey, members, proofs, nmembers, challenge, updater_proof,
				(uint16)ctx->updater_origin_node_id))
			return CMDL_UNKNOWN;
		return CMDL_UPDATED;
	}

	/*
	 * KeyShare is compatible with an in-progress NoKeyUpdate, but native
	 * follow_updates semantics still require the exact successor chain to be
	 * locked.  Keep that continuation distinct from ordinary CONTINUE so the
	 * heap caller cannot stamp only the stale root.  The first successor is
	 * usable only after the same full-key proof required for a committed
	 * updater.
	 */
	if (active_updater >= 0 && !have_active_conflict
		&& ctx->action == CCM_ACTION_LOCK && ctx->follow_updates) {
		if (ctx->tuple_shape != CCM_SHAPE_UPDATED
			|| !validate_updater_proof_state(
				&ctx->mxkey, members, proofs, nmembers, challenge, updater_proof,
				(uint16)ctx->updater_origin_node_id, CCM_ACTIVE))
			return CMDL_UNKNOWN;
		return CMDL_FOLLOW_UPDATED;
	}

	if (have_active_conflict)
		return active_conflict_decision(ctx, &selected_wait_key, wait_key);

	return CMDL_CONTINUE;
}


ClusterMxDescribeResult
cluster_multixact_current_describe(const ClusterCurrentMxKey *key,
								   ClusterCurrentMxMemberDesc *members, uint16 members_cap,
								   uint16 *nmembers, uint32 *reported_total_members)
{
	uint64 current_epoch;
	MultiXactMember *native_members = NULL;
	int native_count = -1;
	ClusterMxDescribeResult result = CMX_DESC_UNKNOWN;
	int origin_slot;
	uint16 i;

	if (members != NULL && members_cap > 0)
		memset(members, 0, sizeof(*members) * members_cap);
	if (nmembers != NULL)
		*nmembers = 0;
	if (reported_total_members != NULL)
		*reported_total_members = 0;
	if (key == NULL || members == NULL || nmembers == NULL || reported_total_members == NULL
		|| members_cap < 2 || !current_mx_key_valid(key) || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES)
		return CMX_DESC_UNKNOWN;

	current_epoch = cluster_epoch_get_current();
	if (current_epoch > UINT32_MAX
		|| key->cluster_epoch != (uint32)current_epoch)
		return CMX_DESC_UNKNOWN;

	origin_slot = cluster_mxid_origin_slot(key->multixact_id);
	if (origin_slot < 0 || origin_slot != (int)key->origin_node_id)
		return CMX_DESC_UNKNOWN;
	if (key->origin_node_id != (uint16)cluster_node_id) {
		cluster_multixact_current_stats_bump(CMX_STAT_DESCRIBE_REMOTE_ASK);
		PG_TRY();
		{
			result = cluster_gcs_current_mx_describe_fetch_and_wait(
				(int32)key->origin_node_id, key, members, members_cap,
				nmembers, reported_total_members);
		}
		PG_CATCH();
		{
			cluster_multixact_current_stats_bump(
				CMX_STAT_DESCRIBE_REMOTE_UNKNOWN);
			PG_RE_THROW();
		}
		PG_END_TRY();
		switch (result) {
		case CMX_DESC_OK:
			cluster_multixact_current_stats_bump(CMX_STAT_DESCRIBE_REMOTE_HIT);
			break;
		case CMX_DESC_DENIED:
			cluster_multixact_current_stats_bump(CMX_STAT_DESCRIBE_REMOTE_DENIED);
			break;
		case CMX_DESC_SUPPORTED_LIMIT:
			cluster_multixact_current_stats_bump(
				CMX_STAT_DESCRIBE_REMOTE_SUPPORTED_LIMIT);
			break;
		case CMX_DESC_TIMEOUT:
			cluster_multixact_current_stats_bump(CMX_STAT_DESCRIBE_REMOTE_TIMEOUT);
			break;
		case CMX_DESC_UNKNOWN:
			cluster_multixact_current_stats_bump(CMX_STAT_DESCRIBE_REMOTE_UNKNOWN);
			break;
		}
		return result;
	}
	if (!cluster_mxid_is_mine(key->multixact_id))
		return CMX_DESC_DENIED;

	cluster_multixact_current_stats_bump(CMX_STAT_DESCRIBE_LOCAL);
	native_count = GetMultiXactIdMembers(key->multixact_id, &native_members, false, false);
	if (native_count > CLUSTER_CURRENT_MX_MAX_MEMBERS) {
		*reported_total_members = (uint32)native_count;
		result = CMX_DESC_SUPPORTED_LIMIT;
	} else if (native_count < 2 || native_count > members_cap) {
		result = CMX_DESC_DENIED;
	} else {
		for (i = 0; i < (uint16)native_count; i++) {
			members[i].xid = native_members[i].xid;
			members[i].member_status = (uint8)native_members[i].status;
		}
		result = cluster_multixact_current_validate_descriptor(
			key, (uint16)cluster_node_id, (uint32)current_epoch, members,
			(uint16)native_count, (uint32)native_count);
		if (result == CMX_DESC_OK) {
			*nmembers = (uint16)native_count;
			*reported_total_members = (uint32)native_count;
		}
	}

	if (native_members != NULL)
		pfree(native_members);
	if (result != CMX_DESC_OK && result != CMX_DESC_SUPPORTED_LIMIT) {
		memset(members, 0, sizeof(*members) * members_cap);
		*nmembers = 0;
		*reported_total_members = 0;
	}
	return result;
}


ClusterMxResolveResult
cluster_multixact_current_members_resolve(const ClusterCurrentMxKey *key,
										  const ClusterCurrentMxMemberDesc *members,
										  uint16 nmembers, uint64 descriptor_hash,
										  const ClusterCurrentUpdaterChallenge *challenge,
										  ClusterCurrentMemberProof *proofs,
										  ClusterCurrentUpdaterProof *updater_proof)
{
	ClusterCurrentMxProofRequestPlan plans[CLUSTER_CURRENT_MX_MAX_CHUNKS];
	uint16 member_origins[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	bool seen[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	uint64 current_epoch;
	uint16 plan_count = 0;
	uint16 i;
	ClusterMxResolveResult result;

	if (updater_proof != NULL) {
		memset(updater_proof, 0, sizeof(*updater_proof));
		updater_proof->verdict = CUCP_UNKNOWN;
	}
	if (nmembers > CLUSTER_CURRENT_MX_MAX_MEMBERS)
		return CMX_RESOLVE_SUPPORTED_LIMIT;
	proof_array_set_unknown(proofs, nmembers);
	if (key == NULL || members == NULL || proofs == NULL || updater_proof == NULL
		|| nmembers < 2 || cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES)
		return CMX_RESOLVE_UNKNOWN;

	current_epoch = cluster_epoch_get_current();
	if (current_epoch > UINT32_MAX
		|| key->cluster_epoch != (uint32)current_epoch
		|| descriptor_hash
			   != cluster_multixact_current_descriptor_hash(key, members, nmembers))
		return CMX_RESOLVE_UNKNOWN;
	for (i = 0; i < nmembers; i++) {
		int origin = cluster_xid_origin_slot(members[i].xid);

		if (origin < 0 || origin >= CLUSTER_MAX_NODES)
			return CMX_RESOLVE_UNKNOWN;
		member_origins[i] = (uint16)origin;
	}

	result = cluster_multixact_current_wire_build_proof_requests(
		key, members, member_origins, nmembers, descriptor_hash, challenge, UINT64CONST(1),
		current_epoch, cluster_node_id, (int32)MyBackendId, plans, lengthof(plans), &plan_count);
	if (result != CMX_RESOLVE_OK)
		return result;

	memset(seen, 0, sizeof(seen));
	for (i = 0; i < plan_count; i++) {
		ClusterCurrentMemberProof chunk_proofs[CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME];
		ClusterCurrentUpdaterProof chunk_updater;
		ClusterCurrentMxProofForwardV2 *request = &plans[i].request;
		uint16 chunk_count = 0;
		uint16 j;

		memset(chunk_proofs, 0, sizeof(chunk_proofs));
		memset(&chunk_updater, 0, sizeof(chunk_updater));
		chunk_updater.verdict = CUCP_UNKNOWN;

		if (plans[i].destination_node_id == (uint16)cluster_node_id) {
			if (request->prefix.body_kind == CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS) {
				for (j = 0; j < request->prefix.entry_count; j++) {
					const ClusterCurrentMxProofAskWire *ask
						= &request->trailer.body.asks[j];
					ClusterTTStatusKey initial_key;
					ClusterTTStatusResult initial_result;

					if (!cluster_runtime_visibility_current_owner_lookup_exact(
							ask->xid, &initial_key, &initial_result)
						|| !cluster_multixact_current_resolve_origin_member_proof(
							ask->xid, ask->member_status, ask->member_ordinal,
							(uint16)cluster_node_id, (uint32)current_epoch,
							TransactionIdIsCurrentTransactionId(ask->xid), &initial_key,
							&initial_result, NULL, NULL,
							&chunk_proofs[j]))
						goto unknown;
				}
				chunk_count = request->prefix.entry_count;
			} else {
				const ClusterCurrentMxUpdaterChallengeWire *wire_challenge
					= &request->trailer.body.updater.challenge;
				ClusterTTStatusKey initial_key;
				ClusterTTStatusResult initial_result;
				ClusterUpdaterCandidateVerdict candidate_verdict;

				candidate_verdict = cluster_multixact_current_updater_candidate_verdict(
					&wire_challenge->candidate_next_xmin_key,
					wire_challenge->updater_xid, (uint16)cluster_node_id,
					(uint32)current_epoch, &initial_key, &initial_result);
				if (candidate_verdict == CUCP_UNKNOWN
					|| !cluster_multixact_current_resolve_origin_member_proof(
						wire_challenge->updater_xid, wire_challenge->member_status,
						wire_challenge->member_ordinal, (uint16)cluster_node_id,
						(uint32)current_epoch,
						TransactionIdIsCurrentTransactionId(wire_challenge->updater_xid),
						&initial_key, &initial_result, NULL, NULL,
						&chunk_proofs[0]))
					goto unknown;
				chunk_count = 1;
				chunk_updater.mxkey = request->prefix.mxkey;
				chunk_updater.candidate_next_xmin_key
					= wire_challenge->candidate_next_xmin_key;
				chunk_updater.updater_xid = wire_challenge->updater_xid;
				chunk_updater.member_ordinal = wire_challenge->member_ordinal;
				chunk_updater.verdict = candidate_verdict;
			}
		} else {
			cluster_multixact_current_stats_bump(CMX_STAT_MEMBER_PROOF_ASK);
			PG_TRY();
			{
				result = cluster_gcs_current_mx_member_proof_fetch_and_wait(
					plans[i].destination_node_id, request, chunk_proofs,
					lengthof(chunk_proofs), &chunk_count, &chunk_updater);
			}
			PG_CATCH();
			{
				cluster_multixact_current_stats_bump(
					CMX_STAT_MEMBER_PROOF_UNKNOWN);
				PG_RE_THROW();
			}
			PG_END_TRY();
			switch (result) {
			case CMX_RESOLVE_OK:
				cluster_multixact_current_stats_bump(CMX_STAT_MEMBER_PROOF_HIT);
				break;
			case CMX_RESOLVE_DENIED:
				cluster_multixact_current_stats_bump(CMX_STAT_MEMBER_PROOF_DENIED);
				break;
			case CMX_RESOLVE_SUPPORTED_LIMIT:
				cluster_multixact_current_stats_bump(
					CMX_STAT_MEMBER_PROOF_SUPPORTED_LIMIT);
				break;
			case CMX_RESOLVE_TIMEOUT:
				cluster_multixact_current_stats_bump(CMX_STAT_MEMBER_PROOF_TIMEOUT);
				break;
			case CMX_RESOLVE_UNKNOWN:
				cluster_multixact_current_stats_bump(CMX_STAT_MEMBER_PROOF_UNKNOWN);
				break;
			}
			if (result != CMX_RESOLVE_OK)
				goto non_ok;
		}

		if (chunk_count != request->prefix.entry_count)
			goto unknown;
		for (j = 0; j < chunk_count; j++) {
			uint16 ordinal = chunk_proofs[j].member_ordinal;

			if (ordinal >= nmembers || seen[ordinal]
				|| member_origins[ordinal] != plans[i].destination_node_id
				|| !proof_entry_semantic_valid(
					&chunk_proofs[j], &members[ordinal], ordinal, (uint32)current_epoch,
					plans[i].destination_node_id)
				|| chunk_proofs[j].state == CCM_UNKNOWN)
				goto unknown;
			seen[ordinal] = true;
			proofs[ordinal] = chunk_proofs[j];
		}

		if (request->prefix.body_kind == CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE) {
			const ClusterCurrentMxUpdaterChallengeWire *wire_challenge
				= &request->trailer.body.updater.challenge;

			if (!current_mx_key_equal(&chunk_updater.mxkey, key)
				|| memcmp(&chunk_updater.candidate_next_xmin_key,
						  &wire_challenge->candidate_next_xmin_key,
						  sizeof(ClusterTTStatusKey))
					   != 0
				|| chunk_updater.updater_xid != wire_challenge->updater_xid
				|| chunk_updater.member_ordinal != wire_challenge->member_ordinal
				|| chunk_updater.verdict > CUCP_UNKNOWN || chunk_updater.reserved8 != 0
				|| chunk_updater.verdict == CUCP_UNKNOWN)
				goto unknown;
			*updater_proof = chunk_updater;
		}
		if (cluster_epoch_get_current() != current_epoch)
			goto unknown;
	}

	for (i = 0; i < nmembers; i++)
		if (!seen[i])
			goto unknown;
	return CMX_RESOLVE_OK;

non_ok:
	proof_array_set_unknown(proofs, nmembers);
	memset(updater_proof, 0, sizeof(*updater_proof));
	updater_proof->verdict = CUCP_UNKNOWN;
	return result;

unknown:
	proof_array_set_unknown(proofs, nmembers);
	memset(updater_proof, 0, sizeof(*updater_proof));
	updater_proof->verdict = CUCP_UNKNOWN;
	return CMX_RESOLVE_UNKNOWN;
}


ClusterMxRecomposeResult
cluster_multixact_current_recompose(const ClusterCurrentMxMemberDesc *members,
									const ClusterCurrentMemberProof *proofs, uint16 nmembers,
									TransactionId requester_xid, MultiXactStatus requester_status,
									MultiXactMember *normalized_members, uint16 normalized_cap,
									uint16 *normalized_count)
{
	MultiXactMember scratch[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	uint16 out_count = 0;
	int requester_index = -1;
	int updater_index = -1;
	uint16 i;

	memset(scratch, 0, sizeof(scratch));
	if (normalized_members != NULL && normalized_cap > 0)
		memset(normalized_members, 0, sizeof(*normalized_members) * normalized_cap);
	if (normalized_count != NULL)
		*normalized_count = 0;

	if (nmembers > CLUSTER_CURRENT_MX_MAX_MEMBERS)
		return CMX_RECOMPOSE_SUPPORTED_LIMIT;
	if (!descriptor_entries_valid(members, nmembers) || proofs == NULL
		|| !TransactionIdIsNormal(requester_xid) || requester_status > MaxMultiXactStatus
		|| normalized_members == NULL || normalized_count == NULL)
		return CMX_RECOMPOSE_UNKNOWN;
	if (normalized_cap == 0 || normalized_cap > CLUSTER_CURRENT_MX_MAX_MEMBERS)
		return CMX_RECOMPOSE_SUPPORTED_LIMIT;

	for (i = 0; i < nmembers; i++) {
		const ClusterCurrentMemberProof *proof = &proofs[i];
		bool keep = false;

		if (proof->member_ordinal != i || proof->member_xid != members[i].xid
			|| proof->member_status != members[i].member_status || proof->state > CCM_UNKNOWN
			|| proof->reserved8[0] != 0 || proof->reserved8[1] != 0
			|| proof->reserved8[2] != 0 || proof->reserved8[3] != 0)
			return CMX_RECOMPOSE_UNKNOWN;

		switch ((ClusterCurrentMemberState)proof->state) {
		case CCM_SELF:
		case CCM_ACTIVE:
			if (proof->commit_scn != InvalidScn
				|| !tt_key_valid_holder(&proof->key, proof->member_xid,
										proof->key.cluster_epoch,
										proof->key.origin_node_id))
				return CMX_RECOMPOSE_UNKNOWN;
			keep = true;
			break;
		case CCM_COMMITTED:
			if (!tt_key_is_zero(&proof->key) || !SCN_VALID(proof->commit_scn))
				return CMX_RECOMPOSE_UNKNOWN;
			/*
			 * A committed updater changes the tuple version and must have
			 * been returned as UPDATED/DELETED by the compositor.  Never
			 * silently normalize it into a writable old-version set.
			 */
			if (ISUPDATE_from_mxstatus(members[i].member_status))
				return CMX_RECOMPOSE_DENIED;
			break;
		case CCM_ABORTED:
			if (!tt_key_is_zero(&proof->key) || proof->commit_scn != InvalidScn)
				return CMX_RECOMPOSE_UNKNOWN;
			break;
		case CCM_UNKNOWN:
			return CMX_RECOMPOSE_UNKNOWN;
		}

		if (keep) {
			uint16 out = out_count;

			if (out_count >= normalized_cap)
				return CMX_RECOMPOSE_SUPPORTED_LIMIT;
			scratch[out].xid = members[i].xid;
			scratch[out].status = (MultiXactStatus)members[i].member_status;
			if (TransactionIdEquals(members[i].xid, requester_xid))
				requester_index = out;
			if (ISUPDATE_from_mxstatus(members[i].member_status))
				updater_index = out;
			out_count++;
		}
	}

	if (requester_index >= 0) {
		MultiXactStatus old_status = scratch[requester_index].status;
		int old_strength;
		int new_strength;
		bool updater;

		switch (old_status) {
		case MultiXactStatusForKeyShare:
			old_strength = 0;
			break;
		case MultiXactStatusForShare:
			old_strength = 1;
			break;
		case MultiXactStatusForNoKeyUpdate:
		case MultiXactStatusNoKeyUpdate:
			old_strength = 2;
			break;
		case MultiXactStatusForUpdate:
		case MultiXactStatusUpdate:
			old_strength = 3;
			break;
		default:
			return CMX_RECOMPOSE_UNKNOWN;
		}
		switch (requester_status) {
		case MultiXactStatusForKeyShare:
			new_strength = 0;
			break;
		case MultiXactStatusForShare:
			new_strength = 1;
			break;
		case MultiXactStatusForNoKeyUpdate:
		case MultiXactStatusNoKeyUpdate:
			new_strength = 2;
			break;
		case MultiXactStatusForUpdate:
		case MultiXactStatusUpdate:
			new_strength = 3;
			break;
		default:
			return CMX_RECOMPOSE_UNKNOWN;
		}

		new_strength = Max(old_strength, new_strength);
		updater = ISUPDATE_from_mxstatus(old_status)
			|| ISUPDATE_from_mxstatus(requester_status);
		if (updater)
			scratch[requester_index].status
				= new_strength == 3 ? MultiXactStatusUpdate : MultiXactStatusNoKeyUpdate;
		else {
			static const MultiXactStatus lock_status[] = {
				MultiXactStatusForKeyShare,
				MultiXactStatusForShare,
				MultiXactStatusForNoKeyUpdate,
				MultiXactStatusForUpdate,
			};

			scratch[requester_index].status = lock_status[new_strength];
		}
		if (updater)
			updater_index = requester_index;
	} else {
		if (ISUPDATE_from_mxstatus(requester_status) && updater_index >= 0)
			return CMX_RECOMPOSE_DENIED;
		if (out_count >= normalized_cap)
			return CMX_RECOMPOSE_SUPPORTED_LIMIT;
		scratch[out_count].xid = requester_xid;
		scratch[out_count].status = requester_status;
		out_count++;
	}

	if (out_count > CLUSTER_CURRENT_MX_MAX_MEMBERS)
		return CMX_RECOMPOSE_SUPPORTED_LIMIT;
	memcpy(normalized_members, scratch, sizeof(*normalized_members) * out_count);
	*normalized_count = out_count;
	return CMX_RECOMPOSE_OK;
}
