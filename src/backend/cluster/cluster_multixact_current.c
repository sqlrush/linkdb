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

#include "cluster/cluster_conf.h"
#include "cluster/cluster_multixact_current.h"
#include "storage/lock.h"


static bool
current_mx_key_equal(const ClusterCurrentMxKey *a, const ClusterCurrentMxKey *b)
{
	return memcmp(a, b, sizeof(*a)) == 0;
}


static bool
current_mx_key_valid(const ClusterCurrentMxKey *key)
{
	return key != NULL && key->origin_node_id < CLUSTER_MAX_NODES && key->reserved16 == 0
		   && key->reserved32 == 0 && MultiXactIdIsValid(key->multixact_id)
		   && key->cluster_epoch != 0;
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
		|| key->cluster_epoch != epoch || key->local_xid != xid)
		return false;
	if (expected_origin >= 0 && key->origin_node_id != (uint16)expected_origin)
		return false;
	return true;
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


bool
cluster_multixact_current_validate_updater_proof(const ClusterCurrentMxKey *key,
												 const ClusterCurrentMxMemberDesc *members,
												 const ClusterCurrentMemberProof *proofs,
												 uint16 nmembers,
												 const ClusterCurrentUpdaterChallenge *challenge,
												 const ClusterCurrentUpdaterProof *updater_proof,
												 uint16 updater_origin_node_id)
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

	if (updater_ordinal < 0 || proofs[updater_ordinal].state != CCM_COMMITTED
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

	if (have_active_conflict)
		return active_conflict_decision(ctx, &selected_wait_key, wait_key);

	return CMDL_CONTINUE;
}


ClusterMxDescribeResult
cluster_multixact_current_describe(const ClusterCurrentMxKey *key,
								   ClusterCurrentMxMemberDesc *members, uint16 members_cap,
								   uint16 *nmembers, uint32 *reported_total_members)
{
	(void)key;
	if (members != NULL && members_cap > 0)
		memset(members, 0, sizeof(*members) * members_cap);
	if (nmembers != NULL)
		*nmembers = 0;
	if (reported_total_members != NULL)
		*reported_total_members = 0;
	return CMX_DESC_UNKNOWN;
}


ClusterMxResolveResult
cluster_multixact_current_members_resolve(const ClusterCurrentMxKey *key,
										  const ClusterCurrentMxMemberDesc *members,
										  uint16 nmembers, uint64 descriptor_hash,
										  const ClusterCurrentUpdaterChallenge *challenge,
										  ClusterCurrentMemberProof *proofs,
										  ClusterCurrentUpdaterProof *updater_proof)
{
	(void)key;
	(void)members;
	(void)descriptor_hash;
	(void)challenge;
	if (updater_proof != NULL) {
		memset(updater_proof, 0, sizeof(*updater_proof));
		updater_proof->verdict = CUCP_UNKNOWN;
	}
	if (nmembers > CLUSTER_CURRENT_MX_MAX_MEMBERS)
		return CMX_RESOLVE_SUPPORTED_LIMIT;
	proof_array_set_unknown(proofs, nmembers);
	return CMX_RESOLVE_UNKNOWN;
}


ClusterMxRecomposeResult
cluster_multixact_current_recompose(const ClusterCurrentMxMemberDesc *members,
									const ClusterCurrentMemberProof *proofs, uint16 nmembers,
									TransactionId requester_xid, MultiXactStatus requester_status,
									MultiXactMember *normalized_members, uint16 normalized_cap,
									uint16 *normalized_count)
{
	(void)members;
	(void)proofs;
	(void)nmembers;
	(void)requester_xid;
	(void)requester_status;
	if (normalized_members != NULL && normalized_cap > 0)
		memset(normalized_members, 0, sizeof(*normalized_members) * normalized_cap);
	if (normalized_count != NULL)
		*normalized_count = 0;
	return CMX_RECOMPOSE_UNKNOWN;
}
