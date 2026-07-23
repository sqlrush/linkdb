/*-------------------------------------------------------------------------
 *
 * cluster_multixact_current.h
 *	  Current-DML authority for cluster MultiXacts.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_multixact_current.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-3.6b-multixact-current-dml.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_MULTIXACT_CURRENT_H
#define CLUSTER_MULTIXACT_CURRENT_H

#include "access/multixact.h"
#include "access/tableam.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_tt_status.h"
#include "nodes/lockoptions.h"


#define CLUSTER_CURRENT_MX_MAX_MEMBERS 256
#define CLUSTER_CURRENT_MX_MAX_CHUNKS 256
#define CLUSTER_CURRENT_MX_MAX_PROOFS_PER_CHUNK 32


/*
 * Stable identity of an immutable member list at its MXID origin.
 */
typedef struct ClusterCurrentMxKey {
	uint16 origin_node_id;
	uint16 reserved16;
	MultiXactId multixact_id;
	uint32 cluster_epoch;
	uint32 reserved32;
} ClusterCurrentMxKey;


/*
 * Immutable member entry returned by the MXID origin.
 */
typedef struct ClusterCurrentMxMemberDesc {
	TransactionId xid;
	uint8 member_status;
	uint8 reserved8[3];
} ClusterCurrentMxMemberDesc;


typedef enum ClusterCurrentMemberState {
	CCM_SELF = 0,
	CCM_ACTIVE,
	CCM_COMMITTED,
	CCM_ABORTED,
	CCM_UNKNOWN
} ClusterCurrentMemberState;


/*
 * Current-state proof returned by the origin of member_xid.
 */
typedef struct ClusterCurrentMemberProof {
	ClusterTTStatusKey key;
	SCN commit_scn;
	TransactionId member_xid;
	uint16 member_ordinal;
	uint8 member_status;
	uint8 state;
	uint8 reserved8[4];
} ClusterCurrentMemberProof;


typedef enum ClusterUpdaterCandidateVerdict {
	CUCP_MATCH = 0,
	CUCP_MISMATCH,
	CUCP_UNKNOWN
} ClusterUpdaterCandidateVerdict;


typedef struct ClusterCurrentUpdaterProof {
	ClusterCurrentMxKey mxkey;
	ClusterTTStatusKey candidate_next_xmin_key;
	TransactionId updater_xid;
	uint16 member_ordinal;
	uint8 verdict;
	uint8 reserved8;
} ClusterCurrentUpdaterProof;


typedef struct ClusterCurrentUpdaterChallenge {
	ClusterTTStatusKey candidate_next_xmin_key;
	TransactionId updater_xid;
	uint16 member_ordinal;
	uint16 reserved16;
} ClusterCurrentUpdaterChallenge;


typedef enum ClusterMxDescribeResult {
	CMX_DESC_OK = 0,
	CMX_DESC_DENIED,
	CMX_DESC_SUPPORTED_LIMIT,
	CMX_DESC_TIMEOUT,
	CMX_DESC_UNKNOWN
} ClusterMxDescribeResult;


typedef enum ClusterMxResolveResult {
	CMX_RESOLVE_OK = 0,
	CMX_RESOLVE_DENIED,
	CMX_RESOLVE_SUPPORTED_LIMIT,
	CMX_RESOLVE_TIMEOUT,
	CMX_RESOLVE_UNKNOWN
} ClusterMxResolveResult;


typedef enum ClusterCurrentTupleAction {
	CCM_ACTION_UPDATE = 0,
	CCM_ACTION_DELETE,
	CCM_ACTION_LOCK,
	CCM_ACTION_HOT_FOLLOW
} ClusterCurrentTupleAction;


typedef enum ClusterCurrentTupleShape {
	CCM_SHAPE_LOCK_ONLY = 0,
	CCM_SHAPE_UPDATED,
	CCM_SHAPE_DELETED
} ClusterCurrentTupleShape;


/*
 * Requester-local decision context.  This structure is not a wire ABI.
 */
typedef struct ClusterCurrentMxRequestContext {
	ClusterCurrentMxKey mxkey;
	TransactionId top_xid;
	TransactionId current_member_xid;
	CommandId curcid;
	CommandId tuple_cmax;
	TM_Result precheck_result;
	MultiXactStatus desired_status;
	LockTupleMode lock_mode;
	LockWaitPolicy wait_policy;
	uint8 action;
	uint8 tuple_shape;
	uint8 follow_updates;
	uint8 wait_for_conflict;
	/* Requester-derived updater member origin; -1 when no challenge exists. */
	int32 updater_origin_node_id;
} ClusterCurrentMxRequestContext;


typedef enum ClusterCurrentMxDecision {
	CMDL_CONTINUE = 0,
	CMDL_INVISIBLE,
	CMDL_SELF_MODIFIED,
	CMDL_BEING_MODIFIED,
	CMDL_WAIT_MEMBER,
	CMDL_WOULD_BLOCK,
	CMDL_LOCK_NOT_AVAILABLE,
	CMDL_UPDATED,
	CMDL_DELETED,
	CMDL_UNKNOWN
} ClusterCurrentMxDecision;


/*
 * Requester-local validated view of one proof reply chunk.  The pointed-to
 * proof entries use the stable wire structure above; the pointer itself is
 * never transmitted.
 */
typedef struct ClusterCurrentProofChunkView {
	uint64 request_id;
	ClusterCurrentMxKey mxkey;
	uint64 descriptor_hash;
	uint32 total_count;
	uint16 source_node_id;
	uint16 chunk_ordinal;
	uint16 chunk_count;
	uint16 proof_count;
	uint16 reserved16;
	const ClusterCurrentMemberProof *proofs;
} ClusterCurrentProofChunkView;


/*
 * Identity for an operation-local immutable-descriptor memo.  The tuple/PCM-X
 * fingerprint is opaque to this module and supplied by the heap caller.
 */
typedef struct ClusterCurrentMxOperationFingerprint {
	uint64 operation_id;
	ClusterCurrentMxKey mxkey;
	MultiXactId captured_raw_xmax;
	uint32 reserved32;
	uint64 tuple_pcm_fingerprint_hi;
	uint64 tuple_pcm_fingerprint_lo;
} ClusterCurrentMxOperationFingerprint;


typedef enum ClusterMxRecomposeResult {
	CMX_RECOMPOSE_OK = 0,
	CMX_RECOMPOSE_DENIED,
	CMX_RECOMPOSE_SUPPORTED_LIMIT,
	CMX_RECOMPOSE_UNKNOWN
} ClusterMxRecomposeResult;


StaticAssertDecl(sizeof(ClusterCurrentMxKey) == 16, "ClusterCurrentMxKey must remain 16 bytes");
StaticAssertDecl(sizeof(ClusterCurrentMxMemberDesc) == 8,
				 "ClusterCurrentMxMemberDesc must remain 8 bytes");
StaticAssertDecl(sizeof(ClusterCurrentMemberProof) == 48,
				 "ClusterCurrentMemberProof must remain 48 bytes");
StaticAssertDecl(sizeof(ClusterCurrentUpdaterProof) == 48,
				 "ClusterCurrentUpdaterProof must remain 48 bytes");
StaticAssertDecl(sizeof(ClusterCurrentUpdaterChallenge) == 32,
				 "ClusterCurrentUpdaterChallenge must remain 32 bytes");


extern ClusterMxDescribeResult cluster_multixact_current_validate_descriptor(
	const ClusterCurrentMxKey *key, uint16 source_node_id, uint32 current_epoch,
	const ClusterCurrentMxMemberDesc *members, uint16 nmembers, uint32 reported_total_members);
extern uint64 cluster_multixact_current_descriptor_hash(const ClusterCurrentMxKey *key,
														const ClusterCurrentMxMemberDesc *members,
														uint16 nmembers);
extern ClusterMxResolveResult cluster_multixact_current_validate_proof_set(
	const ClusterCurrentMxKey *key, const ClusterCurrentMxMemberDesc *members,
	/* Requester-derived before send; never copied from reply data. */
	const uint16 *member_origin_nodes, uint16 nmembers, uint64 request_id, uint64 descriptor_hash,
	const ClusterCurrentProofChunkView *chunks, uint16 nchunks,
	ClusterCurrentMemberProof *ordered_proofs);
extern bool cluster_multixact_current_validate_updater_proof(
	const ClusterCurrentMxKey *key, const ClusterCurrentMxMemberDesc *members,
	const ClusterCurrentMemberProof *proofs, uint16 nmembers,
	const ClusterCurrentUpdaterChallenge *challenge,
	const ClusterCurrentUpdaterProof *updater_proof, uint16 updater_origin_node_id);
extern bool cluster_multixact_current_status_conflicts(uint8 member_status,
													   LockTupleMode wanted_mode, bool *valid_out);
extern ClusterCurrentMxDecision cluster_multixact_current_decide(
	const ClusterCurrentMxMemberDesc *members, const ClusterCurrentMemberProof *proofs,
	uint16 nmembers, const ClusterCurrentMxRequestContext *ctx,
	const ClusterCurrentUpdaterChallenge *challenge,
	const ClusterCurrentUpdaterProof *updater_proof, ClusterTTStatusKey *wait_key);

extern ClusterMxDescribeResult
cluster_multixact_current_describe(const ClusterCurrentMxKey *key,
								   ClusterCurrentMxMemberDesc *members, uint16 members_cap,
								   uint16 *nmembers, uint32 *reported_total_members);
extern ClusterMxResolveResult cluster_multixact_current_members_resolve(
	const ClusterCurrentMxKey *key, const ClusterCurrentMxMemberDesc *members, uint16 nmembers,
	uint64 descriptor_hash, const ClusterCurrentUpdaterChallenge *challenge,
	ClusterCurrentMemberProof *proofs, ClusterCurrentUpdaterProof *updater_proof);
extern ClusterMxRecomposeResult cluster_multixact_current_recompose(
	const ClusterCurrentMxMemberDesc *members, const ClusterCurrentMemberProof *proofs,
	uint16 nmembers, TransactionId requester_xid, MultiXactStatus requester_status,
	MultiXactMember *normalized_members, uint16 normalized_cap, uint16 *normalized_count);

#endif /* CLUSTER_MULTIXACT_CURRENT_H */
