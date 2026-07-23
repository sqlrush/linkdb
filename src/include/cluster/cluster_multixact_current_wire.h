/*-------------------------------------------------------------------------
 *
 * cluster_multixact_current_wire.h
 *	  Strict wire ABI for current-DML MultiXact authority requests.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_multixact_current_wire.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-3.6b-multixact-current-dml.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_MULTIXACT_CURRENT_WIRE_H
#define CLUSTER_MULTIXACT_CURRENT_WIRE_H

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_multixact_current.h"


#define CLUSTER_CURRENT_MX_WIRE_MAGIC ((uint32)0x5047434d)
#define CLUSTER_CURRENT_MX_WIRE_VERSION 1
#define CLUSTER_CURRENT_MX_WIRE_FLAGS_NONE 0
#define CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME 7


typedef enum ClusterCurrentMxProofBodyKind {
	CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS = 1,
	CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE = 2
} ClusterCurrentMxProofBodyKind;


/*
 * The generic DATA router may consume only these byte-stable identity fields.
 * kind_body_a/kind_body_b are opaque there and become typed only after the
 * exact 128-byte kind branch.
 */
typedef struct ClusterCurrentMxRoutingPrefixWire {
	uint64 request_id;					 /* [ 0,  8) */
	uint64 epoch;						 /* [ 8, 16) */
	uint8 kind_body_a[20];				 /* [16, 36) */
	int32 original_requester_node;		 /* [36, 40) */
	int32 requester_backend_id;			 /* [40, 44) */
	uint8 kind_body_b[19];				 /* [44, 63) */
	uint8 kind;							 /* [63, 64) */
} ClusterCurrentMxRoutingPrefixWire;


/*
 * Describe uses the old BufferTag bytes for its MXID key.  Every other
 * overlaid prefix byte and every unused trailer byte is zero in V1.
 */
typedef struct ClusterCurrentMxDescribePrefixWire {
	uint64 request_id;					 /* [ 0,  8) */
	uint64 epoch;						 /* [ 8, 16) */
	ClusterCurrentMxKey mxkey;			 /* [16, 32) */
	uint8 reserved_a[4];				 /* [32, 36) */
	int32 original_requester_node;		 /* [36, 40) */
	int32 requester_backend_id;			 /* [40, 44) */
	uint8 reserved_b[19];				 /* [44, 63) */
	uint8 kind;							 /* [63, 64) */
} ClusterCurrentMxDescribePrefixWire;


typedef struct ClusterCurrentMxDescribeTrailerWire {
	uint32 magic;
	uint16 version;
	uint16 flags;
	uint8 reserved[56];
} ClusterCurrentMxDescribeTrailerWire;


typedef struct ClusterCurrentMxDescribeForwardV2 {
	ClusterCurrentMxDescribePrefixWire prefix;
	ClusterCurrentMxDescribeTrailerWire trailer;
} ClusterCurrentMxDescribeForwardV2;


typedef struct ClusterCurrentMxProofAskWire {
	TransactionId xid;
	uint16 member_ordinal;
	uint8 member_status;
	uint8 reserved8;
} ClusterCurrentMxProofAskWire;


typedef struct ClusterCurrentMxUpdaterChallengeWire {
	ClusterTTStatusKey candidate_next_xmin_key;
	TransactionId updater_xid;
	uint16 member_ordinal;
	uint8 member_status;
	uint8 reserved8;
} ClusterCurrentMxUpdaterChallengeWire;


typedef struct ClusterCurrentMxUpdaterChallengeBodyWire {
	ClusterCurrentMxUpdaterChallengeWire challenge;
	uint8 reserved[24];
} ClusterCurrentMxUpdaterChallengeBodyWire;


typedef union ClusterCurrentMxProofRequestBodyWire {
	ClusterCurrentMxProofAskWire asks[CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME];
	ClusterCurrentMxUpdaterChallengeBodyWire updater;
	uint8 raw[56];
} ClusterCurrentMxProofRequestBodyWire;


/*
 * Proof V2 consumes all kind-specific prefix overlay bytes.  The descriptor
 * hash sits at the old unaligned SCN carrier and is therefore accessed only
 * through memcpy byte helpers.
 */
typedef struct ClusterCurrentMxProofPrefixWire {
	uint64 request_id;					 /* [ 0,  8) */
	uint64 epoch;						 /* [ 8, 16) */
	ClusterCurrentMxKey mxkey;			 /* [16, 32) */
	uint8 reserved_a[4];				 /* [32, 36) */
	int32 original_requester_node;		 /* [36, 40) */
	int32 requester_backend_id;			 /* [40, 44) */
	uint32 total_count;					 /* [44, 48) */
	uint8 chunk_ordinal;				 /* [48, 49) */
	uint8 descriptor_hash_bytes[8];		 /* [49, 57) */
	uint8 chunk_count_minus_one;			 /* [57, 58) */
	uint8 entry_count;					 /* [58, 59) */
	uint8 body_kind;					 /* [59, 60) */
	uint8 flags;						 /* [60, 61) */
	uint8 reserved_b[2];				 /* [61, 63) */
	uint8 kind;							 /* [63, 64) */
} ClusterCurrentMxProofPrefixWire;


typedef struct ClusterCurrentMxProofTrailerWire {
	uint32 magic;
	uint16 version;
	uint16 reserved16;
	ClusterCurrentMxProofRequestBodyWire body;
} ClusterCurrentMxProofTrailerWire;


typedef struct ClusterCurrentMxProofForwardV2 {
	ClusterCurrentMxProofPrefixWire prefix;
	ClusterCurrentMxProofTrailerWire trailer;
} ClusterCurrentMxProofForwardV2;


typedef struct ClusterCurrentMxProofRequestPlan {
	uint16 destination_node_id;
	uint16 reserved16;
	ClusterCurrentMxProofForwardV2 request;
} ClusterCurrentMxProofRequestPlan;


/*
 * The outer GCS reply header remains shipped and checksum-protects one BLCKSZ
 * page.  wire_length is the exact meaningful prefix inside that page; every
 * byte after it is zero.
 */
typedef struct ClusterCurrentMxDescribeReplyHeader {
	uint32 magic;				 /* [ 0,  4) */
	uint16 version;			 /* [ 4,  6) */
	uint8 kind;				 /* [ 6,  7) */
	uint8 result;				 /* [ 7,  8) */
	uint32 flags;				 /* [ 8, 12) */
	uint32 source_node_id;		 /* [12, 16) */
	uint64 request_id;			 /* [16, 24) */
	ClusterCurrentMxKey mxkey;	 /* [24, 40) */
	uint64 descriptor_hash;		 /* [40, 48) */
	uint32 total_count;			 /* [48, 52) */
	uint16 entry_count;			 /* [52, 54) */
	uint8 chunk_ordinal;		 /* [54, 55) */
	uint8 chunk_count_minus_one; /* [55, 56) */
	uint16 wire_length;			 /* [56, 58) */
	uint16 reserved16;			 /* [58, 60) */
	uint32 reserved32;			 /* [60, 64) */
} ClusterCurrentMxDescribeReplyHeader;


#define CLUSTER_CURRENT_MX_DESCRIBE_REPLY_RESERVED_SIZE                                         \
	(BLCKSZ - sizeof(ClusterCurrentMxDescribeReplyHeader)                                       \
	 - CLUSTER_CURRENT_MX_MAX_MEMBERS * sizeof(ClusterCurrentMxMemberDesc))

typedef struct ClusterCurrentMxDescribeReplyPage {
	ClusterCurrentMxDescribeReplyHeader header;
	ClusterCurrentMxMemberDesc members[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	uint8 reserved[CLUSTER_CURRENT_MX_DESCRIBE_REPLY_RESERVED_SIZE];
} ClusterCurrentMxDescribeReplyPage;


typedef struct ClusterCurrentMxProofReplyHeader {
	uint32 magic;				 /* [ 0,  4) */
	uint16 version;			 /* [ 4,  6) */
	uint8 kind;				 /* [ 6,  7) */
	uint8 result;				 /* [ 7,  8) */
	uint32 flags;				 /* [ 8, 12) */
	uint32 source_node_id;		 /* [12, 16) */
	uint64 request_id;			 /* [16, 24) */
	ClusterCurrentMxKey mxkey;	 /* [24, 40) */
	uint64 descriptor_hash;		 /* [40, 48) */
	uint32 total_count;			 /* [48, 52) */
	uint16 entry_count;			 /* [52, 54) */
	uint8 chunk_ordinal;		 /* [54, 55) */
	uint8 chunk_count_minus_one; /* [55, 56) */
	uint16 wire_length;			 /* [56, 58) */
	uint16 reserved16;			 /* [58, 60) */
	uint32 reserved32;			 /* [60, 64) */
} ClusterCurrentMxProofReplyHeader;


typedef struct ClusterCurrentMxUpdaterProofReplyBodyWire {
	ClusterCurrentMemberProof member_proof;
	ClusterCurrentUpdaterProof updater_proof;
	uint8 reserved[CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME
					   * sizeof(ClusterCurrentMemberProof)
				   - sizeof(ClusterCurrentMemberProof)
				   - sizeof(ClusterCurrentUpdaterProof)];
} ClusterCurrentMxUpdaterProofReplyBodyWire;


typedef union ClusterCurrentMxProofReplyBodyWire {
	ClusterCurrentMemberProof proofs[CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME];
	ClusterCurrentMxUpdaterProofReplyBodyWire updater;
	uint8 raw[CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME
			  * sizeof(ClusterCurrentMemberProof)];
} ClusterCurrentMxProofReplyBodyWire;


#define CLUSTER_CURRENT_MX_PROOF_REPLY_RESERVED_SIZE                                           \
	(BLCKSZ - sizeof(ClusterCurrentMxProofReplyHeader)                                          \
	 - sizeof(ClusterCurrentMxProofReplyBodyWire))

typedef struct ClusterCurrentMxProofReplyPage {
	ClusterCurrentMxProofReplyHeader header;
	ClusterCurrentMxProofReplyBodyWire body;
	uint8 reserved[CLUSTER_CURRENT_MX_PROOF_REPLY_RESERVED_SIZE];
} ClusterCurrentMxProofReplyPage;


StaticAssertDecl(sizeof(ClusterCurrentMxRoutingPrefixWire) == sizeof(GcsBlockForwardPayload),
				 "current MX routing prefix must preserve the shipped 64-byte frame");
StaticAssertDecl(offsetof(ClusterCurrentMxRoutingPrefixWire, request_id)
					 == offsetof(GcsBlockForwardPayload, request_id)
					 && offsetof(ClusterCurrentMxRoutingPrefixWire, epoch)
							== offsetof(GcsBlockForwardPayload, epoch)
					 && offsetof(ClusterCurrentMxRoutingPrefixWire, original_requester_node)
							== offsetof(GcsBlockForwardPayload, original_requester_node)
					 && offsetof(ClusterCurrentMxRoutingPrefixWire, requester_backend_id)
							== offsetof(GcsBlockForwardPayload, requester_backend_id)
					 && offsetof(ClusterCurrentMxRoutingPrefixWire, kind)
							== offsetof(GcsBlockForwardPayload, reserved_0) + 6,
				 "current MX routing identity offsets must preserve the shipped prefix");
StaticAssertDecl(sizeof(ClusterCurrentMxDescribePrefixWire) == 64,
				 "current MX describe prefix wire ABI must remain 64 bytes");
StaticAssertDecl(offsetof(ClusterCurrentMxDescribePrefixWire, request_id)
						 == offsetof(ClusterCurrentMxRoutingPrefixWire, request_id)
					 && offsetof(ClusterCurrentMxDescribePrefixWire, epoch)
							== offsetof(ClusterCurrentMxRoutingPrefixWire, epoch)
					 && offsetof(ClusterCurrentMxDescribePrefixWire, original_requester_node)
							== offsetof(ClusterCurrentMxRoutingPrefixWire,
										original_requester_node)
					 && offsetof(ClusterCurrentMxDescribePrefixWire, requester_backend_id)
							== offsetof(ClusterCurrentMxRoutingPrefixWire, requester_backend_id)
					 && offsetof(ClusterCurrentMxDescribePrefixWire, kind)
							== offsetof(ClusterCurrentMxRoutingPrefixWire, kind),
				 "current MX describe routing offsets must match the generic view");
StaticAssertDecl(sizeof(ClusterCurrentMxDescribeTrailerWire) == 64,
				 "current MX describe trailer wire ABI must remain 64 bytes");
StaticAssertDecl(sizeof(ClusterCurrentMxDescribeForwardV2)
					 == GCS_BLOCK_FORWARD_CURRENT_MX_V2_SIZE,
				 "current MX describe forward V2 must remain 128 bytes");
StaticAssertDecl(offsetof(ClusterCurrentMxDescribeForwardV2, trailer) == 64,
				 "current MX describe trailer must begin at byte 64");
StaticAssertDecl(sizeof(ClusterCurrentMxProofAskWire) == 8,
				 "current MX proof ask wire ABI must remain 8 bytes");
StaticAssertDecl(sizeof(ClusterCurrentMxUpdaterChallengeWire) == 32,
				 "current MX updater challenge wire ABI must remain 32 bytes");
StaticAssertDecl(sizeof(ClusterCurrentMxUpdaterChallengeBodyWire) == 56,
				 "current MX updater challenge body must fill the proof union");
StaticAssertDecl(sizeof(ClusterCurrentMxProofRequestBodyWire) == 56,
				 "current MX proof request body wire ABI must remain 56 bytes");
StaticAssertDecl(sizeof(ClusterCurrentMxProofPrefixWire) == 64,
				 "current MX proof prefix wire ABI must remain 64 bytes");
StaticAssertDecl(offsetof(ClusterCurrentMxProofPrefixWire, request_id)
						 == offsetof(ClusterCurrentMxRoutingPrefixWire, request_id)
					 && offsetof(ClusterCurrentMxProofPrefixWire, epoch)
							== offsetof(ClusterCurrentMxRoutingPrefixWire, epoch)
					 && offsetof(ClusterCurrentMxProofPrefixWire, original_requester_node)
							== offsetof(ClusterCurrentMxRoutingPrefixWire,
										original_requester_node)
					 && offsetof(ClusterCurrentMxProofPrefixWire, requester_backend_id)
							== offsetof(ClusterCurrentMxRoutingPrefixWire, requester_backend_id)
					 && offsetof(ClusterCurrentMxProofPrefixWire, kind)
							== offsetof(ClusterCurrentMxRoutingPrefixWire, kind),
				 "current MX proof routing offsets must match the generic view");
StaticAssertDecl(offsetof(ClusterCurrentMxProofPrefixWire, mxkey) == 16
					 && offsetof(ClusterCurrentMxProofPrefixWire, total_count) == 44
					 && offsetof(ClusterCurrentMxProofPrefixWire, chunk_ordinal) == 48
					 && offsetof(ClusterCurrentMxProofPrefixWire, descriptor_hash_bytes) == 49
					 && offsetof(ClusterCurrentMxProofPrefixWire, chunk_count_minus_one) == 57
					 && offsetof(ClusterCurrentMxProofPrefixWire, kind) == 63,
				 "current MX proof overlay offsets changed");
StaticAssertDecl(sizeof(ClusterCurrentMxProofTrailerWire) == 64,
				 "current MX proof trailer wire ABI must remain 64 bytes");
StaticAssertDecl(sizeof(ClusterCurrentMxProofForwardV2) == GCS_BLOCK_FORWARD_CURRENT_MX_V2_SIZE,
				 "current MX proof forward V2 must remain 128 bytes");
StaticAssertDecl(offsetof(ClusterCurrentMxProofForwardV2, trailer) == 64,
				 "current MX proof trailer must begin at byte 64");
StaticAssertDecl(offsetof(ClusterCurrentMxProofRequestPlan, request) == 8,
				 "current MX proof plan request alignment changed");
StaticAssertDecl(sizeof(ClusterCurrentMxDescribeReplyHeader) == 64,
				 "current MX describe reply header wire ABI must remain 64 bytes");
StaticAssertDecl(offsetof(ClusterCurrentMxDescribeReplyHeader, entry_count) == 52,
				 "current MX describe reply entry count offset must remain 52");
StaticAssertDecl(offsetof(ClusterCurrentMxDescribeReplyHeader, wire_length) == 56,
				 "current MX describe reply wire length offset must remain 56");
StaticAssertDecl(offsetof(ClusterCurrentMxDescribeReplyPage, members) == 64,
				 "current MX describe reply members must begin at offset 64");
StaticAssertDecl(sizeof(ClusterCurrentMxDescribeReplyPage) == BLCKSZ,
				 "current MX describe reply page must fill one GCS block data area");
StaticAssertDecl(CLUSTER_CURRENT_MX_DESCRIBE_REPLY_RESERVED_SIZE > 0,
				 "current MX describe reply must fit the supported member cap");
StaticAssertDecl(sizeof(ClusterCurrentMxProofReplyHeader) == 64,
				 "current MX proof reply header wire ABI must remain 64 bytes");
StaticAssertDecl(offsetof(ClusterCurrentMxProofReplyHeader, entry_count) == 52,
				 "current MX proof reply entry count offset must remain 52");
StaticAssertDecl(offsetof(ClusterCurrentMxProofReplyHeader, wire_length) == 56,
				 "current MX proof reply wire length offset must remain 56");
StaticAssertDecl(sizeof(ClusterCurrentMxUpdaterProofReplyBodyWire)
					 == CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME
							* sizeof(ClusterCurrentMemberProof),
				 "current MX updater proof reply body size changed");
StaticAssertDecl(sizeof(ClusterCurrentMxProofReplyBodyWire)
					 == CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME
							* sizeof(ClusterCurrentMemberProof),
				 "current MX proof reply body size changed");
StaticAssertDecl(offsetof(ClusterCurrentMxProofReplyPage, body) == 64,
				 "current MX proof reply body must begin at offset 64");
StaticAssertDecl(sizeof(ClusterCurrentMxProofReplyPage) == BLCKSZ,
				 "current MX proof reply page must fill one GCS block data area");
StaticAssertDecl(CLUSTER_CURRENT_MX_PROOF_REPLY_RESERVED_SIZE > 0,
				 "current MX proof reply must fit one block");


static inline void
ClusterCurrentMxProofPrefixSetDescriptorHash(ClusterCurrentMxProofPrefixWire *prefix, uint64 hash)
{
	prefix->descriptor_hash_bytes[0] = (uint8)(hash & 0xff);
	prefix->descriptor_hash_bytes[1] = (uint8)((hash >> 8) & 0xff);
	prefix->descriptor_hash_bytes[2] = (uint8)((hash >> 16) & 0xff);
	prefix->descriptor_hash_bytes[3] = (uint8)((hash >> 24) & 0xff);
	prefix->descriptor_hash_bytes[4] = (uint8)((hash >> 32) & 0xff);
	prefix->descriptor_hash_bytes[5] = (uint8)((hash >> 40) & 0xff);
	prefix->descriptor_hash_bytes[6] = (uint8)((hash >> 48) & 0xff);
	prefix->descriptor_hash_bytes[7] = (uint8)((hash >> 56) & 0xff);
}


static inline uint64
ClusterCurrentMxProofPrefixGetDescriptorHash(const ClusterCurrentMxProofPrefixWire *prefix)
{
	uint64 hash = 0;

	hash |= (uint64)prefix->descriptor_hash_bytes[0];
	hash |= (uint64)prefix->descriptor_hash_bytes[1] << 8;
	hash |= (uint64)prefix->descriptor_hash_bytes[2] << 16;
	hash |= (uint64)prefix->descriptor_hash_bytes[3] << 24;
	hash |= (uint64)prefix->descriptor_hash_bytes[4] << 32;
	hash |= (uint64)prefix->descriptor_hash_bytes[5] << 40;
	hash |= (uint64)prefix->descriptor_hash_bytes[6] << 48;
	hash |= (uint64)prefix->descriptor_hash_bytes[7] << 56;
	return hash;
}


extern bool cluster_multixact_current_wire_validate_describe_forward(
	const void *payload, uint32 payload_length, int32 envelope_source, int32 local_node,
	uint64 current_epoch, ClusterCurrentMxDescribeForwardV2 *decoded);

/*
 * The caller must first validate the enclosing GCS reply header and checksum.
 * This function validates and copies only the stable BLCKSZ describe page.
 */
extern ClusterMxDescribeResult cluster_multixact_current_wire_validate_describe_reply(
	const void *payload, uint32 payload_length, int32 expected_source, uint64 current_epoch,
	uint64 expected_request_id, const ClusterCurrentMxKey *expected_key,
	ClusterCurrentMxMemberDesc *members, uint16 members_cap, uint16 *members_count,
	uint32 *reported_total_members);
extern bool cluster_multixact_current_wire_validate_proof_forward(
	const void *payload, uint32 payload_length, int32 envelope_source, int32 local_node,
	uint64 current_epoch, ClusterCurrentMxProofForwardV2 *decoded);
extern ClusterMxResolveResult cluster_multixact_current_wire_build_proof_requests(
	const ClusterCurrentMxKey *key, const ClusterCurrentMxMemberDesc *members,
	const uint16 *member_origin_nodes, uint16 nmembers, uint64 descriptor_hash,
	const ClusterCurrentUpdaterChallenge *challenge, uint64 request_id, uint64 current_epoch,
	int32 requester_node, int32 requester_backend_id, ClusterCurrentMxProofRequestPlan *plans,
	uint16 plans_cap, uint16 *plan_count);
extern ClusterMxResolveResult cluster_multixact_current_wire_validate_proof_reply(
	const void *payload, uint32 payload_length, int32 expected_source, uint64 current_epoch,
	const ClusterCurrentMxProofForwardV2 *expected_request,
	ClusterCurrentMemberProof *proofs, uint16 proofs_cap, uint16 *proof_count,
	ClusterCurrentUpdaterProof *updater_proof);
extern bool cluster_multixact_current_wire_validate_proof_reply_frame(
	const void *payload, uint32 payload_length, int32 expected_source, uint64 current_epoch,
	const ClusterCurrentMxProofForwardV2 *expected_request, ClusterMxResolveResult *result,
	ClusterCurrentMemberProof *proofs, uint16 proofs_cap, uint16 *proof_count,
	ClusterCurrentUpdaterProof *updater_proof);


#endif /* CLUSTER_MULTIXACT_CURRENT_WIRE_H */
