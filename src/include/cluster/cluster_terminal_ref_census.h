/*-------------------------------------------------------------------------
 *
 * cluster_terminal_ref_census.h
 *	  Canonical terminal-reference census and release protocol.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_terminal_ref_census.h
 *
 * NOTES
 *	  This is a pgrac-original file.  CTRC records reference-release
 *	  evidence only; canonical transaction status remains in the physical
 *	  transaction-table slot.  All exported symbols use the cluster_ctrc_
 *	  prefix.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_TERMINAL_REF_CENSUS_H
#define CLUSTER_TERMINAL_REF_CENSUS_H

#include "c.h"

#include "access/transam.h"
#include "access/xlogdefs.h"
#include "cluster/cluster_multixact_current.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_undo_format.h"
#include "storage/block.h"

#define CLUSTER_CTRC_FORMAT_VERSION UINT8_C(1)
#define CLUSTER_CTRC_TXN_KEY_BYTES 96
#define CLUSTER_CTRC_SEAL_REQUEST_BYTES 128
#define CLUSTER_CTRC_REPLY_HEADER_BYTES 64
#define CLUSTER_CTRC_LOCAL_ACK_BYTES 416
#define CLUSTER_CTRC_WIRE_MAGIC UINT32_C(0x50474354)
#define CLUSTER_CTRC_MAX_PARTICIPANTS CLUSTER_SF_DEP_MAX_ORIGINS

#define CTRC_ACK_FLAG_ZERO_RANGE UINT16_C(0x0001)
#define CTRC_ACK_FLAG_ALL_DURABLE UINT16_C(0x0002)

typedef enum ClusterCtrcReferenceKind
{
	CTRC_REF_HEAP_ITL_UBA = 1,
	CTRC_REF_CURRENT_MX_LOCKER = 2,
	CTRC_REF_CURRENT_MX_UPDATER = 3,
	CTRC_REF_RECOMPOSED_SURVIVOR = 4,
	CTRC_REF_HOT_FOLLOW_EDGE = 5
} ClusterCtrcReferenceKind;

typedef enum ClusterCtrcTargetKind
{
	CTRC_TARGET_EXACT_ITL_SLOT = 1,
	CTRC_TARGET_EXACT_TID = 2,
	CTRC_TARGET_PAGE_PENDING_ITL_SLOT = 3,
	CTRC_TARGET_PAGE_PENDING_OFFNUM = 4
} ClusterCtrcTargetKind;

typedef enum ClusterCtrcProofClass
{
	CTRC_PROOF_UNKNOWN = 0,
	CTRC_PROOF_SELF = 1,
	CTRC_PROOF_ACTIVE = 2,
	CTRC_PROOF_COMMITTED = 3,
	CTRC_PROOF_ABORTED = 4
} ClusterCtrcProofClass;

typedef enum ClusterCtrcReceiptState
{
	CTRC_RECEIPT_FREE = 0,
	CTRC_RECEIPT_PREPARED,
	CTRC_RECEIPT_APPLIED,
	CTRC_RECEIPT_CLEANED,
	CTRC_RECEIPT_CANCELLED,
	CTRC_RECEIPT_ACK_FROZEN,
	CTRC_RECEIPT_BLOCKED
} ClusterCtrcReceiptState;

typedef enum ClusterCtrcReleaseDisposition
{
	CTRC_RELEASE_NONE = 0,
	CTRC_RELEASE_CANCELLED_PREMUTATION = 1,
	CTRC_RELEASE_CLEANED_ABSENT = 2,
	CTRC_RELEASE_CLEANED_TERMINAL_REWRITE = 3,
	CTRC_RELEASE_CLEANED_SUCCESSOR_REPLACED = 4
} ClusterCtrcReleaseDisposition;

typedef enum ClusterCtrcParticipantState
{
	CTRC_PARTICIPANT_EMPTY = 0,
	CTRC_PARTICIPANT_OPEN,
	CTRC_PARTICIPANT_CLOSED_DRAINING,
	CTRC_PARTICIPANT_ACK_READY,
	CTRC_PARTICIPANT_ACK_FROZEN,
	CTRC_PARTICIPANT_BLOCKED
} ClusterCtrcParticipantState;

typedef enum ClusterCtrcOriginState
{
	CTRC_ORIGIN_EMPTY = 0,
	CTRC_ORIGIN_OPEN,
	CTRC_ORIGIN_SEALING,
	CTRC_ORIGIN_SEALED,
	CTRC_ORIGIN_CLEANING,
	CTRC_ORIGIN_CERTIFYING,
	CTRC_ORIGIN_RELEASE_PROVEN,
	CTRC_ORIGIN_BLOCKED
} ClusterCtrcOriginState;

typedef struct ClusterCtrcTxnKeyV1
{
	uint8 format_version;
	uint8 owner_instance;
	uint16 origin_node_id;
	uint32 segment_id;
	uint32 segment_generation;
	uint16 slot_offset;
	uint16 slot_wrap;
	TransactionId xid;
	uint32 cluster_epoch;
	uint64 system_identifier;
	uint64 origin_boot_incarnation;
	uint64 formation_epoch;
	uint64 admission_record_generation;
	uint64 root_descriptor_incarnation;
	uint64 root_id;
	uint64 root_generation;
	uint8 reserved[16];
} ClusterCtrcTxnKeyV1;

typedef struct ClusterCtrcPublicationIdV1
{
	uint16 requester_node_id;
	uint64 requester_boot_incarnation;
	uint32 capability_record_generation;
	int32 requester_backend_id;
	uint64 wire_request_id;
	uint64 operation_id;
	uint32 attempt_generation;
	uint64 descriptor_hash;
	uint16 member_ordinal;
	uint8 member_role;
	uint8 reference_kind;
	uint8 target_kind;
	uint8 reserved8;
	uint64 journal_sequence;
	uint64 key_sequence;
	uint64 journal_slot_generation;
	uint32 grant_generation;
} ClusterCtrcPublicationIdV1;

typedef struct ClusterCtrcTargetV1
{
	uint8 kind;
	uint8 relation_persistence;
	uint8 needs_wal;
	uint8 page_operation_kind;
	uint32 spc_oid;
	uint32 db_oid;
	uint32 rel_number;
	int32 fork_number;
	BlockNumber block_number;
	uint64 predecessor_page_lsn;
	uint64 predecessor_page_scn;
	uint64 publication_own_generation;
	uint64 publication_acquisition_epoch;
	uint16 itl_slot_index;
	uint16 itl_slot_wrap;
	TransactionId itl_xid;
	uint8 itl_class;
	uint8 reserved8[3];
	uint8 uba[16];
	uint8 planned_predecessor_sha256[32];
	uint8 planned_successor_sha256[32];
	uint16 offset_number;
	uint16 itemid_flags;
	uint16 itemid_offset;
	uint16 itemid_length;
	uint8 tuple_header_sha256[32];
	uint16 mx_origin_node_id;
	uint16 reserved16;
	uint32 multixact_id;
	uint32 mx_cluster_epoch;
	uint64 descriptor_hash;
	uint8 successor_topology_sha256[32];
	uint64 intended_descriptor_hash;
} ClusterCtrcTargetV1;

typedef struct ClusterCtrcParticipantIdentity
{
	uint16 node_id;
	uint16 reserved16;
	uint32 capability_record_generation;
	uint64 boot_incarnation;
	uint64 formation_epoch;
	uint64 admission_record_generation;
} ClusterCtrcParticipantIdentity;

typedef struct ClusterCtrcOriginEntry
{
	ClusterCtrcTxnKeyV1 key;
	uint64 reservation_generation;
	uint32 grant_generation;
	uint32 touched_bitmap;
	uint64 seal_generation;
	uint8 state;
	uint8 touched_count;
	uint8 reserved8[6];
	ClusterCtrcParticipantIdentity touched[CLUSTER_CTRC_MAX_PARTICIPANTS];
} ClusterCtrcOriginEntry;

typedef enum ClusterCtrcOriginReservationKind
{
	CTRC_ORIGIN_RESERVATION_INVALID = 0,
	CTRC_ORIGIN_RESERVATION_PENDING_OWNED = 1,
	CTRC_ORIGIN_RESERVATION_EXISTING_OPEN = 2
} ClusterCtrcOriginReservationKind;

/* Stack-only handle for the EMPTY-reserved -> OPEN origin transition. */
typedef struct ClusterCtrcOriginReservation
{
	ClusterCtrcTxnKeyV1 key;
	uint64 origin_index;
	uint64 reservation_generation;
	uint8 kind;
	bool valid;
	uint8 reserved8[6];
} ClusterCtrcOriginReservation;

typedef struct ClusterCtrcParticipantEntry
{
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcParticipantIdentity identity;
	uint32 grant_generation;
	uint8 state;
	uint8 reserved8[3];
	uint64 seal_generation;
	uint64 next_key_sequence;
	uint64 last_key_sequence;
	uint64 receipt_count;
	uint64 prepared_count;
	uint64 applied_count;
	uint64 cancelled_count;
	uint64 cleaned_count;
	uint64 ack_frozen_count;
} ClusterCtrcParticipantEntry;

typedef struct ClusterCtrcReceipt
{
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcPublicationIdV1 publication;
	ClusterCtrcTargetV1 target;
	uint32 state;
	uint8 disposition;
	uint8 reserved8[3];
	XLogRecPtr highest_local_wal_lsn;
	XLogRecPtr required_lsn[CLUSTER_SF_DEP_MAX_ORIGINS];
} ClusterCtrcReceipt;

typedef struct ClusterCtrcApplyToken
{
	bool valid;
	uint8 reserved8[7];
	uint64 journal_sequence;
	uint64 key_sequence;
	uint64 journal_slot_generation;
} ClusterCtrcApplyToken;

/* Backend-local ownership handle for one bounded shared receipt slot.  This
 * is not a second receipt, wire object, shared ABI or transaction authority. */
typedef struct ClusterCtrcReceiptHandle
{
	ClusterCtrcParticipantEntry *participant;
	ClusterCtrcReceipt *receipt;
	ClusterCtrcTxnKeyV1 key;
	uint64 participant_index;
	uint64 receipt_index;
	uint64 journal_slot_generation;
	bool valid;
	uint8 reserved8[7];
} ClusterCtrcReceiptHandle;

typedef struct ClusterCtrcDurability
{
	XLogRecPtr highest_local_lsn;
	XLogRecPtr local_flush_lsn;
	XLogRecPtr required_lsn[CLUSTER_SF_DEP_MAX_ORIGINS];
	XLogRecPtr durable_lsn[CLUSTER_SF_DEP_MAX_ORIGINS];
} ClusterCtrcDurability;

/* Backend-local expected identity supplied by an exact page owner when an
 * ITL terminal rewrite attempts to discharge its receipt.  This is neither a
 * wire nor shared-memory ABI: the shared wrapper compares every field while
 * holding the receipt-table locks, so a stale/recycled handle cannot clean a
 * different ITL reference. */
typedef struct ClusterCtrcItlTargetIdentity
{
	uint32 spc_oid;
	uint32 db_oid;
	uint32 rel_number;
	int32 fork_number;
	BlockNumber block_number;
	TransactionId itl_xid;
	uint16 itl_slot_index;
	uint16 itl_slot_wrap;
	uint8 itl_class;
	bool needs_wal;
	uint8 reserved8[2];
	uint8 uba[16];
} ClusterCtrcItlTargetIdentity;

typedef struct ClusterCtrcLocalReleaseAckV1
{
	ClusterCtrcTxnKeyV1 transaction_key;
	uint32 grant_generation;
	uint8 result;
	uint8 first_failed_predicate;
	uint16 flags;
	uint64 seal_generation;
	uint16 participant_node_id;
	uint16 dependency_entry_count;
	uint32 capability_record_generation;
	uint64 participant_boot_incarnation;
	uint64 formation_epoch;
	uint64 admission_record_generation;
	uint64 first_key_sequence;
	uint64 last_key_sequence;
	uint64 minimum_journal_sequence;
	uint64 maximum_journal_sequence;
	uint64 total_receipt_count;
	uint64 prepared_count;
	uint64 applied_count;
	uint64 cancelled_count;
	uint64 cleaned_count;
	uint64 ack_frozen_count;
	uint8 row_digest_sha256[32];
	XLogRecPtr highest_local_cleanout_lsn;
	XLogRecPtr required_lsn_vector[CLUSTER_SF_DEP_MAX_ORIGINS];
	uint8 reserved[20];
	uint32 crc32c;
} ClusterCtrcLocalReleaseAckV1;

typedef struct ClusterCtrcSealRequestV1
{
	uint64 request_id;
	uint64 cluster_epoch;
	uint8 tt_status_key_0_19[20];
	int32 original_requester_node;
	int32 requester_backend_id;
	uint8 tt_status_key_20_23[4];
	uint32 grant_generation;
	uint16 slot_wrap;
	uint8 owner_instance;
	uint8 suboperation;
	uint32 segment_generation;
	uint16 request_flags;
	uint8 selector_version;
	uint8 forward_kind;
	uint32 magic;
	uint16 wire_version;
	uint16 wire_length;
	uint64 origin_boot_incarnation;
	uint64 formation_epoch;
	uint64 admission_record_generation;
	uint64 root_descriptor_incarnation;
	uint64 root_id;
	uint64 root_generation;
	uint64 seal_generation;
} ClusterCtrcSealRequestV1;

typedef struct ClusterCtrcSealReplyHeaderV1
{
	uint32 magic;
	uint16 wire_version;
	uint16 header_length;
	uint64 request_id;
	uint64 cluster_epoch;
	uint64 seal_generation;
	int32 source_node;
	int32 destination_node;
	uint8 result;
	uint8 suboperation;
	uint16 first_reason;
	uint16 reply_flags;
	uint16 ack_length;
	uint8 request_sha256_prefix[8];
	uint32 body_length;
	uint32 header_crc32c;
} ClusterCtrcSealReplyHeaderV1;

typedef enum ClusterCtrcOriginOpenResult
{
	CLUSTER_CTRC_ORIGIN_REFUSED = 0,
	CLUSTER_CTRC_ORIGIN_OPENED = 1,
	CLUSTER_CTRC_ORIGIN_DUPLICATE = 2
} ClusterCtrcOriginOpenResult;

typedef enum ClusterCtrcOriginReserveResult
{
	CLUSTER_CTRC_ORIGIN_RESERVE_REFUSED = 0,
	CLUSTER_CTRC_ORIGIN_RESERVED_PENDING = 1,
	CLUSTER_CTRC_ORIGIN_RESERVED_EXISTING_OPEN = 2
} ClusterCtrcOriginReserveResult;

typedef enum ClusterCtrcTouchResult
{
	CLUSTER_CTRC_TOUCH_REFUSED = 0,
	CLUSTER_CTRC_TOUCH_RECORDED = 1,
	CLUSTER_CTRC_TOUCH_DUPLICATE = 2,
	CLUSTER_CTRC_TOUCH_TERMINAL_NO_GRANT = 3
} ClusterCtrcTouchResult;

typedef enum ClusterCtrcParticipantOpenResult
{
	CLUSTER_CTRC_PARTICIPANT_REFUSED = 0,
	CLUSTER_CTRC_PARTICIPANT_OPENED = 1,
	CLUSTER_CTRC_PARTICIPANT_DUPLICATE = 2
} ClusterCtrcParticipantOpenResult;

typedef enum ClusterCtrcPrepareResult
{
	CLUSTER_CTRC_PREPARE_REFUSED = 0,
	CLUSTER_CTRC_PREPARE_READY = 1,
	CLUSTER_CTRC_PREPARE_DUPLICATE = 2,
	CLUSTER_CTRC_PREPARE_CAPACITY = 3
} ClusterCtrcPrepareResult;

typedef enum ClusterCtrcApplyResult
{
	CLUSTER_CTRC_APPLY_FAIL_CLOSED = 0,
	CLUSTER_CTRC_APPLY_APPLIED = 1,
	CLUSTER_CTRC_APPLY_RETRY_REQUIRED = 2
} ClusterCtrcApplyResult;

typedef enum ClusterCtrcItlProjection
{
	CTRC_ITL_NEEDS_CLEANOUT = 0,
	CTRC_ITL_HINT_SKIPPED,
	CTRC_ITL_TERMINAL_INDEPENDENT,
	CTRC_ITL_TARGET_ABSENT
} ClusterCtrcItlProjection;

typedef enum ClusterCtrcDischargeResult
{
	CLUSTER_CTRC_DISCHARGE_RETAIN = 0,
	CLUSTER_CTRC_DISCHARGE_CLEANED = 1
} ClusterCtrcDischargeResult;

typedef enum ClusterCtrcCloseResult
{
	CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN = 0,
	CLUSTER_CTRC_CLOSE_PENDING_DRAIN = 1,
	CLUSTER_CTRC_CLOSE_ACK_READY = 2
} ClusterCtrcCloseResult;

typedef enum ClusterCtrcLossResult
{
	CLUSTER_CTRC_LOSS_BLOCKED = 0
} ClusterCtrcLossResult;

typedef enum ClusterCtrcTargetState
{
	CTRC_TARGET_ABSENT = 0,
	CTRC_TARGET_AMBIGUOUS,
	CTRC_TARGET_TERMINAL_LOCK_ONLY,
	CTRC_TARGET_ABORTED_UPDATER,
	CTRC_TARGET_COMMITTED_UPDATER,
	CTRC_TARGET_ACTIVE_SURVIVOR
} ClusterCtrcTargetState;

typedef struct ClusterCtrcCleanReferenceInput
{
	uint8 target_state;
	bool source_transition_censused;
	bool page_authority_exact;
	bool successor_topology_exact;
	uint16 active_companions;
	uint16 unknown_companions;
} ClusterCtrcCleanReferenceInput;

typedef enum ClusterCtrcCleanResult
{
	CTRC_CLEAN_RETAIN = 0,
	CTRC_CLEANED_ABSENT = 1,
	CTRC_CLEANED_TERMINAL_REWRITE = 2,
	CTRC_CLEANED_SUCCESSOR_REPLACED = 3
} ClusterCtrcCleanResult;

typedef struct ClusterCtrcTransferState
{
	uint16 active_survivor_count;
	uint16 successor_receipt_count;
	bool descriptor_durable;
	bool predecessor_removed;
} ClusterCtrcTransferState;

typedef enum ClusterCtrcTransferResult
{
	CLUSTER_CTRC_TRANSFER_REFUSED = 0,
	CLUSTER_CTRC_TRANSFER_PENDING_DESCRIPTOR = 1,
	CLUSTER_CTRC_TRANSFER_READY = 2,
	CLUSTER_CTRC_TRANSFER_REMOVED = 3
} ClusterCtrcTransferResult;

typedef enum ClusterCtrcAckResult
{
	CLUSTER_CTRC_ACK_DENIED = 0,
	CLUSTER_CTRC_ACK_RELEASED = 1,
	CTRC_ACK_RELEASED = CLUSTER_CTRC_ACK_RELEASED
} ClusterCtrcAckResult;

typedef struct ClusterCtrcCertificateInput
{
	const ClusterCtrcLocalReleaseAckV1 *acks;
	uint16 ack_count;
	uint16 reserved16;
	uint64 frozen_touched_bitmap;
	uint64 seal_generation;
	bool block0_terminal_exact;
	bool all_dependencies_durable;
} ClusterCtrcCertificateInput;

typedef enum ClusterCtrcCertificateResult
{
	CLUSTER_CTRC_CERTIFICATE_RETAIN = 0,
	CLUSTER_CTRC_CERTIFICATE_READY = 1
} ClusterCtrcCertificateResult;

typedef enum ClusterCtrcTerminalStatus
{
	CTRC_TERMINAL_UNKNOWN = 0,
	CTRC_TERMINAL_COMMITTED = 1,
	CTRC_TERMINAL_ABORTED = 2
} ClusterCtrcTerminalStatus;

typedef struct ClusterCtrcRecycleInput
{
	uint8 status;
	bool release_proven;
	bool durable_aborted;
	bool horizon_valid;
	uint64 commit_scn;
	uint64 horizon_scn;
} ClusterCtrcRecycleInput;

typedef enum ClusterCtrcCrashCut
{
	CTRC_CRASH_BEFORE_TOUCH = 1,
	CTRC_CRASH_AFTER_TOUCH_BEFORE_PROOF,
	CTRC_CRASH_PREPARED_BEFORE_APPLIED,
	CTRC_CRASH_APPLIED_BEFORE_PAGE_MUTATION,
	CTRC_CRASH_CLEANOUT_WAL_BEFORE_DURABLE,
	CTRC_CRASH_ALL_ACKS_BEFORE_CERTIFICATE,
	CTRC_CRASH_CERTIFICATE_BEFORE_FLUSH,
	CTRC_CRASH_DURABLE_CERTIFICATE_BEFORE_NOTIFICATION,
	CTRC_CRASH_ORIGIN_PRECERT_LOSS
} ClusterCtrcCrashCut;

typedef enum ClusterCtrcCrashDisposition
{
	CLUSTER_CTRC_CRASH_RETAIN = 0,
	CLUSTER_CTRC_CRASH_RELEASE_PROVEN = 1
} ClusterCtrcCrashDisposition;

typedef struct ClusterCtrcCapacity
{
	uint64 origin_key_entries;
	uint64 participant_key_entries;
	uint64 receipt_entries;
	uint64 ack_summary_entries;
	Size total_bytes;
} ClusterCtrcCapacity;

typedef enum ClusterCtrcCapacityDisposition
{
	CLUSTER_CTRC_CAPACITY_REFUSE_BEFORE_MUTATION = 0
} ClusterCtrcCapacityDisposition;

StaticAssertDecl(sizeof(ClusterCtrcTxnKeyV1) == CLUSTER_CTRC_TXN_KEY_BYTES,
				 "CTRC transaction key must remain exactly 96 bytes");
StaticAssertDecl(sizeof(ClusterCtrcSealRequestV1) == CLUSTER_CTRC_SEAL_REQUEST_BYTES,
				 "CTRC seal request must remain exactly 128 bytes");
StaticAssertDecl(sizeof(ClusterCtrcSealReplyHeaderV1) == CLUSTER_CTRC_REPLY_HEADER_BYTES,
				 "CTRC reply header must remain exactly 64 bytes");
StaticAssertDecl(sizeof(ClusterCtrcLocalReleaseAckV1) == CLUSTER_CTRC_LOCAL_ACK_BYTES,
				 "CTRC local release ACK must remain exactly 416 bytes");
StaticAssertDecl(offsetof(ClusterCtrcLocalReleaseAckV1, row_digest_sha256) == 224,
				 "CTRC ACK digest offset must remain 224");
StaticAssertDecl(offsetof(ClusterCtrcLocalReleaseAckV1, crc32c) == 412,
				 "CTRC ACK CRC offset must remain 412");

extern const uint8 cluster_ctrc_empty_sha256[32];
extern bool cluster_ctrc_sha256_exact(const void *bytes, Size length,
	uint8 digest[32]);

extern bool cluster_ctrc_capacity_compute(Size n_buffers, int max_backends,
										  int declared_nodes,
										  ClusterCtrcCapacity *capacity);
extern ClusterCtrcCapacityDisposition cluster_ctrc_runtime_full_disposition(void);
extern Size cluster_ctrc_shmem_size(void);
extern void cluster_ctrc_shmem_init(void);
extern void cluster_ctrc_shmem_register(void);
extern bool cluster_ctrc_shmem_ready(void);

extern ClusterCtrcOriginReserveResult cluster_ctrc_origin_reserve_active(
	const ClusterCtrcTxnKeyV1 *key,
	ClusterCtrcOriginReservation *reservation);
extern ClusterCtrcOriginReserveResult cluster_ctrc_origin_reserve_entry(
	ClusterCtrcOriginEntry *origin, const ClusterCtrcTxnKeyV1 *key,
	uint64 origin_index, uint64 reservation_generation,
	ClusterCtrcOriginReservation *reservation);
extern bool cluster_ctrc_origin_cancel_pre_bind_entry(
	ClusterCtrcOriginEntry *origin, uint64 origin_index,
	const ClusterCtrcOriginReservation *reservation);
extern bool cluster_ctrc_origin_block_post_bind_entry(
	ClusterCtrcOriginEntry *origin, uint64 origin_index,
	const ClusterCtrcOriginReservation *reservation);
extern bool cluster_ctrc_origin_cancel_pre_bind(
	const ClusterCtrcOriginReservation *reservation);
extern bool cluster_ctrc_origin_block_post_bind(
	const ClusterCtrcOriginReservation *reservation);
extern bool cluster_ctrc_origin_open_reserved(
	const ClusterCtrcOriginReservation *reservation,
	uint32 *grant_generation);
extern ClusterCtrcTouchResult cluster_ctrc_origin_touch_exact(
	const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *participant,
	ClusterCtrcProofClass proof_class, uint32 *grant_out);

extern ClusterCtrcOriginOpenResult cluster_ctrc_origin_open_active(
	ClusterCtrcOriginEntry *origin, const ClusterCtrcTxnKeyV1 *key,
	uint32 grant_generation);
extern ClusterCtrcTouchResult cluster_ctrc_origin_record_touched(
	ClusterCtrcOriginEntry *origin,
	const ClusterCtrcParticipantIdentity *participant,
	ClusterCtrcProofClass proof_class, uint32 *grant_out);
extern bool cluster_ctrc_origin_has_exact_touch(
	const ClusterCtrcOriginEntry *origin,
	const ClusterCtrcParticipantIdentity *participant);
extern ClusterCtrcParticipantOpenResult cluster_ctrc_participant_open(
	ClusterCtrcParticipantEntry *participant, const ClusterCtrcTxnKeyV1 *key,
	uint32 grant_generation,
	const ClusterCtrcParticipantIdentity *identity);
extern ClusterCtrcPrepareResult cluster_ctrc_receipt_prepare(
	ClusterCtrcParticipantEntry *participant,
	const ClusterCtrcPublicationIdV1 *publication,
	const ClusterCtrcTargetV1 *target, ClusterCtrcReceipt *receipt);
/* Caller holds the participant and receipt table locks. */
extern ClusterCtrcPrepareResult cluster_ctrc_receipt_prepare_table_locked(
	ClusterCtrcParticipantEntry *participant,
	const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *identity,
	uint32 grant_generation,
	const ClusterCtrcPublicationIdV1 *publication,
	const ClusterCtrcTargetV1 *target,
	ClusterCtrcReceipt *receipts, Size receipt_count,
	uint64 journal_sequence, uint64 *receipt_index_out);
extern ClusterCtrcPrepareResult cluster_ctrc_receipt_prepare_shared(
	const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *identity,
	uint32 grant_generation,
	const ClusterCtrcPublicationIdV1 *publication,
	const ClusterCtrcTargetV1 *target,
	ClusterCtrcReceiptHandle *handle);
extern ClusterCtrcApplyResult cluster_ctrc_receipt_apply_prepared(
	ClusterCtrcParticipantEntry *participant, ClusterCtrcReceipt *receipt,
	const ClusterCtrcTargetV1 *final_target, ClusterCtrcApplyToken *token);
extern ClusterCtrcApplyResult cluster_ctrc_receipt_apply_shared(
	const ClusterCtrcReceiptHandle *handle,
	const ClusterCtrcTargetV1 *final_target, ClusterCtrcApplyToken *token);
extern bool cluster_ctrc_receipt_cancel_prepared(
	ClusterCtrcParticipantEntry *participant, ClusterCtrcReceipt *receipt);
extern bool cluster_ctrc_receipt_cancel_shared(
	const ClusterCtrcReceiptHandle *handle);
extern ClusterCtrcDischargeResult cluster_ctrc_receipt_discharge_itl(
	ClusterCtrcParticipantEntry *participant, ClusterCtrcReceipt *receipt,
	ClusterCtrcItlProjection projection,
	const ClusterCtrcDurability *durability);
extern bool cluster_ctrc_itl_target_identity_matches(
	const ClusterCtrcTargetV1 *target,
	const ClusterCtrcItlTargetIdentity *expected);
extern ClusterCtrcDischargeResult cluster_ctrc_receipt_discharge_itl_shared(
	const ClusterCtrcReceiptHandle *handle,
	const ClusterCtrcItlTargetIdentity *expected_target,
	ClusterCtrcItlProjection projection,
	const ClusterCtrcDurability *durability);
extern ClusterCtrcCloseResult cluster_ctrc_participant_close(
	ClusterCtrcParticipantEntry *participant,
	const ClusterCtrcParticipantIdentity *identity, uint32 grant_generation,
	uint64 seal_generation);
extern ClusterCtrcLossResult cluster_ctrc_participant_note_owner_loss(
	ClusterCtrcParticipantEntry *participant, ClusterCtrcReceipt *receipt);
extern ClusterCtrcCleanResult cluster_ctrc_clean_reference(
	const ClusterCtrcCleanReferenceInput *input);
extern ClusterCtrcTransferResult cluster_ctrc_transfer_note_successor_receipt(
	ClusterCtrcTransferState *transfer);
extern ClusterCtrcTransferResult cluster_ctrc_transfer_note_descriptor_durable(
	ClusterCtrcTransferState *transfer);
extern ClusterCtrcTransferResult cluster_ctrc_transfer_remove_predecessor(
	ClusterCtrcTransferState *transfer);
extern ClusterCtrcAckResult cluster_ctrc_participant_build_ack(
	ClusterCtrcParticipantEntry *participant,
	const ClusterCtrcDurability *durability,
	ClusterCtrcLocalReleaseAckV1 *ack);
extern ClusterCtrcCertificateResult cluster_ctrc_origin_certificate_validate(
	const ClusterCtrcCertificateInput *input);
extern bool cluster_ctrc_terminal_recyclable(
	const ClusterCtrcRecycleInput *input);
extern ClusterCtrcCrashDisposition cluster_ctrc_crash_cut_disposition(
	ClusterCtrcCrashCut cut);

#endif /* CLUSTER_TERMINAL_REF_CENSUS_H */
