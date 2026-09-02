/*-------------------------------------------------------------------------
 *
 * test_cluster_terminal_ref_census.c
 *	  Focused tests for canonical terminal-reference census and release.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_terminal_ref_census.c
 *
 * NOTES
 *	  This is a pgrac-original file.  It exercises the dependency-light
 *	  CTRC state engine with literal identities and deterministic event
 *	  order.  See spec-8.4d-current-mx-transaction-authority-state-matrix.md.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <ctype.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include "cluster/cluster_terminal_ref_census.h"

#undef printf
#undef fprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

#ifndef CTRC_SOURCE_MANIFEST_PATH
#error "CTRC_SOURCE_MANIFEST_PATH must identify the CTRC source census"
#endif
#ifndef HEAP_REFERENCE_PRODUCER_MANIFEST_PATH
#error "HEAP_REFERENCE_PRODUCER_MANIFEST_PATH must identify the heap producer census"
#endif
#ifndef CURRENT_MX_CARDINALITY_MANIFEST_PATH
#error "CURRENT_MX_CARDINALITY_MANIFEST_PATH must identify the current-MX cardinality census"
#endif
#ifndef PGRAC_SOURCE_ROOT_PATH
#error "PGRAC_SOURCE_ROOT_PATH must identify the source tree"
#endif

#define TEST_GRANT UINT32_C(7)
#define TEST_SEAL UINT64_C(41)
#define TEST_BOOT UINT64_C(1101)
#define TEST_CAPABILITY UINT32_C(19)
#define TEST_FORMATION UINT64_C(23)
#define TEST_ADMISSION UINT64_C(29)

static ClusterCtrcTxnKeyV1
test_key(void)
{
	ClusterCtrcTxnKeyV1 key;

	MemSet(&key, 0, sizeof(key));
	key.format_version = CLUSTER_CTRC_FORMAT_VERSION;
	key.owner_instance = 1;
	key.origin_node_id = 0;
	key.segment_id = 17;
	key.segment_generation = 5;
	key.slot_offset = 3;
	key.slot_wrap = 9;
	key.xid = 7001;
	key.cluster_epoch = 13;
	key.system_identifier = UINT64_C(0x1122334455667788);
	key.origin_boot_incarnation = TEST_BOOT;
	key.formation_epoch = TEST_FORMATION;
	key.admission_record_generation = TEST_ADMISSION;
	key.root_descriptor_incarnation = 31;
	key.root_id = 37;
	key.root_generation = 43;
	return key;
}

static ClusterCtrcParticipantIdentity
test_participant_identity(uint16 node_id)
{
	ClusterCtrcParticipantIdentity identity;

	MemSet(&identity, 0, sizeof(identity));
	identity.node_id = node_id;
	identity.boot_incarnation = TEST_BOOT + node_id;
	identity.capability_record_generation = TEST_CAPABILITY;
	identity.formation_epoch = TEST_FORMATION;
	identity.admission_record_generation = TEST_ADMISSION;
	return identity;
}

static ClusterCtrcPublicationIdV1
test_publication(uint64 operation_id, ClusterCtrcReferenceKind reference_kind,
				ClusterCtrcTargetKind target_kind)
{
	ClusterCtrcPublicationIdV1 publication;

	MemSet(&publication, 0, sizeof(publication));
	publication.requester_node_id = 2;
	publication.requester_boot_incarnation = TEST_BOOT + 2;
	publication.capability_record_generation = TEST_CAPABILITY;
	publication.requester_backend_id = 11;
	publication.wire_request_id = 101;
	publication.operation_id = operation_id;
	publication.attempt_generation = 1;
	if (reference_kind == CTRC_REF_HEAP_ITL_UBA)
	{
		publication.descriptor_hash = 0;
		publication.member_ordinal = UINT16_MAX;
		publication.member_role = 0;
	}
	else
	{
		publication.descriptor_hash = UINT64_C(0x9988776655443322);
		publication.member_ordinal = 0;
		publication.member_role = 1;
	}
	publication.reference_kind = reference_kind;
	publication.target_kind = target_kind;
	publication.grant_generation = TEST_GRANT;
	return publication;
}

static ClusterCtrcTargetV1
test_pending_itl_target(void)
{
	ClusterCtrcTargetV1 target;

	MemSet(&target, 0, sizeof(target));
	target.kind = CTRC_TARGET_PAGE_PENDING_ITL_SLOT;
	target.spc_oid = 1663;
	target.db_oid = 5;
	target.rel_number = 9001;
	target.fork_number = 0;
	target.block_number = 44;
	/* A newly initialized heap page legitimately has no predecessor LSN/SCN;
	 * exact zero is data, not an absent target. */
	target.predecessor_page_lsn = 0;
	target.predecessor_page_scn = 0;
	target.publication_own_generation = 17;
	target.publication_acquisition_epoch = 19;
	target.relation_persistence = 'p';
	target.needs_wal = true;
	target.page_operation_kind = 1;
	return target;
}

static ClusterCtrcTargetV1
test_exact_itl_target(void)
{
	ClusterCtrcTargetV1 target = test_pending_itl_target();

	target.kind = CTRC_TARGET_EXACT_ITL_SLOT;
	target.itl_slot_index = 2;
	/* Heap ITL wrap is independent of the canonical TT slot wrap. */
	target.itl_slot_wrap = 3;
	target.itl_xid = 7001;
	target.itl_class = 1;
	MemSet(target.uba, 0x5a, sizeof(target.uba));
	MemSet(target.planned_predecessor_sha256, 0x11,
		   sizeof(target.planned_predecessor_sha256));
	MemSet(target.planned_successor_sha256, 0x22,
		   sizeof(target.planned_successor_sha256));
	return target;
}

static ClusterCtrcTargetV1
test_pending_offnum_target(uint64 descriptor_hash)
{
	ClusterCtrcTargetV1 target = test_pending_itl_target();

	target.kind = CTRC_TARGET_PAGE_PENDING_OFFNUM;
	target.page_operation_kind = 0;
	target.intended_descriptor_hash = descriptor_hash;
	return target;
}

static ClusterCtrcTargetV1
test_exact_tid_target(uint64 descriptor_hash)
{
	ClusterCtrcTargetV1 target = test_pending_offnum_target(descriptor_hash);

	target.kind = CTRC_TARGET_EXACT_TID;
	target.offset_number = 4;
	target.itemid_flags = 1;
	target.itemid_offset = 128;
	target.itemid_length = 40;
	MemSet(target.tuple_header_sha256, 0x33,
		   sizeof(target.tuple_header_sha256));
	target.mx_origin_node_id = 1;
	target.multixact_id = 9001;
	target.mx_cluster_epoch = 13;
	target.descriptor_hash = descriptor_hash;
	target.intended_descriptor_hash = 0;
	return target;
}

static void
test_open_origin(ClusterCtrcOriginEntry *origin)
{
	ClusterCtrcTxnKeyV1 key = test_key();

	MemSet(origin, 0, sizeof(*origin));
	UT_ASSERT_EQ(cluster_ctrc_origin_open_active(origin, &key, TEST_GRANT),
				 CLUSTER_CTRC_ORIGIN_OPENED);
}

/* MXA L04/L05: the pending origin reservation is owned by one exact
 * generation.  Before BIND it may return to the byte-zero FREE image; after
 * BIND it may only retain as BLOCKED.  A stale or foreign handle must never
 * clear a newer occupant. */
UT_TEST(test_ctrc_origin_reservation_prebind_cancel_and_postbind_block_are_exact)
{
	ClusterCtrcOriginEntry origin;
	ClusterCtrcOriginReservation reservation;
	ClusterCtrcOriginReservation stale;
	ClusterCtrcTxnKeyV1 key = test_key();
	ClusterCtrcTxnKeyV1 next_key = key;
	ClusterCtrcOriginEntry zero = {0};

	MemSet(&origin, 0, sizeof(origin));
	MemSet(&reservation, 0, sizeof(reservation));
	UT_ASSERT_EQ(cluster_ctrc_origin_reserve_entry(
		&origin, &key, 3, 11, &reservation),
		CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
	UT_ASSERT_EQ(reservation.kind, CTRC_ORIGIN_RESERVATION_PENDING_OWNED);
	UT_ASSERT_EQ(reservation.reservation_generation, 11);
	stale = reservation;
	stale.reservation_generation++;
	UT_ASSERT(!cluster_ctrc_origin_cancel_pre_bind_entry(&origin, 3, &stale));
	UT_ASSERT_EQ(origin.state, CTRC_ORIGIN_EMPTY);
	UT_ASSERT_EQ(origin.reservation_generation, 11);
	UT_ASSERT_EQ(memcmp(&origin.key, &key, sizeof(key)), 0);
	UT_ASSERT(cluster_ctrc_origin_cancel_pre_bind_entry(
		&origin, 3, &reservation));
	UT_ASSERT_EQ(memcmp(&origin, &zero, sizeof(origin)), 0);

	/* A conflicting physical-slot incarnation can reserve after exact cancel. */
	next_key.xid++;
	next_key.slot_wrap++;
	MemSet(&reservation, 0, sizeof(reservation));
	UT_ASSERT_EQ(cluster_ctrc_origin_reserve_entry(
		&origin, &next_key, 3, 12, &reservation),
		CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
	UT_ASSERT(cluster_ctrc_origin_block_post_bind_entry(
		&origin, 3, &reservation));
	UT_ASSERT_EQ(origin.state, CTRC_ORIGIN_BLOCKED);
	UT_ASSERT_EQ(origin.reservation_generation, 12);
	UT_ASSERT_EQ(memcmp(&origin.key, &next_key, sizeof(next_key)), 0);
	UT_ASSERT(!cluster_ctrc_origin_cancel_pre_bind_entry(
		&origin, 3, &reservation));
	UT_ASSERT(cluster_ctrc_origin_block_post_bind_entry(
		&origin, 3, &reservation));
}

UT_TEST(test_ctrc_origin_reservation_open_duplicate_and_aba_cleanup_do_not_mutate)
{
	ClusterCtrcOriginEntry origin;
	ClusterCtrcOriginEntry before;
	ClusterCtrcOriginReservation pending;
	ClusterCtrcOriginReservation opened;
	ClusterCtrcOriginReservation foreign;
	ClusterCtrcTxnKeyV1 key = test_key();

	MemSet(&origin, 0, sizeof(origin));
	MemSet(&pending, 0, sizeof(pending));
	UT_ASSERT_EQ(cluster_ctrc_origin_reserve_entry(
		&origin, &key, 7, 41, &pending),
		CLUSTER_CTRC_ORIGIN_RESERVED_PENDING);
	UT_ASSERT_EQ(cluster_ctrc_origin_open_active(&origin, &key, TEST_GRANT),
		CLUSTER_CTRC_ORIGIN_OPENED);
	MemSet(&opened, 0, sizeof(opened));
	UT_ASSERT_EQ(cluster_ctrc_origin_reserve_entry(
		&origin, &key, 7, 99, &opened),
		CLUSTER_CTRC_ORIGIN_RESERVED_EXISTING_OPEN);
	UT_ASSERT_EQ(opened.kind, CTRC_ORIGIN_RESERVATION_EXISTING_OPEN);
	UT_ASSERT_EQ(opened.reservation_generation, 41);
	before = origin;
	UT_ASSERT(!cluster_ctrc_origin_cancel_pre_bind_entry(&origin, 7, &opened));
	UT_ASSERT(!cluster_ctrc_origin_block_post_bind_entry(&origin, 7, &opened));
	UT_ASSERT_EQ(memcmp(&origin, &before, sizeof(origin)), 0);

	foreign = pending;
	foreign.origin_index++;
	UT_ASSERT(!cluster_ctrc_origin_cancel_pre_bind_entry(&origin, 7, &foreign));
	UT_ASSERT(!cluster_ctrc_origin_block_post_bind_entry(&origin, 7, &foreign));
	UT_ASSERT_EQ(memcmp(&origin, &before, sizeof(origin)), 0);
	foreign = pending;
	foreign.key.xid++;
	UT_ASSERT(!cluster_ctrc_origin_cancel_pre_bind_entry(&origin, 7, &foreign));
	UT_ASSERT(!cluster_ctrc_origin_block_post_bind_entry(&origin, 7, &foreign));
	UT_ASSERT_EQ(memcmp(&origin, &before, sizeof(origin)), 0);
}

static void
test_open_participant(ClusterCtrcParticipantEntry *participant)
{
	ClusterCtrcTxnKeyV1 key = test_key();
	ClusterCtrcParticipantIdentity identity = test_participant_identity(2);

	MemSet(participant, 0, sizeof(*participant));
	UT_ASSERT_EQ(cluster_ctrc_participant_open(participant, &key, TEST_GRANT,
											 &identity),
				 CLUSTER_CTRC_PARTICIPANT_OPENED);
}

/* MXA-T20: removing the touched-record call must make a positive grant
 * impossible.  Terminal and unknown decisions never inherit the old grant. */
UT_TEST(test_ctrc_active_grant_records_touched_node_before_positive_proof)
{
	ClusterCtrcOriginEntry origin;
	ClusterCtrcOriginEntry before_terminal;
	ClusterCtrcOriginEntry conflicting;
	ClusterCtrcParticipantIdentity participant = test_participant_identity(2);
	ClusterCtrcParticipantIdentity drifted = participant;
	ClusterCtrcTxnKeyV1 key = test_key();
	uint32 grant = UINT32_MAX;

	test_open_origin(&origin);
	UT_ASSERT_EQ(cluster_ctrc_origin_open_active(&origin, &key, TEST_GRANT),
				 CLUSTER_CTRC_ORIGIN_DUPLICATE);
	conflicting = origin;
	UT_ASSERT_EQ(cluster_ctrc_origin_open_active(&conflicting, &key,
											 TEST_GRANT + 1),
				 CLUSTER_CTRC_ORIGIN_REFUSED);
	UT_ASSERT_EQ(conflicting.state, CTRC_ORIGIN_BLOCKED);
	UT_ASSERT_EQ(cluster_ctrc_origin_record_touched(&origin, &participant,
											  CTRC_PROOF_ACTIVE, &grant),
				 CLUSTER_CTRC_TOUCH_RECORDED);
	UT_ASSERT_EQ(grant, TEST_GRANT);
	UT_ASSERT(cluster_ctrc_origin_has_exact_touch(&origin, &participant));
	grant = UINT32_MAX;
	UT_ASSERT_EQ(cluster_ctrc_origin_record_touched(&origin, &participant,
											  CTRC_PROOF_ACTIVE, &grant),
				 CLUSTER_CTRC_TOUCH_DUPLICATE);
	UT_ASSERT_EQ(grant, TEST_GRANT);

	drifted.boot_incarnation++;
	grant = UINT32_MAX;
	UT_ASSERT_EQ(cluster_ctrc_origin_record_touched(&origin, &drifted,
											  CTRC_PROOF_ACTIVE, &grant),
				 CLUSTER_CTRC_TOUCH_REFUSED);
	UT_ASSERT_EQ(grant, 0);
	UT_ASSERT_EQ(origin.state, CTRC_ORIGIN_BLOCKED);
	UT_ASSERT(!cluster_ctrc_origin_has_exact_touch(&origin, &drifted));

	before_terminal = origin;
	grant = UINT32_MAX;
	UT_ASSERT_EQ(cluster_ctrc_origin_record_touched(&origin, &participant,
											  CTRC_PROOF_COMMITTED, &grant),
				 CLUSTER_CTRC_TOUCH_TERMINAL_NO_GRANT);
	UT_ASSERT_EQ(grant, 0);
	UT_ASSERT(memcmp(&origin, &before_terminal, sizeof(origin)) == 0);
	grant = UINT32_MAX;
	UT_ASSERT_EQ(cluster_ctrc_origin_record_touched(&origin, &participant,
											  CTRC_PROOF_UNKNOWN, &grant),
				 CLUSTER_CTRC_TOUCH_TERMINAL_NO_GRANT);
	UT_ASSERT_EQ(grant, 0);
	UT_ASSERT(memcmp(&origin, &before_terminal, sizeof(origin)) == 0);
}

/* MXA-T21: changing any full identity byte must prevent APPLY.  Exact
 * duplicates reuse the same participant-key sequence and receipt slot. */
UT_TEST(test_ctrc_receipt_prepare_apply_full_identity_cross_product)
{
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcPublicationIdV1 publication = test_publication(1,
		CTRC_REF_HEAP_ITL_UBA, CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
	ClusterCtrcTargetV1 pending = test_pending_itl_target();
	ClusterCtrcTargetV1 exact = test_exact_itl_target();
	ClusterCtrcReceipt receipt;
	ClusterCtrcReceipt duplicate;
	ClusterCtrcApplyToken token;

	test_open_participant(&participant);
	MemSet(&receipt, 0, sizeof(receipt));
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant, &publication,
										  &pending, &receipt),
				 CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT_EQ(receipt.state, CTRC_RECEIPT_PREPARED);
	UT_ASSERT_EQ(receipt.publication.key_sequence, 1);
	UT_ASSERT(receipt.publication.journal_sequence != 0);
	UT_ASSERT(receipt.publication.journal_slot_generation != 0);

	duplicate = receipt;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant, &publication,
										  &pending, &duplicate),
				 CLUSTER_CTRC_PREPARE_DUPLICATE);
	UT_ASSERT_EQ(duplicate.publication.key_sequence, 1);
	UT_ASSERT_EQ(duplicate.publication.journal_sequence,
				 receipt.publication.journal_sequence);

	MemSet(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant, &receipt,
											 &exact, &token),
				 CLUSTER_CTRC_APPLY_APPLIED);
	UT_ASSERT(token.valid);
	UT_ASSERT_EQ(receipt.state, CTRC_RECEIPT_APPLIED);
	UT_ASSERT_EQ(receipt.target.kind, CTRC_TARGET_EXACT_ITL_SLOT);
}

UT_TEST(test_ctrc_shared_table_exact_duplicate_is_idempotent)
{
	ClusterCtrcTxnKeyV1 key = test_key();
	ClusterCtrcParticipantIdentity identity = test_participant_identity(2);
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcPublicationIdV1 publication = test_publication(81,
		CTRC_REF_HEAP_ITL_UBA, CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
	ClusterCtrcTargetV1 target = test_pending_itl_target();
	ClusterCtrcReceipt receipts[3];
	ClusterCtrcReceipt original;
	uint64 receipt_index = UINT64_MAX;

	MemSet(&participant, 0, sizeof(participant));
	MemSet(receipts, 0, sizeof(receipts));
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_table_locked(&participant,
		&key, &identity, TEST_GRANT, &publication, &target,
		receipts, lengthof(receipts), 101, &receipt_index),
		CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT_EQ(receipt_index, 0);
	original = receipts[0];
	receipt_index = UINT64_MAX;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_table_locked(&participant,
		&key, &identity, TEST_GRANT, &publication, &target,
		receipts, lengthof(receipts), 999, &receipt_index),
		CLUSTER_CTRC_PREPARE_DUPLICATE);
	UT_ASSERT_EQ(receipt_index, 0);
	UT_ASSERT(memcmp(&receipts[0], &original, sizeof(original)) == 0);
	UT_ASSERT_EQ(participant.receipt_count, 1);
	UT_ASSERT_EQ(participant.last_key_sequence, 1);
}

UT_TEST(test_ctrc_shared_table_same_publication_different_target_blocks)
{
	ClusterCtrcTxnKeyV1 key = test_key();
	ClusterCtrcParticipantIdentity identity = test_participant_identity(2);
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcPublicationIdV1 publication = test_publication(82,
		CTRC_REF_HEAP_ITL_UBA, CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
	ClusterCtrcTargetV1 target = test_pending_itl_target();
	ClusterCtrcTargetV1 conflicting = target;
	ClusterCtrcReceipt receipts[3];
	uint64 receipt_index = UINT64_MAX;

	MemSet(&participant, 0, sizeof(participant));
	MemSet(receipts, 0, sizeof(receipts));
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_table_locked(&participant,
		&key, &identity, TEST_GRANT, &publication, &target,
		receipts, lengthof(receipts), 102, &receipt_index),
		CLUSTER_CTRC_PREPARE_READY);
	conflicting.block_number++;
	receipt_index = UINT64_MAX;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_table_locked(&participant,
		&key, &identity, TEST_GRANT, &publication, &conflicting,
		receipts, lengthof(receipts), 103, &receipt_index),
		CLUSTER_CTRC_PREPARE_REFUSED);
	UT_ASSERT_EQ(participant.state, CTRC_PARTICIPANT_BLOCKED);
	UT_ASSERT_EQ(participant.receipt_count, 1);
	UT_ASSERT_EQ(receipts[1].state, CTRC_RECEIPT_FREE);
	UT_ASSERT_EQ(receipts[2].state, CTRC_RECEIPT_FREE);
}

UT_TEST(test_ctrc_shared_table_different_publication_allocates_new_receipt)
{
	ClusterCtrcTxnKeyV1 key = test_key();
	ClusterCtrcParticipantIdentity identity = test_participant_identity(2);
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcPublicationIdV1 first = test_publication(83,
		CTRC_REF_HEAP_ITL_UBA, CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
	ClusterCtrcPublicationIdV1 second = first;
	ClusterCtrcTargetV1 target = test_pending_itl_target();
	ClusterCtrcReceipt receipts[3];
	uint64 receipt_index = UINT64_MAX;

	MemSet(&participant, 0, sizeof(participant));
	MemSet(receipts, 0, sizeof(receipts));
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_table_locked(&participant,
		&key, &identity, TEST_GRANT, &first, &target,
		receipts, lengthof(receipts), 104, &receipt_index),
		CLUSTER_CTRC_PREPARE_READY);
	second.operation_id++;
	second.attempt_generation++;
	receipt_index = UINT64_MAX;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_table_locked(&participant,
		&key, &identity, TEST_GRANT, &second, &target,
		receipts, lengthof(receipts), 105, &receipt_index),
		CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT_EQ(receipt_index, 1);
	UT_ASSERT_EQ(participant.state, CTRC_PARTICIPANT_OPEN);
	UT_ASSERT_EQ(participant.receipt_count, 2);
	UT_ASSERT_EQ(receipts[0].publication.key_sequence, 1);
	UT_ASSERT_EQ(receipts[1].publication.key_sequence, 2);
}

UT_TEST(test_ctrc_shared_table_participant_key_and_grant_drift_fail_closed)
{
	ClusterCtrcTxnKeyV1 key = test_key();
	ClusterCtrcParticipantIdentity identity = test_participant_identity(2);
	ClusterCtrcPublicationIdV1 publication = test_publication(84,
		CTRC_REF_HEAP_ITL_UBA, CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
	ClusterCtrcTargetV1 target = test_pending_itl_target();
	int drift;

	for (drift = 0; drift < 4; drift++)
	{
		ClusterCtrcTxnKeyV1 request_key = key;
		ClusterCtrcParticipantIdentity request_identity = identity;
		ClusterCtrcParticipantEntry participant;
		ClusterCtrcReceipt receipts[2];
		uint32 request_grant = TEST_GRANT;
		uint64 receipt_index = UINT64_MAX;

		MemSet(&participant, 0, sizeof(participant));
		MemSet(receipts, 0, sizeof(receipts));
		UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_table_locked(&participant,
			&key, &identity, TEST_GRANT, &publication, &target,
			receipts, lengthof(receipts), 106 + drift, &receipt_index),
			CLUSTER_CTRC_PREPARE_READY);
		if (drift == 0)
			request_identity.capability_record_generation++;
		else if (drift == 1)
			request_identity.boot_incarnation++;
		else if (drift == 2)
			request_key.xid++;
		else
			request_grant++;
		receipt_index = UINT64_MAX;
		UT_ASSERT_EQ(cluster_ctrc_receipt_prepare_table_locked(&participant,
			&request_key, &request_identity, request_grant,
			&publication, &target, receipts, lengthof(receipts),
			120 + drift, &receipt_index),
			CLUSTER_CTRC_PREPARE_REFUSED);
		UT_ASSERT_EQ(participant.state, CTRC_PARTICIPANT_BLOCKED);
		UT_ASSERT_EQ(participant.receipt_count, 1);
		UT_ASSERT_EQ(receipts[1].state, CTRC_RECEIPT_FREE);
	}
}

/* MXA-T22: a mismatched finalize is a pre-mutation retry and leaves the
 * receipt PREPARED.  Only the exact release-CAS returns a mutation token. */
UT_TEST(test_ctrc_heap_apply_is_nonblocking_and_precedes_all_reference_mutation)
{
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcPublicationIdV1 publication = test_publication(2,
		CTRC_REF_HEAP_ITL_UBA, CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
	ClusterCtrcTargetV1 pending = test_pending_itl_target();
	ClusterCtrcTargetV1 exact = test_exact_itl_target();
	ClusterCtrcReceipt receipt;
	ClusterCtrcApplyToken token;

	test_open_participant(&participant);
	MemSet(&receipt, 0, sizeof(receipt));
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant, &publication,
										  &pending, &receipt),
				 CLUSTER_CTRC_PREPARE_READY);
	exact.block_number++;
	MemSet(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant, &receipt,
											 &exact, &token),
				 CLUSTER_CTRC_APPLY_RETRY_REQUIRED);
	UT_ASSERT(!token.valid);
	UT_ASSERT_EQ(receipt.state, CTRC_RECEIPT_PREPARED);

	exact.block_number--;
	UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant, &receipt,
											 &exact, &token),
				 CLUSTER_CTRC_APPLY_APPLIED);
	UT_ASSERT(token.valid);
}

/* MXA-T21/T22 current-MX half: every ACTIVE member publication reserves a
 * page/offnum receipt and converts it to the exact tuple/MX identity before
 * the descriptor can become reachable.  Zero-grant terminal proof and every
 * target identity drift remain pre-mutation refusals. */
UT_TEST(test_ctrc_current_mx_receipt_applies_only_exact_active_target)
{
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcPublicationIdV1 publication = test_publication(22,
		CTRC_REF_RECOMPOSED_SURVIVOR, CTRC_TARGET_PAGE_PENDING_OFFNUM);
	ClusterCtrcTargetV1 pending = test_pending_offnum_target(
		publication.descriptor_hash);
	ClusterCtrcTargetV1 exact = test_exact_tid_target(
		publication.descriptor_hash);
	ClusterCtrcReceipt receipt;
	ClusterCtrcApplyToken token;

	test_open_participant(&participant);
	MemSet(&receipt, 0, sizeof(receipt));
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant, &publication,
										  &pending, &receipt),
				 CLUSTER_CTRC_PREPARE_READY);
	exact.block_number++;
	UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant, &receipt,
										 &exact, &token),
				 CLUSTER_CTRC_APPLY_RETRY_REQUIRED);
	UT_ASSERT_EQ(receipt.state, CTRC_RECEIPT_PREPARED);
	exact.block_number--;
	UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant, &receipt,
										 &exact, &token),
				 CLUSTER_CTRC_APPLY_APPLIED);
	UT_ASSERT(token.valid);
	UT_ASSERT_EQ(receipt.target.kind, CTRC_TARGET_EXACT_TID);

	MemSet(&receipt, 0, sizeof(receipt));
	publication.operation_id++;
	publication.grant_generation = 0;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant, &publication,
										  &pending, &receipt),
				 CLUSTER_CTRC_PREPARE_REFUSED);
}

/* MXA-T23: NEEDS_CLEANOUT and skipped legacy hints are not release proof. */
UT_TEST(test_ctrc_itl_uba_registers_before_mutation_and_only_exact_projection_discharges)
{
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcPublicationIdV1 publication = test_publication(3,
		CTRC_REF_HEAP_ITL_UBA, CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
	ClusterCtrcTargetV1 pending = test_pending_itl_target();
	ClusterCtrcTargetV1 exact = test_exact_itl_target();
	ClusterCtrcReceipt receipt;
	ClusterCtrcApplyToken token;
	ClusterCtrcDurability durability;
	ClusterCtrcItlTargetIdentity target_identity;
	ClusterCtrcItlTargetIdentity wrong_identity;

	test_open_participant(&participant);
	MemSet(&receipt, 0, sizeof(receipt));
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant, &publication,
										  &pending, &receipt),
				 CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant, &receipt,
											 &exact, &token),
				 CLUSTER_CTRC_APPLY_APPLIED);
	MemSet(&target_identity, 0, sizeof(target_identity));
	target_identity.spc_oid = exact.spc_oid;
	target_identity.db_oid = exact.db_oid;
	target_identity.rel_number = exact.rel_number;
	target_identity.fork_number = exact.fork_number;
	target_identity.block_number = exact.block_number;
	target_identity.itl_slot_index = exact.itl_slot_index;
	target_identity.itl_slot_wrap = exact.itl_slot_wrap;
	target_identity.itl_xid = exact.itl_xid;
	target_identity.itl_class = exact.itl_class;
	target_identity.needs_wal = exact.needs_wal;
	memcpy(target_identity.uba, exact.uba, sizeof(target_identity.uba));
	UT_ASSERT(cluster_ctrc_itl_target_identity_matches(
		&receipt.target, &target_identity));
	target_identity.uba[sizeof(target_identity.uba) - 1]++;
	UT_ASSERT(!cluster_ctrc_itl_target_identity_matches(
		&receipt.target, &target_identity));
	target_identity.uba[sizeof(target_identity.uba) - 1]--;
	wrong_identity = target_identity;
	wrong_identity.block_number++;
	UT_ASSERT(!cluster_ctrc_itl_target_identity_matches(
		&receipt.target, &wrong_identity));
	wrong_identity = target_identity;
	wrong_identity.itl_slot_index++;
	UT_ASSERT(!cluster_ctrc_itl_target_identity_matches(
		&receipt.target, &wrong_identity));
	wrong_identity = target_identity;
	wrong_identity.itl_slot_wrap++;
	UT_ASSERT(!cluster_ctrc_itl_target_identity_matches(
		&receipt.target, &wrong_identity));
	wrong_identity = target_identity;
	wrong_identity.itl_xid++;
	UT_ASSERT(!cluster_ctrc_itl_target_identity_matches(
		&receipt.target, &wrong_identity));
	wrong_identity = target_identity;
	wrong_identity.itl_class = target_identity.itl_class == 1 ? 2 : 1;
	UT_ASSERT(!cluster_ctrc_itl_target_identity_matches(
		&receipt.target, &wrong_identity));
	wrong_identity = target_identity;
	wrong_identity.needs_wal = !target_identity.needs_wal;
	UT_ASSERT(!cluster_ctrc_itl_target_identity_matches(
		&receipt.target, &wrong_identity));
	MemSet(&durability, 0, sizeof(durability));
	durability.highest_local_lsn = 300;
	durability.local_flush_lsn = 300;

	UT_ASSERT_EQ(cluster_ctrc_receipt_discharge_itl(&participant, &receipt,
												 CTRC_ITL_NEEDS_CLEANOUT,
												 &durability),
				 CLUSTER_CTRC_DISCHARGE_RETAIN);
	UT_ASSERT_EQ(cluster_ctrc_receipt_discharge_itl(&participant, &receipt,
											 CTRC_ITL_HINT_SKIPPED,
											 &durability),
				 CLUSTER_CTRC_DISCHARGE_RETAIN);
	durability.local_flush_lsn = 299;
	UT_ASSERT_EQ(cluster_ctrc_receipt_discharge_itl(&participant, &receipt,
											 CTRC_ITL_TERMINAL_INDEPENDENT,
											 &durability),
				 CLUSTER_CTRC_DISCHARGE_RETAIN);
	durability.local_flush_lsn = 300;
	durability.required_lsn[4] = 400;
	durability.durable_lsn[4] = 399;
	UT_ASSERT_EQ(cluster_ctrc_receipt_discharge_itl(&participant, &receipt,
											 CTRC_ITL_TERMINAL_INDEPENDENT,
											 &durability),
				 CLUSTER_CTRC_DISCHARGE_RETAIN);
	durability.durable_lsn[4] = 400;
	UT_ASSERT_EQ(cluster_ctrc_receipt_discharge_itl(&participant, &receipt,
											 CTRC_ITL_TERMINAL_INDEPENDENT,
											 &durability),
				 CLUSTER_CTRC_DISCHARGE_CLEANED);
	UT_ASSERT_EQ(receipt.state, CTRC_RECEIPT_CLEANED);
	UT_ASSERT_EQ(receipt.disposition,
				 CTRC_RELEASE_CLEANED_TERMINAL_REWRITE);
	UT_ASSERT_EQ(receipt.highest_local_wal_lsn,
				 durability.highest_local_lsn);
	UT_ASSERT_EQ(receipt.required_lsn[4], durability.required_lsn[4]);
	UT_ASSERT_EQ(participant.applied_count, UINT64_C(0));
	UT_ASSERT_EQ(participant.cleaned_count, UINT64_C(1));
	UT_ASSERT_EQ(cluster_ctrc_receipt_discharge_itl(&participant, &receipt,
											 CTRC_ITL_TERMINAL_INDEPENDENT,
											 &durability),
				 CLUSTER_CTRC_DISCHARGE_CLEANED);
	UT_ASSERT_EQ(participant.cleaned_count, UINT64_C(1));
}

/* MXA-T24: CLOSE racing an old PREPARED receipt either observes exact APPLY
 * or keeps the range blocked; time and backend loss never cancel it. */
UT_TEST(test_ctrc_close_race_drains_prepared_without_timeout_cancellation)
{
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcParticipantIdentity identity = test_participant_identity(2);
	ClusterCtrcPublicationIdV1 publication = test_publication(4,
		CTRC_REF_HEAP_ITL_UBA, CTRC_TARGET_PAGE_PENDING_ITL_SLOT);
	ClusterCtrcTargetV1 pending = test_pending_itl_target();
	ClusterCtrcTargetV1 exact = test_exact_itl_target();
	ClusterCtrcReceipt receipt;
	ClusterCtrcReceipt late_receipt;
	ClusterCtrcApplyToken token;
	ClusterCtrcDurability durability;

	test_open_participant(&participant);
	MemSet(&receipt, 0, sizeof(receipt));
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant, &publication,
										  &pending, &receipt),
				 CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT_EQ(cluster_ctrc_participant_close(&participant, &identity,
										TEST_GRANT, TEST_SEAL),
				 CLUSTER_CTRC_CLOSE_PENDING_DRAIN);
	UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant, &receipt,
											 &exact, &token),
				 CLUSTER_CTRC_APPLY_RETRY_REQUIRED);
	UT_ASSERT_EQ(cluster_ctrc_participant_note_owner_loss(&participant,
												&receipt),
				 CLUSTER_CTRC_LOSS_BLOCKED);
	UT_ASSERT_EQ(receipt.state, CTRC_RECEIPT_BLOCKED);
	UT_ASSERT_EQ(participant.state, CTRC_PARTICIPANT_BLOCKED);

	/* If APPLY wins before CLOSE, CLOSE drains that APPLIED receipt through
	 * exact cleanout and only then freezes the local range. */
	test_open_participant(&participant);
	MemSet(&receipt, 0, sizeof(receipt));
	publication.operation_id++;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant, &publication,
										  &pending, &receipt),
				 CLUSTER_CTRC_PREPARE_READY);
	UT_ASSERT_EQ(cluster_ctrc_receipt_apply_prepared(&participant, &receipt,
											 &exact, &token),
				 CLUSTER_CTRC_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_ctrc_participant_close(&participant, &identity,
										TEST_GRANT, TEST_SEAL),
				 CLUSTER_CTRC_CLOSE_PENDING_DRAIN);
	MemSet(&durability, 0, sizeof(durability));
	durability.highest_local_lsn = 300;
	durability.local_flush_lsn = 300;
	UT_ASSERT_EQ(cluster_ctrc_receipt_discharge_itl(&participant, &receipt,
											 CTRC_ITL_TERMINAL_INDEPENDENT,
											 &durability),
				 CLUSTER_CTRC_DISCHARGE_CLEANED);
	UT_ASSERT_EQ(cluster_ctrc_participant_close(&participant, &identity,
										TEST_GRANT, TEST_SEAL),
				 CLUSTER_CTRC_CLOSE_ACK_READY);
	UT_ASSERT_EQ(participant.state, CTRC_PARTICIPANT_ACK_READY);

	/* A frozen close never reopens for a late proof/prepare. */
	MemSet(&late_receipt, 0, sizeof(late_receipt));
	publication.operation_id++;
	UT_ASSERT_EQ(cluster_ctrc_receipt_prepare(&participant, &publication,
										  &pending, &late_receipt),
				 CLUSTER_CTRC_PREPARE_REFUSED);
}

/* MXA-T25: only exact target absence under the censused transfer contract is
 * a discharge; ambiguity or changed page authority retains. */
UT_TEST(test_ctrc_exact_target_absence_and_ambiguity_cleanout_table)
{
	ClusterCtrcCleanReferenceInput input;

	MemSet(&input, 0, sizeof(input));
	input.target_state = CTRC_TARGET_ABSENT;
	input.source_transition_censused = true;
	input.page_authority_exact = true;
	UT_ASSERT_EQ(cluster_ctrc_clean_reference(&input),
				 CTRC_CLEANED_ABSENT);
	input.source_transition_censused = false;
	UT_ASSERT_EQ(cluster_ctrc_clean_reference(&input), CTRC_CLEAN_RETAIN);
	input.source_transition_censused = true;
	input.page_authority_exact = false;
	UT_ASSERT_EQ(cluster_ctrc_clean_reference(&input), CTRC_CLEAN_RETAIN);
	input.page_authority_exact = true;
	input.target_state = CTRC_TARGET_AMBIGUOUS;
	UT_ASSERT_EQ(cluster_ctrc_clean_reference(&input), CTRC_CLEAN_RETAIN);
}

/* MXA-T26: committed updater semantics cannot be erased while any ACTIVE or
 * UNKNOWN companion remains.  Terminal lockers and aborted updaters clean. */
UT_TEST(test_ctrc_terminal_member_cleanout_semantics_cross_product)
{
	ClusterCtrcCleanReferenceInput input;

	MemSet(&input, 0, sizeof(input));
	input.page_authority_exact = true;
	input.target_state = CTRC_TARGET_TERMINAL_LOCK_ONLY;
	UT_ASSERT_EQ(cluster_ctrc_clean_reference(&input),
				 CTRC_CLEANED_TERMINAL_REWRITE);
	input.target_state = CTRC_TARGET_ABORTED_UPDATER;
	UT_ASSERT_EQ(cluster_ctrc_clean_reference(&input),
				 CTRC_CLEANED_TERMINAL_REWRITE);
	input.target_state = CTRC_TARGET_COMMITTED_UPDATER;
	input.active_companions = 1;
	UT_ASSERT_EQ(cluster_ctrc_clean_reference(&input), CTRC_CLEAN_RETAIN);
	input.active_companions = 0;
	input.unknown_companions = 1;
	UT_ASSERT_EQ(cluster_ctrc_clean_reference(&input), CTRC_CLEAN_RETAIN);
	input.unknown_companions = 0;
	input.successor_topology_exact = true;
	UT_ASSERT_EQ(cluster_ctrc_clean_reference(&input),
				 CTRC_CLEANED_TERMINAL_REWRITE);
}

/* MXA-T27: deleting the predecessor first must fail even when the eventual
 * successor is byte-identical. */
UT_TEST(test_ctrc_successor_receipts_and_descriptor_precede_predecessor_removal)
{
	ClusterCtrcTransferState transfer;

	MemSet(&transfer, 0, sizeof(transfer));
	transfer.active_survivor_count = 2;
	UT_ASSERT_EQ(cluster_ctrc_transfer_remove_predecessor(&transfer),
				 CLUSTER_CTRC_TRANSFER_REFUSED);
	UT_ASSERT_EQ(cluster_ctrc_transfer_note_successor_receipt(&transfer),
				 CLUSTER_CTRC_TRANSFER_PENDING_DESCRIPTOR);
	UT_ASSERT_EQ(cluster_ctrc_transfer_note_successor_receipt(&transfer),
				 CLUSTER_CTRC_TRANSFER_PENDING_DESCRIPTOR);
	UT_ASSERT_EQ(cluster_ctrc_transfer_note_descriptor_durable(&transfer),
				 CLUSTER_CTRC_TRANSFER_READY);
	UT_ASSERT_EQ(cluster_ctrc_transfer_remove_predecessor(&transfer),
				 CLUSTER_CTRC_TRANSFER_REMOVED);
}

/* MXA-T28: a touched participant is immutable for one grant. */
UT_TEST(test_ctrc_participant_boot_capability_and_membership_loss_retain)
{
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcParticipantIdentity identity = test_participant_identity(2);

	test_open_participant(&participant);
	UT_ASSERT_EQ(cluster_ctrc_participant_close(&participant, &identity,
										TEST_GRANT, TEST_SEAL),
				 CLUSTER_CTRC_CLOSE_ACK_READY);
	UT_ASSERT_EQ(participant.state, CTRC_PARTICIPANT_ACK_READY);
	UT_ASSERT_EQ(participant.seal_generation, TEST_SEAL);
	UT_ASSERT_EQ(cluster_ctrc_participant_close(&participant, &identity,
										TEST_GRANT, TEST_SEAL),
				 CLUSTER_CTRC_CLOSE_ACK_READY);

	test_open_participant(&participant);
	identity.boot_incarnation++;
	UT_ASSERT_EQ(cluster_ctrc_participant_close(&participant, &identity,
										TEST_GRANT, TEST_SEAL),
				 CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN);
	UT_ASSERT_EQ(participant.state, CTRC_PARTICIPANT_BLOCKED);

	identity = test_participant_identity(2);
	test_open_participant(&participant);
	UT_ASSERT_EQ(cluster_ctrc_participant_close(&participant, &identity,
										TEST_GRANT, TEST_SEAL),
				 CLUSTER_CTRC_CLOSE_ACK_READY);
	UT_ASSERT_EQ(cluster_ctrc_participant_close(&participant, &identity,
										TEST_GRANT, TEST_SEAL + 1),
				 CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN);
	UT_ASSERT_EQ(participant.state, CTRC_PARTICIPANT_BLOCKED);
}

/* MXA-T29: inter-key global gaps do not affect the exact local 1..N range;
 * local gaps, non-durable dependencies and non-tombstone N=0 all refuse. */
UT_TEST(test_ctrc_local_release_ack_range_digest_and_dependency_durability)
{
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcParticipantIdentity identity = test_participant_identity(2);
	ClusterCtrcLocalReleaseAckV1 ack;
	ClusterCtrcDurability durability;

	test_open_participant(&participant);
	UT_ASSERT_EQ(cluster_ctrc_participant_close(&participant, &identity,
										TEST_GRANT, TEST_SEAL),
				 CLUSTER_CTRC_CLOSE_ACK_READY);
	MemSet(&durability, 0, sizeof(durability));
	UT_ASSERT_EQ(cluster_ctrc_participant_build_ack(&participant, &durability,
												 &ack),
				 CLUSTER_CTRC_ACK_RELEASED);
	UT_ASSERT_EQ(sizeof(ack), CLUSTER_CTRC_LOCAL_ACK_BYTES);
	UT_ASSERT_EQ(ack.total_receipt_count, 0);
	UT_ASSERT((ack.flags & CTRC_ACK_FLAG_ZERO_RANGE) != 0);
	UT_ASSERT_EQ(memcmp(ack.row_digest_sha256,
						 cluster_ctrc_empty_sha256,
						 sizeof(ack.row_digest_sha256)), 0);

	test_open_participant(&participant);
	participant.last_key_sequence = 2;
	participant.receipt_count = 1;
	UT_ASSERT_EQ(cluster_ctrc_participant_close(&participant, &identity,
										TEST_GRANT, TEST_SEAL),
				 CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN);
}

/* MXA-T30: the certificate input is a set, not a quorum. */
UT_TEST(test_ctrc_origin_certificate_requires_exact_complete_sorted_ack_set)
{
	ClusterCtrcCertificateInput input;
	ClusterCtrcLocalReleaseAckV1 acks[2];

	MemSet(&input, 0, sizeof(input));
	MemSet(acks, 0, sizeof(acks));
	input.acks = acks;
	input.ack_count = 1;
	input.frozen_touched_bitmap = UINT64_C(0x6);
	acks[0].participant_node_id = 1;
	UT_ASSERT_EQ(cluster_ctrc_origin_certificate_validate(&input),
				 CLUSTER_CTRC_CERTIFICATE_RETAIN);
	input.ack_count = 2;
	acks[1].participant_node_id = 2;
	acks[0].result = CTRC_ACK_RELEASED;
	acks[1].result = CTRC_ACK_RELEASED;
	acks[0].seal_generation = TEST_SEAL;
	acks[1].seal_generation = TEST_SEAL;
	input.seal_generation = TEST_SEAL;
	input.block0_terminal_exact = true;
	input.all_dependencies_durable = true;
	UT_ASSERT_EQ(cluster_ctrc_origin_certificate_validate(&input),
				 CLUSTER_CTRC_CERTIFICATE_READY);
	acks[1].participant_node_id = 1;
	UT_ASSERT_EQ(cluster_ctrc_origin_certificate_validate(&input),
				 CLUSTER_CTRC_CERTIFICATE_RETAIN);
}

/* MXA-T31: COMMITTED retains the independent SCN horizon; ABORTED does not
 * invent one, but both require the canonical release bit. */
UT_TEST(test_ctrc_release_certificate_enables_exact_l11_l12_and_ack_reclaim)
{
	ClusterCtrcRecycleInput input;

	MemSet(&input, 0, sizeof(input));
	input.status = CTRC_TERMINAL_COMMITTED;
	input.release_proven = true;
	input.commit_scn = 100;
	input.horizon_valid = true;
	input.horizon_scn = 99;
	UT_ASSERT(!cluster_ctrc_terminal_recyclable(&input));
	input.horizon_scn = 100;
	UT_ASSERT(cluster_ctrc_terminal_recyclable(&input));
	input.release_proven = false;
	UT_ASSERT(!cluster_ctrc_terminal_recyclable(&input));

	MemSet(&input, 0, sizeof(input));
	input.status = CTRC_TERMINAL_ABORTED;
	input.release_proven = true;
	input.durable_aborted = true;
	UT_ASSERT(cluster_ctrc_terminal_recyclable(&input));
	input.release_proven = false;
	UT_ASSERT(!cluster_ctrc_terminal_recyclable(&input));
}

/* MXA-T32: only the durable certificate cut releases.  Earlier cuts retain,
 * and the later participant notification affects reclamation only. */
UT_TEST(test_ctrc_crash_cut_matrix_releases_only_after_durable_certificate)
{
	int cut;

	for (cut = CTRC_CRASH_BEFORE_TOUCH; cut <= CTRC_CRASH_ORIGIN_PRECERT_LOSS;
		 cut++)
	{
		ClusterCtrcCrashDisposition expected
			= cut == CTRC_CRASH_DURABLE_CERTIFICATE_BEFORE_NOTIFICATION
			? CLUSTER_CTRC_CRASH_RELEASE_PROVEN
			: CLUSTER_CTRC_CRASH_RETAIN;

		UT_ASSERT_EQ(cluster_ctrc_crash_cut_disposition(
			(ClusterCtrcCrashCut)cut), expected);
	}
}

/* MXA-T34: integer overflow or a table smaller than the frozen formula must
 * block activation; runtime full never authorizes eviction. */
UT_TEST(test_ctrc_capacity_is_activation_sized_and_runtime_full_refuses_before_mutation)
{
	ClusterCtrcCapacity capacity;

	UT_ASSERT(cluster_ctrc_capacity_compute(131072, 512, 4, &capacity));
	UT_ASSERT_EQ(capacity.origin_key_entries,
				 CLUSTER_UNDO_SEGS_PER_INSTANCE * TT_SLOTS_PER_SEGMENT);
	UT_ASSERT_EQ(capacity.participant_key_entries,
				 capacity.origin_key_entries * 4);
	UT_ASSERT_EQ(capacity.receipt_entries,
				 UINT64_C(131072) + UINT64_C(512)
				 * CLUSTER_CURRENT_MX_MAX_MEMBERS);
	UT_ASSERT_EQ(capacity.ack_summary_entries,
				 capacity.participant_key_entries);
	UT_ASSERT(!cluster_ctrc_capacity_compute(SIZE_MAX, INT_MAX, 16,
										 &capacity));
	UT_ASSERT_EQ(cluster_ctrc_runtime_full_disposition(),
				 CLUSTER_CTRC_CAPACITY_REFUSE_BEFORE_MUTATION);
}

static bool
source_class_known(const char *source_class)
{
	static const char *const known[] = {
		"REGISTERED_REFERENCE",
		"TERMINAL_PROJECTION_DISCHARGE",
		"SUCCESSOR_BEFORE_PREDECESSOR",
		"PROVEN_LOCAL_NONCLUSTER",
		"FAIL_CLOSED_UNREACHABLE",
	};
	size_t i;

	for (i = 0; i < lengthof(known); i++)
		if (strcmp(source_class, known[i]) == 0)
			return true;
	return false;
}

static int
split_tsv_line(char *line, char **fields, int fields_cap)
{
	char *cursor = line;
	int field_count = 0;

	while (field_count < fields_cap)
	{
		fields[field_count++] = cursor;
		cursor = strchr(cursor, '\t');
		if (cursor == NULL)
			break;
		*cursor++ = '\0';
	}
	cursor = strchr(fields[field_count - 1], '\n');
	if (cursor != NULL)
		*cursor = '\0';
	return field_count;
}

static bool
source_file_contains(const char *relative_path, const char *needle)
{
	char path[MAXPGPATH * 2];
	char *contents;
	FILE *file;
	long length;
	bool found;

	if (relative_path == NULL || relative_path[0] == '\0'
		|| needle == NULL || needle[0] == '\0'
		|| snprintf(path, sizeof(path), "%s/%s", PGRAC_SOURCE_ROOT_PATH,
			relative_path) >= (int) sizeof(path))
		return false;
	file = fopen(path, "rb");
	if (file == NULL)
		return false;
	if (fseek(file, 0, SEEK_END) != 0)
	{
		fclose(file);
		return false;
	}
	length = ftell(file);
	if (length < 0 || fseek(file, 0, SEEK_SET) != 0)
	{
		fclose(file);
		return false;
	}
	contents = (char *) malloc((size_t) length + 1);
	if (contents == NULL)
	{
		fclose(file);
		return false;
	}
	if (fread(contents, 1, (size_t) length, file) != (size_t) length)
	{
		free(contents);
		fclose(file);
		return false;
	}
	contents[length] = '\0';
	found = strstr(contents, needle) != NULL;
	free(contents);
	fclose(file);
	return found;
}

/* MXA-T35: every manifest row must carry one closed semantic class.  A row
 * cannot earn classification from a filename or comment alone. */
UT_TEST(test_ctrc_source_census_has_no_unclassified_reference_or_release_writer)
{
	FILE *file;
	char line[2048];
	int rows = 0;

	file = fopen(CTRC_SOURCE_MANIFEST_PATH, "r");
	UT_ASSERT_NOT_NULL(file);
	if (file == NULL)
		return;
	while (fgets(line, sizeof(line), file) != NULL)
	{
		char *fields[8];
		int field_count;

		if (line[0] == '#' || line[0] == '\n')
			continue;
		field_count = split_tsv_line(line, fields, lengthof(fields));
		UT_ASSERT_EQ(field_count, 8);
		if (field_count != 8)
			continue;
		UT_ASSERT(fields[0][0] != '\0');
		UT_ASSERT(fields[1][0] != '\0');
		UT_ASSERT(source_class_known(fields[2]));
		UT_ASSERT(source_file_contains(fields[1], fields[0]));
		UT_ASSERT_STR_EQ(fields[7], "MXA-T35");
		rows++;
	}
	fclose(file);
	UT_ASSERT(rows > 0);
}

/* The ordinary heap producer set is closed over every source entrypoint that
 * can publish a tuple ITL/UBA reference.  Delegation is explicit; an old
 * touch-only or receipt-free direct producer cannot satisfy a row. */
UT_TEST(test_ctrc_heap_reference_producer_census_is_closed)
{
	static const char *const required[] = {
		"heap_insert",
		"heap_multi_insert",
		"heap_delete",
		"heap_update",
		"heap_lock_tuple",
		"heap_lock_updated_tuple_rec",
	};
	bool seen[lengthof(required)];
	FILE *file;
	char line[4096];
	int rows = 0;

	MemSet(seen, 0, sizeof(seen));
	file = fopen(HEAP_REFERENCE_PRODUCER_MANIFEST_PATH, "r");
	UT_ASSERT_NOT_NULL(file);
	if (file == NULL)
		return;
	while (fgets(line, sizeof(line), file) != NULL)
	{
		char *fields[10];
		int field_count;
		size_t i;

		if (line[0] == '#' || line[0] == '\n')
			continue;
		field_count = split_tsv_line(line, fields, lengthof(fields));
		UT_ASSERT_EQ(field_count, 10);
		if (field_count != 10)
			continue;
		for (i = 0; i < lengthof(required); i++)
			if (strcmp(fields[0], required[i]) == 0)
			{
				UT_ASSERT(!seen[i]);
				seen[i] = true;
				break;
			}
		UT_ASSERT(i < lengthof(required));
		UT_ASSERT(strcmp(fields[2], "DIRECT_EXACT") == 0
			|| strcmp(fields[2], "PER_TUPLE_DELEGATE") == 0);
		UT_ASSERT(strncmp(fields[7], "FAIL_CLOSED", 11) == 0);
		UT_ASSERT(source_file_contains(fields[1], fields[0]));
		UT_ASSERT(source_file_contains(fields[1], fields[3]));
		UT_ASSERT(source_file_contains(fields[1], fields[4]));
		UT_ASSERT(source_file_contains(fields[1], fields[5]));
		UT_ASSERT(source_file_contains(fields[1], fields[6]));
		UT_ASSERT(source_file_contains(fields[8], fields[9]));
		rows++;
	}
	fclose(file);
	UT_ASSERT_EQ(rows, lengthof(required));
	for (size_t i = 0; i < lengthof(required); i++)
		UT_ASSERT(seen[i]);
	UT_ASSERT(!source_file_contains("src/backend/access/heap/heapam.c",
		"cluster_itl_touch_register_exact("));
	UT_ASSERT(source_file_contains("src/backend/access/heap/heapam.c",
		"cluster_itl_touch_register_exact_ctrc("));
}

/* Every cardinality-sensitive owner is named once and linked to an actual
 * one-member behavior test.  This keeps a helper-only success from masking a
 * reject in memo, wire, remote CR, ordinary heap, or HOT consumption. */
UT_TEST(test_ctrc_current_mx_cardinality_census_allows_one_end_to_end)
{
	static const char *const required[] = {
		"PRODUCER_PLAN", "MATERIALIZER", "LOCAL_DESCRIBE",
		"REMOTE_DESCRIBE_SERVE", "REMOTE_DESCRIBE_FETCH",
		"WIRE_FORWARD", "WIRE_REPLY", "MEMBER_PROOF", "RESOLVE",
		"RECOMPOSE", "MEMO", "ORDINARY_HEAP_CONSUMER", "HOT_CONSUMER",
	};
	bool seen[lengthof(required)];
	FILE *file;
	char line[4096];
	int rows = 0;

	MemSet(seen, 0, sizeof(seen));
	file = fopen(CURRENT_MX_CARDINALITY_MANIFEST_PATH, "r");
	UT_ASSERT_NOT_NULL(file);
	if (file == NULL)
		return;
	while (fgets(line, sizeof(line), file) != NULL)
	{
		char *fields[7];
		int field_count;
		size_t i;

		if (line[0] == '#' || line[0] == '\n')
			continue;
		field_count = split_tsv_line(line, fields, lengthof(fields));
		UT_ASSERT_EQ(field_count, 7);
		if (field_count != 7)
			continue;
		for (i = 0; i < lengthof(required); i++)
			if (strcmp(fields[0], required[i]) == 0)
			{
				UT_ASSERT(!seen[i]);
				seen[i] = true;
				break;
			}
		UT_ASSERT(i < lengthof(required));
		UT_ASSERT_STR_EQ(fields[3], "ALLOW_ONE");
		UT_ASSERT(fields[4][0] != '\0' && strcmp(fields[4], "-") != 0);
		UT_ASSERT(source_file_contains(fields[2], fields[1]));
		UT_ASSERT(source_file_contains(fields[5], fields[6]));
		rows++;
	}
	fclose(file);
	UT_ASSERT_EQ(rows, lengthof(required));
	for (size_t i = 0; i < lengthof(required); i++)
		UT_ASSERT(seen[i]);
}

typedef void (*CtrcTestFn)(void);

typedef struct CtrcTestCase
{
	const char *name;
	CtrcTestFn function;
} CtrcTestCase;

#define CTRC_TEST_ENTRY(name) {#name, name}

int
main(void)
{
	static const CtrcTestCase tests[] = {
		CTRC_TEST_ENTRY(test_ctrc_origin_reservation_prebind_cancel_and_postbind_block_are_exact),
		CTRC_TEST_ENTRY(test_ctrc_origin_reservation_open_duplicate_and_aba_cleanup_do_not_mutate),
		CTRC_TEST_ENTRY(test_ctrc_active_grant_records_touched_node_before_positive_proof),
		CTRC_TEST_ENTRY(test_ctrc_receipt_prepare_apply_full_identity_cross_product),
		CTRC_TEST_ENTRY(test_ctrc_shared_table_exact_duplicate_is_idempotent),
		CTRC_TEST_ENTRY(test_ctrc_shared_table_same_publication_different_target_blocks),
		CTRC_TEST_ENTRY(test_ctrc_shared_table_different_publication_allocates_new_receipt),
		CTRC_TEST_ENTRY(test_ctrc_shared_table_participant_key_and_grant_drift_fail_closed),
		CTRC_TEST_ENTRY(test_ctrc_heap_apply_is_nonblocking_and_precedes_all_reference_mutation),
		CTRC_TEST_ENTRY(test_ctrc_current_mx_receipt_applies_only_exact_active_target),
		CTRC_TEST_ENTRY(test_ctrc_itl_uba_registers_before_mutation_and_only_exact_projection_discharges),
		CTRC_TEST_ENTRY(test_ctrc_close_race_drains_prepared_without_timeout_cancellation),
		CTRC_TEST_ENTRY(test_ctrc_exact_target_absence_and_ambiguity_cleanout_table),
		CTRC_TEST_ENTRY(test_ctrc_terminal_member_cleanout_semantics_cross_product),
		CTRC_TEST_ENTRY(test_ctrc_successor_receipts_and_descriptor_precede_predecessor_removal),
		CTRC_TEST_ENTRY(test_ctrc_participant_boot_capability_and_membership_loss_retain),
		CTRC_TEST_ENTRY(test_ctrc_local_release_ack_range_digest_and_dependency_durability),
		CTRC_TEST_ENTRY(test_ctrc_origin_certificate_requires_exact_complete_sorted_ack_set),
		CTRC_TEST_ENTRY(test_ctrc_release_certificate_enables_exact_l11_l12_and_ack_reclaim),
		CTRC_TEST_ENTRY(test_ctrc_crash_cut_matrix_releases_only_after_durable_certificate),
		CTRC_TEST_ENTRY(test_ctrc_capacity_is_activation_sized_and_runtime_full_refuses_before_mutation),
		CTRC_TEST_ENTRY(test_ctrc_source_census_has_no_unclassified_reference_or_release_writer),
		CTRC_TEST_ENTRY(test_ctrc_heap_reference_producer_census_is_closed),
		CTRC_TEST_ENTRY(test_ctrc_current_mx_cardinality_census_allows_one_end_to_end),
	};
	const char *filter = getenv("PGRAC_CTRC_UNIT_FILTER");
	size_t i;
	int selected = 0;

	for (i = 0; i < lengthof(tests); i++)
		if (filter == NULL || filter[0] == '\0'
			|| strcmp(filter, tests[i].name) == 0)
			selected++;
	UT_PLAN(selected);
	for (i = 0; i < lengthof(tests); i++)
	{
		if (filter != NULL && filter[0] != '\0'
			&& strcmp(filter, tests[i].name) != 0)
			continue;
		ut_test_count++;
		ut_current_failed = 0;
		tests[i].function();
		if (ut_current_failed == 0)
			printf("ok %d - %s\n", ut_test_count, tests[i].name);
		else
		{
			printf("not ok %d - %s\n", ut_test_count, tests[i].name);
			ut_failed_count++;
		}
	}
	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
