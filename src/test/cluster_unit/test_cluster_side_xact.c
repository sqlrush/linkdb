/*-------------------------------------------------------------------------
 * test_cluster_side_xact.c
 *    RF-SIDE immutable XACT decode tests.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/rmgr.h"
#include "access/twophase_rmgr.h"
#include "access/xact.h"
#include "cluster/cluster_side_xact.h"
#include "cluster/cluster_side_online_plan.h"
#include "cluster/cluster_side_undo.h"
#include "cluster/cluster_tt_2pc.h"
#include "cluster/storage/cluster_undo_xlog.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

#include <stdio.h>

bool
cluster_remote_xact_prepare_digest_v2(
	uint64 system_identifier pg_attribute_unused(),
	int origin_node pg_attribute_unused(),
	TransactionId xid pg_attribute_unused(), Oid database pg_attribute_unused(),
	const char *gid pg_attribute_unused(),
	uint8 digest[CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES])
{
	memset(digest, 0x5a, CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES);
	return true;
}

typedef struct FakeXactRecord
{
	XLogReaderState reader;
	uint8 data[512];
	union
	{
		DecodedXLogRecord decoded;
		char pad[sizeof(DecodedXLogRecord) + 2 * sizeof(DecodedBkpBlock)];
	} u;
} FakeXactRecord;

static XLogReaderState *
make_commit(FakeXactRecord *fake, TransactionId xid, SCN scn,
			TimestampTz timestamp, bool conflicting_tt)
{
	xl_xact_commit commit;
	xl_xact_xinfo xinfo;
	xl_xact_scn wal_scn;
	xl_xact_tt_commit tt;
	uint8 *cursor;

	memset(fake, 0, sizeof(*fake));
	memset(&commit, 0, sizeof(commit));
	memset(&xinfo, 0, sizeof(xinfo));
	memset(&wal_scn, 0, sizeof(wal_scn));
	memset(&tt, 0, sizeof(tt));
	commit.xact_time = timestamp;
	xinfo.xinfo = XACT_XINFO_HAS_SCN | XACT_XINFO_HAS_TT_COMMIT;
	wal_scn.scn = scn;
	tt.instance = 3;
	tt.segment_id = 9;
	tt.slot_offset = 4;
	tt.wrap = 7;
	tt.xid = conflicting_tt ? xid + 1 : xid;
	tt.commit_scn = scn;
	cursor = fake->data;
	memcpy(cursor, &commit, sizeof(commit));
	cursor += sizeof(commit);
	memcpy(cursor, &xinfo, sizeof(xinfo));
	cursor += sizeof(xinfo);
	memcpy(cursor, &wal_scn, sizeof(wal_scn));
	cursor += sizeof(wal_scn);
	memcpy(cursor, &tt, sizeof(tt));
	cursor += sizeof(tt);
	fake->u.decoded.header.xl_rmid = RM_XACT_ID;
	fake->u.decoded.header.xl_info = XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO;
	fake->u.decoded.header.xl_xid = xid;
	fake->u.decoded.main_data = (char *) fake->data;
	fake->u.decoded.main_data_len = (uint32) (cursor - fake->data);
	fake->reader.record = &fake->u.decoded;
	return &fake->reader;
}

static XLogReaderState *
make_prepare(FakeXactRecord *fake, TransactionId xid, bool with_tt,
			 bool truncated_gid)
{
	typedef struct FakeTwoPhaseRecordOnDisk
	{
		uint32 len;
		TwoPhaseRmgrId rmid;
		uint16 info;
	} FakeTwoPhaseRecordOnDisk;
	xl_xact_prepare prepare;
	ClusterTT2PCBinding binding;
	FakeTwoPhaseRecordOnDisk disk_record;
	Size		header_size = MAXALIGN(sizeof(prepare));
	Size		payload_size;
	uint32		tt_size;
	char	   *cursor;

	memset(fake, 0, sizeof(*fake));
	memset(&prepare, 0, sizeof(prepare));
	memset(&binding, 0, sizeof(binding));
	memset(&disk_record, 0, sizeof(disk_record));
	prepare.magic = UINT32_C(0x57F94534);
	prepare.xid = xid;
	prepare.database = 16384;
	prepare.prepared_at = INT64_C(123456);
	prepare.gidlen = 4;
	cursor = (char *) fake->data + header_size;
	memcpy(cursor, "gid", 4);
	cursor += MAXALIGN(4);
	if (with_tt)
	{
		binding.undo_segment_id = 513;
		binding.slot_offset = 4;
		binding.wrap = 7;
		binding.cluster_epoch = 11;
		binding.xid = xid;
		tt_size = cluster_tt_2pc_record_size(CLUSTER_TT_2PC_VERSION, 1, 0);
		disk_record.len = tt_size;
		disk_record.rmid = TWOPHASE_RM_CLUSTER_TT_ID;
		memcpy(cursor, &disk_record, sizeof(disk_record));
		cursor += MAXALIGN(sizeof(disk_record));
		UT_ASSERT_EQ(cluster_tt_2pc_serialize(&binding, NULL, 1, NULL, 0,
			cursor, tt_size), tt_size);
		cursor += MAXALIGN(tt_size);
	}
	memset(&disk_record, 0, sizeof(disk_record));
	disk_record.rmid = TWOPHASE_RM_END_ID;
	memcpy(cursor, &disk_record, sizeof(disk_record));
	cursor += MAXALIGN(sizeof(disk_record));
	payload_size = (Size) (cursor - (char *) fake->data);
	prepare.total_len = (uint32) payload_size + sizeof(uint32);
	memcpy(fake->data, &prepare, sizeof(prepare));
	fake->u.decoded.header.xl_rmid = RM_XACT_ID;
	fake->u.decoded.header.xl_info = XLOG_XACT_PREPARE;
	fake->u.decoded.main_data = (char *) fake->data;
	fake->u.decoded.main_data_len = truncated_gid ?
		(uint32) header_size : (uint32) payload_size;
	fake->reader.record = &fake->u.decoded;
	return &fake->reader;
}

static RfPageOnlineRecordIdentityV1
make_identity(FakeXactRecord *fake, uint8 storage_uuid[16])
{
	RfPageOnlineRecordIdentityV1 identity;

	memset(&identity, 0, sizeof(identity));
	identity.record.system_identifier = UINT64_C(0x11223344);
	memcpy(identity.record.storage_uuid, storage_uuid, 16);
	identity.record.origin_thread = 3;
	identity.record.timeline_id = 7;
	identity.record.read_rec_ptr = UINT64_C(100);
	identity.record.end_rec_ptr = UINT64_C(200);
	identity.record.record_crc = UINT32_C(0xabc123);
	identity.record.rmid = fake->u.decoded.header.xl_rmid;
	identity.record.info = fake->u.decoded.header.xl_info;
	fake->reader.system_identifier = identity.record.system_identifier;
	fake->reader.ReadRecPtr = identity.record.read_rec_ptr;
	fake->reader.EndRecPtr = identity.record.end_rec_ptr;
	fake->u.decoded.lsn = identity.record.read_rec_ptr;
	fake->u.decoded.next_lsn = identity.record.end_rec_ptr;
	fake->u.decoded.header.xl_crc = identity.record.record_crc;
	return identity;
}

static XLogReaderState *
make_undo_delta(FakeXactRecord *fake)
{
	xl_undo_block_write undo;
	uint32 body_len = UNDO_BLOCK_HDR_PREFIX_LEN + 24 +
		sizeof(UndoSlotDirEntry);

	memset(fake, 0, sizeof(*fake));
	memset(&undo, 0, sizeof(undo));
	undo.instance = 3;
	undo.segment_id = 513;
	undo.block_no = 9;
	undo.rec_off = sizeof(UndoBlockHeader);
	undo.rec_len = 24;
	undo.slot_off = BLCKSZ - sizeof(UndoSlotDirEntry);
	memcpy(fake->data, &undo, sizeof(undo));
	memset(fake->data + sizeof(undo), 0x6b, body_len);
	fake->u.decoded.header.xl_rmid = RM_CLUSTER_UNDO_ID;
	fake->u.decoded.header.xl_info = XLOG_UNDO_BLOCK_WRITE;
	fake->u.decoded.main_data = (char *) fake->data;
	fake->u.decoded.main_data_len = sizeof(undo) + body_len;
	fake->reader.record = &fake->u.decoded;
	return &fake->reader;
}

static RfDetachedRecordPlanV1
make_undo_record_plan(FakeXactRecord *fake)
{
	RfDetachedRecordPlanV1 record_plan;

	memset(&record_plan, 0, sizeof(record_plan));
	record_plan.source_record = &fake->reader;
	record_plan.route.rmid = RM_CLUSTER_UNDO_ID;
	record_plan.route.normalized_info = XLOG_UNDO_BLOCK_WRITE;
	record_plan.route.record_owner = RF_ROUTE_OWNER_SIDE_TYPED;
	record_plan.route.block_policy = RF_ROUTE_BLOCKS_FORBIDDEN;
	record_plan.route.codec_id = RF_ROUTE_CODEC_SIDE_CLUSTER_UNDO;
	record_plan.preflight_complete = true;
	return record_plan;
}

static RfDetachedRecordPlanV1
make_record_plan(FakeXactRecord *fake)
{
	RfDetachedRecordPlanV1 record_plan;

	memset(&record_plan, 0, sizeof(record_plan));
	record_plan.source_record = &fake->reader;
	record_plan.route.rmid = RM_XACT_ID;
	record_plan.route.normalized_info = XLOG_XACT_COMMIT;
	record_plan.route.legal_info_flags = XLOG_XACT_HAS_INFO;
	record_plan.route.record_owner = RF_ROUTE_OWNER_SIDE_TYPED;
	record_plan.route.block_policy = RF_ROUTE_BLOCKS_FORBIDDEN;
	record_plan.route.codec_id = RF_ROUTE_CODEC_SIDE_STANDARD;
	record_plan.preflight_complete = true;
	return record_plan;
}

typedef struct ApplyCapture
{
	uint32 count;
	uint32 undo_count;
	TransactionId xid;
	SCN scn;
	uint8 undo_first_byte;
} ApplyCapture;

static bool
capture_apply(void *arg, const RfSideOnlineOperationV1 *operation)
{
	ApplyCapture *capture = (ApplyCapture *) arg;

	capture->count++;
	capture->xid = operation->xact.xid;
	capture->scn = operation->xact.terminal_scn;
	return true;
}

static bool
capture_apply_undo(void *arg, const RfSideOnlineOperationV1 *operation)
{
	ApplyCapture *capture = (ApplyCapture *) arg;

	capture->undo_count++;
	capture->undo_first_byte = operation->owned_payload[0];
	return operation->kind == RF_SIDE_ONLINE_OPERATION_UNDO &&
		operation->owned_payload_length > 0;
}

UT_TEST(test_commit_decodes_to_immutable_truth_operation)
{
	FakeXactRecord fake;
	RfSideXactOperationV1 operation;

	UT_ASSERT(rf_side_xact_decode_v1(
		make_commit(&fake, 800, UINT64_C(901), INT64_C(123456), false),
		UINT64_C(0x11223344), 3, &operation));
	UT_ASSERT_EQ(operation.kind, RF_SIDE_XACT_COMMIT);
	UT_ASSERT_EQ(operation.origin_thread, 3);
	UT_ASSERT_EQ(operation.xid, 800);
	UT_ASSERT_EQ(operation.terminal_scn, UINT64_C(901));
	UT_ASSERT_EQ(operation.terminal_timestamp, INT64_C(123456));
	UT_ASSERT(operation.has_tt_delta);
	UT_ASSERT_EQ(operation.tt_delta.xid, 800);
	UT_ASSERT_EQ(operation.tt_delta.commit_scn, UINT64_C(901));
	UT_ASSERT(rf_side_xact_structural_preflight_v1(&operation));
}

UT_TEST(test_commit_tt_conflict_is_blocked_before_apply)
{
	FakeXactRecord fake;
	RfSideXactOperationV1 operation;

	UT_ASSERT(!rf_side_xact_decode_v1(
		make_commit(&fake, 800, UINT64_C(901), INT64_C(123456), true),
		UINT64_C(0x11223344), 3, &operation));
	UT_ASSERT_EQ(operation.kind, RF_SIDE_XACT_INVALID);
}

UT_TEST(test_non_xact_and_missing_tt_are_blocked)
{
	FakeXactRecord fake;
	RfSideXactOperationV1 operation;
	XLogReaderState *record = make_commit(
		&fake, 800, UINT64_C(901), INT64_C(123456), false);

	fake.u.decoded.header.xl_rmid = RM_HEAP_ID;
	UT_ASSERT(!rf_side_xact_decode_v1(record, UINT64_C(0x11223344), 3,
		&operation));
	record = make_commit(&fake, 800, UINT64_C(901), INT64_C(123456), false);
	fake.u.decoded.main_data_len -= sizeof(xl_xact_tt_commit);
	UT_ASSERT(!rf_side_xact_decode_v1(record, UINT64_C(0x11223344), 3,
		&operation));
	UT_ASSERT(!rf_side_xact_structural_preflight_v1(NULL));
}

UT_TEST(test_prepare_requires_bounded_aligned_gid)
{
	FakeXactRecord fake;
	RfSideXactOperationV1 operation;

	UT_ASSERT(rf_side_xact_decode_v1(
		make_prepare(&fake, 801, true, false), UINT64_C(0x11223344), 3,
		&operation));
	UT_ASSERT_EQ(operation.kind, RF_SIDE_XACT_PREPARE);
	UT_ASSERT_EQ(operation.xid, 801);
	UT_ASSERT_EQ(operation.prepared_binding_count, 1);
	UT_ASSERT_EQ(operation.prepared_bindings[0].undo_segment_id, 513);
	UT_ASSERT_EQ(operation.prepared_bindings[0].slot_offset, 4);
	UT_ASSERT_EQ(operation.prepared_bindings[0].wrap, 7);
	UT_ASSERT_EQ(operation.prepared_bindings[0].xid, 801);
	UT_ASSERT(!rf_side_xact_decode_v1(
		make_prepare(&fake, 801, false, false), UINT64_C(0x11223344), 3,
		&operation));
	UT_ASSERT(!rf_side_xact_decode_v1(
		make_prepare(&fake, 801, true, true), UINT64_C(0x11223344), 3,
		&operation));
	UT_ASSERT_EQ(operation.kind, RF_SIDE_XACT_INVALID);
}

UT_TEST(test_online_plan_owns_decoded_operation_not_raw_record)
{
	FakeXactRecord fake;
	RfDetachedRecordPlanV1 record_plan;
	RfPageOnlineRecordIdentityV1 identity;
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	RfSideOnlineOperationV1 operation;
	RfSideOnlineApplyOpsV1 apply_ops;
	ApplyCapture capture;
	uint8 storage_uuid[16];

	memset(storage_uuid, 0x42, sizeof(storage_uuid));
	(void) make_commit(&fake, 800, UINT64_C(901), INT64_C(123456), false);
	identity = make_identity(&fake, storage_uuid);
	record_plan = make_record_plan(&fake);
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 200;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = identity.record.system_identifier;
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan),
		RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(
		plan, &record_plan, &identity), RF_PAGE_PROOF_DETAIL_OK);
	memset(fake.data, 0xee, sizeof(fake.data));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan),
		RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 1);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
	UT_ASSERT_EQ(operation.xact.xid, 800);
	UT_ASSERT_EQ(operation.xact.terminal_scn, UINT64_C(901));
	memset(&capture, 0, sizeof(capture));
	memset(&apply_ops, 0, sizeof(apply_ops));
	apply_ops.arg = &capture;
	apply_ops.apply_xact = capture_apply;
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &apply_ops),
		RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(capture.count, 1);
	UT_ASSERT_EQ(capture.xid, 800);
	UT_ASSERT_EQ(capture.scn, UINT64_C(901));
	rf_side_online_plan_destroy_v1(&plan);
	UT_ASSERT(plan == NULL);
}

UT_TEST(test_online_plan_denies_incomplete_physical_cut)
{
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	uint8 storage_uuid[16];

	memset(storage_uuid, 0x41, sizeof(storage_uuid));
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 200;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = UINT64_C(0x11223344);
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan),
		RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan),
		RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
	UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 0);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_online_plan_owns_undo_payload_not_raw_record)
{
	FakeXactRecord fake;
	RfDetachedRecordPlanV1 record_plan;
	RfPageOnlineRecordIdentityV1 identity;
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	RfSideOnlineOperationV1 operation;
	RfSideOnlineApplyOpsV1 apply_ops;
	ApplyCapture capture;
	uint8 storage_uuid[16];

	memset(storage_uuid, 0x44, sizeof(storage_uuid));
	(void) make_undo_delta(&fake);
	identity = make_identity(&fake, storage_uuid);
	record_plan = make_undo_record_plan(&fake);
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 200;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = identity.record.system_identifier;
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan),
		RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(
		plan, &record_plan, &identity), RF_PAGE_PROOF_DETAIL_OK);
	memset(fake.data, 0xee, sizeof(fake.data));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan),
		RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
	UT_ASSERT_EQ(operation.kind, RF_SIDE_ONLINE_OPERATION_UNDO);
	UT_ASSERT_EQ((int) operation.undo.kind,
		(int) CLUSTER_UNDO_KIND_BLOCK_WRITE);
	UT_ASSERT_EQ(operation.owned_payload_length,
		UNDO_BLOCK_HDR_PREFIX_LEN + 24 + sizeof(UndoSlotDirEntry));
	UT_ASSERT_EQ(operation.owned_payload[0], 0x6b);
	memset(&capture, 0, sizeof(capture));
	memset(&apply_ops, 0, sizeof(apply_ops));
	apply_ops.arg = &capture;
	apply_ops.apply_xact = capture_apply;
	apply_ops.apply_undo = capture_apply_undo;
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &apply_ops),
		RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(capture.undo_count, 1);
	UT_ASSERT_EQ(capture.undo_first_byte, 0x6b);
	rf_side_online_plan_destroy_v1(&plan);
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_commit_decodes_to_immutable_truth_operation);
	UT_RUN(test_commit_tt_conflict_is_blocked_before_apply);
	UT_RUN(test_non_xact_and_missing_tt_are_blocked);
	UT_RUN(test_prepare_requires_bounded_aligned_gid);
	UT_RUN(test_online_plan_owns_decoded_operation_not_raw_record);
	UT_RUN(test_online_plan_denies_incomplete_physical_cut);
	UT_RUN(test_online_plan_owns_undo_payload_not_raw_record);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
