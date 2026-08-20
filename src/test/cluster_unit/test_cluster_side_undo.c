/*-------------------------------------------------------------------------
 *
 * test_cluster_side_undo.c
 *    RF-SIDE D-SIDE-02 focused unit tests: the TT/undo decode primitive
 *    (pure parse, malformed -> BLOCKED) and the preflight gate
 *    (route + field integrity -> zero mutation on any false).
 *
 *    RED mapping (spec §5.1 U-SIDE-04/05 + §2.1):
 *      - decode extracts the exact parsed fields per opcode; apply
 *        never re-reads the raw record (the parsed struct is the only
 *        apply input);
 *      - malformed length / unknown info -> decode false (BLOCKED,
 *        pre-mutation);
 *      - preflight: route must be TT_UNDO (XLOG_HW_RESERVE stays
 *        BLOCKED under STOP-RF-SIDE-SPACE-ABI); TT slot bounds and a
 *        valid xid are required (U-SIDE-05 one-at-a-time facts).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/rmgr.h"
#include "access/xlogrecord.h"
#include "cluster/cluster_side_undo.h"
#include "cluster/cluster_tt_slot.h"
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

/* ----------
 * Record fabrication: a minimal XLogReaderState wrapping a
 * DecodedXLogRecord whose main_data points at an external payload.
 * ---------- */
typedef struct FakeRecord {
	XLogReaderState st;
	union {
		DecodedXLogRecord dec;
		/* cppcheck-suppress unusedStructMember */
		char pad[sizeof(DecodedXLogRecord) + 2 * sizeof(DecodedBkpBlock)];
	} u;
	uint8		data[512];
} FakeRecord;

static XLogReaderState *
make_record(FakeRecord *fr, RmgrId rmid, uint8 info, const void *payload,
			uint32 payload_len)
{
	DecodedXLogRecord *dec = &fr->u.dec;

	memset(fr, 0, sizeof(*fr));
	dec->header.xl_rmid = rmid;
	dec->header.xl_info = info;
	if (payload != NULL && payload_len > 0)
	{
		memcpy(fr->data, payload, payload_len);
		dec->main_data = fr->data;
		dec->main_data_len = payload_len;
	}
	fr->st.record = dec;
	return &fr->st;
}

UT_TEST(test_decode_tt_commit_fields)
{
	FakeRecord fr;
	xl_undo_tt_slot_commit payload;
	ClusterUndoDecoded out;
	XLogReaderState *rec;

	memset(&payload, 0, sizeof(payload));
	payload.instance = 1;
	payload.segment_id = 7;
	payload.slot_offset = 3;
	payload.wrap = 2;
	payload.xid = 1234;
	payload.commit_scn = UINT64_C(0x123456789);

	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_COMMIT,
					  &payload, sizeof(payload));
	UT_ASSERT(cluster_undo_decode(rec, &out));
	UT_ASSERT_EQ((int) out.kind, (int) CLUSTER_UNDO_KIND_TT_COMMIT);
	UT_ASSERT_EQ((unsigned) out.instance, 1U);
	UT_ASSERT_EQ((unsigned) out.segment_id, 7U);
	UT_ASSERT_EQ((unsigned) out.slot_offset, 3U);
	UT_ASSERT_EQ((unsigned) out.wrap, 2U);
	UT_ASSERT_EQ((unsigned) out.xid, 1234U);
	UT_ASSERT_EQ((unsigned long long) out.commit_scn,
				 (unsigned long long) UINT64_C(0x123456789));

	/* preflight passes for a valid TT commit. */
	UT_ASSERT(cluster_undo_preflight(&out));
}

UT_TEST(test_decode_malformed_and_unknown_blocked)
{
	FakeRecord fr;
	xl_undo_tt_slot_commit payload;
	ClusterUndoDecoded out;
	XLogReaderState *rec;

	/* Wrong rmgr. */
	memset(&payload, 0, sizeof(payload));
	rec = make_record(&fr, RM_HEAP_ID, XLOG_UNDO_TT_SLOT_COMMIT,
					  &payload, sizeof(payload));
	UT_ASSERT(!cluster_undo_decode(rec, &out));

	/* Unknown info byte. */
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, 0x05, &payload, sizeof(payload));
	UT_ASSERT(!cluster_undo_decode(rec, &out));

	/* Malformed length (too short) — U-SIDE-04: BLOCKED pre-mutation. */
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_COMMIT,
					  &payload, sizeof(payload) - 1);
	UT_ASSERT(!cluster_undo_decode(rec, &out));

	/* Malformed length (too long). */
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_COMMIT,
					  &payload, sizeof(payload) + 1);
	UT_ASSERT(!cluster_undo_decode(rec, &out));

	/* NULL inputs. */
	UT_ASSERT(!cluster_undo_decode(NULL, &out));
	UT_ASSERT(!cluster_undo_decode(rec, NULL));
}

UT_TEST(test_preflight_field_integrity_and_route)
{
	FakeRecord fr;
	xl_undo_tt_slot_commit payload;
	xl_hw_reserve hw;
	ClusterUndoDecoded out;
	XLogReaderState *rec;

	memset(&payload, 0, sizeof(payload));
	payload.instance = 1;
	payload.segment_id = 7;
	payload.slot_offset = 3;
	payload.wrap = 2;
	payload.xid = 1234;
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_COMMIT,
					  &payload, sizeof(payload));
	UT_ASSERT(cluster_undo_decode(rec, &out));
	UT_ASSERT(cluster_undo_preflight(&out));

	/* U-SIDE-05 one-at-a-time: slot offset out of range -> BLOCKED. */
	out.slot_offset = TT_SLOTS_PER_SEGMENT;
	UT_ASSERT(!cluster_undo_preflight(&out));
	out.slot_offset = 3;

	/* Invalid xid -> BLOCKED. */
	out.xid = InvalidTransactionId;
	UT_ASSERT(!cluster_undo_preflight(&out));
	out.xid = 1234;

	/* NULL / unknown -> BLOCKED. */
	UT_ASSERT(!cluster_undo_preflight(NULL));
	out.kind = CLUSTER_UNDO_KIND_UNKNOWN;
	UT_ASSERT(!cluster_undo_preflight(&out));
	out.kind = CLUSTER_UNDO_KIND_TT_COMMIT;

	/* XLOG_HW_RESERVE decodes but stays BLOCKED (STOP-RF-SIDE-SPACE-ABI). */
	memset(&hw, 0, sizeof(hw));
	hw.new_hwm = 42;
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_HW_RESERVE,
					  &hw, sizeof(hw));
	UT_ASSERT(cluster_undo_decode(rec, &out));
	UT_ASSERT_EQ((int) out.kind, (int) CLUSTER_UNDO_KIND_HW_RESERVE);
	UT_ASSERT(!cluster_undo_preflight(&out));
}

UT_TEST(test_decode_block_write_fields)
{
	FakeRecord fr;
	xl_undo_block_write payload;
	ClusterUndoDecoded out;
	XLogReaderState *rec;
	uint8		extra[32];

	memset(&payload, 0, sizeof(payload));
	payload.instance = 2;
	payload.segment_id = 9;
	payload.block_no = 5;
	payload.has_fpi = 1;
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE,
					  &payload, sizeof(payload));
	UT_ASSERT(cluster_undo_decode(rec, &out));
	UT_ASSERT_EQ((int) out.kind, (int) CLUSTER_UNDO_KIND_BLOCK_WRITE);
	UT_ASSERT_EQ((unsigned) out.segment_id, 9U);
	UT_ASSERT_EQ((unsigned) out.block_no, 5U);
	UT_ASSERT(!out.has_payload); /* exactly the fixed header */

	/* With a payload image after the header. */
	memset(extra, 0x5a, sizeof(extra));
	rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE,
					  &extra, sizeof(extra));
	/* payload bytes are the extra buffer; rebuild a real payload */
	{
		uint8		buf[sizeof(payload) + 32];

		memcpy(buf, &payload, sizeof(payload));
		memset(buf + sizeof(payload), 0x5a, 32);
		rec = make_record(&fr, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE,
						  buf, sizeof(buf));
		UT_ASSERT(cluster_undo_decode(rec, &out));
		UT_ASSERT(out.has_payload);
	}
}

int
main(void)
{
	UT_PLAN(4);

	UT_RUN(test_decode_tt_commit_fields);
	UT_RUN(test_decode_malformed_and_unknown_blocked);
	UT_RUN(test_preflight_field_integrity_and_route);
	UT_RUN(test_decode_block_write_fields);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
