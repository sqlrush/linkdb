/*-------------------------------------------------------------------------
 *
 * test_cluster_space_metadata.c
 *	  Golden behavior vectors for the JIT Task3 strong interfaces.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_space_metadata.c
 *
 * NOTES
 *	  This is a pgrac-original test of Task3 value and transition behavior.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <stdlib.h>
#include <string.h>

#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "cluster/cluster_space.h"
#include "storage/relfilelocator.h"

static int	control_count = 0;
static int	control_failures = 0;
static int	semantic_failures = 0;

typedef union RecordFixture
{
	XLogRecord	align;
	uint8		bytes[512];
}			RecordFixture;

static void
store_u16(uint8 *dst, uint16 value)
{
	memcpy(dst, &value, sizeof(value));
}

static void
store_u64(uint8 *dst, uint64 value)
{
	memcpy(dst, &value, sizeof(value));
}

static void
control(bool condition, const char *name)
{
	control_count++;
	if (condition)
		printf("JIT_CONTROL:%s:PASS\n", name);
	else
	{
		printf("JIT_CONTROL:%s:FAIL\n", name);
		control_failures++;
	}
}

static void
semantic(bool condition, const char *name)
{
	if (!condition)
	{
		printf("JIT_SEMANTIC_RED:%s\n", name);
		semantic_failures++;
	}
}

static RfPageVersionEdgeEntryV1
make_absent_entry(uint8 block_id, uint16 component_ordinal)
{
	RfPageVersionEdgeEntryV1 entry;

	memset(&entry, 0, sizeof(entry));
	entry.block_id = block_id;
	entry.page_class = RF_PAGE_CLASS_ORDINARY;
	entry.before_kind = RF_PAGE_STATE_ABSENT;
	entry.result_kind = RF_PAGE_STATE_PRESENT;
	entry.edge_flags = RF_PAGE_EDGE_WILL_INIT |
		RF_PAGE_EDGE_FULL_COVERAGE;
	entry.component_ordinal = component_ordinal;
	entry.result_incarnation[0] = 0x80;
	entry.result_incarnation[15] = (uint8) (block_id + 1);
	return entry;
}

static bool
encode_unchanged(uint64 result_token,
				 const RfPageVersionEdgeEntryV1 * entries,
				 uint8 entry_count, Size capacity)
{
	uint8		output[XLR_PAGE_VERSION_EDGE_MAX_SIZE];
	uint8		before[sizeof(output)];
	Size		output_size = 37;
	bool		result;

	memset(output, 0xa5, sizeof(output));
	memcpy(before, output, sizeof(output));
	result = XLogEncodePageVersionEdgeV1(output, capacity, result_token,
										 entries, entry_count, &output_size);
	return !result && output_size == 37 &&
		memcmp(output, before, sizeof(output)) == 0;
}

static void
test_page_version_equality(void)
{
	RfPageVersionV1 left;
	RfPageVersionV1 right;

	memset(&left, 0, sizeof(left));
	for (int i = 0; i < 16; i++)
		left.segment_incarnation[i] = (uint8) (i + 1);
	left.mutation_token = UINT64CONST(0x1112131415161718);
	right = left;

	semantic(rf_page_version_equal_v1(&left, &right),
			 "T3-B-PAGEVERSION-EQUALITY-BEHAVIOR");

	for (int i = 0; i < 16; i++)
	{
		right = left;
		right.segment_incarnation[i] ^= 0x40;
		control(!rf_page_version_equal_v1(&left, &right),
				"T3-B-PAGEVERSION-PER-BYTE-INEQUALITY");
	}
	right = left;
	right.mutation_token++;
	control(!rf_page_version_equal_v1(&left, &right),
			"T3-B-PAGEVERSION-TOKEN-INEQUALITY");
	control(!rf_page_version_equal_v1(NULL, &right) &&
			!rf_page_version_equal_v1(&left, NULL),
			"T3-B-PAGEVERSION-NULL-FAIL-CLOSED");
}

static void
test_edge_encoder_golden(void)
{
	RfPageVersionEdgeEntryV1 entry = make_absent_entry(0, 0);
	uint8		output[64];
	uint8		expected[64];
	Size		output_size = 0;
	uint64		result_token = UINT64CONST(0x2122232425262728);
	bool		result;

	memset(output, 0xcc, sizeof(output));
	memset(expected, 0, sizeof(expected));
	expected[0] = XLR_BLOCK_ID_PAGE_VERSION_EDGE;
	expected[1] = XLR_PAGE_VERSION_EDGE_FORMAT_V1;
	expected[2] = 1;
	expected[3] = XLR_PAGE_VERSION_EDGE_ENTRY_SIZE;
	store_u16(expected + 4, 0);
	store_u16(expected + 6, 0);
	store_u64(expected + 8, result_token);
	expected[16] = entry.block_id;
	expected[17] = entry.page_class;
	expected[18] = entry.before_kind;
	expected[19] = entry.result_kind;
	store_u16(expected + 20, entry.edge_flags);
	store_u16(expected + 22, entry.component_ordinal);
	memcpy(expected + 24, entry.before.segment_incarnation, 16);
	store_u64(expected + 40, entry.before.mutation_token);
	memcpy(expected + 48, entry.result_incarnation, 16);

	result = XLogEncodePageVersionEdgeV1(output, sizeof(output), result_token,
										 &entry, 1, &output_size);
	semantic(result && output_size == sizeof(expected) &&
			 memcmp(output, expected, sizeof(expected)) == 0,
			 "T3-B-EDGE-ENCODER-BEHAVIOR");
}

static void
test_edge_encoder_decoder_roundtrip(void)
{
	RecordFixture fixture;
	XLogRecord *record = &fixture.align;
	RfPageVersionEdgeEntryV1 entry = make_absent_entry(0, 0);
	RelFileLocator locator = {0};
	BlockNumber block_number = 42;
	XLogReaderState state;
	DecodedXLogRecord *decoded = NULL;
	DecodedRfPageVersionEdgeEntryV1 *decoded_entry;
	char		error[1000] = {0};
	char	   *errormsg = NULL;
	uint8	   *ptr;
	Size		edge_size = 0;
	size_t		decoded_size;
	uint64		result_token = UINT64CONST(0x3132333435363738);
	bool		encoded;
	bool		decoded_ok = false;
	bool		matches = false;

	memset(&fixture, 0, sizeof(fixture));
	ptr = fixture.bytes + SizeOfXLogRecord;
	encoded = XLogEncodePageVersionEdgeV1(ptr,
										  sizeof(fixture.bytes) - SizeOfXLogRecord,
										  result_token, &entry, 1, &edge_size);
	if (encoded)
	{
		ptr += edge_size;
		*ptr++ = entry.block_id;
		*ptr++ = BKPBLOCK_WILL_INIT | MAIN_FORKNUM;
		store_u16(ptr, 0);
		ptr += sizeof(uint16);
		memcpy(ptr, &locator, sizeof(locator));
		ptr += sizeof(locator);
		memcpy(ptr, &block_number, sizeof(block_number));
		ptr += sizeof(block_number);
		record->xl_tot_len = (uint32) (ptr - fixture.bytes);

		memset(&state, 0, sizeof(state));
		state.ReadRecPtr = UINT64CONST(0x1000);
		state.errormsg_buf = error;
		decoded_size = DecodeXLogRecordRequiredSpace(record->xl_tot_len);
		decoded = (DecodedXLogRecord *) calloc(1, decoded_size);
		if (decoded != NULL)
			decoded_ok = DecodeXLogRecord(&state, decoded, record,
										  UINT64CONST(0x1000), &errormsg);
	}

	if (decoded_ok && decoded->page_version_edge_count == 1 &&
		decoded->page_version_edge_entries != NULL)
	{
		decoded_entry = &decoded->page_version_edge_entries[0];
		matches = decoded->page_version_edge_result_token == result_token &&
			decoded_entry->block_id == entry.block_id &&
			decoded_entry->page_class == entry.page_class &&
			decoded_entry->before_kind == entry.before_kind &&
			decoded_entry->result_kind == entry.result_kind &&
			decoded_entry->edge_flags == entry.edge_flags &&
			decoded_entry->component_ordinal == entry.component_ordinal &&
			memcmp(&decoded_entry->before, &entry.before,
				   sizeof(entry.before)) == 0 &&
			memcmp(decoded_entry->result_incarnation,
				   entry.result_incarnation,
				   sizeof(entry.result_incarnation)) == 0 &&
			decoded->max_block_id == entry.block_id &&
			decoded->blocks[entry.block_id].in_use &&
			decoded->blocks[entry.block_id].forknum == MAIN_FORKNUM &&
			decoded->blocks[entry.block_id].blkno == block_number;
	}
	control(encoded && decoded_ok && matches,
			"T3-B-ENCODE-DECODE-TRANSITION-ROUNDTRIP");
	free(decoded);
}

static void
test_edge_encoder_maximum(void)
{
	RfPageVersionEdgeEntryV1 entries[XLR_PAGE_VERSION_EDGE_MAX_ENTRIES];
	uint8		output[XLR_PAGE_VERSION_EDGE_MAX_SIZE];
	Size		output_size = 0;
	bool		result;

	for (int i = 0; i < XLR_PAGE_VERSION_EDGE_MAX_ENTRIES; i++)
		entries[i] = make_absent_entry((uint8) i, (uint16) i);
	memset(output, 0, sizeof(output));
	result = XLogEncodePageVersionEdgeV1(output, sizeof(output), 29,
										 entries, XLR_PAGE_VERSION_EDGE_MAX_ENTRIES,
										 &output_size);
	semantic(result && output_size == XLR_PAGE_VERSION_EDGE_MAX_SIZE &&
			 output[0] == XLR_BLOCK_ID_PAGE_VERSION_EDGE &&
			 output[2] == XLR_PAGE_VERSION_EDGE_MAX_ENTRIES &&
			 output[XLR_PAGE_VERSION_EDGE_HEADER_SIZE +
					(XLR_PAGE_VERSION_EDGE_MAX_ENTRIES - 1) *
					XLR_PAGE_VERSION_EDGE_ENTRY_SIZE] == XLR_MAX_BLOCK_ID,
			 "T3-B-EDGE-ENCODER-MAX-BEHAVIOR");
}

static void
test_edge_encoder_invalid_zero_mutation(void)
{
	RfPageVersionEdgeEntryV1 entries[2];
	uint8		output[64];
	uint8		before[sizeof(output)];
	Size		output_size;

	entries[0] = make_absent_entry(0, 0);
	entries[1] = make_absent_entry(1, 1);
	control(encode_unchanged(0, entries, 1, 64),
			"T3-B-ENCODER-ZERO-TOKEN-NO-MUTATION");
	control(encode_unchanged(31, NULL, 1, 64),
			"T3-B-ENCODER-NULL-ENTRIES-NO-MUTATION");
	control(encode_unchanged(31, entries, 0, 64),
			"T3-B-ENCODER-ZERO-COUNT-NO-MUTATION");
	control(encode_unchanged(31, entries,
							 XLR_PAGE_VERSION_EDGE_MAX_ENTRIES + 1, 64),
			"T3-B-ENCODER-OVERMAX-COUNT-NO-MUTATION");
	control(encode_unchanged(31, entries, 1, 63),
			"T3-B-ENCODER-CAPACITY-NO-MUTATION");

	entries[1].block_id = 0;
	control(encode_unchanged(31, entries, 2, 112),
			"T3-B-ENCODER-DUPLICATE-ID-NO-MUTATION");
	entries[0] = make_absent_entry(1, 0);
	entries[1] = make_absent_entry(0, 1);
	control(encode_unchanged(31, entries, 2, 112),
			"T3-B-ENCODER-ORDER-NO-MUTATION");

	entries[0] = make_absent_entry(0, 0);
	entries[0].edge_flags = RF_PAGE_EDGE_FULL_COVERAGE;
	control(encode_unchanged(31, entries, 1, 64),
			"T3-B-ENCODER-TRANSITION-NO-MUTATION");

	memset(output, 0xa5, sizeof(output));
	memcpy(before, output, sizeof(output));
	output_size = 41;
	control(!XLogEncodePageVersionEdgeV1(NULL, sizeof(output), 31,
										 entries, 1, &output_size) &&
			output_size == 41 && memcmp(output, before, sizeof(output)) == 0,
			"T3-B-ENCODER-NULL-OUTPUT-NO-MUTATION");
	control(!XLogEncodePageVersionEdgeV1(output, sizeof(output), 31,
										 entries, 1, NULL) &&
			memcmp(output, before, sizeof(output)) == 0,
			"T3-B-ENCODER-NULL-SIZE-NO-MUTATION");
}

static void
test_space_identity_equality(void)
{
	ClusterSpaceIdentityV1 left;
	ClusterSpaceIdentityV1 right;

	memset(&left, 0, sizeof(left));
	left.system_identifier = 1;
	left.spc_oid = 2;
	left.db_oid = 3;
	left.rel_number = 4;
	left.target_fork = 0;
	for (int i = 0; i < 16; i++)
		left.space_incarnation[i] = (uint8) (0x70 + i);
	right = left;

	semantic(cluster_space_identity_equal(&left, &right),
			 "T3-B-SPACE-IDENTITY-EQUALITY-BEHAVIOR");
	right = left;
	right.system_identifier++;
	control(!cluster_space_identity_equal(&left, &right),
			"T3-B-SPACE-SYSTEM-INEQUALITY");
	right = left;
	right.spc_oid++;
	control(!cluster_space_identity_equal(&left, &right),
			"T3-B-SPACE-SPC-INEQUALITY");
	right = left;
	right.db_oid++;
	control(!cluster_space_identity_equal(&left, &right),
			"T3-B-SPACE-DB-INEQUALITY");
	right = left;
	right.rel_number++;
	control(!cluster_space_identity_equal(&left, &right),
			"T3-B-SPACE-REL-INEQUALITY");
	right = left;
	right.target_fork++;
	control(!cluster_space_identity_equal(&left, &right),
			"T3-B-SPACE-FORK-INEQUALITY");
	for (int i = 0; i < 16; i++)
	{
		right = left;
		right.space_incarnation[i] ^= 0x20;
		control(!cluster_space_identity_equal(&left, &right),
				"T3-B-SPACE-PER-BYTE-INEQUALITY");
	}
	control(!cluster_space_identity_equal(NULL, &right) &&
			!cluster_space_identity_equal(&left, NULL),
			"T3-B-SPACE-NULL-FAIL-CLOSED");
}

static void
test_space_layout_copy_controls(void)
{
	ClusterSpaceReservationV1 source;
	ClusterSpaceReservationV1 copy;

	memset(&source, 0x5a, sizeof(source));
	memcpy(&copy, &source, sizeof(copy));
	control(sizeof(ClusterSpaceLocatorV1) == 24 &&
			sizeof(ClusterSpaceIdentityV1) == 40 &&
			sizeof(ClusterSpaceReservationV1) == 96,
			"T3-B-SPACE-LAYOUT-SIZES");
	control(memcmp(&source, &copy, sizeof(source)) == 0,
			"T3-B-SPACE-RESERVATION-UNTOUCHED-COPY");
	control(CLUSTER_SPACE_OK == 0 && CLUSTER_SPACE_ROOT_STALE == 4 &&
			CLUSTER_SPACE_DUTY_TAIL_PENDING == 5 &&
			CLUSTER_SPACE_DUTY_STALE == 6 &&
			CLUSTER_SPACE_INTERNAL == 24,
			"T3-B-SPACE-CLOSED-RESULT-VALUES");
}

int
main(void)
{
	printf("JIT_CONTROL:T3-B-REAL-STRONG-INTERFACES:PASS\n");
	test_page_version_equality();
	test_edge_encoder_golden();
	test_edge_encoder_decoder_roundtrip();
	test_edge_encoder_maximum();
	test_edge_encoder_invalid_zero_mutation();
	test_space_identity_equality();
	test_space_layout_copy_controls();
	printf("JIT_SUMMARY:controls=%d control_failures=%d semantic_failures=%d\n",
		   control_count, control_failures, semantic_failures);

	return control_failures == 0 && semantic_failures == 0 ? 0 : 1;
}
