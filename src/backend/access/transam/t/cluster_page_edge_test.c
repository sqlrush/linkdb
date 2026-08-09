/*-------------------------------------------------------------------------
 *
 * cluster_page_edge_test.c
 *	  Golden decoder tests for the page-version edge WAL fragment.
 *
 * This binary links PostgreSQL's real frontend xlogreader implementation.
 * The wire builders below only construct fixtures; they do not decode them.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/access/transam/t/cluster_page_edge_test.c
 *
 * NOTES
 *	  This is a pgrac-original test that links the real frontend decoder.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <stdlib.h>
#include <string.h>

#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "storage/relfilelocator.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* Frozen v1 wire values, kept local so the pre-product RED still compiles. */
#define TEST_EDGE_ID 251
#define TEST_EDGE_FORMAT 1
#define TEST_EDGE_HEADER_SIZE 16
#define TEST_EDGE_ENTRY_SIZE 48
#define TEST_EDGE_HEADER_RESULT_TOKEN_OFFSET 8
#define TEST_EDGE_ENTRY_BEFORE_UUID_OFFSET 8
#define TEST_EDGE_ENTRY_BEFORE_TOKEN_OFFSET 24
#define TEST_EDGE_ENTRY_RESULT_UUID_OFFSET 32

#define TEST_PAGE_CLASS_ORDINARY 1
#define TEST_PAGE_STATE_PRESENT 1
#define TEST_PAGE_STATE_UNFORMATTED 2
#define TEST_EDGE_FLAG_WILL_INIT 0x0002
#define TEST_EDGE_FLAG_FULL_COVERAGE 0x0004

typedef struct DecodeFixtureResult
{
	bool		ok;
	char		error[1000];
	DecodedXLogRecord *decoded;
}			DecodeFixtureResult;

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

static size_t
build_edge_record(uint8 *record_bytes, uint8 entry_size, uint16 header_flags,
				  uint8 block_id)
{
	XLogRecord *record = (XLogRecord *) record_bytes;
	RelFileLocator locator = {0};
	BlockNumber block_number = 42;
	uint8	   *ptr;
	uint8		incarnation[16];
	uint64		result_token = UINT64CONST(0x0102030405060708);
	uint16		edge_flags = TEST_EDGE_FLAG_WILL_INIT |
		TEST_EDGE_FLAG_FULL_COVERAGE;

	memset(record_bytes, 0, 256);
	for (int i = 0; i < 16; i++)
		incarnation[i] = (uint8) (0xa0 + i);

	ptr = record_bytes + SizeOfXLogRecord;
	ptr[0] = TEST_EDGE_ID;
	ptr[1] = TEST_EDGE_FORMAT;
	ptr[2] = 1;
	ptr[3] = entry_size;
	store_u16(ptr + 4, header_flags);
	store_u16(ptr + 6, 0);
	store_u64(ptr + TEST_EDGE_HEADER_RESULT_TOKEN_OFFSET, result_token);
	ptr += TEST_EDGE_HEADER_SIZE;

	ptr[0] = block_id;
	ptr[1] = TEST_PAGE_CLASS_ORDINARY;
	ptr[2] = TEST_PAGE_STATE_UNFORMATTED;
	ptr[3] = TEST_PAGE_STATE_PRESENT;
	store_u16(ptr + 4, edge_flags);
	store_u16(ptr + 6, 0);
	memcpy(ptr + TEST_EDGE_ENTRY_BEFORE_UUID_OFFSET, incarnation,
		   sizeof(incarnation));
	store_u64(ptr + TEST_EDGE_ENTRY_BEFORE_TOKEN_OFFSET, 0);
	memcpy(ptr + TEST_EDGE_ENTRY_RESULT_UUID_OFFSET, incarnation,
		   sizeof(incarnation));
	ptr += TEST_EDGE_ENTRY_SIZE;

	/* Matching ordinary block reference: WILL_INIT and no payload. */
	*ptr++ = block_id;
	*ptr++ = BKPBLOCK_WILL_INIT | MAIN_FORKNUM;
	store_u16(ptr, 0);
	ptr += sizeof(uint16);
	memcpy(ptr, &locator, sizeof(locator));
	ptr += sizeof(locator);
	memcpy(ptr, &block_number, sizeof(block_number));
	ptr += sizeof(block_number);

	record->xl_tot_len = (uint32) (ptr - record_bytes);
	return record->xl_tot_len;
}

static void
insert_duplicate_edge_fragment(uint8 *record_bytes)
{
	XLogRecord *record = (XLogRecord *) record_bytes;
	size_t		edge_offset = SizeOfXLogRecord;
	size_t		edge_size = TEST_EDGE_HEADER_SIZE + TEST_EDGE_ENTRY_SIZE;
	size_t		block_offset = edge_offset + edge_size;

	memmove(record_bytes + block_offset + edge_size,
			record_bytes + block_offset,
			record->xl_tot_len - block_offset);
	memcpy(record_bytes + block_offset, record_bytes + edge_offset, edge_size);
	record->xl_tot_len += edge_size;
}

static void
insert_out_of_order_entry(uint8 *record_bytes)
{
	XLogRecord *record = (XLogRecord *) record_bytes;
	size_t		entry_offset = SizeOfXLogRecord + TEST_EDGE_HEADER_SIZE;
	size_t		block_offset = entry_offset + TEST_EDGE_ENTRY_SIZE;

	memmove(record_bytes + block_offset + TEST_EDGE_ENTRY_SIZE,
			record_bytes + block_offset,
			record->xl_tot_len - block_offset);
	memcpy(record_bytes + block_offset, record_bytes + entry_offset,
		   TEST_EDGE_ENTRY_SIZE);
	record_bytes[SizeOfXLogRecord + 2] = 2;
	record_bytes[entry_offset] = 1;
	record_bytes[entry_offset + TEST_EDGE_ENTRY_SIZE] = 0;
	record->xl_tot_len += TEST_EDGE_ENTRY_SIZE;
}

static DecodeFixtureResult
decode_fixture(uint8 *record_bytes)
{
	XLogRecord *record = (XLogRecord *) record_bytes;
	XLogReaderState state;
	DecodeFixtureResult result;
	char	   *errormsg = NULL;
	size_t		decoded_size;

	memset(&state, 0, sizeof(state));
	memset(&result, 0, sizeof(result));
	state.ReadRecPtr = UINT64CONST(0x1000);
	state.errormsg_buf = result.error;

	decoded_size = DecodeXLogRecordRequiredSpace(record->xl_tot_len);
	result.decoded = (DecodedXLogRecord *) calloc(1, decoded_size);
	if (result.decoded == NULL)
	{
		strlcpy(result.error, "calloc failed", sizeof(result.error));
		return result;
	}

	result.ok = DecodeXLogRecord(&state, result.decoded, record,
								 UINT64CONST(0x1000), &errormsg);
	if (!result.ok && errormsg != NULL && errormsg != result.error)
		strlcpy(result.error, errormsg, sizeof(result.error));
	return result;
}

static void
free_fixture(DecodeFixtureResult * result)
{
	free(result->decoded);
	result->decoded = NULL;
}

UT_TEST(test_decoder_control_accepts_record_without_edge)
{
	RecordFixture fixture;
	XLogRecord *record = &fixture.align;
	DecodeFixtureResult result;

	memset(&fixture, 0, sizeof(fixture));
	record->xl_tot_len = SizeOfXLogRecord;
	result = decode_fixture(fixture.bytes);
	UT_ASSERT(result.ok);
	free_fixture(&result);
}

UT_TEST(test_decoder_accepts_valid_id251_edge)
{
	RecordFixture fixture;
	DecodeFixtureResult result;

	(void) build_edge_record(fixture.bytes, TEST_EDGE_ENTRY_SIZE, 0, 0);
	result = decode_fixture(fixture.bytes);
	if (!result.ok)
		printf("# JIT_SEMANTIC_RED:T3-D-ID251-VALID-DECODE: %s\n",
			   result.error);
	UT_ASSERT(result.ok);

#ifdef PGRAC_JIT_PAGE_VERSION_EDGE_ABI_V1
	if (result.ok)
	{
		UT_ASSERT_EQ(result.decoded->page_version_edge_count, 1);
		UT_ASSERT_EQ(result.decoded->page_version_edge_result_token,
					 UINT64CONST(0x0102030405060708));
		UT_ASSERT_NOT_NULL(result.decoded->page_version_edge_entries);
		UT_ASSERT_EQ(result.decoded->page_version_edge_entries[0].block_id, 0);
	}
#endif

	free_fixture(&result);
}

UT_TEST(test_decoder_rejects_legacy_entry_size_32)
{
	RecordFixture fixture;
	DecodeFixtureResult result;

	(void) build_edge_record(fixture.bytes, 32, 0, 0);
	result = decode_fixture(fixture.bytes);
	UT_ASSERT(!result.ok);
	free_fixture(&result);
}

UT_TEST(test_decoder_rejects_nonzero_header_flags)
{
	RecordFixture fixture;
	DecodeFixtureResult result;

	(void) build_edge_record(fixture.bytes, TEST_EDGE_ENTRY_SIZE, 1, 0);
	result = decode_fixture(fixture.bytes);
	UT_ASSERT(!result.ok);
	free_fixture(&result);
}

UT_TEST(test_decoder_rejects_edge_for_missing_block)
{
	RecordFixture fixture;
	DecodeFixtureResult result;

	(void) build_edge_record(fixture.bytes, TEST_EDGE_ENTRY_SIZE, 0, 1);
	/* Make the actual block reference ID disagree with the edge ID. */
	fixture.bytes[SizeOfXLogRecord + TEST_EDGE_HEADER_SIZE +
				  TEST_EDGE_ENTRY_SIZE] = 0;
	result = decode_fixture(fixture.bytes);
	UT_ASSERT(!result.ok);
	free_fixture(&result);
}

UT_TEST(test_decoder_rejects_zero_result_token)
{
	RecordFixture fixture;
	DecodeFixtureResult result;

	(void) build_edge_record(fixture.bytes, TEST_EDGE_ENTRY_SIZE, 0, 0);
	store_u64(fixture.bytes + SizeOfXLogRecord +
			  TEST_EDGE_HEADER_RESULT_TOKEN_OFFSET, 0);
	result = decode_fixture(fixture.bytes);
	UT_ASSERT(!result.ok);
	free_fixture(&result);
}

UT_TEST(test_decoder_rejects_nonzero_header_reserved)
{
	RecordFixture fixture;
	DecodeFixtureResult result;

	(void) build_edge_record(fixture.bytes, TEST_EDGE_ENTRY_SIZE, 0, 0);
	store_u16(fixture.bytes + SizeOfXLogRecord + 6, 1);
	result = decode_fixture(fixture.bytes);
	UT_ASSERT(!result.ok);
	free_fixture(&result);
}

UT_TEST(test_decoder_rejects_duplicate_id251_fragment)
{
	RecordFixture fixture;
	DecodeFixtureResult result;

	(void) build_edge_record(fixture.bytes, TEST_EDGE_ENTRY_SIZE, 0, 0);
	insert_duplicate_edge_fragment(fixture.bytes);
	result = decode_fixture(fixture.bytes);
	UT_ASSERT(!result.ok);
	free_fixture(&result);
}

UT_TEST(test_decoder_rejects_out_of_order_edge_entries)
{
	RecordFixture fixture;
	DecodeFixtureResult result;

	(void) build_edge_record(fixture.bytes, TEST_EDGE_ENTRY_SIZE, 0, 0);
	insert_out_of_order_entry(fixture.bytes);
	result = decode_fixture(fixture.bytes);
	UT_ASSERT(!result.ok);
	free_fixture(&result);
}

UT_TEST(test_decoder_rejects_will_init_mismatch)
{
	RecordFixture fixture;
	DecodeFixtureResult result;
	size_t		block_flags_offset = SizeOfXLogRecord +
		TEST_EDGE_HEADER_SIZE + TEST_EDGE_ENTRY_SIZE + 1;

	(void) build_edge_record(fixture.bytes, TEST_EDGE_ENTRY_SIZE, 0, 0);
	fixture.bytes[block_flags_offset] = MAIN_FORKNUM;
	result = decode_fixture(fixture.bytes);
	UT_ASSERT(!result.ok);
	free_fixture(&result);
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_decoder_control_accepts_record_without_edge);
	UT_RUN(test_decoder_accepts_valid_id251_edge);
	UT_RUN(test_decoder_rejects_legacy_entry_size_32);
	UT_RUN(test_decoder_rejects_nonzero_header_flags);
	UT_RUN(test_decoder_rejects_edge_for_missing_block);
	UT_RUN(test_decoder_rejects_zero_result_token);
	UT_RUN(test_decoder_rejects_nonzero_header_reserved);
	UT_RUN(test_decoder_rejects_duplicate_id251_fragment);
	UT_RUN(test_decoder_rejects_out_of_order_edge_entries);
	UT_RUN(test_decoder_rejects_will_init_mismatch);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
