/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_activation_record.c
 *	  Exact PGSA512 codec and strict-majority policy tests for R4 D13.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_semantic_activation.h"
#include "port/pg_crc32c.h"
#include "storage/shmem.h"

#include "cluster_r4_activation_test_stubs.h"

int cluster_node_id = 0;

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size pg_attribute_unused(),
				bool *foundPtr pg_attribute_unused())
{
	return NULL;
}

/* Exercise the real product-local policy helpers without exporting a test API. */
#include "../../backend/cluster/cluster_semantic_activation.c"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

bool
cluster_replacement_phase3_handoff_poll_local(
	ClusterReplacementPhase3HandoffItem *item pg_attribute_unused())
{
	return false;
}

void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

static ClusterSemanticActivationRecord
valid_record(ClusterSemanticActivationPhase phase, uint64 generation)
{
	ClusterSemanticActivationRecord record;

	memset(&record, 0, sizeof(record));
	record.source_feature_bitmap = UINT64_C(0x11);
	record.target_feature_bitmap = UINT64_C(0x22);
	record.transition_epoch = UINT64_C(0x33445566778899aa);
	record.record_generation = generation;
	record.admitted_members_lo = UINT64_C(0x0123456789abcdef);
	record.admitted_members_hi = UINT64_C(0xfedcba9876543210);
	record.capability_sample_digest = UINT64_C(0x8877665544332211);
	record.rollback_feature_bitmap
		= phase == CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE ? UINT64_C(0x20) : 0;
	record.coordinator_incarnation = UINT64_C(0x1020304050607080);
	record.coordinator_node = 37;
	record.phase = phase;
	return record;
}

static uint16
read_u16_le(const uint8 *p)
{
	return (uint16)p[0] | ((uint16)p[1] << 8);
}

static uint32
read_u32_le(const uint8 *p)
{
	return (uint32)p[0] | ((uint32)p[1] << 8) | ((uint32)p[2] << 16) | ((uint32)p[3] << 24);
}

static uint64
read_u64_le(const uint8 *p)
{
	uint64 value = 0;
	int i;

	for (i = 7; i >= 0; i--)
		value = (value << 8) | p[i];
	return value;
}

static void
write_u32_le(uint8 *p, uint32 value)
{
	p[0] = (uint8)value;
	p[1] = (uint8)(value >> 8);
	p[2] = (uint8)(value >> 16);
	p[3] = (uint8)(value >> 24);
}

static void
refresh_crc(uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES])
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, CLUSTER_SEMANTIC_RECORD_CRC_OFFSET);
	FIN_CRC32C(crc);
	write_u32_le(bytes + CLUSTER_SEMANTIC_RECORD_CRC_OFFSET, (uint32)crc);
}

static bool
encode(ClusterSemanticActivationRecord record,
	   uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES])
{
	memset(bytes, 0xa5, CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES);
	return cluster_semantic_activation_record_encode(&record, bytes);
}

static bool
decode(const uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES],
	   ClusterSemanticActivationRecord *record, ClusterSemanticActivationRefusal *refusal)
{
	memset(record, 0, sizeof(*record));
	memset(refusal, 0, sizeof(*refusal));
	return cluster_semantic_activation_record_decode(bytes, record, refusal);
}

static ClusterSemanticRecordSample
sample_from(const uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES], bool readable)
{
	ClusterSemanticRecordSample sample;

	memset(&sample, 0, sizeof(sample));
	sample.readable = readable;
	if (bytes != NULL)
		memcpy(sample.bytes, bytes, sizeof(sample.bytes));
	return sample;
}

static void
assert_roundtrip(ClusterSemanticActivationPhase phase)
{
	ClusterSemanticActivationRecord input = valid_record(phase, 9);
	ClusterSemanticActivationRecord output;
	ClusterSemanticActivationRefusal refusal;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(input, bytes));
	UT_ASSERT(decode(bytes, &output, &refusal));
	UT_ASSERT_EQ(memcmp(&input, &output, sizeof(input)), 0);
}

UT_TEST(test_01_record_constants)
{
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES, 512);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_RECORD_MAGIC, UINT32_C(0x50475341));
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_RECORD_VERSION, 1);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_RECORD_HEADER_LEN, 104);
}

UT_TEST(test_02_phase_numeric_values)
{
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_PHASE_NONE, 0);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_PHASE_PREPARE, 1);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_PHASE_COMMIT, 2);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_PHASE_OPEN, 3);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE, 4);
}

UT_TEST(test_03_encode_rejects_null_record)
{
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(!cluster_semantic_activation_record_encode(NULL, bytes));
}

UT_TEST(test_04_encode_rejects_null_bytes)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);

	UT_ASSERT(!cluster_semantic_activation_record_encode(&record, NULL));
}

UT_TEST(test_05_decode_rejects_null_bytes)
{
	ClusterSemanticActivationRecord record;
	ClusterSemanticActivationRefusal refusal;

	UT_ASSERT(!cluster_semantic_activation_record_decode(NULL, &record, &refusal));
}

UT_TEST(test_06_decode_rejects_null_record)
{
	ClusterSemanticActivationRefusal refusal;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES] = { 0 };

	UT_ASSERT(!cluster_semantic_activation_record_decode(bytes, NULL, &refusal));
}

UT_TEST(test_07_roundtrip_prepare)
{
	assert_roundtrip(CLUSTER_SEMANTIC_PHASE_PREPARE);
}

UT_TEST(test_08_roundtrip_commit)
{
	assert_roundtrip(CLUSTER_SEMANTIC_PHASE_COMMIT);
}

UT_TEST(test_09_roundtrip_open)
{
	assert_roundtrip(CLUSTER_SEMANTIC_PHASE_OPEN);
}

UT_TEST(test_10_roundtrip_rollback_complete)
{
	assert_roundtrip(CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE);
}

UT_TEST(test_11_generation_one_is_valid)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u64_le(bytes + 8), 1);
}

UT_TEST(test_12_generation_uint64_max_is_preserved)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, UINT64_MAX);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u64_le(bytes + 8), UINT64_MAX);
}

UT_TEST(test_13_epoch_zero_is_valid)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_PREPARE, 2);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	record.transition_epoch = 0;
	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u64_le(bytes + 40), 0);
}

UT_TEST(test_14_all_member_bits_are_preserved)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_COMMIT, 2);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	record.admitted_members_lo = UINT64_MAX;
	record.admitted_members_hi = UINT64_MAX;
	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u64_le(bytes + 64), UINT64_MAX);
	UT_ASSERT_EQ(read_u64_le(bytes + 72), UINT64_MAX);
}

UT_TEST(test_15_rollback_bitmap_is_preserved_only_for_terminal_phase)
{
	ClusterSemanticActivationRecord record
		= valid_record(CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE, 4);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u64_le(bytes + 88), UINT64_C(0x20));
}

UT_TEST(test_16_magic_is_little_endian_at_zero)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u32_le(bytes), UINT32_C(0x50475341));
}

UT_TEST(test_17_version_is_little_endian_at_four)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u16_le(bytes + 4), 1);
}

UT_TEST(test_18_header_len_is_little_endian_at_six)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u16_le(bytes + 6), 104);
}

UT_TEST(test_19_generation_is_at_eight)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 77);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u64_le(bytes + 8), 77);
}

UT_TEST(test_20_phase_is_at_sixteen)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_COMMIT, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(bytes[16], 2);
}

UT_TEST(test_21_phase_reserved_bytes_are_zero)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_COMMIT, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	int i;

	UT_ASSERT(encode(record, bytes));
	for (i = 17; i < 24; i++)
		UT_ASSERT_EQ(bytes[i], 0);
}

UT_TEST(test_22_source_bitmap_is_at_twenty_four)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u64_le(bytes + 24), UINT64_C(0x11));
}

UT_TEST(test_23_target_bitmap_is_at_thirty_two)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u64_le(bytes + 32), UINT64_C(0x22));
}

UT_TEST(test_24_epoch_is_at_forty)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u64_le(bytes + 40), UINT64_C(0x33445566778899aa));
}

UT_TEST(test_25_coordinator_node_is_at_forty_eight)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u32_le(bytes + 48), 37);
}

UT_TEST(test_26_coordinator_reserved_word_is_zero)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u32_le(bytes + 52), 0);
}

UT_TEST(test_27_coordinator_incarnation_is_at_fifty_six)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u64_le(bytes + 56), UINT64_C(0x1020304050607080));
}

UT_TEST(test_28_member_words_are_at_sixty_four_and_seventy_two)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u64_le(bytes + 64), UINT64_C(0x0123456789abcdef));
	UT_ASSERT_EQ(read_u64_le(bytes + 72), UINT64_C(0xfedcba9876543210));
}

UT_TEST(test_29_digest_and_rollback_are_at_eighty_and_eighty_eight)
{
	ClusterSemanticActivationRecord record
		= valid_record(CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	UT_ASSERT_EQ(read_u64_le(bytes + 80), UINT64_C(0x8877665544332211));
	UT_ASSERT_EQ(read_u64_le(bytes + 88), UINT64_C(0x20));
}

UT_TEST(test_30_crc_covers_exactly_bytes_zero_through_ninety_five)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint32 stored;
	pg_crc32c expected;

	UT_ASSERT(encode(record, bytes));
	INIT_CRC32C(expected);
	COMP_CRC32C(expected, bytes, 96);
	FIN_CRC32C(expected);
	stored = read_u32_le(bytes + 96);
	UT_ASSERT_EQ(stored, (uint32)expected);
}

UT_TEST(test_31_bytes_one_hundred_through_end_are_zero)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	int i;

	UT_ASSERT(encode(record, bytes));
	for (i = 100; i < CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES; i++)
		UT_ASSERT_EQ(bytes[i], 0);
}

UT_TEST(test_32_encode_rejects_generation_zero)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 0);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(!encode(record, bytes));
}

UT_TEST(test_33_encode_rejects_phase_none)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_NONE, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(!encode(record, bytes));
}

UT_TEST(test_34_encode_rejects_rollback_bitmap_before_terminal_phase)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	record.rollback_feature_bitmap = 1;
	UT_ASSERT(!encode(record, bytes));
}

UT_TEST(test_35_decode_rejects_magic_mutation)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	ClusterSemanticActivationRecord output;
	ClusterSemanticActivationRefusal refusal;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	bytes[0] ^= 1;
	refresh_crc(bytes);
	UT_ASSERT(!decode(bytes, &output, &refusal));
}

UT_TEST(test_36_decode_rejects_version_mutation)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	ClusterSemanticActivationRecord output;
	ClusterSemanticActivationRefusal refusal;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	bytes[4] = 2;
	refresh_crc(bytes);
	UT_ASSERT(!decode(bytes, &output, &refusal));
}

UT_TEST(test_37_decode_rejects_header_len_mutation)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	ClusterSemanticActivationRecord output;
	ClusterSemanticActivationRefusal refusal;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	bytes[6] = 105;
	refresh_crc(bytes);
	UT_ASSERT(!decode(bytes, &output, &refusal));
}

UT_TEST(test_38_decode_rejects_generation_zero)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	ClusterSemanticActivationRecord output;
	ClusterSemanticActivationRefusal refusal;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	memset(bytes + 8, 0, 8);
	refresh_crc(bytes);
	UT_ASSERT(!decode(bytes, &output, &refusal));
}

UT_TEST(test_39_decode_rejects_unknown_phase)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	ClusterSemanticActivationRecord output;
	ClusterSemanticActivationRefusal refusal;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	bytes[16] = 5;
	refresh_crc(bytes);
	UT_ASSERT(!decode(bytes, &output, &refusal));
}

UT_TEST(test_40_decode_rejects_phase_reserved_mutation)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	ClusterSemanticActivationRecord output;
	ClusterSemanticActivationRefusal refusal;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	bytes[19] = 1;
	refresh_crc(bytes);
	UT_ASSERT(!decode(bytes, &output, &refusal));
}

UT_TEST(test_41_decode_rejects_word_reserved_mutation)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	ClusterSemanticActivationRecord output;
	ClusterSemanticActivationRefusal refusal;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	bytes[52] = 1;
	refresh_crc(bytes);
	UT_ASSERT(!decode(bytes, &output, &refusal));
}

UT_TEST(test_42_decode_rejects_crc_and_tail_mutations)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 1);
	ClusterSemanticActivationRecord output;
	ClusterSemanticActivationRefusal refusal;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];

	UT_ASSERT(encode(record, bytes));
	bytes[96] ^= 1;
	UT_ASSERT(!decode(bytes, &output, &refusal));
	UT_ASSERT(encode(record, bytes));
	bytes[511] = 1;
	UT_ASSERT(!decode(bytes, &output, &refusal));
}

UT_TEST(test_43_two_of_three_identical_records_select)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 7);
	ClusterSemanticRecordSample samples[3];
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 selected[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	bool implicit_open = true;

	UT_ASSERT(encode(record, bytes));
	samples[0] = sample_from(bytes, true);
	samples[1] = sample_from(bytes, true);
	samples[2] = sample_from(NULL, false);
	UT_ASSERT_EQ(semantic_activation_select_majority(samples, 3, selected, &implicit_open),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT(!implicit_open);
	UT_ASSERT_EQ(memcmp(selected, bytes, sizeof(selected)), 0);
}

UT_TEST(test_44_three_of_five_identical_records_select)
{
	ClusterSemanticActivationRecord record = valid_record(CLUSTER_SEMANTIC_PHASE_COMMIT, 8);
	ClusterSemanticRecordSample samples[5];
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 selected[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	bool implicit_open = true;
	int i;

	UT_ASSERT(encode(record, bytes));
	for (i = 0; i < 3; i++)
		samples[i] = sample_from(bytes, true);
	for (; i < 5; i++)
		samples[i] = sample_from(NULL, false);
	UT_ASSERT_EQ(semantic_activation_select_majority(samples, 5, selected, &implicit_open),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT(!implicit_open);
}

UT_TEST(test_45_minority_higher_generation_does_not_win)
{
	ClusterSemanticActivationRecord old = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 7);
	ClusterSemanticActivationRecord newer = valid_record(CLUSTER_SEMANTIC_PHASE_PREPARE, 8);
	ClusterSemanticRecordSample samples[3];
	uint8 old_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 new_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 selected[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	bool implicit_open = true;

	UT_ASSERT(encode(old, old_bytes));
	UT_ASSERT(encode(newer, new_bytes));
	samples[0] = sample_from(old_bytes, true);
	samples[1] = sample_from(old_bytes, true);
	samples[2] = sample_from(new_bytes, true);
	UT_ASSERT_EQ(semantic_activation_select_majority(samples, 3, selected, &implicit_open),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(memcmp(selected, old_bytes, sizeof(selected)), 0);
}

UT_TEST(test_46_readable_all_zero_majority_is_implicit_open_zero)
{
	ClusterSemanticRecordSample samples[3];
	uint8 selected[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	bool implicit_open = false;

	samples[0] = sample_from(NULL, true);
	samples[1] = sample_from(NULL, true);
	samples[2] = sample_from(NULL, false);
	UT_ASSERT_EQ(semantic_activation_select_majority(samples, 3, selected, &implicit_open),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT(implicit_open);
	UT_ASSERT_EQ(read_u64_le(selected + 8), 0);
}

UT_TEST(test_47_less_than_readable_majority_holds)
{
	ClusterSemanticRecordSample samples[3];
	uint8 selected[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	bool implicit_open = false;

	samples[0] = sample_from(NULL, true);
	samples[1] = sample_from(NULL, false);
	samples[2] = sample_from(NULL, false);
	UT_ASSERT_EQ(semantic_activation_select_majority(samples, 3, selected, &implicit_open),
				 CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
}

UT_TEST(test_48_equal_generation_conflict_without_majority_is_not_legacy)
{
	ClusterSemanticActivationRecord a = valid_record(CLUSTER_SEMANTIC_PHASE_OPEN, 7);
	ClusterSemanticActivationRecord b = a;
	ClusterSemanticRecordSample samples[4];
	uint8 a_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 b_bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 selected[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	bool implicit_open = true;

	b.target_feature_bitmap++;
	UT_ASSERT(encode(a, a_bytes));
	UT_ASSERT(encode(b, b_bytes));
	samples[0] = sample_from(a_bytes, true);
	samples[1] = sample_from(a_bytes, true);
	samples[2] = sample_from(b_bytes, true);
	samples[3] = sample_from(b_bytes, true);
	UT_ASSERT_EQ(semantic_activation_select_majority(samples, 4, selected, &implicit_open),
				 CLUSTER_SEMANTIC_ACTIVATION_RECORD_CONFLICT);
	UT_ASSERT(!implicit_open);
}

UT_TEST(test_49_record_cas_mailbox_exact_sequence_lifecycle)
{
	ClusterSemanticActivationShmem shmem;
	ClusterSemanticActivationCasRequest request;
	ClusterSemanticActivationRecord desired_record = valid_record(CLUSTER_SEMANTIC_PHASE_COMMIT, 8);
	ClusterSemanticActivationResult result = CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint64 seq = UINT64_MAX;

	memset(&shmem, 0, sizeof(shmem));
	pg_atomic_init_u64(&shmem.record_cas_request_seq, 0);
	pg_atomic_init_u64(&shmem.record_cas_completion_seq, 0);
	pg_atomic_init_u32(&shmem.record_cas_result, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
	SemanticActivationShmem = NULL;
	UT_ASSERT(encode(desired_record, desired));
	UT_ASSERT(!semantic_activation_record_cas_mailbox_submit(7, UINT64_C(0x11), desired, &seq));

	SemanticActivationShmem = &shmem;
	UT_ASSERT(!semantic_activation_record_cas_mailbox_submit(7, UINT64_C(0x11), NULL, &seq));
	UT_ASSERT(!semantic_activation_record_cas_mailbox_submit(7, UINT64_C(0x11), desired, NULL));
	UT_ASSERT(semantic_activation_record_cas_mailbox_submit(7, UINT64_C(0x11), desired, &seq));
	UT_ASSERT_EQ(seq, 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&shmem.record_cas_request_seq), 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&shmem.record_cas_completion_seq), 0);
	UT_ASSERT(!semantic_activation_record_cas_mailbox_submit(8, UINT64_C(0x22), desired, &seq));

	memset(&request, 0, sizeof(request));
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_record_cas(&request));
	UT_ASSERT_EQ(request.request_seq, 1);
	UT_ASSERT_EQ(request.expected_generation, 7);
	UT_ASSERT_EQ(request.expected_source_feature_bitmap, UINT64_C(0x11));
	UT_ASSERT_EQ(memcmp(request.desired_bytes, desired, sizeof(desired)), 0);
	UT_ASSERT(
		!cluster_semantic_activation_qvotec_complete_record_cas(0, CLUSTER_SEMANTIC_ACTIVATION_OK));
	UT_ASSERT(
		!cluster_semantic_activation_qvotec_complete_record_cas(2, CLUSTER_SEMANTIC_ACTIVATION_OK));
	UT_ASSERT_EQ(pg_atomic_read_u64(&shmem.record_cas_completion_seq), 0);
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_record_cas(
		1, CLUSTER_SEMANTIC_ACTIVATION_RECORD_CONFLICT));
	UT_ASSERT(
		!cluster_semantic_activation_qvotec_complete_record_cas(1, CLUSTER_SEMANTIC_ACTIVATION_OK));
	UT_ASSERT(semantic_activation_record_cas_mailbox_poll_completion(1, &result));
	UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ACTIVATION_RECORD_CONFLICT);
	UT_ASSERT(!semantic_activation_record_cas_mailbox_poll_completion(2, &result));

	UT_ASSERT(semantic_activation_record_cas_mailbox_submit(8, UINT64_C(0x22), desired, &seq));
	UT_ASSERT_EQ(seq, 2);
	pg_atomic_write_u64(&shmem.record_cas_request_seq, UINT64_MAX);
	pg_atomic_write_u64(&shmem.record_cas_completion_seq, UINT64_MAX);
	UT_ASSERT(!semantic_activation_record_cas_mailbox_submit(8, UINT64_C(0x22), desired, &seq));
	UT_ASSERT_EQ(pg_atomic_read_u64(&shmem.record_cas_request_seq), UINT64_MAX);
	UT_ASSERT_EQ(pg_atomic_read_u64(&shmem.record_cas_completion_seq), UINT64_MAX);
	SemanticActivationShmem = NULL;
}

UT_TEST(test_50_pgrd_uses_distinct_kind_in_existing_512_byte_mailbox)
{
	ClusterSemanticActivationShmem shmem;
	ClusterSemanticActivationUtilityMailboxShmem utility;
	ClusterSemanticFormationBinding formation = {
		.utility_request_seq = 1,
		.formation_epoch = 0,
		.coordinator_incarnation = 1,
		.expected_record_generation = 0,
	};
	ClusterUndoRootDescriptorRequest request;
	ClusterSemanticActivationCasRequest record_request;
	ClusterSemanticActivationResult result = CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint64 seq = 0;
	int i;

	memset(&shmem, 0, sizeof(shmem));
	pg_atomic_init_u64(&shmem.record_cas_request_seq, 0);
	pg_atomic_init_u64(&shmem.record_cas_completion_seq, 0);
	pg_atomic_init_u32(&shmem.record_cas_result,
				   CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
	pg_atomic_init_u32(&shmem.record_cas_request_kind,
				   CLUSTER_SEMANTIC_AUTHORITY_REQUEST_NONE);
	memset(&utility, 0, sizeof(utility));
	pg_atomic_init_u64(&utility.utility_request_seq, 1);
	pg_atomic_init_u64(&utility.utility_completion_seq, 0);
	pg_atomic_init_u32(&utility.utility_mailbox_state,
				   SEMANTIC_ACTIVATION_UTILITY_MAILBOX_PENDING);
	for (i = 0; i < (int)sizeof(desired); i++)
		desired[i] = (uint8)(i ^ 0x5c);
	SemanticActivationShmem = &shmem;
	SemanticActivationUtilityMailbox = &utility;
	cluster_r4_activation_test_formation_valid = true;

	UT_ASSERT(cluster_semantic_activation_undo_root_descriptor_mailbox_submit(
		&formation, UINT64_C(0x0123456789abcdef), desired, &seq));
	UT_ASSERT_EQ(seq, 1);
	UT_ASSERT(!cluster_semantic_activation_qvotec_poll_record_cas(
		&record_request));
	memset(&request, 0, sizeof(request));
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_undo_root_descriptor(
		&request));
	UT_ASSERT_EQ(request.request_seq, 1);
	UT_ASSERT_EQ(request.system_identifier,
				 UINT64_C(0x0123456789abcdef));
	UT_ASSERT_EQ(request.formation.utility_request_seq, UINT64_C(1));
	UT_ASSERT_EQ(request.formation.formation_epoch, UINT64_C(0));
	UT_ASSERT_EQ(request.formation.coordinator_incarnation, UINT64_C(1));
	UT_ASSERT_EQ(request.formation.expected_record_generation, UINT64_C(0));
	UT_ASSERT_EQ(memcmp(request.desired_bytes, desired, sizeof(desired)), 0);
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_undo_root_descriptor(
		request.request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK));
	UT_ASSERT(cluster_semantic_activation_undo_root_descriptor_mailbox_poll_completion(
		seq, &result));
	UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(offsetof(ClusterSemanticActivationShmem,
					  record_cas_request_kind), 20);
	UT_ASSERT_EQ(offsetof(ClusterSemanticActivationShmem, admission_seq), 552);
	UT_ASSERT_EQ(sizeof(ClusterSemanticActivationShmem), 1104);
	SemanticActivationShmem = NULL;
	SemanticActivationUtilityMailbox = NULL;
	cluster_r4_activation_test_formation_valid = false;
}

int
main(void)
{
	UT_PLAN(50);
	UT_RUN(test_01_record_constants);
	UT_RUN(test_02_phase_numeric_values);
	UT_RUN(test_03_encode_rejects_null_record);
	UT_RUN(test_04_encode_rejects_null_bytes);
	UT_RUN(test_05_decode_rejects_null_bytes);
	UT_RUN(test_06_decode_rejects_null_record);
	UT_RUN(test_07_roundtrip_prepare);
	UT_RUN(test_08_roundtrip_commit);
	UT_RUN(test_09_roundtrip_open);
	UT_RUN(test_10_roundtrip_rollback_complete);
	UT_RUN(test_11_generation_one_is_valid);
	UT_RUN(test_12_generation_uint64_max_is_preserved);
	UT_RUN(test_13_epoch_zero_is_valid);
	UT_RUN(test_14_all_member_bits_are_preserved);
	UT_RUN(test_15_rollback_bitmap_is_preserved_only_for_terminal_phase);
	UT_RUN(test_16_magic_is_little_endian_at_zero);
	UT_RUN(test_17_version_is_little_endian_at_four);
	UT_RUN(test_18_header_len_is_little_endian_at_six);
	UT_RUN(test_19_generation_is_at_eight);
	UT_RUN(test_20_phase_is_at_sixteen);
	UT_RUN(test_21_phase_reserved_bytes_are_zero);
	UT_RUN(test_22_source_bitmap_is_at_twenty_four);
	UT_RUN(test_23_target_bitmap_is_at_thirty_two);
	UT_RUN(test_24_epoch_is_at_forty);
	UT_RUN(test_25_coordinator_node_is_at_forty_eight);
	UT_RUN(test_26_coordinator_reserved_word_is_zero);
	UT_RUN(test_27_coordinator_incarnation_is_at_fifty_six);
	UT_RUN(test_28_member_words_are_at_sixty_four_and_seventy_two);
	UT_RUN(test_29_digest_and_rollback_are_at_eighty_and_eighty_eight);
	UT_RUN(test_30_crc_covers_exactly_bytes_zero_through_ninety_five);
	UT_RUN(test_31_bytes_one_hundred_through_end_are_zero);
	UT_RUN(test_32_encode_rejects_generation_zero);
	UT_RUN(test_33_encode_rejects_phase_none);
	UT_RUN(test_34_encode_rejects_rollback_bitmap_before_terminal_phase);
	UT_RUN(test_35_decode_rejects_magic_mutation);
	UT_RUN(test_36_decode_rejects_version_mutation);
	UT_RUN(test_37_decode_rejects_header_len_mutation);
	UT_RUN(test_38_decode_rejects_generation_zero);
	UT_RUN(test_39_decode_rejects_unknown_phase);
	UT_RUN(test_40_decode_rejects_phase_reserved_mutation);
	UT_RUN(test_41_decode_rejects_word_reserved_mutation);
	UT_RUN(test_42_decode_rejects_crc_and_tail_mutations);
	UT_RUN(test_43_two_of_three_identical_records_select);
	UT_RUN(test_44_three_of_five_identical_records_select);
	UT_RUN(test_45_minority_higher_generation_does_not_win);
	UT_RUN(test_46_readable_all_zero_majority_is_implicit_open_zero);
	UT_RUN(test_47_less_than_readable_majority_holds);
	UT_RUN(test_48_equal_generation_conflict_without_majority_is_not_legacy);
	UT_RUN(test_49_record_cas_mailbox_exact_sequence_lifecycle);
	UT_RUN(test_50_pgrd_uses_distinct_kind_in_existing_512_byte_mailbox);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
