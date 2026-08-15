/*-------------------------------------------------------------------------
 *
 * test_pgrac_fenced_journal.c
 *	  RF-ROOT P4 exact local journal tests.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include "pgrac_fenced_journal.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void *
palloc(Size size)
{
	return malloc(size);
}

void
pfree(void *pointer)
{
	free(pointer);
}

void
ExceptionalCondition(const char *condition_name, const char *file_name,
				 int line_number)
{
	printf("# Assert failed: %s at %s:%d\n", condition_name, file_name,
		   line_number);
	abort();
}

static void
fill_nonzero(uint8 *bytes, size_t len, uint8 value)
{
	memset(bytes, value, len);
}

static void
make_config_record(PgracFencedJournalRecordV1 *record, uint64 seq)
{
	memset(record, 0, sizeof(*record));
	record->record_kind = PGRAC_FENCED_JOURNAL_KIND_CONFIG_LOADED;
	record->seq = seq;
	fill_nonzero(record->daemon_boot_id, sizeof(record->daemon_boot_id), 0x11);
	record->event_mono_ns = 100;
	record->provider_result = PGRAC_FENCED_JOURNAL_PROVIDER_UNAVAILABLE;
	fill_nonzero(record->semantic_config_digest,
			 sizeof(record->semantic_config_digest), 0x22);
}

UT_TEST(test_journal_exact_codec_roundtrip)
{
	PgracFencedJournalRecordV1 record;
	PgracFencedJournalRecordV1 decoded;
	uint8 frame[PGRAC_FENCED_JOURNAL_RECORD_BYTES];

	make_config_record(&record, 1);
	UT_ASSERT(pgrac_fenced_journal_record_encode(&record, frame));
	UT_ASSERT_EQ(frame[0], 'P');
	UT_ASSERT_EQ(frame[1], 'F');
	UT_ASSERT_EQ(frame[2], 'G');
	UT_ASSERT_EQ(frame[3], 'J');
	UT_ASSERT_EQ(frame[248], 'P');
	UT_ASSERT_EQ(frame[249], 'F');
	UT_ASSERT_EQ(frame[250], 'G');
	UT_ASSERT_EQ(frame[251], 'Z');
	UT_ASSERT(pgrac_fenced_journal_record_decode(frame, sizeof(frame),
		&decoded));
	UT_ASSERT_EQ(decoded.record_kind, record.record_kind);
	UT_ASSERT_EQ(decoded.seq, record.seq);
	UT_ASSERT(memcmp(decoded.daemon_boot_id, record.daemon_boot_id,
		sizeof(record.daemon_boot_id)) == 0);
	UT_ASSERT(memcmp(decoded.semantic_config_digest,
		record.semantic_config_digest,
		sizeof(record.semantic_config_digest)) == 0);
}

UT_TEST(test_journal_rejects_crc_reserved_unknown_and_bad_proof)
{
	PgracFencedJournalRecordV1 record;
	uint8 frame[PGRAC_FENCED_JOURNAL_RECORD_BYTES];

	make_config_record(&record, 1);
	UT_ASSERT(pgrac_fenced_journal_record_encode(&record, frame));
	frame[240] = 1;
	UT_ASSERT(!pgrac_fenced_journal_record_decode(frame, sizeof(frame),
		&record));

	make_config_record(&record, 1);
	record.record_kind = 11;
	UT_ASSERT(!pgrac_fenced_journal_record_encode(&record, frame));

	make_config_record(&record, 1);
	record.record_kind = PGRAC_FENCED_JOURNAL_KIND_PROOF_SERVED;
	fill_nonzero(record.operation_id, sizeof(record.operation_id), 0x33);
	UT_ASSERT(!pgrac_fenced_journal_record_encode(&record, frame));
}

UT_TEST(test_journal_scan_verifies_seq_and_full_record_hash_chain)
{
	PgracFencedJournalRecordV1 first;
	PgracFencedJournalRecordV1 second;
	PgracFencedJournalScanState state;
	uint8 frames[PGRAC_FENCED_JOURNAL_RECORD_BYTES * 2];

	make_config_record(&first, 1);
	UT_ASSERT(pgrac_fenced_journal_record_encode(&first, frames));
	make_config_record(&second, 2);
	second.record_kind = PGRAC_FENCED_JOURNAL_KIND_REQUEST_ACCEPTED;
	fill_nonzero(second.operation_id, sizeof(second.operation_id), 0x44);
	UT_ASSERT(pgrac_fenced_journal_record_digest(frames,
		second.previous_record_digest));
	UT_ASSERT(pgrac_fenced_journal_record_encode(&second,
		frames + PGRAC_FENCED_JOURNAL_RECORD_BYTES));

	pgrac_fenced_journal_scan_state_init(&state);
	UT_ASSERT_EQ(pgrac_fenced_journal_scan_bytes(frames, sizeof(frames),
		false, &state), PGRAC_FENCED_JOURNAL_SCAN_OK);
	UT_ASSERT_EQ(state.next_seq, 3);
	UT_ASSERT_EQ(state.segment_record_count, 2);
	UT_ASSERT_EQ(state.valid_bytes, sizeof(frames));

	frames[PGRAC_FENCED_JOURNAL_RECORD_BYTES + 56] ^= 1;
	pgrac_fenced_journal_scan_state_init(&state);
	UT_ASSERT_EQ(pgrac_fenced_journal_scan_bytes(frames, sizeof(frames),
		false, &state), PGRAC_FENCED_JOURNAL_SCAN_CORRUPT);
	UT_ASSERT(!state.available);
}

UT_TEST(test_only_active_final_partial_record_is_truncatable)
{
	PgracFencedJournalRecordV1 record;
	PgracFencedJournalScanState state;
	uint8 bytes[PGRAC_FENCED_JOURNAL_RECORD_BYTES + 13];

	make_config_record(&record, 1);
	UT_ASSERT(pgrac_fenced_journal_record_encode(&record, bytes));
	memset(bytes + PGRAC_FENCED_JOURNAL_RECORD_BYTES, 0x55, 13);

	pgrac_fenced_journal_scan_state_init(&state);
	UT_ASSERT_EQ(pgrac_fenced_journal_scan_bytes(bytes, sizeof(bytes), true,
		&state), PGRAC_FENCED_JOURNAL_SCAN_PARTIAL_TAIL);
	UT_ASSERT(state.available);
	UT_ASSERT_EQ(state.valid_bytes, PGRAC_FENCED_JOURNAL_RECORD_BYTES);
	UT_ASSERT_EQ(state.next_seq, 2);

	pgrac_fenced_journal_scan_state_init(&state);
	UT_ASSERT_EQ(pgrac_fenced_journal_scan_bytes(bytes, sizeof(bytes), false,
		&state), PGRAC_FENCED_JOURNAL_SCAN_CORRUPT);
	UT_ASSERT(!state.available);
}

UT_TEST(test_append_advances_only_after_full_write_and_fsync)
{
	PgracFencedJournalRecordV1 record;
	PgracFencedJournalScanState state;
	PgracFencedJournalScanState scanned;
	uint8 frames[PGRAC_FENCED_JOURNAL_RECORD_BYTES * 2];
	char path[] = "/tmp/pgrac-fenced-journal.XXXXXX";
	int fd;

	fd = mkstemp(path);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_APPEND), 0);
	pgrac_fenced_journal_scan_state_init(&state);

	make_config_record(&record, 0);
	UT_ASSERT_EQ(pgrac_fenced_journal_append_fd(fd, &state, &record),
		PGRAC_FENCED_JOURNAL_APPEND_OK);
	UT_ASSERT_EQ(record.seq, 1);

	make_config_record(&record, 0);
	record.record_kind = PGRAC_FENCED_JOURNAL_KIND_REQUEST_ACCEPTED;
	fill_nonzero(record.operation_id, sizeof(record.operation_id), 0x66);
	UT_ASSERT_EQ(pgrac_fenced_journal_append_fd(fd, &state, &record),
		PGRAC_FENCED_JOURNAL_APPEND_OK);
	UT_ASSERT_EQ(record.seq, 2);
	UT_ASSERT_EQ(state.next_seq, 3);

	UT_ASSERT_EQ(lseek(fd, 0, SEEK_SET), 0);
	UT_ASSERT_EQ(read(fd, frames, sizeof(frames)), sizeof(frames));
	pgrac_fenced_journal_scan_state_init(&scanned);
	UT_ASSERT_EQ(pgrac_fenced_journal_scan_bytes(frames, sizeof(frames),
		true, &scanned), PGRAC_FENCED_JOURNAL_SCAN_OK);
	UT_ASSERT_EQ(scanned.next_seq, 3);

	(void) close(fd);
	(void) unlink(path);
}

UT_TEST(test_append_requests_rotation_at_exact_segment_limit)
{
	PgracFencedJournalRecordV1 record;
	PgracFencedJournalScanState state;

	pgrac_fenced_journal_scan_state_init(&state);
	state.segment_record_count = PGRAC_FENCED_JOURNAL_SEGMENT_RECORDS;
	make_config_record(&record, 0);
	UT_ASSERT_EQ(pgrac_fenced_journal_append_fd(-1, &state, &record),
		PGRAC_FENCED_JOURNAL_APPEND_ROTATION_REQUIRED);
	UT_ASSERT(state.available);
	UT_ASSERT_EQ(state.next_seq, 1);
}

UT_TEST(test_reconcile_actions_cover_all_durable_record_kinds)
{
	PgracFencedJournalRecordV1 record;
	PgracFencedJournalRestartAction action;
	uint16 kind;

	for (kind = PGRAC_FENCED_JOURNAL_KIND_CONFIG_LOADED;
		 kind <= PGRAC_FENCED_JOURNAL_KIND_RECONCILED; kind++)
	{
		make_config_record(&record, 1);
		record.record_kind = kind;
		if (kind != PGRAC_FENCED_JOURNAL_KIND_CONFIG_LOADED)
			fill_nonzero(record.operation_id, sizeof(record.operation_id),
						 0x77);
		if (kind == PGRAC_FENCED_JOURNAL_KIND_REENABLE_RESULT)
			record.target_state = PGRAC_FENCED_JOURNAL_TARGET_ON;
		if (kind == PGRAC_FENCED_JOURNAL_KIND_RECONCILED)
			record.target_state = PGRAC_FENCED_JOURNAL_TARGET_UNKNOWN;
		UT_ASSERT(pgrac_fenced_journal_restart_action(&record, &action));
		UT_ASSERT_NE(action, PGRAC_FENCED_JOURNAL_RESTART_UNAVAILABLE);
	}

	make_config_record(&record, 1);
	record.record_kind = PGRAC_FENCED_JOURNAL_KIND_RECONCILED;
	fill_nonzero(record.operation_id, sizeof(record.operation_id), 0x88);
	record.target_state = PGRAC_FENCED_JOURNAL_TARGET_OFF;
	UT_ASSERT(pgrac_fenced_journal_restart_action(&record, &action));
	UT_ASSERT_EQ(action, PGRAC_FENCED_JOURNAL_RESTART_FRESH_READBACK);
	record.target_state = PGRAC_FENCED_JOURNAL_TARGET_ON;
	UT_ASSERT(pgrac_fenced_journal_restart_action(&record, &action));
	UT_ASSERT_EQ(action, PGRAC_FENCED_JOURNAL_RESTART_RETURN_OFF_BEFORE_REJOIN);
	record.target_state = PGRAC_FENCED_JOURNAL_TARGET_TRANSITIONING;
	UT_ASSERT(!pgrac_fenced_journal_restart_action(&record, &action));
	UT_ASSERT_EQ(action, PGRAC_FENCED_JOURNAL_RESTART_UNAVAILABLE);
}

UT_TEST(test_rotation_seals_exact_name_and_creates_new_active)
{
	PgracFencedJournalScanState state;
	char dir_path[] = "/tmp/pgrac-fenced-rotate.XXXXXX";
	char sealed_name[PGRAC_FENCED_JOURNAL_SEALED_NAME_MAX];
	char sealed_path[MAXPGPATH];
	char active_path[MAXPGPATH];
	struct stat st;
	int dir_fd;
	int active_fd;

	UT_ASSERT_NOT_NULL(mkdtemp(dir_path));
	UT_ASSERT(snprintf(active_path, sizeof(active_path), "%s/%s", dir_path,
		PGRAC_FENCED_JOURNAL_ACTIVE_NAME) > 0);
	active_fd = open(active_path, O_RDWR | O_CREAT | O_EXCL | O_APPEND, 0600);
	UT_ASSERT(active_fd >= 0);
	UT_ASSERT_EQ(ftruncate(active_fd,
		(off_t) PGRAC_FENCED_JOURNAL_SEGMENT_BYTES), 0);
	dir_fd = open(dir_path, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(dir_fd >= 0);

	pgrac_fenced_journal_scan_state_init(&state);
	state.segment_first_seq = 1;
	state.next_seq = UINT64_C(262145);
	state.segment_record_count = PGRAC_FENCED_JOURNAL_SEGMENT_RECORDS;
	state.valid_bytes = PGRAC_FENCED_JOURNAL_SEGMENT_BYTES;
	fill_nonzero(state.previous_record_digest,
			 sizeof(state.previous_record_digest), 0xab);
	UT_ASSERT(pgrac_fenced_journal_rotate_at(dir_fd, &active_fd, 7, &state,
		sealed_name, sizeof(sealed_name)));
	UT_ASSERT_EQ(strcmp(sealed_name,
		"journal.1-262144."
		"abababababababababababababababab"
		"abababababababababababababababab.sealed"), 0);
	UT_ASSERT_EQ(state.segment_record_count, 0);
	UT_ASSERT_EQ(state.segment_first_seq, UINT64_C(262145));
	UT_ASSERT_EQ(state.next_seq, UINT64_C(262145));
	UT_ASSERT(state.available);
	UT_ASSERT(active_fd >= 0);
	UT_ASSERT_EQ(fstat(active_fd, &st), 0);
	UT_ASSERT_EQ(st.st_size, 0);

	UT_ASSERT(snprintf(sealed_path, sizeof(sealed_path), "%s/%s", dir_path,
		sealed_name) > 0);
	UT_ASSERT_EQ(stat(sealed_path, &st), 0);
	UT_ASSERT_EQ(st.st_size, (off_t) PGRAC_FENCED_JOURNAL_SEGMENT_BYTES);

	(void) close(active_fd);
	(void) close(dir_fd);
	(void) unlink(active_path);
	(void) unlink(sealed_path);
	(void) rmdir(dir_path);
}

UT_TEST(test_ninth_rotation_fails_closed_without_rename)
{
	PgracFencedJournalScanState state;
	char sealed_name[PGRAC_FENCED_JOURNAL_SEALED_NAME_MAX];
	int active_fd = -1;

	pgrac_fenced_journal_scan_state_init(&state);
	state.segment_record_count = PGRAC_FENCED_JOURNAL_SEGMENT_RECORDS;
	UT_ASSERT(!pgrac_fenced_journal_rotate_at(-1, &active_fd,
		PGRAC_FENCED_JOURNAL_MAX_SEALED, &state, sealed_name,
		sizeof(sealed_name)));
	UT_ASSERT(!state.available);
}

UT_TEST(test_semantic_config_digest_has_exact_domain_and_length)
{
	static const uint8 expected[PGRAC_FENCED_JOURNAL_DIGEST_BYTES] = {
		0xd4, 0x40, 0x05, 0x32, 0x5d, 0x7a, 0x43, 0xf4,
		0xb1, 0x81, 0xda, 0x56, 0x53, 0x8f, 0xf2, 0xe3,
		0xcc, 0x0a, 0x32, 0xa3, 0xf8, 0x50, 0xfc, 0x66,
		0xbe, 0x0f, 0x80, 0xcd, 0x4d, 0x0f, 0x3c, 0x63
	};
	static const uint8 config[] = "x\n";
	uint8 digest[PGRAC_FENCED_JOURNAL_DIGEST_BYTES];

	UT_ASSERT(pgrac_fenced_journal_config_digest_v1(config,
		sizeof(config) - 1, digest));
	UT_ASSERT(memcmp(digest, expected, sizeof(expected)) == 0);
	UT_ASSERT(!pgrac_fenced_journal_config_digest_v1(NULL, 0, digest));
}

UT_TEST(test_partial_tail_repair_truncates_and_fsyncs_active)
{
	PgracFencedJournalRecordV1 record;
	PgracFencedJournalScanState state;
	uint8 bytes[PGRAC_FENCED_JOURNAL_RECORD_BYTES + 13];
	char path[] = "/tmp/pgrac-fenced-partial.XXXXXX";
	struct stat st;
	int fd;

	make_config_record(&record, 1);
	UT_ASSERT(pgrac_fenced_journal_record_encode(&record, bytes));
	memset(bytes + PGRAC_FENCED_JOURNAL_RECORD_BYTES, 0x55, 13);
	fd = mkstemp(path);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_APPEND), 0);
	UT_ASSERT_EQ(write(fd, bytes, sizeof(bytes)), sizeof(bytes));
	pgrac_fenced_journal_scan_state_init(&state);
	UT_ASSERT_EQ(pgrac_fenced_journal_scan_bytes(bytes, sizeof(bytes), true,
		&state), PGRAC_FENCED_JOURNAL_SCAN_PARTIAL_TAIL);
	UT_ASSERT(pgrac_fenced_journal_repair_partial_tail_fd(fd, &state));
	UT_ASSERT_EQ(fstat(fd, &st), 0);
	UT_ASSERT_EQ(st.st_size, PGRAC_FENCED_JOURNAL_RECORD_BYTES);
	UT_ASSERT(state.available);
	(void) close(fd);
	(void) unlink(path);
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_journal_exact_codec_roundtrip);
	UT_RUN(test_journal_rejects_crc_reserved_unknown_and_bad_proof);
	UT_RUN(test_journal_scan_verifies_seq_and_full_record_hash_chain);
	UT_RUN(test_only_active_final_partial_record_is_truncatable);
	UT_RUN(test_append_advances_only_after_full_write_and_fsync);
	UT_RUN(test_append_requests_rotation_at_exact_segment_limit);
	UT_RUN(test_reconcile_actions_cover_all_durable_record_kinds);
	UT_RUN(test_rotation_seals_exact_name_and_creates_new_active);
	UT_RUN(test_ninth_rotation_fails_closed_without_rename);
	UT_RUN(test_semantic_config_digest_has_exact_domain_and_length);
	UT_RUN(test_partial_tail_repair_truncates_and_fsyncs_active);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
