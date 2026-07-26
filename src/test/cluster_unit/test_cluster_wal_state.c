/*-------------------------------------------------------------------------
 *
 * test_cluster_wal_state.c
 *	  pgrac spec-4.2 D6 — cluster_unit tests for the ClusterWalState
 *	  registry pure helpers (cluster_wal_state.h, header-only inline).
 *
 *	  16 tests covering:
 *	    T1   header sizeof == 512 + offsetof locks (incl. explicit
 *	         _pad_12 at 12..15 -- v0.2 P2)
 *	    T2   slot sizeof == 512 + offsetof locks
 *	    T3   SLOT_OFFSET single-source macro: thread 1 -> 512,
 *	         thread 128 -> 65536, last slot end == FILE_SIZE (v0.2 P0)
 *	    T4   header fill/validate round-trip
 *	    T5   header corruption rejected: crc flip / magic / version /
 *	         slot_count
 *	    T6   slot fill/validate round-trip (owner mode, OK)
 *	    T7   all-zero slot classifies EMPTY
 *	    T8   slot corruption -> CORRUPT: magic / version / crc
 *	    T9   slot self-description mismatch -> CORRUPT (mis-addressed)
 *	    T10  invalid state value -> CORRUPT (0 and 3+; L3 three-band)
 *	    T11  foreign node identity -> FOREIGN (owner mode)
 *	    T12  reader mode (expect_node = -1) accepts any node
 *	    T13  state enum on-disk values locked (ACTIVE=1 / STOPPED=2)
 *	    T14  EMPTY requires the full 512B zero: zeroed fields + body
 *	         garbage -> CORRUPT (round-2 P1, absence-as-proof)
 *	    T15  crc covers tli/lsn/scn fields (flip each -> bad crc)
 *	    T16  first-boot publication is atomic: the final pathname is
 *	         installed from a complete durable temp image with link(2),
 *	         never exposed by O_CREAT before initialisation completes
 *
 *	  Linkage mirrors test_cluster_wal_thread: header-only inclusion +
 *	  libpgcommon/libpgport for pg_crc32c -- no module .o, no stubs.
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-4.2-wal-thread-metadata-catalog.md (FROZEN v1.0)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cluster/cluster_wal_state.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

#define CLUSTER_WAL_STATE_SOURCE_PATH "../../../src/backend/cluster/cluster_wal_state.c"


/* cassert builds pull libpgport objects that reference this. */
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

static char *
read_source(const char *path)
{
	FILE *f;
	long size;
	char *buf;

	f = fopen(path, "rb");
	UT_ASSERT(f != NULL);
	UT_ASSERT_EQ(fseek(f, 0, SEEK_END), 0);
	size = ftell(f);
	UT_ASSERT(size > 0);
	UT_ASSERT_EQ(fseek(f, 0, SEEK_SET), 0);
	buf = malloc((size_t)size + 1);
	UT_ASSERT(buf != NULL);
	UT_ASSERT_EQ((long)fread(buf, 1, (size_t)size, f), size);
	buf[size] = '\0';
	UT_ASSERT_EQ(fclose(f), 0);
	return buf;
}


UT_TEST(test_header_layout_locks)
{
	UT_ASSERT_EQ((int)sizeof(ClusterWalStateHeader), 512);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateHeader, slot_count), 8);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateHeader, _pad_12), 12);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateHeader, created_at), 16);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateHeader, _reserved), 24);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateHeader, crc), 504);
}

UT_TEST(test_slot_layout_locks)
{
	UT_ASSERT_EQ((int)sizeof(ClusterWalStateSlot), 512);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateSlot, thread_id), 6);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateSlot, node_id), 8);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateSlot, state), 12);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateSlot, tli), 16);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateSlot, started_at), 24);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateSlot, last_updated), 32);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateSlot, highest_lsn), 40);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateSlot, highest_scn), 48);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateSlot, crc), 504);
}

/* ---- T3: the single-source offset macro (spec-4.2 v0.2 P0 lock) ---- */
UT_TEST(test_slot_offset_macro_locks)
{
	UT_ASSERT_EQ((long)CLUSTER_WAL_STATE_SLOT_OFFSET(1), 512L);
	UT_ASSERT_EQ((long)CLUSTER_WAL_STATE_SLOT_OFFSET(2), 1024L);
	UT_ASSERT_EQ((long)CLUSTER_WAL_STATE_SLOT_OFFSET(128), 65536L);
	/* last slot ends exactly at the fixed file size: a write through
	 * the macro can never extend the file */
	UT_ASSERT_EQ((long)(CLUSTER_WAL_STATE_SLOT_OFFSET(128) + CLUSTER_WAL_STATE_SLOT_SIZE),
				 (long)CLUSTER_WAL_STATE_FILE_SIZE);
	UT_ASSERT_EQ((int)CLUSTER_WAL_STATE_FILE_SIZE, 66048);
}

UT_TEST(test_header_roundtrip)
{
	ClusterWalStateHeader h;
	const char *reason = (const char *)0x1;

	cluster_wal_state_header_fill(&h, 1234567890LL);
	UT_ASSERT_EQ(h.magic == CLUSTER_WAL_STATE_HEADER_MAGIC, true);
	UT_ASSERT_EQ((int)h.slot_count, 128);
	UT_ASSERT_EQ(cluster_wal_state_header_validate(&h, &reason), true);
	UT_ASSERT_EQ(reason == NULL, true);
}

UT_TEST(test_header_corruption_rejected)
{
	ClusterWalStateHeader h;
	const char *reason = NULL;

	cluster_wal_state_header_fill(&h, 42);
	h.created_at ^= 1;
	UT_ASSERT_EQ(cluster_wal_state_header_validate(&h, &reason), false);
	UT_ASSERT_EQ(strcmp(reason, "bad crc"), 0);

	cluster_wal_state_header_fill(&h, 42);
	h.magic = 0xDEADBEEF;
	UT_ASSERT_EQ(cluster_wal_state_header_validate(&h, &reason), false);
	UT_ASSERT_EQ(strcmp(reason, "bad magic"), 0);

	cluster_wal_state_header_fill(&h, 42);
	h.version = 99;
	UT_ASSERT_EQ(cluster_wal_state_header_validate(&h, &reason), false);
	UT_ASSERT_EQ(strcmp(reason, "bad version"), 0);

	/* slot_count is covered by crc; emulate a v2-style mismatch by
	 * refilling crc over a tampered count */
	cluster_wal_state_header_fill(&h, 42);
	h.slot_count = 64;
	h.crc = cluster_wal_state_block_crc(&h);
	UT_ASSERT_EQ(cluster_wal_state_header_validate(&h, &reason), false);
	UT_ASSERT_EQ(strcmp(reason, "bad slot_count"), 0);
}

UT_TEST(test_slot_roundtrip_owner_ok)
{
	ClusterWalStateSlot s;
	const char *reason = (const char *)0x1;

	cluster_wal_state_slot_fill(&s, 4, 3, CLUSTER_WAL_SLOT_STATE_ACTIVE, 1, 100, 200, 0x1234,
								0x5678);
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, 3, &reason), (int)CLUSTER_WAL_SLOT_OK);
	UT_ASSERT_EQ(reason == NULL, true);
}

UT_TEST(test_slot_empty)
{
	ClusterWalStateSlot s;

	memset(&s, 0, sizeof(s));
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 7, -1, NULL),
				 (int)CLUSTER_WAL_SLOT_EMPTY);
}

UT_TEST(test_slot_corruption_rejected)
{
	ClusterWalStateSlot s;
	const char *reason = NULL;

	cluster_wal_state_slot_fill(&s, 4, 3, CLUSTER_WAL_SLOT_STATE_ACTIVE, 1, 100, 200, 1, 2);
	s.last_updated ^= 1;
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, 3, &reason),
				 (int)CLUSTER_WAL_SLOT_CORRUPT);
	UT_ASSERT_EQ(strcmp(reason, "bad crc"), 0);

	cluster_wal_state_slot_fill(&s, 4, 3, CLUSTER_WAL_SLOT_STATE_ACTIVE, 1, 100, 200, 1, 2);
	s.magic = 0xDEADBEEF;
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, 3, &reason),
				 (int)CLUSTER_WAL_SLOT_CORRUPT);
	UT_ASSERT_EQ(strcmp(reason, "bad magic"), 0);

	cluster_wal_state_slot_fill(&s, 4, 3, CLUSTER_WAL_SLOT_STATE_ACTIVE, 1, 100, 200, 1, 2);
	s.version = 9;
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, 3, &reason),
				 (int)CLUSTER_WAL_SLOT_CORRUPT);
	UT_ASSERT_EQ(strcmp(reason, "bad version"), 0);
}

UT_TEST(test_slot_self_description_mismatch)
{
	ClusterWalStateSlot s;
	const char *reason = NULL;

	/* slot says thread 5 but was read from slot 4: mis-addressed write */
	cluster_wal_state_slot_fill(&s, 5, 3, CLUSTER_WAL_SLOT_STATE_ACTIVE, 1, 100, 200, 1, 2);
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, -1, &reason),
				 (int)CLUSTER_WAL_SLOT_CORRUPT);
	UT_ASSERT_EQ(strcmp(reason, "slot self-description mismatch"), 0);
}

UT_TEST(test_slot_invalid_state_rejected)
{
	ClusterWalStateSlot s;
	const char *reason = NULL;

	cluster_wal_state_slot_fill(&s, 4, 3, 0, 1, 100, 200, 1, 2);
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, 3, &reason),
				 (int)CLUSTER_WAL_SLOT_CORRUPT);
	UT_ASSERT_EQ(strcmp(reason, "invalid state"), 0);

	cluster_wal_state_slot_fill(&s, 4, 3, 3, 1, 100, 200, 1, 2);
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, 3, &reason),
				 (int)CLUSTER_WAL_SLOT_CORRUPT);
	UT_ASSERT_EQ(strcmp(reason, "invalid state"), 0);
}

UT_TEST(test_slot_foreign_identity)
{
	ClusterWalStateSlot s;
	const char *reason = NULL;

	cluster_wal_state_slot_fill(&s, 4, 9, CLUSTER_WAL_SLOT_STATE_ACTIVE, 1, 100, 200, 1, 2);
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, 3, &reason),
				 (int)CLUSTER_WAL_SLOT_FOREIGN);
	UT_ASSERT_EQ(strcmp(reason, "node_id mismatch"), 0);
}

UT_TEST(test_slot_reader_mode_any_node)
{
	ClusterWalStateSlot s;

	cluster_wal_state_slot_fill(&s, 4, 9, CLUSTER_WAL_SLOT_STATE_STOPPED, 1, 100, 200, 1, 2);
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, -1, NULL), (int)CLUSTER_WAL_SLOT_OK);
}

UT_TEST(test_state_enum_on_disk_values)
{
	UT_ASSERT_EQ((int)CLUSTER_WAL_SLOT_STATE_ACTIVE, 1);
	UT_ASSERT_EQ((int)CLUSTER_WAL_SLOT_STATE_STOPPED, 2);
}

/*
 * EMPTY demands the full 512B be zero (spec-4.2 round-2 P1): zeroed
 * magic/version/state/crc glued to body garbage is CORRUPT, not EMPTY.
 */
UT_TEST(test_slot_zero_fields_nonzero_body_is_corrupt)
{
	ClusterWalStateSlot s;
	const char *reason = NULL;

	memset(&s, 0, sizeof(s));
	s.highest_lsn = 1;
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, -1, &reason),
				 (int)CLUSTER_WAL_SLOT_CORRUPT);

	memset(&s, 0, sizeof(s));
	s._reserved[100] = 1;
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, -1, &reason),
				 (int)CLUSTER_WAL_SLOT_CORRUPT);

	memset(&s, 0, sizeof(s));
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, -1, &reason),
				 (int)CLUSTER_WAL_SLOT_EMPTY);
}

UT_TEST(test_crc_covers_watermark_fields)
{
	ClusterWalStateSlot s;
	const char *reason = NULL;

	cluster_wal_state_slot_fill(&s, 4, 3, CLUSTER_WAL_SLOT_STATE_ACTIVE, 1, 100, 200, 1, 2);
	s.tli ^= 1;
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, 3, &reason),
				 (int)CLUSTER_WAL_SLOT_CORRUPT);

	cluster_wal_state_slot_fill(&s, 4, 3, CLUSTER_WAL_SLOT_STATE_ACTIVE, 1, 100, 200, 1, 2);
	s.highest_lsn ^= 1;
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, 3, &reason),
				 (int)CLUSTER_WAL_SLOT_CORRUPT);

	cluster_wal_state_slot_fill(&s, 4, 3, CLUSTER_WAL_SLOT_STATE_ACTIVE, 1, 100, 200, 1, 2);
	s.highest_scn ^= 1;
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, 3, &reason),
				 (int)CLUSTER_WAL_SLOT_CORRUPT);
}

/*
 * A shared first boot is concurrent.  Creating the final path before its
 * 66048-byte image is complete exposes a zero/short/torn registry to peers.
 * Require create-private + durable-write + link(2) no-clobber publication.
 * t/361 supplies the process-level concurrent startup RED/GREEN.
 */
UT_TEST(test_first_boot_registry_publish_is_atomic)
{
	char *source = read_source(CLUSTER_WAL_STATE_SOURCE_PATH);
	char *temp_create = strstr(source, "BasicOpenFile(tmp_path,");
	char *publish = strstr(source, "link(tmp_path, path)");

	UT_ASSERT(temp_create != NULL);
	UT_ASSERT(publish != NULL);
	if (temp_create != NULL && publish != NULL)
		UT_ASSERT(temp_create < publish);
	UT_ASSERT(strstr(source,
					 "BasicOpenFile(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY)")
			  == NULL);
	free(source);
}


int
main(int argc, char **argv)
{
	UT_PLAN(16);

	UT_RUN(test_header_layout_locks);
	UT_RUN(test_slot_layout_locks);
	UT_RUN(test_slot_offset_macro_locks);
	UT_RUN(test_header_roundtrip);
	UT_RUN(test_header_corruption_rejected);
	UT_RUN(test_slot_roundtrip_owner_ok);
	UT_RUN(test_slot_empty);
	UT_RUN(test_slot_corruption_rejected);
	UT_RUN(test_slot_self_description_mismatch);
	UT_RUN(test_slot_invalid_state_rejected);
	UT_RUN(test_slot_foreign_identity);
	UT_RUN(test_slot_reader_mode_any_node);
	UT_RUN(test_state_enum_on_disk_values);
	UT_RUN(test_slot_zero_fields_nonzero_body_is_corrupt);
	UT_RUN(test_crc_covers_watermark_fields);
	UT_RUN(test_first_boot_registry_publish_is_atomic);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
