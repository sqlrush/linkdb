/*-------------------------------------------------------------------------
 *
 * test_cluster_ges_reply_wait.c
 *	  Standalone tests for cooperative reply-wait polling (Spec 8.4A D1).
 *
 * The real reply-wait object runs against a bounded fake dynahash.  Tests pin
 * the consumer-visible contract: pending entries stay installed, delivered
 * verdicts are copied before atomic removal, abandoned/missing entries have
 * distinct outcomes, and every byte of the five-field key participates in
 * correlation.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "cluster/cluster_ges_reply_wait.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_xnode_profile.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"

UT_DEFINE_GLOBALS();


/* ============================================================
 * Minimal backend runtime and profiling stubs.
 * ============================================================ */

bool IsUnderPostmaster = false;
bool cluster_xnode_profile_enabled = false;
ClusterXnodeProfileShared *ClusterXnodeProfileCtl = NULL;
int cluster_ges_reply_wait_max_entries = 1024;

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(), int lineNumber pg_attribute_unused())
{
	abort();
}

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

bool
errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

Size
add_size(Size s1, Size s2)
{
	return s1 + s2;
}

Size
hash_estimate_size(long num_entries, Size entrysize)
{
	return (Size)num_entries * entrysize;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}


/* ============================================================
 * One-lock fake dynahash.
 * ============================================================ */

#define FAKE_REPLY_WAIT_CAP 32

typedef struct FakeReplyWaitHash {
	char entries[FAKE_REPLY_WAIT_CAP][sizeof(GesReplyWaitEntry)];
	long count;
} FakeReplyWaitHash;

static FakeReplyWaitHash fake_hash;
static Size fake_keysize;
static Size fake_entrysize;
static long fake_init_size;
static long fake_max_size;
static int fake_lock_depth;
static int fake_lock_acquires;
static int fake_lock_releases;
static union {
	uint64 force_align;
	char data[4096];
} fake_shared;
static bool fake_shared_found;

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *foundPtr)
{
	Assert(size <= sizeof(fake_shared.data));
	*foundPtr = fake_shared_found;
	fake_shared_found = true;
	return fake_shared.data;
}

HTAB *
ShmemInitHash(const char *name pg_attribute_unused(), long init_size pg_attribute_unused(),
			  long max_size pg_attribute_unused(), HASHCTL *infoP, int hash_flags)
{
	Assert(infoP != NULL);
	Assert((hash_flags & HASH_ELEM) != 0);
	Assert(infoP->keysize == sizeof(GesReplyWaitKey));
	Assert(infoP->entrysize == sizeof(GesReplyWaitEntry));
	fake_keysize = infoP->keysize;
	fake_entrysize = infoP->entrysize;
	fake_init_size = init_size;
	fake_max_size = max_size;
	memset(&fake_hash, 0, sizeof(fake_hash));
	return (HTAB *)&fake_hash;
}

void *
hash_search(HTAB *hashp, const void *keyPtr, HASHACTION action, bool *foundPtr)
{
	FakeReplyWaitHash *hash = (FakeReplyWaitHash *)hashp;
	long i;

	Assert(fake_lock_depth == 1);
	Assert(hash == &fake_hash);
	Assert(keyPtr != NULL);

	for (i = 0; i < hash->count; i++) {
		char *entry = hash->entries[i];

		if (memcmp(entry, keyPtr, fake_keysize) != 0)
			continue;
		if (foundPtr != NULL)
			*foundPtr = true;
		if (action == HASH_REMOVE) {
			if (i + 1 < hash->count)
				memcpy(entry, hash->entries[hash->count - 1], fake_entrysize);
			memset(hash->entries[hash->count - 1], 0xA5, fake_entrysize);
			hash->count--;
		}
		return entry;
	}

	if (foundPtr != NULL)
		*foundPtr = false;
	if (action == HASH_FIND || action == HASH_REMOVE)
		return NULL;
	if ((action == HASH_ENTER || action == HASH_ENTER_NULL)
		&& hash->count < FAKE_REPLY_WAIT_CAP) {
		char *entry = hash->entries[hash->count++];

		memset(entry, 0, fake_entrysize);
		memcpy(entry, keyPtr, fake_keysize);
		return entry;
	}
	return NULL;
}

long
hash_get_num_entries(HTAB *hashp)
{
	Assert(fake_lock_depth == 1);
	Assert(hashp == (HTAB *)&fake_hash);
	return fake_hash.count;
}

void
hash_seq_init(HASH_SEQ_STATUS *status, HTAB *hashp)
{
	Assert(fake_lock_depth == 1);
	status->hashp = hashp;
	status->curBucket = 0;
	status->curEntry = NULL;
}

void *
hash_seq_search(HASH_SEQ_STATUS *status)
{
	FakeReplyWaitHash *hash = (FakeReplyWaitHash *)status->hashp;

	Assert(fake_lock_depth == 1);
	if (status->curBucket >= (uint32)hash->count)
		return NULL;
	return hash->entries[status->curBucket++];
}

void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	Assert(fake_lock_depth == 0);
	Assert(mode == LW_EXCLUSIVE || mode == LW_SHARED);
	fake_lock_depth = 1;
	fake_lock_acquires++;
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{
	Assert(fake_lock_depth == 1);
	fake_lock_depth = 0;
	fake_lock_releases++;
}

void
ConditionVariableInit(ConditionVariable *cv)
{
	memset(cv, 0, sizeof(*cv));
}

void
ConditionVariableBroadcast(ConditionVariable *cv pg_attribute_unused())
{
	Assert(fake_lock_depth == 0);
}


/* ============================================================
 * Fixtures.
 * ============================================================ */

static GesReplyWaitKey
make_key(uint64 request_id, int32 source_node_id, int32 dest_node_id, uint32 request_opcode,
		 uint64 cluster_epoch)
{
	GesReplyWaitKey key;

	memset(&key, 0, sizeof(key));
	key.request_id = request_id;
	key.source_node_id = source_node_id;
	key.dest_node_id = dest_node_id;
	key.request_opcode = request_opcode;
	key.cluster_epoch = cluster_epoch;
	return key;
}

static void
reset_reply_wait_with_cap(int max_entries)
{
	memset(&fake_shared, 0, sizeof(fake_shared));
	fake_shared_found = false;
	fake_keysize = 0;
	fake_entrysize = 0;
	fake_init_size = 0;
	fake_max_size = 0;
	fake_lock_depth = 0;
	fake_lock_acquires = 0;
	fake_lock_releases = 0;
	cluster_ges_reply_wait_max_entries = max_entries;
	cluster_ges_reply_wait_shmem_init();
}

static void
reset_reply_wait(void)
{
	reset_reply_wait_with_cap(1024);
}


/* ============================================================
 * Tests.
 * ============================================================ */

UT_TEST(test_poll_pending_keeps_exact_entry)
{
	GesReplyWaitKey key = make_key(11, 1, 3, 7, 101);
	GesReplyWaitVerdict verdict = { 0xAAAAAAAAU, 0xBBBBBBBBU };
	GesReplyWaitPollResult result;

	reset_reply_wait();
	UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_insert(&key, 9000));
	result = cluster_ges_reply_wait_poll_consume(&key, &verdict);

	UT_ASSERT_EQ(result, GES_REPLY_WAIT_POLL_PENDING);
	UT_ASSERT_EQ(verdict.reply_opcode, 0xAAAAAAAAU);
	UT_ASSERT_EQ(verdict.reject_reason, 0xBBBBBBBBU);
	UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 1);
	UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_lookup(&key));
	UT_ASSERT_EQ(fake_lock_acquires, fake_lock_releases);
}

UT_TEST(test_poll_delivered_copies_complete_verdict_then_consumes)
{
	GesReplyWaitKey key = make_key(12, 1, 3, 7, 102);
	GesReplyWaitVerdict verdict = { 0, 0 };
	GesReplyWaitPollResult result;

	reset_reply_wait();
	UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_insert(&key, 9000));
	UT_ASSERT_EQ(cluster_ges_reply_wait_deliver(&key, 0x12345678U, 0x87654321U),
				 GES_REPLY_DELIVER_WOKE);
	result = cluster_ges_reply_wait_poll_consume(&key, &verdict);

	UT_ASSERT_EQ(result, GES_REPLY_WAIT_POLL_DELIVERED);
	UT_ASSERT_EQ(verdict.reply_opcode, 0x12345678U);
	UT_ASSERT_EQ(verdict.reject_reason, 0x87654321U);
	UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 0);
	UT_ASSERT_NULL(cluster_ges_reply_wait_lookup(&key));
	UT_ASSERT_EQ(fake_lock_acquires, fake_lock_releases);
}

UT_TEST(test_poll_abandoned_is_explicit_and_preserves_tombstone)
{
	GesReplyWaitKey key = make_key(13, 1, 3, 7, 103);
	GesReplyWaitVerdict verdict = { 0xAAAAAAAAU, 0xBBBBBBBBU };
	GesReplyWaitPollResult result;

	reset_reply_wait();
	UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_insert(&key, 9000));
	UT_ASSERT(!cluster_ges_reply_wait_mark_abandoned(&key, 10000));
	result = cluster_ges_reply_wait_poll_consume(&key, &verdict);

	UT_ASSERT_EQ(result, GES_REPLY_WAIT_POLL_ABANDONED);
	UT_ASSERT_EQ(verdict.reply_opcode, 0xAAAAAAAAU);
	UT_ASSERT_EQ(verdict.reject_reason, 0xBBBBBBBBU);
	UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 1);
	UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_lookup(&key));
}

UT_TEST(test_poll_missing_is_explicit_without_output_mutation)
{
	GesReplyWaitKey key = make_key(14, 1, 3, 7, 104);
	GesReplyWaitVerdict verdict = { 0xAAAAAAAAU, 0xBBBBBBBBU };

	reset_reply_wait();
	UT_ASSERT_EQ(cluster_ges_reply_wait_poll_consume(&key, &verdict),
				 GES_REPLY_WAIT_POLL_MISSING);
	UT_ASSERT_EQ(verdict.reply_opcode, 0xAAAAAAAAU);
	UT_ASSERT_EQ(verdict.reject_reason, 0xBBBBBBBBU);
	UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 0);
}

UT_TEST(test_poll_matches_all_five_key_fields)
{
	GesReplyWaitKey base = make_key(20, 1, 3, 7, 200);
	GesReplyWaitKey variants[5];
	GesReplyWaitVerdict verdict;
	int i;

	variants[0] = make_key(21, 1, 3, 7, 200);
	variants[1] = make_key(20, 2, 3, 7, 200);
	variants[2] = make_key(20, 1, 4, 7, 200);
	variants[3] = make_key(20, 1, 3, 8, 200);
	variants[4] = make_key(20, 1, 3, 7, 201);

	reset_reply_wait();
	UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_insert(&base, 9000));
	for (i = 0; i < 5; i++) {
		UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_insert(&variants[i], 9000));
		UT_ASSERT_EQ(cluster_ges_reply_wait_deliver(&variants[i], (uint32)(100 + i),
												(uint32)(200 + i)),
					 GES_REPLY_DELIVER_WOKE);
	}

	verdict.reply_opcode = 0xAAAAAAAAU;
	verdict.reject_reason = 0xBBBBBBBBU;
	UT_ASSERT_EQ(cluster_ges_reply_wait_poll_consume(&base, &verdict),
				 GES_REPLY_WAIT_POLL_PENDING);
	UT_ASSERT_EQ(verdict.reply_opcode, 0xAAAAAAAAU);
	UT_ASSERT_EQ(verdict.reject_reason, 0xBBBBBBBBU);

	for (i = 0; i < 5; i++) {
		memset(&verdict, 0, sizeof(verdict));
		UT_ASSERT_EQ(cluster_ges_reply_wait_poll_consume(&variants[i], &verdict),
					 GES_REPLY_WAIT_POLL_DELIVERED);
		UT_ASSERT_EQ(verdict.reply_opcode, 100 + i);
		UT_ASSERT_EQ(verdict.reject_reason, 200 + i);
	}
	UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 1);
	UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_lookup(&base));
}

UT_TEST(test_configured_cap_controls_shmem_and_live_admission)
{
	GesReplyWaitKey keys[3] = {
		make_key(31, 0, 1, 7, 300),
		make_key(32, 0, 1, 7, 300),
		make_key(33, 0, 1, 7, 300),
	};
	Size size_at_two;
	Size size_at_five;

	cluster_ges_reply_wait_max_entries = 2;
	size_at_two = cluster_ges_reply_wait_shmem_size();
	cluster_ges_reply_wait_max_entries = 5;
	size_at_five = cluster_ges_reply_wait_shmem_size();
	UT_ASSERT_EQ(size_at_five - size_at_two,
				 (Size)(3 * sizeof(GesReplyWaitEntry)));

	reset_reply_wait_with_cap(2);
	UT_ASSERT_EQ(fake_init_size, 2);
	UT_ASSERT_EQ(fake_max_size, 2);
	UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_insert(&keys[0], 9000));
	UT_ASSERT_NOT_NULL(cluster_ges_reply_wait_insert(&keys[1], 9000));
	UT_ASSERT_NULL(cluster_ges_reply_wait_insert(&keys[2], 9000));
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_poll_pending_keeps_exact_entry);
	UT_RUN(test_poll_delivered_copies_complete_verdict_then_consumes);
	UT_RUN(test_poll_abandoned_is_explicit_and_preserves_tombstone);
	UT_RUN(test_poll_missing_is_explicit_without_output_mutation);
	UT_RUN(test_poll_matches_all_five_key_fields);
	UT_RUN(test_configured_cap_controls_shmem_and_live_admission);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
