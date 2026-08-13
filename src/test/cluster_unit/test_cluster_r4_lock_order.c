/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_lock_order.c
 *	  Control-plane wait-for and held-lock policy tests for R4 D13.
 *
 *-------------------------------------------------------------------------
 */
#define USE_CLUSTER_UNIT 1

#include "postgres.h"

#include "access/heapam.h"
#include "access/heaptoast.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/xact.h"
#include "access/tableam.h"
#include "catalog/pg_class.h"
#include "cluster/cluster_cr.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_itl.h"
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_visibility_resolve.h"
#include "common/hashfn.h"
#include "executor/tuptable.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/shmem.h"
#include "utils/datum.h"
#include "utils/expandeddatum.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "cluster_r4_activation_test_stubs.h"
#include "../../backend/access/heap/heapam_r4_private.h"

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

/* Minimal backend boundary for the real executor/heaptuple objects. */
static char ut_memory_context_storage;
MemoryContext CurrentMemoryContext = (MemoryContext)&ut_memory_context_storage;
MemoryContext TopMemoryContext = (MemoryContext)&ut_memory_context_storage;
int NBuffers = 1;
int NLocBuffer = 0;
char *BufferBlocks = NULL;
Block *LocalBufferBlockPointers = NULL;
TransactionId RecentXmin = FirstNormalTransactionId;
int XactIsoLevel = XACT_READ_COMMITTED;
bool cluster_enabled = true;
int cluster_node_id = 0;
bool cluster_recmerge_window_active = false;
uint64 cluster_recmerge_window_scn = 0;
uint64 cluster_recmerge_window_own_lsn = 0;
bool cluster_recmerge_apply_foreign = false;
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

static int ut_alloc_calls;
static int ut_free_calls;
static int ut_invalid_free_calls;
static int ut_buffer_incr_calls;
static int ut_buffer_release_calls;
static void *ut_allocations[16];

/*
 * Authority-boundary fixture for the real scratch-only MVCC evaluator.
 * The product evaluator owns the visibility policy; these stubs provide only
 * the already-approved exact ITL ref and typed origin verdict.
 */
static Page ut_scratch_expected_page;
static Page ut_scratch_forbidden_live_page;
static ClusterUndoTTSlotRef ut_scratch_expected_ref;
static ClusterVisEvidence ut_scratch_resolve_evidence;
static ClusterTTStatus ut_scratch_resolve_status;
static SCN ut_scratch_resolve_scn;
static SCN ut_scratch_expected_read_scn;
static XLogRecPtr ut_scratch_expected_lsn;
static TransactionId ut_scratch_expected_xid;
static bool ut_scratch_ref_available;
static int ut_scratch_ref_calls;
static int ut_scratch_exact_resolve_calls;
static int ut_scratch_live_resolve_calls;
static int ut_scratch_cr_calls;
static int ut_scratch_ssi_calls;
static int ut_scratch_hint_calls;
static int ut_scratch_dirty_calls;
static int ut_live_visibility_calls;
static Page ut_hot_live_ref_page;
static ClusterUndoTTSlotRef ut_hot_live_ref;
static int ut_hot_live_ref_calls;
static bool ut_hot_content_lock_held;
static bool ut_hot_production_core_active;

bool
cluster_itl_get_tt_ref(Page page, uint8 itl_slot_idx,
						   ClusterUndoTTSlotRef *ref)
{
	if (page == ut_hot_live_ref_page)
	{
		ut_hot_live_ref_calls++;
		UT_ASSERT_EQ(itl_slot_idx, 2);
		*ref = ut_hot_live_ref;
		return true;
	}

	ut_scratch_ref_calls++;
	UT_ASSERT(page == ut_scratch_expected_page);
	UT_ASSERT(page != ut_scratch_forbidden_live_page);
	UT_ASSERT_EQ(itl_slot_idx, 1);
	if (!ut_scratch_ref_available)
		return false;
	*ref = ut_scratch_expected_ref;
	return true;
}

void
cluster_visibility_resolve_from_ref_scn(TransactionId raw_xid,
										const ClusterUndoTTSlotRef *ref,
										XLogRecPtr anchor_lsn, SCN read_scn,
										ClusterVisResolve *out)
{
	ut_scratch_exact_resolve_calls++;
	if (ut_hot_production_core_active)
		UT_ASSERT(!ut_hot_content_lock_held);
	UT_ASSERT_EQ(raw_xid, ut_scratch_expected_xid);
	UT_ASSERT(memcmp(ref, &ut_scratch_expected_ref, sizeof(*ref)) == 0);
	UT_ASSERT_EQ((uint64)anchor_lsn, (uint64)ut_scratch_expected_lsn);
	UT_ASSERT_EQ((uint64)read_scn, (uint64)ut_scratch_expected_read_scn);
	memset(out, 0, sizeof(*out));
	out->evidence = ut_scratch_resolve_evidence;
	out->status = ut_scratch_resolve_status;
	out->commit_scn = ut_scratch_resolve_scn;
}

void
cluster_visibility_resolve_tuple_scn(Buffer buffer pg_attribute_unused(),
								 HeapTupleHeader tuple pg_attribute_unused(),
								 TransactionId raw_xid pg_attribute_unused(),
								 ClusterVisXidKind which pg_attribute_unused(),
								 SCN read_scn pg_attribute_unused(),
								 ClusterVisResolve *out)
{
	ut_scratch_live_resolve_calls++;
	memset(out, 0, sizeof(*out));
}

ClusterCrVerdict
cluster_cr_satisfies_mvcc(HeapTuple htup pg_attribute_unused(),
						  Snapshot snapshot pg_attribute_unused(),
						  Buffer buffer pg_attribute_unused(),
						  bool *visible pg_attribute_unused())
{
	ut_scratch_cr_calls++;
	return CLUSTER_CR_FAILCLOSED;
}

void
cluster_heap_test_r4_conflict_out(bool visible pg_attribute_unused(),
								 Relation relation pg_attribute_unused(),
								 HeapTuple tuple pg_attribute_unused(),
								 Buffer buffer pg_attribute_unused(),
								 Snapshot snapshot pg_attribute_unused())
{
	ut_scratch_ssi_calls++;
}

void
PredicateLockTID(Relation relation pg_attribute_unused(),
				 ItemPointer tid pg_attribute_unused(),
				 Snapshot snapshot pg_attribute_unused(),
				 TransactionId xid pg_attribute_unused())
{
	ut_scratch_ssi_calls++;
}

bool
cluster_heap_test_r4_live_visibility(HeapTuple tuple pg_attribute_unused(),
									 Snapshot snapshot pg_attribute_unused(),
									 Buffer buffer pg_attribute_unused())
{
	ut_live_visibility_calls++;
	return true;
}

void
HeapTupleSetHintBits(HeapTupleHeader tuple pg_attribute_unused(),
					 Buffer buffer pg_attribute_unused(),
					 uint16 infomask pg_attribute_unused(),
					 TransactionId xid pg_attribute_unused())
{
	ut_scratch_hint_calls++;
}

void
MarkBufferDirty(Buffer buffer pg_attribute_unused())
{
	ut_scratch_dirty_calls++;
}

GlobalVisState *
GlobalVisTestFor(Relation relation pg_attribute_unused())
{
	return NULL;
}

bool
cluster_vis_prune_must_defer(bool storage_mode pg_attribute_unused(),
							bool cluster_horizon_available pg_attribute_unused())
{
	return true;
}

bool
cluster_heap_test_r4_surely_dead(HeapTuple tuple pg_attribute_unused(),
								 GlobalVisState *vistest pg_attribute_unused())
{
	return false;
}

int
GetMultiXactIdMembers(MultiXactId multi pg_attribute_unused(),
					  MultiXactMember **members pg_attribute_unused(),
					  bool from_pgupgrade pg_attribute_unused(),
					  bool isLockOnly pg_attribute_unused())
{
	return -1;
}

int
scn_time_cmp(SCN a, SCN b)
{
	return a < b ? -1 : a > b ? 1 : 0;
}

static void *
ut_alloc(Size size, bool zero)
{
	void *ptr = zero ? calloc(1, size) : malloc(size);

	if (ptr == NULL)
		abort();
	if (ut_alloc_calls >= lengthof(ut_allocations))
		abort();
	ut_allocations[ut_alloc_calls] = ptr;
	ut_alloc_calls++;
	return ptr;
}

void *
palloc(Size size)
{
	return ut_alloc(size, false);
}

void *
palloc0(Size size)
{
	return ut_alloc(size, true);
}

void *
MemoryContextAlloc(MemoryContext context pg_attribute_unused(), Size size)
{
	return ut_alloc(size, false);
}

void *
MemoryContextAllocZero(MemoryContext context pg_attribute_unused(), Size size)
{
	return ut_alloc(size, true);
}

void *
MemoryContextAllocZeroAligned(MemoryContext context pg_attribute_unused(), Size size)
{
	return ut_alloc(size, true);
}

void
pfree(void *pointer)
{
	if (pointer != NULL)
	{
		int i;

		for (i = 0; i < ut_alloc_calls; i++)
		{
			if (ut_allocations[i] == pointer)
			{
				ut_allocations[i] = NULL;
				ut_free_calls++;
				free(pointer);
				return;
			}
		}
		ut_invalid_free_calls++;
	}
}

void
IncrBufferRefCount(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	ut_buffer_incr_calls++;
}

void
ReleaseBuffer(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	ut_buffer_release_calls++;
}

void
IncrTupleDescRefCount(TupleDesc tupdesc pg_attribute_unused())
{}

void
DecrTupleDescRefCount(TupleDesc tupdesc pg_attribute_unused())
{}

ExpandedObjectHeader *
DatumGetEOHP(Datum datum pg_attribute_unused())
{
	return NULL;
}

Size
EOH_get_flat_size(ExpandedObjectHeader *eohptr pg_attribute_unused())
{
	return 0;
}

void
EOH_flatten_into(ExpandedObjectHeader *eohptr pg_attribute_unused(),
				 void *result pg_attribute_unused(),
				 Size allocated_size pg_attribute_unused())
{}

Datum
datumCopy(Datum value, bool typByVal pg_attribute_unused(),
		  int typLen pg_attribute_unused())
{
	return value;
}

uint32
hash_bytes(const unsigned char *key pg_attribute_unused(),
		   int keylen pg_attribute_unused())
{
	return 0;
}

HTAB *
hash_create(const char *tabname pg_attribute_unused(),
			long nelem pg_attribute_unused(),
			const HASHCTL *info pg_attribute_unused(),
			int flags pg_attribute_unused())
{
	return NULL;
}

void *
hash_search(HTAB *hashp pg_attribute_unused(),
			const void *keyPtr pg_attribute_unused(),
			HASHACTION action pg_attribute_unused(),
			bool *foundPtr pg_attribute_unused())
{
	return NULL;
}

Datum
toast_flatten_tuple_to_datum(HeapTupleHeader tuple pg_attribute_unused(),
							 uint32 tuple_len pg_attribute_unused(),
							 TupleDesc tuple_desc pg_attribute_unused())
{
	return (Datum)0;
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

const char *
cluster_cr_build_reason_name(ClusterCrBuildReason reason pg_attribute_unused())
{
	return "unit-test";
}

void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

#define DEFINE_WAIT_ALLOWED_TEST(test_name, edge_value)                                            \
	UT_TEST(test_name)                                                                             \
	{                                                                                              \
		UT_ASSERT(semantic_activation_control_wait_allowed((edge_value),                           \
														   SEMANTIC_ACTIVATION_HELD_NONE));        \
	}

#define DEFINE_WAIT_FORBIDDEN_TEST(test_name, edge_value, lock_value)                              \
	UT_TEST(test_name)                                                                             \
	{                                                                                              \
		UT_ASSERT(!semantic_activation_control_wait_allowed((edge_value), (lock_value)));          \
	}

UT_TEST(test_01_held_lock_bits_are_independent)
{
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_HELD_RESOURCE, UINT32_C(1));
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_HELD_BUFFER, UINT32_C(2));
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_HELD_SLRU, UINT32_C(4));
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_HELD_UNDO_IO, UINT32_C(8));
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_HELD_IC_DISPATCH, UINT32_C(16));
}

UT_TEST(test_02_wait_edge_values_are_closed)
{
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON, 0);
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC, 1);
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_WAIT_LMON_TO_PEER_ACK, 2);
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER, 3);
}

DEFINE_WAIT_ALLOWED_TEST(test_03_utility_to_lmon_wait_with_no_lock_is_allowed,
						 SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON)
DEFINE_WAIT_FORBIDDEN_TEST(test_04_utility_wait_rejects_resource_lock,
						   SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON,
						   SEMANTIC_ACTIVATION_HELD_RESOURCE)
DEFINE_WAIT_FORBIDDEN_TEST(test_05_utility_wait_rejects_buffer_lock,
						   SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON,
						   SEMANTIC_ACTIVATION_HELD_BUFFER)
DEFINE_WAIT_FORBIDDEN_TEST(test_06_utility_wait_rejects_slru_lock,
						   SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON, SEMANTIC_ACTIVATION_HELD_SLRU)
DEFINE_WAIT_FORBIDDEN_TEST(test_07_utility_wait_rejects_undo_io_ownership,
						   SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON,
						   SEMANTIC_ACTIVATION_HELD_UNDO_IO)
DEFINE_WAIT_FORBIDDEN_TEST(test_08_utility_wait_rejects_ic_dispatch_ownership,
						   SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON,
						   SEMANTIC_ACTIVATION_HELD_IC_DISPATCH)
DEFINE_WAIT_FORBIDDEN_TEST(test_09_utility_wait_rejects_combined_forbidden_locks,
						   SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON,
						   SEMANTIC_ACTIVATION_HELD_RESOURCE | SEMANTIC_ACTIVATION_HELD_BUFFER
							   | SEMANTIC_ACTIVATION_HELD_SLRU)

DEFINE_WAIT_ALLOWED_TEST(test_10_lmon_to_qvotec_wait_with_no_lock_is_allowed,
						 SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC)
DEFINE_WAIT_FORBIDDEN_TEST(test_11_qvotec_wait_rejects_resource_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC,
						   SEMANTIC_ACTIVATION_HELD_RESOURCE)
DEFINE_WAIT_FORBIDDEN_TEST(test_12_qvotec_wait_rejects_buffer_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC, SEMANTIC_ACTIVATION_HELD_BUFFER)
DEFINE_WAIT_FORBIDDEN_TEST(test_13_qvotec_wait_rejects_slru_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC, SEMANTIC_ACTIVATION_HELD_SLRU)
DEFINE_WAIT_FORBIDDEN_TEST(test_14_qvotec_wait_rejects_undo_io_ownership,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC,
						   SEMANTIC_ACTIVATION_HELD_UNDO_IO)
DEFINE_WAIT_FORBIDDEN_TEST(test_15_qvotec_wait_rejects_ic_dispatch_ownership,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC,
						   SEMANTIC_ACTIVATION_HELD_IC_DISPATCH)
DEFINE_WAIT_FORBIDDEN_TEST(test_16_qvotec_wait_rejects_all_forbidden_locks,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC,
						   SEMANTIC_ACTIVATION_HELD_ALL_FORBIDDEN)

DEFINE_WAIT_ALLOWED_TEST(test_17_peer_ack_wait_with_no_lock_is_allowed,
						 SEMANTIC_ACTIVATION_WAIT_LMON_TO_PEER_ACK)
DEFINE_WAIT_FORBIDDEN_TEST(test_18_peer_ack_wait_rejects_resource_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_PEER_ACK,
						   SEMANTIC_ACTIVATION_HELD_RESOURCE)
DEFINE_WAIT_FORBIDDEN_TEST(test_19_peer_ack_wait_rejects_buffer_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_PEER_ACK,
						   SEMANTIC_ACTIVATION_HELD_BUFFER)
DEFINE_WAIT_FORBIDDEN_TEST(test_20_peer_ack_wait_rejects_slru_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_PEER_ACK, SEMANTIC_ACTIVATION_HELD_SLRU)
DEFINE_WAIT_FORBIDDEN_TEST(test_21_peer_ack_wait_rejects_undo_io_ownership,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_PEER_ACK,
						   SEMANTIC_ACTIVATION_HELD_UNDO_IO)
DEFINE_WAIT_FORBIDDEN_TEST(test_22_peer_ack_wait_rejects_ic_dispatch_ownership,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_PEER_ACK,
						   SEMANTIC_ACTIVATION_HELD_IC_DISPATCH)

DEFINE_WAIT_ALLOWED_TEST(test_23_control_barrier_with_no_lock_is_allowed,
						 SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER)
DEFINE_WAIT_FORBIDDEN_TEST(test_24_control_barrier_rejects_resource_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER,
						   SEMANTIC_ACTIVATION_HELD_RESOURCE)
DEFINE_WAIT_FORBIDDEN_TEST(test_25_control_barrier_rejects_buffer_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER,
						   SEMANTIC_ACTIVATION_HELD_BUFFER)
DEFINE_WAIT_FORBIDDEN_TEST(test_26_control_barrier_rejects_slru_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER,
						   SEMANTIC_ACTIVATION_HELD_SLRU)
DEFINE_WAIT_FORBIDDEN_TEST(test_27_control_barrier_rejects_undo_io_ownership,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER,
						   SEMANTIC_ACTIVATION_HELD_UNDO_IO)
DEFINE_WAIT_FORBIDDEN_TEST(test_28_control_barrier_rejects_ic_dispatch_ownership,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER,
						   SEMANTIC_ACTIVATION_HELD_IC_DISPATCH)

UT_TEST(test_29_process_utility_may_only_wait_on_lmon)
{
	UT_ASSERT(semantic_activation_actor_edge_allowed(SEMANTIC_ACTIVATION_ACTOR_PROCESS_UTILITY,
													 SEMANTIC_ACTIVATION_ACTOR_LMON));
}

UT_TEST(test_30_lmon_may_only_delegate_durable_io_to_qvotec)
{
	UT_ASSERT(semantic_activation_actor_edge_allowed(SEMANTIC_ACTIVATION_ACTOR_LMON,
													 SEMANTIC_ACTIVATION_ACTOR_QVOTEC));
}

UT_TEST(test_31_lmon_control_path_never_enters_holder_lms)
{
	UT_ASSERT(!semantic_activation_actor_edge_allowed(SEMANTIC_ACTIVATION_ACTOR_LMON,
													  SEMANTIC_ACTIVATION_ACTOR_LMS));
}

UT_TEST(test_32_qvotec_completion_never_enters_origin_data)
{
	UT_ASSERT(!semantic_activation_actor_edge_allowed(SEMANTIC_ACTIVATION_ACTOR_QVOTEC,
													  SEMANTIC_ACTIVATION_ACTOR_DATA));
}

#define UT_HOT_BLOCK ((BlockNumber)17)
#define UT_HOT_ROOT_OFF FirstOffsetNumber
#define UT_HOT_TUPLE_LEN 64
#define UT_HOT_DATA_OFF (BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE - UT_HOT_TUPLE_LEN)
#define UT_HOT_CONTENT_SHARE UINT32_C(0x01)
#define UT_HOT_BUFFER_HEADER UINT32_C(0x02)
#define UT_HOT_GRD UINT32_C(0x04)
#define UT_HOT_SLRU UINT32_C(0x08)
#define UT_HOT_FORBIDDEN_LOCKS \
	(UT_HOT_CONTENT_SHARE | UT_HOT_BUFFER_HEADER | UT_HOT_GRD | UT_HOT_SLRU)
#define UT_HOT_BUFFER 1
#define UT_HOT_LIVE_XMIN ((TransactionId)900)
#define UT_HOT_FULL_XMIN ((TransactionId)700)
#define UT_HOT_READ_SCN ((SCN)UINT64_C(0x123456))
#define UT_HOT_TABLE_OID ((Oid)4242)
#define UT_HOT_PAYLOAD ((unsigned char)0x3c)

typedef struct UtR4HotLifecycleFixture {
	char live_page[BLCKSZ] pg_attribute_aligned(MAXIMUM_ALIGNOF);
	char full_source[BLCKSZ] pg_attribute_aligned(MAXIMUM_ALIGNOF);
	BufferTag expected_tag;
	ItemPointerData expected_root;
	SCN expected_read_scn;
	char *expected_scratch_page;
	HeapTuple expected_result_tuple;
	uint32 held_locks;
	bool live_poisoned;
	bool full_source_poisoned;
	int event;
	int unlock_calls;
	int fetch_calls;
	int scratch_search_calls;
	int relock_calls;
} UtR4HotLifecycleFixture;

typedef struct UtR4HotProductFixture
{
	char live_page[BLCKSZ] pg_attribute_aligned(MAXIMUM_ALIGNOF);
	char full_source[BLCKSZ] pg_attribute_aligned(MAXIMUM_ALIGNOF);
	BufferTag expected_tag;
	HeapHotSearchResult *expected_result;
	SCN expected_read_scn;
	int lock_modes[5];
	int lock_calls;
	int fetch_calls;
	bool mutate_non_target_itl;
	bool fail_first_after_mutation;
	bool live_poisoned;
	bool full_source_poisoned;
} UtR4HotProductFixture;

static UtR4HotProductFixture *ut_hot_product_fixture;

static HeapTupleHeader ut_r4_hot_tuple_at(Page page, OffsetNumber offnum);

BlockNumber
BufferGetBlockNumber(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, UT_HOT_BUFFER);
	UT_ASSERT_NOT_NULL(ut_hot_product_fixture);
	return UT_HOT_BLOCK;
}

void
BufferGetTag(Buffer buffer, RelFileLocator *rlocator,
			 ForkNumber *forknum, BlockNumber *blocknum)
{
	UT_ASSERT_EQ(buffer, UT_HOT_BUFFER);
	UT_ASSERT_NOT_NULL(ut_hot_product_fixture);
	rlocator->spcOid = ut_hot_product_fixture->expected_tag.spcOid;
	rlocator->dbOid = ut_hot_product_fixture->expected_tag.dbOid;
	rlocator->relNumber = ut_hot_product_fixture->expected_tag.relNumber;
	*forknum = ut_hot_product_fixture->expected_tag.forkNum;
	*blocknum = ut_hot_product_fixture->expected_tag.blockNum;
}

void
LockBuffer(Buffer buffer, int mode)
{
	UtR4HotProductFixture *fixture = ut_hot_product_fixture;

	UT_ASSERT_EQ(buffer, UT_HOT_BUFFER);
	UT_ASSERT_NOT_NULL(fixture);
	if (fixture == NULL)
		return;
	UT_ASSERT(fixture->lock_calls < lengthof(fixture->lock_modes));
	if (fixture->lock_calls < lengthof(fixture->lock_modes))
		fixture->lock_modes[fixture->lock_calls] = mode;
	fixture->lock_calls++;

	if (mode == BUFFER_LOCK_UNLOCK)
	{
		UT_ASSERT(ut_hot_content_lock_held);
		ut_hot_content_lock_held = false;
	}
	else if (mode == BUFFER_LOCK_SHARE)
	{
		UT_ASSERT(!ut_hot_content_lock_held);
		ut_hot_content_lock_held = true;
	}
	else
		UT_ASSERT(false);
}

ClusterCrBuildResult
cluster_gcs_block_cr_fetch_and_wait(BufferTag tag, SCN read_scn,
									char dst_page[BLCKSZ],
									ClusterCrBuildReason *reason_out)
{
	UtR4HotProductFixture *fixture = ut_hot_product_fixture;

	UT_ASSERT_NOT_NULL(fixture);
	if (fixture == NULL)
		return CLUSTER_CR_BUILD_FAIL_CLOSED;
	fixture->fetch_calls++;
	UT_ASSERT(!ut_hot_content_lock_held);
	UT_ASSERT(BufferTagsEqual(&tag, &fixture->expected_tag));
	UT_ASSERT_EQ((uint64) read_scn, (uint64) fixture->expected_read_scn);
	UT_ASSERT(dst_page == fixture->expected_result->scratch_page);
	UT_ASSERT(dst_page != fixture->live_page);
	UT_ASSERT(dst_page != fixture->full_source);

	memcpy(dst_page, fixture->full_source, BLCKSZ);
	if (fixture->fetch_calls == 1)
	{
		if (fixture->mutate_non_target_itl)
		{
			ClusterItlSlotData *non_target_slot
				= &ClusterPageGetItlSlots((Page) fixture->live_page)[0];

			non_target_slot->wrap++;
		}
		else
		{
			HeapTupleHeader live_tuple = ut_r4_hot_tuple_at(
				(Page) fixture->live_page, UT_HOT_ROOT_OFF);

			live_tuple->t_infomask2 ^= HEAP_KEYS_UPDATED;
		}
		fixture->live_poisoned = true;
		if (fixture->fail_first_after_mutation)
		{
			*reason_out = CLUSTER_CR_BUILD_IO_ERROR;
			return CLUSTER_CR_BUILD_FAIL_CLOSED;
		}
	}
	else
	{
		memset(fixture->full_source, 0x5a, BLCKSZ);
		fixture->full_source_poisoned = true;
	}
	*reason_out = CLUSTER_CR_BUILD_NONE;
	return CLUSTER_CR_BUILD_FULL;
}

static HeapTupleHeader
ut_r4_hot_tuple_at(Page page, OffsetNumber offnum)
{
	ItemId item_id = PageGetItemId(page, offnum);

	return (HeapTupleHeader)PageGetItem(page, item_id);
}

static void
ut_r4_hot_set_tuple(HeapTupleHeader tuple, TransactionId xmin,
					uint8 itl_slot_index, unsigned char payload)
{
	memset(tuple, 0, UT_HOT_TUPLE_LEN);
	HeapTupleHeaderSetXmin(tuple, xmin);
	HeapTupleHeaderSetXmax(tuple, InvalidTransactionId);
	tuple->t_infomask = HEAP_XMIN_COMMITTED | HEAP_XMAX_INVALID;
	tuple->t_infomask2 = 0;
	tuple->t_hoff = SizeofHeapTupleHeader;
	tuple->t_itl_slot_idx = itl_slot_index;
	ItemPointerSet(&tuple->t_ctid, UT_HOT_BLOCK, UT_HOT_ROOT_OFF);
	memset((char *)tuple + tuple->t_hoff, payload,
		   UT_HOT_TUPLE_LEN - tuple->t_hoff);
}

static Page
ut_r4_hot_build_page(char storage[BLCKSZ], TransactionId xmin,
					 uint8 itl_slot_index, TransactionId itl_xid,
					 uint16 itl_wrap, unsigned char payload)
{
	Page page = (Page)storage;
	PageHeader header;
	ItemId item_id;
	ClusterItlSlotData *slot;

	memset(storage, 0, BLCKSZ);
	header = (PageHeader)page;
	header->pd_flags = PD_HAS_ITL;
	header->pd_special = (LocationIndex)(BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE);
	header->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	header->pd_lower = SizeOfPageHeaderData + sizeof(ItemIdData);
	header->pd_upper = (LocationIndex)UT_HOT_DATA_OFF;
	item_id = PageGetItemId(page, UT_HOT_ROOT_OFF);
	ItemIdSetNormal(item_id, UT_HOT_DATA_OFF, UT_HOT_TUPLE_LEN);
	ut_r4_hot_set_tuple((HeapTupleHeader)(storage + UT_HOT_DATA_OFF),
						xmin, itl_slot_index, payload);
	slot = &ClusterPageGetItlSlots(page)[itl_slot_index];
	slot->xid = itl_xid;
	slot->wrap = itl_wrap;
	slot->flags = ITL_FLAG_ACTIVE;
	return page;
}

static void
ut_r4_hot_init_product_fixture(UtR4HotProductFixture *fixture,
								HeapHotSearchResult *result)
{
	RelFileLocator locator = {
		.spcOid = 1663,
		.dbOid = 5,
		.relNumber = 9001,
	};
	HeapTupleHeader full_tuple;

	memset(fixture, 0, sizeof(*fixture));
	memset(result, 0, sizeof(*result));
	(void) ut_r4_hot_build_page(fixture->live_page, UT_HOT_LIVE_XMIN, 2,
									UT_HOT_LIVE_XMIN, 7, 0x77);
	(void) ut_r4_hot_build_page(fixture->full_source, UT_HOT_FULL_XMIN, 1,
									UT_HOT_FULL_XMIN, 4, UT_HOT_PAYLOAD);
	full_tuple = ut_r4_hot_tuple_at((Page) fixture->full_source,
									UT_HOT_ROOT_OFF);
	full_tuple->t_infomask = HEAP_XMAX_INVALID;
	PageSetLSN((Page) fixture->full_source, UINT64_C(0x123450));
	InitBufferTag(&fixture->expected_tag, &locator, MAIN_FORKNUM, UT_HOT_BLOCK);
	fixture->expected_result = result;
	fixture->expected_read_scn = UT_HOT_READ_SCN;

	memset(&ut_hot_live_ref, 0, sizeof(ut_hot_live_ref));
	ut_hot_live_ref.origin_node_id = 1;
	ut_hot_live_ref.undo_segment_id = 257;
	ut_hot_live_ref.tt_slot_id = 7;
	ut_hot_live_ref.cluster_epoch = 9;
	ut_hot_live_ref.local_xid = UT_HOT_LIVE_XMIN + 1;
	ut_hot_live_ref_page = (Page) fixture->live_page;
	ut_hot_live_ref_calls = 0;
	ut_hot_product_fixture = fixture;
	ut_hot_content_lock_held = true;
	ut_hot_production_core_active = true;
	BufferBlocks = fixture->live_page;
}

static void
ut_r4_hot_poison_live(UtR4HotLifecycleFixture *fixture)
{
	memset(fixture->live_page, 0xa5, BLCKSZ);
	fixture->live_poisoned = true;
}

static void
ut_r4_hot_content_share(void *arg, bool acquire)
{
	UtR4HotLifecycleFixture *fixture = (UtR4HotLifecycleFixture *)arg;

	if (acquire)
	{
		UT_ASSERT_EQ(fixture->event, 3);
		UT_ASSERT_EQ(fixture->held_locks & UT_HOT_FORBIDDEN_LOCKS, 0);
		ut_r4_hot_poison_live(fixture);
		fixture->held_locks |= UT_HOT_CONTENT_SHARE;
		fixture->relock_calls++;
		fixture->event = 4;
	}
	else
	{
		UT_ASSERT_EQ(fixture->event, 0);
		UT_ASSERT_EQ(fixture->held_locks & UT_HOT_FORBIDDEN_LOCKS,
					 UT_HOT_CONTENT_SHARE);
		fixture->held_locks &= ~UT_HOT_CONTENT_SHARE;
		fixture->unlock_calls++;
		fixture->event = 1;
	}
}

static bool
ut_r4_hot_fetch_full(void *arg, const BufferTag *tag, SCN read_scn,
					 char dst_page[BLCKSZ])
{
	UtR4HotLifecycleFixture *fixture = (UtR4HotLifecycleFixture *)arg;

	UT_ASSERT_EQ(fixture->event, 1);
	fixture->fetch_calls++;
	UT_ASSERT_EQ(fixture->held_locks & UT_HOT_FORBIDDEN_LOCKS, 0);
	UT_ASSERT(BufferTagsEqual(tag, &fixture->expected_tag));
	UT_ASSERT_EQ((uint64)read_scn, (uint64)fixture->expected_read_scn);
	UT_ASSERT(dst_page == fixture->expected_scratch_page);
	UT_ASSERT(dst_page != fixture->full_source);
	UT_ASSERT(dst_page != fixture->live_page);

	memcpy(dst_page, fixture->full_source, BLCKSZ);
	memset(fixture->full_source, 0x5a, BLCKSZ);
	fixture->full_source_poisoned = true;
	ut_r4_hot_poison_live(fixture);
	fixture->event = 2;
	return true;
}

static bool
ut_r4_hot_search_scratch(void *arg,
						 const ClusterR4HotScratchTestContext *context,
						 HeapTuple scratch_tuple)
{
	UtR4HotLifecycleFixture *fixture = (UtR4HotLifecycleFixture *)arg;
	ItemId item_id;

	UT_ASSERT_EQ(fixture->event, 2);
	UT_ASSERT_EQ(fixture->held_locks & UT_HOT_FORBIDDEN_LOCKS, 0);
	UT_ASSERT(fixture->live_poisoned);
	UT_ASSERT(fixture->full_source_poisoned);
	UT_ASSERT(context->scratch_page == (Page)fixture->expected_scratch_page);
	UT_ASSERT(BufferTagsEqual(&context->tag, &fixture->expected_tag));
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(&context->logical_root),
				 UT_HOT_BLOCK);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&context->logical_root),
				 UT_HOT_ROOT_OFF);
	UT_ASSERT_EQ((uint64)context->read_scn,
				 (uint64)fixture->expected_read_scn);
	UT_ASSERT(context->already_full);
	UT_ASSERT(!context->allow_hint);
	UT_ASSERT(!context->allow_cleanout);
	UT_ASSERT(scratch_tuple == fixture->expected_result_tuple);

	item_id = PageGetItemId(context->scratch_page, UT_HOT_ROOT_OFF);
	UT_ASSERT(ItemIdIsNormal(item_id));
	scratch_tuple->t_data =
		(HeapTupleHeader)PageGetItem(context->scratch_page, item_id);
	scratch_tuple->t_len = ItemIdGetLength(item_id);
	ItemPointerSet(&scratch_tuple->t_self, UT_HOT_BLOCK, UT_HOT_ROOT_OFF);
	scratch_tuple->t_tableOid = UT_HOT_TABLE_OID;
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(scratch_tuple->t_data),
				 UT_HOT_FULL_XMIN);
	UT_ASSERT_EQ(*((unsigned char *)scratch_tuple->t_data +
					 scratch_tuple->t_data->t_hoff), UT_HOT_PAYLOAD);

	fixture->scratch_search_calls++;
	fixture->event = 3;
	return true;
}

static void
ut_r4_hot_init_fixture(UtR4HotLifecycleFixture *fixture,
					   HeapHotSearchResult *result)
{
	RelFileLocator locator = {
		.spcOid = 1663,
		.dbOid = 5,
		.relNumber = 9001,
	};

	memset(fixture, 0, sizeof(*fixture));
	memset(result, 0, sizeof(*result));
	(void)ut_r4_hot_build_page(fixture->live_page, UT_HOT_LIVE_XMIN, 2,
							 UT_HOT_LIVE_XMIN, 7, 0x77);
	(void)ut_r4_hot_build_page(fixture->full_source, UT_HOT_FULL_XMIN, 1,
							 UT_HOT_FULL_XMIN, 4, UT_HOT_PAYLOAD);
	InitBufferTag(&fixture->expected_tag, &locator, MAIN_FORKNUM, UT_HOT_BLOCK);
	ItemPointerSet(&fixture->expected_root, UT_HOT_BLOCK, UT_HOT_ROOT_OFF);
	fixture->expected_read_scn = UT_HOT_READ_SCN;
	fixture->expected_scratch_page = result->scratch_page;
	fixture->expected_result_tuple = &result->tuple;
	fixture->held_locks = UT_HOT_CONTENT_SHARE;
}

static TupleTableSlot *
ut_r4_hot_make_slot(TupleDescData *tuple_desc)
{
	memset(tuple_desc, 0, sizeof(*tuple_desc));
	tuple_desc->natts = 0;
	tuple_desc->tdrefcount = -1;
	return MakeSingleTupleTableSlot(tuple_desc, &TTSOpsBufferHeapTuple);
}

static void
ut_r4_hot_reset_resources(void)
{
	memset(ut_allocations, 0, sizeof(ut_allocations));
	ut_alloc_calls = 0;
	ut_free_calls = 0;
	ut_invalid_free_calls = 0;
	ut_buffer_incr_calls = 0;
	ut_buffer_release_calls = 0;
}

UT_TEST(test_33_buffer_backed_result_keeps_real_buffer_pin)
{
	HeapHotSearchResult result;
	TupleDescData tuple_desc;
	TupleTableSlot *slot;
	BufferHeapTupleTableSlot *buffer_slot;
	char live_page[BLCKSZ] pg_attribute_aligned(MAXIMUM_ALIGNOF);
	HeapTupleHeader live_tuple_data;
	bool call_again = true;
	bool all_dead = true;
	int frees_before_clear;
	TableIndexFetchTupleResult stored;

	ut_r4_hot_reset_resources();
	memset(&result, 0, sizeof(result));
	(void)ut_r4_hot_build_page(live_page, UT_HOT_LIVE_XMIN, 2,
							 UT_HOT_LIVE_XMIN, 7, 0x77);
	result.kind = HEAP_HOT_SEARCH_BUFFER_BACKED;
	result.tuple.t_data = ut_r4_hot_tuple_at((Page)live_page, UT_HOT_ROOT_OFF);
	result.tuple.t_len = UT_HOT_TUPLE_LEN;
	ItemPointerSet(&result.tuple.t_self, UT_HOT_BLOCK, UT_HOT_ROOT_OFF);
	result.tuple.t_tableOid = UT_HOT_TABLE_OID;
	live_tuple_data = result.tuple.t_data;
	slot = ut_r4_hot_make_slot(&tuple_desc);
	buffer_slot = (BufferHeapTupleTableSlot *)slot;

	stored = cluster_heap_test_r4_store_hot_result(
		&result, slot, UT_HOT_BUFFER, &call_again, &all_dead);
	UT_ASSERT_EQ(stored, TABLE_INDEX_FETCH_FOUND);
	UT_ASSERT(!call_again);
	UT_ASSERT(!all_dead);
	UT_ASSERT(buffer_slot->base.tuple == &buffer_slot->base.tupdata);
	UT_ASSERT(buffer_slot->base.tuple != &result.tuple);
	UT_ASSERT(buffer_slot->base.tupdata.t_data == live_tuple_data);
	UT_ASSERT_EQ(buffer_slot->base.tupdata.t_len, UT_HOT_TUPLE_LEN);
	UT_ASSERT_EQ(buffer_slot->buffer, UT_HOT_BUFFER);
	UT_ASSERT(!TTS_SHOULDFREE(slot));
	UT_ASSERT_EQ(ut_buffer_incr_calls, 1);
	UT_ASSERT_EQ(ut_buffer_release_calls, 0);
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(&slot->tts_tid), UT_HOT_BLOCK);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&slot->tts_tid), UT_HOT_ROOT_OFF);
	UT_ASSERT_EQ(slot->tts_tableOid, UT_HOT_TABLE_OID);

	/* The slot owns its embedded descriptor; the page bytes remain pin-backed. */
	memset(&result.tuple, 0, sizeof(result.tuple));
	UT_ASSERT(buffer_slot->base.tuple == &buffer_slot->base.tupdata);
	UT_ASSERT(buffer_slot->base.tupdata.t_data == live_tuple_data);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(buffer_slot->base.tupdata.t_data),
				 UT_HOT_LIVE_XMIN);
	UT_ASSERT_EQ(*((unsigned char *)buffer_slot->base.tupdata.t_data +
					 buffer_slot->base.tupdata.t_data->t_hoff), 0x77);
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(&buffer_slot->base.tupdata.t_self),
				 UT_HOT_BLOCK);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&buffer_slot->base.tupdata.t_self),
				 UT_HOT_ROOT_OFF);
	UT_ASSERT_EQ(buffer_slot->base.tupdata.t_tableOid, UT_HOT_TABLE_OID);

	frees_before_clear = ut_free_calls;
	ExecClearTuple(slot);
	UT_ASSERT_EQ(ut_free_calls, frees_before_clear);
	UT_ASSERT_EQ(ut_buffer_release_calls, 1);
	UT_ASSERT_EQ(buffer_slot->buffer, InvalidBuffer);
	ExecDropSingleTupleTableSlot(slot);
	UT_ASSERT_EQ(ut_alloc_calls, ut_free_calls);
	UT_ASSERT_EQ(ut_invalid_free_calls, 0);
}

UT_TEST(test_34_owned_scratch_result_survives_source_and_live_poison)
{
	UtR4HotLifecycleFixture fixture;
	HeapHotSearchResult result;
	TupleDescData tuple_desc;
	TupleTableSlot *slot;
	BufferHeapTupleTableSlot *buffer_slot;
	HeapTuple owned_tuple;
	bool call_again = true;
	bool all_dead = true;
	int frees_before_clear;
	int releases_before_clear;
	uintptr_t owned_data_addr;
	uintptr_t scratch_begin;
	uintptr_t scratch_end;
	HeapHotSearchResultKind kind;
	TableIndexFetchTupleResult stored;

	ut_r4_hot_reset_resources();
	ut_r4_hot_init_fixture(&fixture, &result);

	kind = cluster_heap_test_r4_hot_full_cycle(
		fixture.expected_tag, fixture.expected_root, fixture.expected_read_scn,
		&result, ut_r4_hot_content_share, ut_r4_hot_fetch_full,
		ut_r4_hot_search_scratch, &fixture, &call_again, &all_dead);
	UT_ASSERT_EQ(kind, HEAP_HOT_SEARCH_OWNED_SCRATCH);
	UT_ASSERT_EQ(result.kind, HEAP_HOT_SEARCH_OWNED_SCRATCH);
	UT_ASSERT(!call_again);
	UT_ASSERT(!all_dead);
	UT_ASSERT_EQ(fixture.event, 4);
	UT_ASSERT_EQ(fixture.unlock_calls, 1);
	UT_ASSERT_EQ(fixture.fetch_calls, 1);
	UT_ASSERT_EQ(fixture.scratch_search_calls, 1);
	UT_ASSERT_EQ(fixture.relock_calls, 1);
	UT_ASSERT_EQ(fixture.held_locks & UT_HOT_FORBIDDEN_LOCKS,
				 UT_HOT_CONTENT_SHARE);
	scratch_begin = (uintptr_t)result.scratch_page;
	scratch_end = scratch_begin + BLCKSZ;
	UT_ASSERT((uintptr_t)result.tuple.t_data >= scratch_begin);
	UT_ASSERT((uintptr_t)result.tuple.t_data + result.tuple.t_len <= scratch_end);

	slot = ut_r4_hot_make_slot(&tuple_desc);
	buffer_slot = (BufferHeapTupleTableSlot *)slot;
	call_again = true;
	all_dead = true;
	stored = cluster_heap_test_r4_store_hot_result(
		&result, slot, UT_HOT_BUFFER, &call_again, &all_dead);
	UT_ASSERT_EQ(stored, TABLE_INDEX_FETCH_FOUND);
	UT_ASSERT(!call_again);
	UT_ASSERT(!all_dead);
	UT_ASSERT_EQ(buffer_slot->buffer, InvalidBuffer);
	UT_ASSERT(TTS_SHOULDFREE(slot));
	UT_ASSERT_EQ(ut_buffer_incr_calls, 0);
	UT_ASSERT_EQ(ut_buffer_release_calls, 0);
	UT_ASSERT_EQ(ut_invalid_free_calls, 0);
	owned_tuple = buffer_slot->base.tuple;
	UT_ASSERT_NOT_NULL(owned_tuple);
	if (owned_tuple != NULL)
	{
		owned_data_addr = (uintptr_t)owned_tuple->t_data;
		UT_ASSERT(owned_tuple != &result.tuple);
		UT_ASSERT(owned_data_addr < scratch_begin || owned_data_addr >= scratch_end);
		UT_ASSERT_EQ(ItemPointerGetBlockNumber(&owned_tuple->t_self), UT_HOT_BLOCK);
		UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&owned_tuple->t_self),
					 UT_HOT_ROOT_OFF);
		UT_ASSERT_EQ(owned_tuple->t_tableOid, UT_HOT_TABLE_OID);
	}
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(&slot->tts_tid), UT_HOT_BLOCK);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&slot->tts_tid), UT_HOT_ROOT_OFF);
	UT_ASSERT_EQ(slot->tts_tableOid, UT_HOT_TABLE_OID);

	memset(result.scratch_page, 0x00, BLCKSZ);
	memset(fixture.live_page, 0x00, BLCKSZ);
	memset(fixture.full_source, 0x00, BLCKSZ);
	if (owned_tuple != NULL)
	{
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(owned_tuple->t_data),
					 UT_HOT_FULL_XMIN);
		UT_ASSERT_EQ(*((unsigned char *)owned_tuple->t_data +
						 owned_tuple->t_data->t_hoff), UT_HOT_PAYLOAD);
	}

	frees_before_clear = ut_free_calls;
	releases_before_clear = ut_buffer_release_calls;
	ExecClearTuple(slot);
	UT_ASSERT_EQ(ut_free_calls, frees_before_clear + 1);
	UT_ASSERT_EQ(ut_buffer_release_calls, releases_before_clear);
	UT_ASSERT(!TTS_SHOULDFREE(slot));
	UT_ASSERT_EQ(buffer_slot->buffer, InvalidBuffer);
	UT_ASSERT(TTS_EMPTY(slot));
	ExecDropSingleTupleTableSlot(slot);
	UT_ASSERT_EQ(ut_alloc_calls, ut_free_calls);
	UT_ASSERT_EQ(ut_invalid_free_calls, 0);
}

static void
ut_r4_hot_reset_scratch_authority(Page scratch_page, Page forbidden_live_page,
								  SCN read_scn, XLogRecPtr page_lsn)
{
	memset(&ut_scratch_expected_ref, 0, sizeof(ut_scratch_expected_ref));
	ut_scratch_expected_page = scratch_page;
	ut_scratch_forbidden_live_page = forbidden_live_page;
	ut_scratch_expected_ref.origin_node_id = 1;
	ut_scratch_expected_ref.undo_segment_id = 257;
	ut_scratch_expected_ref.tt_slot_id = 4;
	ut_scratch_expected_ref.cluster_epoch = 9;
	ut_scratch_expected_ref.local_xid = UT_HOT_FULL_XMIN;
	ut_scratch_expected_xid = UT_HOT_FULL_XMIN;
	ut_scratch_expected_read_scn = read_scn;
	ut_scratch_expected_lsn = page_lsn;
	ut_scratch_ref_available = true;
	ut_scratch_resolve_evidence = CLUSTER_VIS_EVIDENCE_REMOTE;
	ut_scratch_resolve_status = CLUSTER_TT_STATUS_COMMITTED;
	ut_scratch_resolve_scn = read_scn - 1;
	ut_scratch_ref_calls = 0;
	ut_scratch_exact_resolve_calls = 0;
	ut_scratch_live_resolve_calls = 0;
	ut_scratch_cr_calls = 0;
	ut_scratch_ssi_calls = 0;
	ut_scratch_hint_calls = 0;
	ut_scratch_dirty_calls = 0;
	ut_live_visibility_calls = 0;
}

UT_TEST(test_35_scratch_mvcc_uses_exact_ref_without_hints_or_live_page)
{
	char scratch_storage[BLCKSZ] pg_attribute_aligned(MAXIMUM_ALIGNOF);
	char live_storage[BLCKSZ] pg_attribute_aligned(MAXIMUM_ALIGNOF);
	char scratch_before[BLCKSZ] pg_attribute_aligned(MAXIMUM_ALIGNOF);
	Page scratch_page;
	Page live_page;
	HeapTupleData tuple;
	SnapshotData snapshot;
	ClusterR4HotScratchTestContext context;
	HeapTupleHeader tuple_header;
	XLogRecPtr page_lsn = UINT64_C(0x123450);
	bool visible;

	scratch_page = ut_r4_hot_build_page(scratch_storage, UT_HOT_FULL_XMIN, 1,
									 UT_HOT_FULL_XMIN, 4, UT_HOT_PAYLOAD);
	live_page = ut_r4_hot_build_page(live_storage, UT_HOT_LIVE_XMIN, 2,
								  UT_HOT_LIVE_XMIN, 7, 0x77);
	tuple_header = ut_r4_hot_tuple_at(scratch_page, UT_HOT_ROOT_OFF);
	/* No xmin hint: the exact resolver, never native CLOG/hinting, decides. */
	tuple_header->t_infomask = HEAP_XMAX_INVALID;
	PageSetLSN(scratch_page, page_lsn);
	memcpy(scratch_before, scratch_storage, BLCKSZ);

	memset(&tuple, 0, sizeof(tuple));
	tuple.t_data = tuple_header;
	tuple.t_len = UT_HOT_TUPLE_LEN;
	ItemPointerSet(&tuple.t_self, UT_HOT_BLOCK, UT_HOT_ROOT_OFF);
	tuple.t_tableOid = UT_HOT_TABLE_OID;
	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.snapshot_type = SNAPSHOT_MVCC;
	snapshot.read_scn = UT_HOT_READ_SCN;
	snapshot.read_epoch = 9;
	snapshot.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
	memset(&context, 0, sizeof(context));
	context.scratch_page = scratch_page;
	context.tag.blockNum = UT_HOT_BLOCK;
	context.logical_root = tuple.t_self;
	context.read_scn = snapshot.read_scn;
	context.already_full = true;
	context.allow_hint = false;
	context.allow_cleanout = false;

	ut_r4_hot_reset_scratch_authority(scratch_page, live_page,
									  snapshot.read_scn, page_lsn);
	visible = HeapTupleSatisfiesMVCCScratch(&tuple, &snapshot, &context);
	UT_ASSERT(visible);
	UT_ASSERT_EQ(ut_scratch_ref_calls, 1);
	UT_ASSERT_EQ(ut_scratch_exact_resolve_calls, 1);
	UT_ASSERT_EQ(ut_scratch_live_resolve_calls, 0);
	UT_ASSERT_EQ(ut_scratch_cr_calls, 0);
	UT_ASSERT_EQ(ut_scratch_ssi_calls, 0);
	UT_ASSERT(memcmp(scratch_storage, scratch_before, BLCKSZ) == 0);

	ut_scratch_resolve_scn = snapshot.read_scn + 1;
	ut_scratch_ref_calls = 0;
	ut_scratch_exact_resolve_calls = 0;
	visible = HeapTupleSatisfiesMVCCScratch(&tuple, &snapshot, &context);
	UT_ASSERT(!visible);
	UT_ASSERT_EQ(ut_scratch_ref_calls, 1);
	UT_ASSERT_EQ(ut_scratch_exact_resolve_calls, 1);
	UT_ASSERT_EQ(ut_scratch_live_resolve_calls, 0);
	UT_ASSERT_EQ(ut_scratch_cr_calls, 0);
	UT_ASSERT_EQ(ut_scratch_ssi_calls, 0);
	UT_ASSERT(memcmp(scratch_storage, scratch_before, BLCKSZ) == 0);

	ut_scratch_resolve_status = CLUSTER_TT_STATUS_ABORTED;
	ut_scratch_resolve_scn = InvalidScn;
	ut_scratch_ref_calls = 0;
	ut_scratch_exact_resolve_calls = 0;
	visible = HeapTupleSatisfiesMVCCScratch(&tuple, &snapshot, &context);
	UT_ASSERT(!visible);
	UT_ASSERT_EQ(ut_scratch_ref_calls, 1);
	UT_ASSERT_EQ(ut_scratch_exact_resolve_calls, 1);
	UT_ASSERT_EQ(ut_scratch_live_resolve_calls, 0);
	UT_ASSERT_EQ(ut_scratch_cr_calls, 0);
	UT_ASSERT_EQ(ut_scratch_ssi_calls, 0);
	UT_ASSERT(memcmp(scratch_storage, scratch_before, BLCKSZ) == 0);
}

UT_TEST(test_36_production_hot_core_full_result_is_owned)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	RelationData relation;
	FormData_pg_class relation_form;
	SnapshotData snapshot;
	ItemPointerData tid;
	TupleDescData tuple_desc;
	TupleTableSlot *slot;
	BufferHeapTupleTableSlot *buffer_slot;
	HeapTuple owned_tuple;
	TableIndexFetchTupleResult stored;
	bool call_again = false;
	bool all_dead = true;
	int frees_before_clear;
	int releases_before_clear;
	uintptr_t scratch_begin;
	uintptr_t scratch_end;

	ut_r4_hot_reset_resources();
	ut_r4_hot_init_product_fixture(&fixture, &result);
	ut_r4_hot_reset_scratch_authority((Page) result.scratch_page,
									  (Page) fixture.live_page,
									  UT_HOT_READ_SCN, UINT64_C(0x123450));

	memset(&relation, 0, sizeof(relation));
	memset(&relation_form, 0, sizeof(relation_form));
	relation.rd_id = UT_HOT_TABLE_OID;
	relation.rd_rel = &relation_form;
	relation_form.relpersistence = RELPERSISTENCE_PERMANENT;
	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.snapshot_type = SNAPSHOT_MVCC;
	snapshot.read_scn = UT_HOT_READ_SCN;
	snapshot.read_epoch = 9;
	snapshot.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
	ItemPointerSet(&tid, UT_HOT_BLOCK, UT_HOT_ROOT_OFF);
	slot = ut_r4_hot_make_slot(&tuple_desc);
	buffer_slot = (BufferHeapTupleTableSlot *) slot;

	stored = cluster_heap_test_r4_index_hot_result(
		&tid, &relation, UT_HOT_BUFFER, &snapshot, &result, slot,
		&call_again, &all_dead);
	UT_ASSERT_EQ(stored, TABLE_INDEX_FETCH_FOUND);
	UT_ASSERT_EQ(result.kind, HEAP_HOT_SEARCH_OWNED_SCRATCH);
	UT_ASSERT(!call_again);
	UT_ASSERT(!all_dead);
	UT_ASSERT_EQ(fixture.lock_calls, 5);
	UT_ASSERT_EQ(fixture.lock_modes[0], BUFFER_LOCK_UNLOCK);
	UT_ASSERT_EQ(fixture.lock_modes[1], BUFFER_LOCK_SHARE);
	UT_ASSERT_EQ(fixture.lock_modes[2], BUFFER_LOCK_UNLOCK);
	UT_ASSERT_EQ(fixture.lock_modes[3], BUFFER_LOCK_SHARE);
	UT_ASSERT_EQ(fixture.lock_modes[4], BUFFER_LOCK_UNLOCK);
	UT_ASSERT(!ut_hot_content_lock_held);
	UT_ASSERT_EQ(fixture.fetch_calls, 2);
	UT_ASSERT(fixture.live_poisoned);
	UT_ASSERT(fixture.full_source_poisoned);
	UT_ASSERT_EQ(ut_hot_live_ref_calls, 2);
	UT_ASSERT_EQ(ut_scratch_ref_calls, 2);
	UT_ASSERT_EQ(ut_scratch_exact_resolve_calls, 2);
	UT_ASSERT_EQ(ut_live_visibility_calls, 0);
	UT_ASSERT_EQ(ut_scratch_live_resolve_calls, 0);
	UT_ASSERT_EQ(ut_scratch_cr_calls, 0);
	UT_ASSERT_EQ(ut_scratch_ssi_calls, 0);
	UT_ASSERT_EQ(ut_scratch_hint_calls, 0);
	UT_ASSERT_EQ(ut_scratch_dirty_calls, 0);

	scratch_begin = (uintptr_t) result.scratch_page;
	scratch_end = scratch_begin + BLCKSZ;
	UT_ASSERT((uintptr_t) result.tuple.t_data >= scratch_begin);
	UT_ASSERT((uintptr_t) result.tuple.t_data + result.tuple.t_len <= scratch_end);
	UT_ASSERT_EQ(buffer_slot->buffer, InvalidBuffer);
	UT_ASSERT(TTS_SHOULDFREE(slot));
	UT_ASSERT_EQ(ut_buffer_incr_calls, 0);
	UT_ASSERT_EQ(ut_buffer_release_calls, 0);
	owned_tuple = buffer_slot->base.tuple;
	UT_ASSERT_NOT_NULL(owned_tuple);
	if (owned_tuple != NULL)
	{
		UT_ASSERT(owned_tuple != &result.tuple);
		UT_ASSERT((uintptr_t) owned_tuple->t_data < scratch_begin
				  || (uintptr_t) owned_tuple->t_data >= scratch_end);
		UT_ASSERT_EQ(ItemPointerGetBlockNumber(&owned_tuple->t_self),
					 UT_HOT_BLOCK);
		UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&owned_tuple->t_self),
					 UT_HOT_ROOT_OFF);
		UT_ASSERT_EQ(owned_tuple->t_tableOid, UT_HOT_TABLE_OID);
	}
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(&slot->tts_tid), UT_HOT_BLOCK);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&slot->tts_tid), UT_HOT_ROOT_OFF);
	UT_ASSERT_EQ(slot->tts_tableOid, UT_HOT_TABLE_OID);

	memset(result.scratch_page, 0, BLCKSZ);
	memset(fixture.live_page, 0, BLCKSZ);
	memset(fixture.full_source, 0, BLCKSZ);
	if (owned_tuple != NULL)
	{
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(owned_tuple->t_data),
					 UT_HOT_FULL_XMIN);
		UT_ASSERT_EQ(*((unsigned char *) owned_tuple->t_data
						 + owned_tuple->t_data->t_hoff), UT_HOT_PAYLOAD);
	}

	ut_hot_production_core_active = false;
	ut_hot_product_fixture = NULL;
	ut_hot_live_ref_page = NULL;
	BufferBlocks = NULL;
	frees_before_clear = ut_free_calls;
	releases_before_clear = ut_buffer_release_calls;
	ExecClearTuple(slot);
	UT_ASSERT_EQ(ut_free_calls, frees_before_clear + 1);
	UT_ASSERT_EQ(ut_buffer_release_calls, releases_before_clear);
	UT_ASSERT(!TTS_SHOULDFREE(slot));
	UT_ASSERT_EQ(buffer_slot->buffer, InvalidBuffer);
	UT_ASSERT(TTS_EMPTY(slot));
	ExecDropSingleTupleTableSlot(slot);
	UT_ASSERT_EQ(ut_alloc_calls, ut_free_calls);
	UT_ASSERT_EQ(ut_invalid_free_calls, 0);
}

UT_TEST(test_37_full_input_recheck_catches_non_target_itl_with_stable_tuple_and_lsn)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	RelationData relation;
	FormData_pg_class relation_form;
	SnapshotData snapshot;
	ItemPointerData tid;
	HeapHotSearchResultKind kind;
	ClusterItlSlotData non_target_before;
	char target_before[UT_HOT_TUPLE_LEN];
	XLogRecPtr stable_lsn = (XLogRecPtr) UINT64_C(0x445566);

	ut_r4_hot_init_product_fixture(&fixture, &result);
	fixture.mutate_non_target_itl = true;
	PageSetLSN((Page) fixture.live_page, stable_lsn);
	memcpy(target_before,
		   ut_r4_hot_tuple_at((Page) fixture.live_page, UT_HOT_ROOT_OFF),
		   sizeof(target_before));
	non_target_before = ClusterPageGetItlSlots((Page) fixture.live_page)[0];
	ut_r4_hot_reset_scratch_authority((Page) result.scratch_page,
									  (Page) fixture.live_page,
									  UT_HOT_READ_SCN, UINT64_C(0x123450));

	memset(&relation, 0, sizeof(relation));
	memset(&relation_form, 0, sizeof(relation_form));
	relation.rd_id = UT_HOT_TABLE_OID;
	relation.rd_rel = &relation_form;
	relation_form.relpersistence = RELPERSISTENCE_PERMANENT;
	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.snapshot_type = SNAPSHOT_MVCC;
	snapshot.read_scn = UT_HOT_READ_SCN;
	snapshot.read_epoch = 9;
	snapshot.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
	ItemPointerSet(&tid, UT_HOT_BLOCK, UT_HOT_ROOT_OFF);

	kind = heap_hot_search_buffer_result(&tid, &relation, UT_HOT_BUFFER,
										&snapshot, &result, NULL, true);
	UT_ASSERT_EQ(kind, HEAP_HOT_SEARCH_OWNED_SCRATCH);
	UT_ASSERT_EQ(fixture.fetch_calls, 2);
	UT_ASSERT_EQ(fixture.lock_calls, 4);
	UT_ASSERT(fixture.live_poisoned);
	UT_ASSERT_EQ(PageGetLSN((Page) fixture.live_page), stable_lsn);
	UT_ASSERT_EQ(memcmp(target_before,
					ut_r4_hot_tuple_at((Page) fixture.live_page,
										UT_HOT_ROOT_OFF),
					sizeof(target_before)), 0);
	UT_ASSERT(ClusterPageGetItlSlots((Page) fixture.live_page)[0].wrap
			  != non_target_before.wrap);
	UT_ASSERT_EQ(ut_hot_live_ref_calls, 2);
	UT_ASSERT_EQ(ut_scratch_ref_calls, 2);

	LockBuffer(UT_HOT_BUFFER, BUFFER_LOCK_UNLOCK);
	UT_ASSERT(!ut_hot_content_lock_held);
	ut_hot_production_core_active = false;
	ut_hot_product_fixture = NULL;
	ut_hot_live_ref_page = NULL;
	BufferBlocks = NULL;
}

UT_TEST(test_38_changed_input_discards_old_fetch_failure_before_error_mapping)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult result;
	RelationData relation;
	FormData_pg_class relation_form;
	SnapshotData snapshot;
	ItemPointerData tid;
	HeapHotSearchResultKind kind;

	ut_r4_hot_init_product_fixture(&fixture, &result);
	fixture.mutate_non_target_itl = true;
	fixture.fail_first_after_mutation = true;
	ut_r4_hot_reset_scratch_authority((Page) result.scratch_page,
									  (Page) fixture.live_page,
									  UT_HOT_READ_SCN, UINT64_C(0x123450));

	memset(&relation, 0, sizeof(relation));
	memset(&relation_form, 0, sizeof(relation_form));
	relation.rd_id = UT_HOT_TABLE_OID;
	relation.rd_rel = &relation_form;
	relation_form.relpersistence = RELPERSISTENCE_PERMANENT;
	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.snapshot_type = SNAPSHOT_MVCC;
	snapshot.read_scn = UT_HOT_READ_SCN;
	snapshot.read_epoch = 9;
	snapshot.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
	ItemPointerSet(&tid, UT_HOT_BLOCK, UT_HOT_ROOT_OFF);

	kind = heap_hot_search_buffer_result(&tid, &relation, UT_HOT_BUFFER,
										&snapshot, &result, NULL, true);
	UT_ASSERT_EQ(kind, HEAP_HOT_SEARCH_OWNED_SCRATCH);
	UT_ASSERT_EQ(fixture.fetch_calls, 2);
	UT_ASSERT_EQ(fixture.lock_calls, 4);
	UT_ASSERT_EQ(ut_hot_live_ref_calls, 2);
	UT_ASSERT_EQ(ut_scratch_ref_calls, 1);

	LockBuffer(UT_HOT_BUFFER, BUFFER_LOCK_UNLOCK);
	UT_ASSERT(!ut_hot_content_lock_held);
	ut_hot_production_core_active = false;
	ut_hot_product_fixture = NULL;
	ut_hot_live_ref_page = NULL;
	BufferBlocks = NULL;
}

int
main(void)
{
	UT_PLAN(38);
	UT_RUN(test_01_held_lock_bits_are_independent);
	UT_RUN(test_02_wait_edge_values_are_closed);
	UT_RUN(test_03_utility_to_lmon_wait_with_no_lock_is_allowed);
	UT_RUN(test_04_utility_wait_rejects_resource_lock);
	UT_RUN(test_05_utility_wait_rejects_buffer_lock);
	UT_RUN(test_06_utility_wait_rejects_slru_lock);
	UT_RUN(test_07_utility_wait_rejects_undo_io_ownership);
	UT_RUN(test_08_utility_wait_rejects_ic_dispatch_ownership);
	UT_RUN(test_09_utility_wait_rejects_combined_forbidden_locks);
	UT_RUN(test_10_lmon_to_qvotec_wait_with_no_lock_is_allowed);
	UT_RUN(test_11_qvotec_wait_rejects_resource_lock);
	UT_RUN(test_12_qvotec_wait_rejects_buffer_lock);
	UT_RUN(test_13_qvotec_wait_rejects_slru_lock);
	UT_RUN(test_14_qvotec_wait_rejects_undo_io_ownership);
	UT_RUN(test_15_qvotec_wait_rejects_ic_dispatch_ownership);
	UT_RUN(test_16_qvotec_wait_rejects_all_forbidden_locks);
	UT_RUN(test_17_peer_ack_wait_with_no_lock_is_allowed);
	UT_RUN(test_18_peer_ack_wait_rejects_resource_lock);
	UT_RUN(test_19_peer_ack_wait_rejects_buffer_lock);
	UT_RUN(test_20_peer_ack_wait_rejects_slru_lock);
	UT_RUN(test_21_peer_ack_wait_rejects_undo_io_ownership);
	UT_RUN(test_22_peer_ack_wait_rejects_ic_dispatch_ownership);
	UT_RUN(test_23_control_barrier_with_no_lock_is_allowed);
	UT_RUN(test_24_control_barrier_rejects_resource_lock);
	UT_RUN(test_25_control_barrier_rejects_buffer_lock);
	UT_RUN(test_26_control_barrier_rejects_slru_lock);
	UT_RUN(test_27_control_barrier_rejects_undo_io_ownership);
	UT_RUN(test_28_control_barrier_rejects_ic_dispatch_ownership);
	UT_RUN(test_29_process_utility_may_only_wait_on_lmon);
	UT_RUN(test_30_lmon_may_only_delegate_durable_io_to_qvotec);
	UT_RUN(test_31_lmon_control_path_never_enters_holder_lms);
	UT_RUN(test_32_qvotec_completion_never_enters_origin_data);
	UT_RUN(test_33_buffer_backed_result_keeps_real_buffer_pin);
	UT_RUN(test_34_owned_scratch_result_survives_source_and_live_poison);
	UT_RUN(test_35_scratch_mvcc_uses_exact_ref_without_hints_or_live_page);
	UT_RUN(test_36_production_hot_core_full_result_is_owned);
	UT_RUN(test_37_full_input_recheck_catches_non_target_itl_with_stable_tuple_and_lsn);
	UT_RUN(test_38_changed_input_discards_old_fetch_failure_before_error_mapping);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
