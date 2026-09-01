/*-------------------------------------------------------------------------
 *
 * heapam_r4_private.h
 *	  Backend-private R4 heap HOT search result and test seams.
 *
 * This contract is shared only by heapam.c, heapam_visibility.c and
 * heapam_handler.c.  It does not change the public heap or TableAM API.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/heapam_r4_private.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HEAPAM_R4_PRIVATE_H
#define HEAPAM_R4_PRIVATE_H

#include "access/htup.h"
#include "access/tableam.h"
#include "cluster/cluster_scn.h"
#include "executor/tuptable.h"
#include "storage/buf_internals.h"
#include "storage/bufpage.h"
#include "utils/snapshot.h"

typedef enum HeapHotSearchResultKind
{
	HEAP_HOT_SEARCH_NOT_FOUND = 0,
	HEAP_HOT_SEARCH_BUFFER_BACKED,
	HEAP_HOT_SEARCH_OWNED_SCRATCH
} HeapHotSearchResultKind;

typedef struct HeapHotSearchResult
{
	HeapHotSearchResultKind kind;
	HeapTupleData tuple;
	char scratch_page[BLCKSZ] pg_attribute_aligned(MAXIMUM_ALIGNOF);
} HeapHotSearchResult;

typedef struct ClusterR4HotScratchTestContext
{
	Page scratch_page;
	BufferTag tag;
	ItemPointerData logical_root;
	SCN read_scn;
	bool already_full;
	bool allow_hint;
	bool allow_cleanout;
} ClusterR4HotScratchTestContext;

typedef void (*ClusterR4HotLockTestHook)(void *arg, bool acquire);
typedef bool (*ClusterR4HotFetchFullTestHook)(void *arg,
												const BufferTag *tag,
												SCN read_scn,
												char dst_page[BLCKSZ]);
typedef bool (*ClusterR4HotScratchSearchTestHook)(
	void *arg, const ClusterR4HotScratchTestContext *context,
	HeapTuple scratch_tuple);
typedef void (*ClusterHeapDmlAuthorityGuardTestHook)(
	Buffer buffer, HeapTuple tuple, void *arg);

/* PK IndexScan companion; public heap_hot_search_buffer() remains unchanged. */
extern HeapHotSearchResultKind heap_hot_search_buffer_result(
	ItemPointer tid, Relation relation, Buffer buffer, Snapshot snapshot,
	HeapHotSearchResult *result, bool *all_dead, bool first_call);

extern bool HeapTupleSatisfiesMVCCScratch(
	HeapTuple tuple, Snapshot snapshot,
	const ClusterR4HotScratchTestContext *context);

#ifdef USE_CLUSTER_UNIT
extern bool cluster_heap_test_r4_target_reachable(void);
extern HeapHotSearchResultKind cluster_heap_test_r4_hot_full_cycle(
	BufferTag tag, ItemPointerData logical_root, SCN read_scn,
	HeapHotSearchResult *result, ClusterR4HotLockTestHook lock_hook,
	ClusterR4HotFetchFullTestHook fetch_full_hook,
	ClusterR4HotScratchSearchTestHook scratch_search_hook, void *hook_arg,
	bool *call_again, bool *all_dead);
extern TableIndexFetchTupleResult cluster_heap_test_r4_store_hot_result(
	HeapHotSearchResult *result, TupleTableSlot *slot, Buffer buffer,
	bool *call_again, bool *all_dead);
#ifdef USE_PGRAC_CLUSTER
extern TableIndexFetchTupleResult cluster_heap_test_r4_index_hot_result(
	ItemPointer tid, Relation relation, Buffer buffer, Snapshot snapshot,
	HeapHotSearchResult *result, TupleTableSlot *slot,
	bool *call_again, bool *all_dead);
extern bool cluster_heap_test_itl_alloc_with_terminal_census(
	Buffer buffer, TransactionId xid, bool lock_only, uint8 *slot_index_out);
extern bool cluster_heap_test_itl_resolve_pair_terminal_census(
	Buffer old_buffer, Buffer new_buffer, Buffer full_buffer);
extern bool cluster_heap_test_itl_update_same_page_failure_cleanup(void);
extern void cluster_heap_test_itl_last_census_stats(
	uint8 *locator_mask, uint8 *attempted_mask,
	uint8 *terminal_mask, uint8 *terminal_count);
extern bool cluster_heap_test_dml_authority_guard_recheck_with_hook(
	Buffer buffer, HeapTuple tuple,
	ClusterHeapDmlAuthorityGuardTestHook hook, void *hook_arg);
#endif
#endif

#endif							/* HEAPAM_R4_PRIVATE_H */
