/*-------------------------------------------------------------------------
 *
 * tableam_barrier.h
 *	  Typed index-fetch result dispatch.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/access/tableam_barrier.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TABLEAM_BARRIER_H
#define TABLEAM_BARRIER_H

#include "access/tableam.h"

typedef TableIndexFetchTupleResult (*TableIndexFetchBarrierTypedFn) (
	void *context, bool *call_again, bool *all_dead);
typedef bool (*TableIndexFetchBarrierLegacyFn) (void *context,
	bool *call_again, bool *all_dead);
typedef void (*TableIndexFetchBarrierCleanupFn) (void *context);

typedef struct TableIndexFetchBarrierOps
{
	TableIndexFetchBarrierTypedFn typed_fetch;
	TableIndexFetchBarrierLegacyFn legacy_fetch;
	TableIndexFetchBarrierCleanupFn cleanup;
} TableIndexFetchBarrierOps;

extern TableIndexFetchTupleResult table_index_fetch_barrier_execute(
	const TableIndexFetchBarrierOps *ops, void *context,
	bool typed_available, bool *all_dead);

#endif							/* TABLEAM_BARRIER_H */
