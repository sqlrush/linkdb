/*-------------------------------------------------------------------------
 *
 * tableam_barrier.c
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
 *	  src/backend/access/table/tableam_barrier.c
 *
 * NOTES
 *	  This is a pgrac-original dependency-light dispatcher.  It carries no
 *	  retained scan state and maps a legacy Boolean only when the optional
 *	  typed callback is absent.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/tableam_barrier.h"


/*
 * table_index_fetch_barrier_execute -- Dispatch one typed index fetch.
 *
 * Normal returns run the caller-owned cleanup exactly once.  Errors from a
 * callback propagate without being converted to a Boolean or typed result;
 * backend ResourceOwner cleanup remains responsible for that path.
 */
TableIndexFetchTupleResult
table_index_fetch_barrier_execute(const TableIndexFetchBarrierOps *ops,
								  void *context,
								  bool typed_available,
								  bool *all_dead)
{
	TableIndexFetchTupleResult result;
	bool		call_again = false;

	Assert(ops != NULL);
	Assert(ops->typed_fetch != NULL);
	Assert(ops->legacy_fetch != NULL);
	Assert(ops->cleanup != NULL);
	if (all_dead != NULL)
		*all_dead = false;
	if (typed_available)
		result = ops->typed_fetch(context, &call_again, all_dead);
	else if (ops->legacy_fetch(context, &call_again, all_dead))
		result = TABLE_INDEX_FETCH_FOUND;
	else
		result = TABLE_INDEX_FETCH_NOT_FOUND;
	ops->cleanup(context);
	return result;
}
