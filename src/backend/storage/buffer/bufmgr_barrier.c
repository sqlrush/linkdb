/*-------------------------------------------------------------------------
 *
 * bufmgr_barrier.c
 *	  Buffer barrier-refusal cleanup sequencing.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/bufmgr_barrier.c
 *
 * NOTES
 *	  This is a pgrac-original dependency-light sequencer.  It owns no
 *	  buffer, lock, grant or ledger state; bufmgr.c supplies the exact
 *	  operations and consumes the typed result.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/bufmgr_barrier.h"


/*
 * cluster_buffer_barrier_unwind_execute -- Run one refusal cleanup.
 *
 * Holder and writer responsibilities must reach a terminal result before a
 * durable master grant is released.  Local convergence follows the master
 * release, and the caller-provided exact proof is the only successful exit.
 */
ClusterBufferBarrierUnwindResult
cluster_buffer_barrier_unwind_execute(const ClusterBufferBarrierUnwindOps *ops,
									  void *context,
									  bool durable_grant,
									  bool pending_reservation)
{
	ClusterBufferBarrierCleanupResult result;

	Assert(ops != NULL);
	Assert(ops->abort_holder != NULL);
	Assert(ops->abort_writer != NULL);
	Assert(ops->release_master != NULL);
	Assert(ops->converge_local != NULL);
	Assert(ops->abort_pending != NULL);
	Assert(ops->prove_empty != NULL);
	Assert(!durable_grant || pending_reservation);

	result = ops->abort_holder(context);
	if (result != CLUSTER_BUFFER_BARRIER_CLEAN)
		return CLUSTER_BUFFER_BARRIER_UNWIND_HOLDER_NOT_EMPTY;

	result = ops->abort_writer(context);
	if (result != CLUSTER_BUFFER_BARRIER_CLEAN)
		return CLUSTER_BUFFER_BARRIER_UNWIND_WRITER_NOT_EMPTY;

	if (durable_grant)
	{
		result = ops->release_master(context);
		if (result != CLUSTER_BUFFER_BARRIER_CLEAN)
			return CLUSTER_BUFFER_BARRIER_UNWIND_MASTER_RELEASE_FAILED;
		result = ops->converge_local(context);
		if (result != CLUSTER_BUFFER_BARRIER_CLEAN)
			return CLUSTER_BUFFER_BARRIER_UNWIND_LOCAL_CONVERGENCE_FAILED;
	}
	else if (pending_reservation)
	{
		result = ops->abort_pending(context);
		if (result != CLUSTER_BUFFER_BARRIER_CLEAN)
			return CLUSTER_BUFFER_BARRIER_UNWIND_PENDING_ABORT_FAILED;
	}

	if (!ops->prove_empty(context))
		return CLUSTER_BUFFER_BARRIER_UNWIND_RESIDUAL;
	return CLUSTER_BUFFER_BARRIER_UNWIND_OK;
}
