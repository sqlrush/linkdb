/*-------------------------------------------------------------------------
 *
 * bufmgr_barrier.h
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
 *	  src/include/storage/bufmgr_barrier.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BUFMGR_BARRIER_H
#define BUFMGR_BARRIER_H

typedef enum ClusterBufferBarrierCleanupResult
{
	CLUSTER_BUFFER_BARRIER_CLEAN = 0,
	CLUSTER_BUFFER_BARRIER_DEFERRED,
	CLUSTER_BUFFER_BARRIER_FAILED
} ClusterBufferBarrierCleanupResult;

typedef enum ClusterBufferBarrierUnwindResult
{
	CLUSTER_BUFFER_BARRIER_UNWIND_OK = 0,
	CLUSTER_BUFFER_BARRIER_UNWIND_HOLDER_NOT_EMPTY,
	CLUSTER_BUFFER_BARRIER_UNWIND_WRITER_NOT_EMPTY,
	CLUSTER_BUFFER_BARRIER_UNWIND_MASTER_RELEASE_FAILED,
	CLUSTER_BUFFER_BARRIER_UNWIND_LOCAL_CONVERGENCE_FAILED,
	CLUSTER_BUFFER_BARRIER_UNWIND_PENDING_ABORT_FAILED,
	CLUSTER_BUFFER_BARRIER_UNWIND_RESIDUAL
} ClusterBufferBarrierUnwindResult;

typedef struct ClusterBufferBarrierUnwindOps
{
	ClusterBufferBarrierCleanupResult (*abort_holder) (void *context);
	ClusterBufferBarrierCleanupResult (*abort_writer) (void *context);
	ClusterBufferBarrierCleanupResult (*release_master) (void *context);
	ClusterBufferBarrierCleanupResult (*converge_local) (void *context);
	ClusterBufferBarrierCleanupResult (*abort_pending) (void *context);
	bool		(*prove_empty) (void *context);
} ClusterBufferBarrierUnwindOps;

extern ClusterBufferBarrierUnwindResult cluster_buffer_barrier_unwind_execute(
	const ClusterBufferBarrierUnwindOps *ops, void *context,
	bool durable_grant, bool pending_reservation);

#endif							/* BUFMGR_BARRIER_H */
