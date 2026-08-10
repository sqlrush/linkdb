/*-------------------------------------------------------------------------
 *
 * cluster_r4_activation_test_stubs.h
 *	  Process-lifecycle stubs for dependency-light semantic activation tests.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/cluster_r4_activation_test_stubs.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_R4_ACTIVATION_TEST_STUBS_H
#define CLUSTER_R4_ACTIVATION_TEST_STUBS_H

#include "storage/ipc.h"

int MyProcPid = 101;
volatile sig_atomic_t InterruptPending = false;
volatile uint32 InterruptHoldoffCount = 0;
volatile uint32 QueryCancelHoldoffCount = 0;
volatile uint32 CritSectionCount = 0;

void ProcessInterrupts(void);

void
ProcessInterrupts(void)
{}

uint64
cluster_epoch_get_current(void)
{
	return 0;
}

bool cluster_sf_peer_capability_generation_matches(int32 peer_id, uint32 required_capabilities,
											uint32 expected_generation);
bool
cluster_sf_peer_capability_generation_matches(int32 peer_id pg_attribute_unused(),
											uint32 required_capabilities pg_attribute_unused(),
											uint32 expected_generation pg_attribute_unused())
{
	return false;
}

void
on_shmem_exit(pg_on_exit_callback function pg_attribute_unused(), Datum arg pg_attribute_unused())
{}

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

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

#endif /* CLUSTER_R4_ACTIVATION_TEST_STUBS_H */
