/*-------------------------------------------------------------------------
 *
 * cluster_rf_route_opcode_mutation.h
 *    Compiler mutation for the Stage 8 route drift proof.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/cluster_rf_route_opcode_mutation.h
 *
 * NOTES
 *    This test-only header changes one real named WAL opcode after its
 *    defining header is loaded.  A compiler-bound manifest must reject the
 *    resulting object through its frozen key census and real identify gate.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RF_ROUTE_OPCODE_MUTATION_H
#define CLUSTER_RF_ROUTE_OPCODE_MUTATION_H

#include "postgres.h"

#include "catalog/pg_control.h"

#undef XLOG_NOOP
#define XLOG_NOOP 0xC0

#endif /* CLUSTER_RF_ROUTE_OPCODE_MUTATION_H */
