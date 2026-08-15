/*-------------------------------------------------------------------------
 *
 * cluster_thread_recovery_driver_srf.c
 *	  Read-only TEST SQL observer for the validated torn-tail boundary.  The
 *	  former authority-bearing data-driver entry point was removed by STOP03.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-4.11-thread-recovery.md (FROZEN v0.3)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_thread_recovery_driver_srf.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"

PG_FUNCTION_INFO_V1(cluster_thread_validated_end_test);

#ifdef USE_PGRAC_CLUSTER

#include "access/xlogdefs.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"

#include "cluster/cluster_thread_recovery.h"

/*
 * cluster_thread_validated_end_test -- drive the D4 validated torn-tail boundary
 * pass (spec-4.11 3b-4a) over a dead thread's per-thread WAL.  Returns
 * "<result>:<valid_end>" (result = done / blocked).  The TAP truncates / corrupts
 * a thread's WAL tail to prove the boundary lands on the last complete record, and
 * a decode below validated_min -> blocked (8.A).  Superuser-only.
 */
Datum
cluster_thread_validated_end_test(PG_FUNCTION_ARGS)
{
	int32 dead_tid;
	XLogRecPtr scan_lower;
	XLogRecPtr validated_min;
	XLogRecPtr valid_end = InvalidXLogRecPtr;
	ClusterThreadRecResult res;
	const char *result_text;
	const char *out;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_thread_validated_end_test is superuser-only")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("dead_tid, scan_lower and validated_min must not be NULL")));

	dead_tid = PG_GETARG_INT32(0);
	scan_lower = PG_GETARG_LSN(1);
	validated_min = PG_GETARG_LSN(2);

	/* Out-of-uint16 ids map to no slot -> the pass returns BLOCKED. */
	if (dead_tid < 0 || dead_tid > PG_UINT16_MAX)
		dead_tid = 0;

	res = cluster_thread_recovery_validated_end((uint16)dead_tid, scan_lower, validated_min,
												&valid_end);

	result_text = (res == CLUSTER_THREADREC_DONE) ? "done" : "blocked";
	out = psprintf("%s:%X/%X", result_text, LSN_FORMAT_ARGS(valid_end));

	PG_RETURN_TEXT_P(cstring_to_text(out));
}

#else /* !USE_PGRAC_CLUSTER */

Datum
cluster_thread_validated_end_test(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_thread_validated_end_test requires --enable-cluster")));
}

#endif /* USE_PGRAC_CLUSTER */
