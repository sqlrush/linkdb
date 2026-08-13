/*-------------------------------------------------------------------------
 *
 * cluster_undo_srf.c
 *	  pgrac spec-3.7 D8 — sanity reader SQL function for cluster undo
 *	  records.
 *
 *	  Provides cluster_undo_get_record(uba bytea) → bytea — own-instance
 *	  read only at spec-3.7;  cross-instance read via Cache Fusion deferred
 *	  to spec-3.9.
 *
 *	  Dual-link pattern (same as cluster_grd_srf etc.):
 *	    --enable-cluster:  real body returns the undo record bytes
 *	    --disable-cluster: stub returns ERRCODE_FEATURE_NOT_SUPPORTED
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.7-undo-record-format-allocator.md (FROZEN v0.4)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_undo_srf.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "utils/builtins.h"

PG_FUNCTION_INFO_V1(cluster_undo_get_record_srf);
PG_FUNCTION_INFO_V1(cluster_undo_test_force_segment_end_srf);

#ifdef USE_PGRAC_CLUSTER

#include "catalog/pg_authid.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "varatt.h" /* VARDATA_ANY / VARSIZE_ANY_EXHDR / SET_VARSIZE */

#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_undo_record_api.h"


/*
 * cluster_undo_get_record(uba bytea) → bytea — enable-cluster body.
 *
 *	Read one undo record by 16-byte UBA.  Returns record bytes on
 *	success;  NULL on failure.  Permission:  pg_read_server_files.
 */
Datum
cluster_undo_get_record_srf(PG_FUNCTION_ARGS)
{
	bytea *uba_bytea;
	int uba_len;
	UBA uba;
	uint32 segment_id;
	uint32 block_no;
	uint16 tt_slot_offset;
	uint16 row_offset;
	char record_buf[BLCKSZ];
	size_t record_len;
	bytea *result;

	if (!has_privs_of_role(GetUserId(), ROLE_PG_READ_SERVER_FILES))
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("permission denied to read cluster undo record"),
						errhint("Only members of the pg_read_server_files role "
								"may read cluster undo records.")));

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	uba_bytea = PG_GETARG_BYTEA_PP(0);
	uba_len = VARSIZE_ANY_EXHDR(uba_bytea);

	if (uba_len != (int)sizeof(UBA))
		PG_RETURN_NULL();

	memcpy(&uba, VARDATA_ANY(uba_bytea), sizeof(UBA));

	if (!uba_decode_record(uba, &segment_id, &block_no, &tt_slot_offset, &row_offset))
		PG_RETURN_NULL();

	record_len = cluster_undo_get_record(uba, record_buf, sizeof(record_buf));

	if (record_len == 0)
		PG_RETURN_NULL();

	result = (bytea *)palloc(VARHDRSZ + record_len);
	SET_VARSIZE(result, VARHDRSZ + record_len);
	memcpy(VARDATA(result), record_buf, record_len);

	PG_RETURN_BYTEA_P(result);
}


/*
 * cluster_undo_test_force_segment_end() → bool — spec-3.8 Fix 6.
 *
 *	Test-only hook for TAP L9-L12: advances the active-segment cursor to
 *	UNDO_BLOCKS_PER_SEGMENT - 1 so the next DML record write triggers
 *	the autoextend / hard-cap path deterministically.  Permission:
 *	superuser only (parity with cluster_inject.c hooks).
 *
 *	Returns true if a forced cursor was published;  false if no active
 *	segment yet (caller must run any DML first).
 */
Datum
cluster_undo_test_force_segment_end_srf(PG_FUNCTION_ARGS pg_attribute_unused())
{
	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_undo_test_force_segment_end is superuser-only")));

	PG_RETURN_BOOL(cluster_undo_test_force_segment_end());
}


#else /* !USE_PGRAC_CLUSTER */

/*
 * Disable-cluster stub:  ereport FEATURE_NOT_SUPPORTED so the symbol
 * resolves at link time but production builds error cleanly.
 */
Datum
cluster_undo_get_record_srf(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_undo_get_record requires --enable-cluster")));
	PG_RETURN_NULL();
}

Datum
cluster_undo_test_force_segment_end_srf(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_undo_test_force_segment_end requires --enable-cluster")));
	PG_RETURN_NULL();
}

#endif /* USE_PGRAC_CLUSTER */
