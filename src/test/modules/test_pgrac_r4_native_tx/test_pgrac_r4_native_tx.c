/*-------------------------------------------------------------------------
 *
 * test_pgrac_r4_native_tx.c
 *		Native transaction authority probes for PGRAC R4 tests.
 *
 * IDENTIFICATION
 *		src/test/modules/test_pgrac_r4_native_tx/test_pgrac_r4_native_tx.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/subtrans.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "fmgr.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_pgrac_r4_two_phase_is_prepared);
PG_FUNCTION_INFO_V1(test_pgrac_r4_current_xid);
PG_FUNCTION_INFO_V1(test_pgrac_r4_subtrans_parent);

Datum
test_pgrac_r4_two_phase_is_prepared(PG_FUNCTION_ARGS)
{
	TransactionId xid = PG_GETARG_TRANSACTIONID(0);

	PG_RETURN_BOOL(TwoPhaseTransactionIdIsPrepared(xid));
}

Datum
test_pgrac_r4_current_xid(PG_FUNCTION_ARGS)
{
	PG_RETURN_TRANSACTIONID(GetCurrentTransactionId());
}

Datum
test_pgrac_r4_subtrans_parent(PG_FUNCTION_ARGS)
{
	TransactionId xid = PG_GETARG_TRANSACTIONID(0);

	PG_RETURN_TRANSACTIONID(SubTransGetParent(xid));
}
