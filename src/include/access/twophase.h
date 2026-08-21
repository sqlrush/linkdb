/*-------------------------------------------------------------------------
 *
 * twophase.h
 *	  Two-phase-commit related declarations.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/twophase.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TWOPHASE_H
#define TWOPHASE_H

#include "access/xact.h"
#include "access/xlogdefs.h"
#include "datatype/timestamp.h"
#include "storage/lock.h"

/*
 * GlobalTransactionData is defined in twophase.c; other places have no
 * business knowing the internal definition.
 */
typedef struct GlobalTransactionData *GlobalTransaction;

/* GUC variable */
extern PGDLLIMPORT int max_prepared_xacts;

extern Size TwoPhaseShmemSize(void);
extern void TwoPhaseShmemInit(void);

extern void AtAbort_Twophase(void);
extern void PostPrepare_Twophase(void);

extern TransactionId TwoPhaseGetXidByVirtualXID(VirtualTransactionId vxid,
												bool *have_more);
extern PGPROC *TwoPhaseGetDummyProc(TransactionId xid, bool lock_held);
extern BackendId TwoPhaseGetDummyBackendId(TransactionId xid, bool lock_held);

extern GlobalTransaction MarkAsPreparing(TransactionId xid, const char *gid,
										 TimestampTz prepared_at,
										 Oid owner, Oid databaseid);

extern void StartPrepare(GlobalTransaction gxact);
extern void EndPrepare(GlobalTransaction gxact);
extern bool StandbyTransactionIdIsPrepared(TransactionId xid);
extern bool TwoPhaseTransactionIdIsPrepared(TransactionId xid);

extern TransactionId PrescanPreparedTransactions(TransactionId **xids_p,
												 int *nxids_p);
extern void StandbyRecoverPreparedTransactions(void);
extern void RecoverPreparedTransactions(void);

extern void CheckPointTwoPhase(XLogRecPtr redo_horizon);

extern void FinishPreparedTransaction(const char *gid, bool isCommit);

extern void PrepareRedoAdd(char *buf, XLogRecPtr start_lsn,
						   XLogRecPtr end_lsn, RepOriginId origin_id);
extern void PrepareRedoRemove(TransactionId xid, bool giveWarning);
extern void restoreTwoPhaseData(void);
extern bool LookupGXact(const char *gid, XLogRecPtr prepare_end_lsn,
						TimestampTz origin_prepare_timestamp);
#ifdef USE_PGRAC_CLUSTER
/* PGRAC (spec-4.12a D1): non-allocating prepared-xact count for the cluster
 * undo record-segment drain gate (硬门 6). */
extern int GetNumberOfPreparedTransactions(void);

/*
 * RF-SIDE recovery-only durable pending owner.  The content is the complete
 * native PREPARE payload (without the trailing state-file CRC).  These APIs
 * never grant terminal state or OPEN; they only classify/install an exact
 * database-scoped pg_twophase pending record.
 */
typedef enum TwoPhaseRecoveryPendingResult
{
	TWOPHASE_RECOVERY_PENDING_OK = 0,
	TWOPHASE_RECOVERY_PENDING_BLOCKED,
	TWOPHASE_RECOVERY_PENDING_CONFLICT,
	TWOPHASE_RECOVERY_PENDING_POST_READ_FAILED
} TwoPhaseRecoveryPendingResult;

extern TwoPhaseRecoveryPendingResult TwoPhaseRecoveryPendingPreflight(
	TransactionId xid, Oid database, Oid owner, TimestampTz prepared_at,
	const char *gid, const void *content, uint32 len);
extern TwoPhaseRecoveryPendingResult TwoPhaseRecoveryPendingInstall(
	TransactionId xid, Oid database, Oid owner, TimestampTz prepared_at,
	const char *gid, const void *content, uint32 len);
extern TwoPhaseRecoveryPendingResult TwoPhaseRecoveryPendingReadExact(
	TransactionId xid, Oid database, const char *gid, void **content_out,
	uint32 *len_out);
extern TwoPhaseRecoveryPendingResult TwoPhaseRecoveryPendingResolveExact(
	TransactionId xid, Oid database, const char *gid, const void *content,
	uint32 len, bool isCommit);
extern TwoPhaseRecoveryPendingResult TwoPhaseRecoveryPendingCleanupExact(
	TransactionId xid, Oid database, const char *gid, const void *content,
	uint32 len, bool isCommit);
#endif
#endif							/* TWOPHASE_H */
