/*-------------------------------------------------------------------------
 *
 * cluster_side_xact.h
 *    RF-SIDE immutable XACT decode contract.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SIDE_XACT_H
#define CLUSTER_SIDE_XACT_H

#include "access/xact.h"
#include "access/xlogreader.h"
#include "cluster/cluster_remote_xact.h"

#define CLUSTER_SIDE_XACT_INTERFACE_V1 1

typedef enum RfSideXactKindV1
{
	RF_SIDE_XACT_INVALID = 0,
	RF_SIDE_XACT_COMMIT = 1,
	RF_SIDE_XACT_ABORT = 2,
	RF_SIDE_XACT_PREPARE = 3,
	RF_SIDE_XACT_COMMIT_PREPARED = 4,
	RF_SIDE_XACT_ABORT_PREPARED = 5
} RfSideXactKindV1;

typedef struct RfSideXactOperationV1
{
	RfSideXactKindV1 kind;
	uint16		origin_thread;
	uint16		reserved_zero;
	TransactionId xid;
	Oid			database;
	uint32		xinfo;
	SCN			terminal_scn;
	TimestampTz terminal_timestamp;
	bool		has_tt_delta;
	uint8		reserved49[7];
	xl_xact_tt_commit tt_delta;
	uint8		prepare_binding[CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES];
} RfSideXactOperationV1;

extern bool rf_side_xact_decode_v1(XLogReaderState *record,
	uint64 system_identifier, uint16 origin_thread,
	RfSideXactOperationV1 *out);
extern bool rf_side_xact_structural_preflight_v1(
	const RfSideXactOperationV1 *operation);

typedef enum RfSideXactApplyResultV1
{
	RF_SIDE_XACT_APPLY_OK = 0,
	RF_SIDE_XACT_APPLY_BLOCKED = 1,
	RF_SIDE_XACT_APPLY_CONFLICT = 2,
	RF_SIDE_XACT_APPLY_POST_READ_FAILED = 3
} RfSideXactApplyResultV1;

/* Apply one already-decoded operation.  This function never reads WAL. */
extern RfSideXactApplyResultV1 rf_side_xact_apply_v1(
	const RfSideXactOperationV1 *operation);

#endif
