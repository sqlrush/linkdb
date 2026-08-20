/*-------------------------------------------------------------------------
 *
 * cluster_side_xact.c
 *    RF-SIDE immutable XACT decode implementation.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/rmgr.h"
#include "access/xact.h"
#include "cluster/cluster_side_xact.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/storage/cluster_undo_xlog.h"

#define RF_SIDE_XACT_KNOWN_XINFO \
	(XACT_XINFO_HAS_DBINFO | XACT_XINFO_HAS_SUBXACTS | \
	 XACT_XINFO_HAS_RELFILELOCATORS | XACT_XINFO_HAS_INVALS | \
	 XACT_XINFO_HAS_TWOPHASE | XACT_XINFO_HAS_ORIGIN | \
	 XACT_XINFO_HAS_AE_LOCKS | XACT_XINFO_HAS_GID | \
	 XACT_XINFO_HAS_DROPPED_STATS | XACT_XINFO_HAS_SCN | \
	 XACT_XINFO_HAS_TT_COMMIT)
#define RF_SIDE_XACT_TWOPHASE_MAGIC UINT32_C(0x57F94534)

typedef struct RfSideXactCursorV1
{
	const char *position;
	const char *end;
} RfSideXactCursorV1;

static bool
side_xact_take(RfSideXactCursorV1 *cursor, Size bytes, const char **start)
{
	if (cursor == NULL || bytes > (Size) (cursor->end - cursor->position))
		return false;
	if (start != NULL)
		*start = cursor->position;
	cursor->position += bytes;
	return true;
}

static bool
side_xact_take_counted(RfSideXactCursorV1 *cursor, Size element_size)
{
	const char *count_bytes;
	int			count;

	if (!side_xact_take(cursor, sizeof(count), &count_bytes))
		return false;
	memcpy(&count, count_bytes, sizeof(count));
	return count >= 0 &&
		(Size) count <= (Size) (cursor->end - cursor->position) /
			element_size &&
		side_xact_take(cursor, (Size) count * element_size, NULL);
}

static bool
side_xact_take_aligned_array(RfSideXactCursorV1 *cursor, int32 count,
							 Size element_size)
{
	Size		bytes;

	if (count < 0 || (Size) count >
		(Size) (cursor->end - cursor->position) / element_size)
		return false;
	bytes = (Size) count * element_size;
	return side_xact_take(cursor, MAXALIGN(bytes), NULL);
}

static bool
side_xact_prepare_shape_valid(XLogReaderState *record)
{
	RfSideXactCursorV1 cursor;
	xl_xact_prepare header;
	const char *data;
	const char *gid;
	uint32		data_len;

	data_len = XLogRecGetDataLen(record);
	data = XLogRecGetData(record);
	if (data == NULL || data_len < MAXALIGN(sizeof(header)))
		return false;
	memcpy(&header, data, sizeof(header));
	if (header.magic != RF_SIDE_XACT_TWOPHASE_MAGIC ||
		(uint64) header.total_len != (uint64) data_len + sizeof(pg_crc32c) ||
		header.gidlen <= 1 || header.gidlen > GIDSIZE)
		return false;
	cursor.position = data + MAXALIGN(sizeof(header));
	cursor.end = data + data_len;
	if (!side_xact_take(&cursor, MAXALIGN(header.gidlen), &gid) ||
		gid[header.gidlen - 1] != '\0' ||
		memchr(gid, '\0', header.gidlen - 1) != NULL ||
		!side_xact_take_aligned_array(&cursor, header.nsubxacts,
			sizeof(TransactionId)) ||
		!side_xact_take_aligned_array(&cursor, header.ncommitrels,
			sizeof(RelFileLocator)) ||
		!side_xact_take_aligned_array(&cursor, header.nabortrels,
			sizeof(RelFileLocator)) ||
		!side_xact_take_aligned_array(&cursor, header.ncommitstats,
			sizeof(xl_xact_stats_item)) ||
		!side_xact_take_aligned_array(&cursor, header.nabortstats,
			sizeof(xl_xact_stats_item)) ||
		!side_xact_take_aligned_array(&cursor, header.ninvalmsgs,
			sizeof(SharedInvalidationMessage)))
		return false;
	return true;
}

static bool
side_xact_completion_shape_valid(XLogReaderState *record, bool commit)
{
	RfSideXactCursorV1 cursor;
	const char *xinfo_bytes;
	uint32		xinfo = 0;
	Size		base_size = commit ? MinSizeOfXactCommit : MinSizeOfXactAbort;
	uint32		data_len;
	const char *data;
	const char *terminator;

	data_len = XLogRecGetDataLen(record);
	data = XLogRecGetData(record);
	if (data == NULL || data_len < base_size)
		return false;
	cursor.position = data + base_size;
	cursor.end = data + data_len;
	if ((XLogRecGetInfo(record) & XLOG_XACT_HAS_INFO) != 0)
	{
		if (!side_xact_take(&cursor, sizeof(xl_xact_xinfo), &xinfo_bytes))
			return false;
		memcpy(&xinfo, xinfo_bytes, sizeof(xinfo));
	}
	if ((xinfo & ~RF_SIDE_XACT_KNOWN_XINFO) != 0 ||
		((xinfo & XACT_XINFO_HAS_GID) != 0 &&
		 (xinfo & XACT_XINFO_HAS_TWOPHASE) == 0) ||
		(!commit && (xinfo & XACT_XINFO_HAS_TT_COMMIT) != 0))
		return false;
	if ((xinfo & XACT_XINFO_HAS_DBINFO) != 0 &&
		!side_xact_take(&cursor, sizeof(xl_xact_dbinfo), NULL))
		return false;
	if ((xinfo & XACT_XINFO_HAS_SUBXACTS) != 0 &&
		!side_xact_take_counted(&cursor, sizeof(TransactionId)))
		return false;
	if ((xinfo & XACT_XINFO_HAS_RELFILELOCATORS) != 0 &&
		!side_xact_take_counted(&cursor, sizeof(RelFileLocator)))
		return false;
	if ((xinfo & XACT_XINFO_HAS_DROPPED_STATS) != 0 &&
		!side_xact_take_counted(&cursor, sizeof(xl_xact_stats_item)))
		return false;
	if (commit && (xinfo & XACT_XINFO_HAS_INVALS) != 0 &&
		!side_xact_take_counted(&cursor, sizeof(SharedInvalidationMessage)))
		return false;
	if ((xinfo & XACT_XINFO_HAS_TWOPHASE) != 0)
	{
		if (!side_xact_take(&cursor, sizeof(xl_xact_twophase), NULL))
			return false;
		if ((xinfo & XACT_XINFO_HAS_GID) != 0)
		{
			terminator = memchr(cursor.position, '\0',
				Min((Size) GIDSIZE,
					(Size) (cursor.end - cursor.position)));
			if (terminator == NULL || terminator == cursor.position ||
				!side_xact_take(&cursor,
					(Size) (terminator - cursor.position) + 1, NULL))
				return false;
		}
	}
	if ((xinfo & XACT_XINFO_HAS_ORIGIN) != 0 &&
		!side_xact_take(&cursor, sizeof(xl_xact_origin), NULL))
		return false;
	if ((xinfo & XACT_XINFO_HAS_SCN) != 0 &&
		!side_xact_take(&cursor, sizeof(xl_xact_scn), NULL))
		return false;
	if (commit && (xinfo & XACT_XINFO_HAS_TT_COMMIT) != 0 &&
		!side_xact_take(&cursor, sizeof(xl_xact_tt_commit), NULL))
		return false;
	return cursor.position == cursor.end;
}

static bool
side_xact_no_commit_effects(const xl_xact_parsed_commit *parsed)
{
	return parsed->nsubxacts == 0 && parsed->nrels == 0 &&
		parsed->nstats == 0 && parsed->nmsgs == 0;
}

static bool
side_xact_no_abort_effects(const xl_xact_parsed_abort *parsed)
{
	return parsed->nsubxacts == 0 && parsed->nrels == 0 &&
		parsed->nstats == 0;
}

static bool
side_xact_tt_delta_valid(const xl_xact_tt_commit *delta,
						 uint16 origin_thread, TransactionId xid, SCN scn)
{
	return delta != NULL && delta->instance == origin_thread &&
		delta->segment_id != 0 && delta->slot_offset < TT_SLOTS_PER_SEGMENT &&
		delta->wrap != 0 && delta->xid == xid &&
		delta->commit_scn == scn;
}

static TimestampTz
side_xact_commit_timestamp(const xl_xact_parsed_commit *parsed)
{
	return (parsed->xinfo & XACT_XINFO_HAS_ORIGIN) != 0
		? parsed->origin_timestamp : parsed->xact_time;
}

bool
rf_side_xact_structural_preflight_v1(
	const RfSideXactOperationV1 *operation)
{
	uint8		binding_seen = 0;
	Size		i;

	if (operation == NULL || operation->kind == RF_SIDE_XACT_INVALID ||
		operation->origin_thread == 0 || operation->origin_thread > 128 ||
		operation->reserved_zero != 0 ||
		!TransactionIdIsNormal(operation->xid))
		return false;
	for (i = 0; i < sizeof(operation->reserved49); i++)
		if (operation->reserved49[i] != 0)
			return false;
	for (i = 0; i < sizeof(operation->prepare_binding); i++)
		binding_seen |= operation->prepare_binding[i];

	switch (operation->kind)
	{
		case RF_SIDE_XACT_COMMIT:
			return SCN_VALID(operation->terminal_scn) &&
				operation->terminal_timestamp != 0 &&
				operation->has_tt_delta &&
				side_xact_tt_delta_valid(&operation->tt_delta,
					operation->origin_thread, operation->xid,
					operation->terminal_scn) && binding_seen == 0;
		case RF_SIDE_XACT_ABORT:
			return SCN_VALID(operation->terminal_scn) &&
				operation->terminal_timestamp == 0 &&
				!operation->has_tt_delta && binding_seen == 0;
		case RF_SIDE_XACT_PREPARE:
			return OidIsValid(operation->database) &&
				operation->terminal_scn == InvalidScn &&
				operation->terminal_timestamp == 0 &&
				!operation->has_tt_delta && binding_seen != 0;
		case RF_SIDE_XACT_COMMIT_PREPARED:
			return OidIsValid(operation->database) &&
				SCN_VALID(operation->terminal_scn) &&
				operation->terminal_timestamp != 0 &&
				!operation->has_tt_delta && binding_seen != 0;
		case RF_SIDE_XACT_ABORT_PREPARED:
			return OidIsValid(operation->database) &&
				SCN_VALID(operation->terminal_scn) &&
				operation->terminal_timestamp == 0 &&
				!operation->has_tt_delta && binding_seen != 0;
		default:
			return false;
	}
}

bool
rf_side_xact_decode_v1(XLogReaderState *record, uint64 system_identifier,
					   uint16 origin_thread, RfSideXactOperationV1 *out)
{
	RfSideXactOperationV1 candidate;
	uint8		opcode;
	TransactionId xid;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (record == NULL || record->record == NULL || system_identifier == 0 ||
		origin_thread == 0 || origin_thread > 128 ||
		XLogRecGetRmid(record) != RM_XACT_ID)
		return false;
	memset(&candidate, 0, sizeof(candidate));
	candidate.origin_thread = origin_thread;
	opcode = XLogRecGetInfo(record) & XLOG_XACT_OPMASK;
	xid = XLogRecGetXid(record);

	switch (opcode)
	{
		case XLOG_XACT_COMMIT:
		{
			xl_xact_parsed_commit parsed;

			if (!side_xact_completion_shape_valid(record, true) ||
				!TransactionIdIsNormal(xid))
				return false;
			ParseCommitRecord(XLogRecGetInfo(record),
				(xl_xact_commit *) XLogRecGetData(record), &parsed);
			if (!side_xact_no_commit_effects(&parsed) ||
				(parsed.xinfo & XACT_XINFO_HAS_TWOPHASE) != 0 ||
				!SCN_VALID(parsed.scn) || side_xact_commit_timestamp(&parsed) == 0 ||
				!parsed.has_tt_commit ||
				!side_xact_tt_delta_valid(&parsed.tt_commit, origin_thread,
					xid, parsed.scn))
				return false;
			candidate.kind = RF_SIDE_XACT_COMMIT;
			candidate.xid = xid;
			candidate.database = parsed.dbId;
			candidate.xinfo = parsed.xinfo;
			candidate.terminal_scn = parsed.scn;
			candidate.terminal_timestamp = side_xact_commit_timestamp(&parsed);
			candidate.has_tt_delta = true;
			candidate.tt_delta = parsed.tt_commit;
			break;
		}
		case XLOG_XACT_ABORT:
		{
			xl_xact_parsed_abort parsed;

			if (!side_xact_completion_shape_valid(record, false) ||
				!TransactionIdIsNormal(xid))
				return false;
			ParseAbortRecord(XLogRecGetInfo(record),
				(xl_xact_abort *) XLogRecGetData(record), &parsed);
			if (!side_xact_no_abort_effects(&parsed) ||
				(parsed.xinfo & XACT_XINFO_HAS_TWOPHASE) != 0 ||
				!SCN_VALID(parsed.scn))
				return false;
			candidate.kind = RF_SIDE_XACT_ABORT;
			candidate.xid = xid;
			candidate.database = parsed.dbId;
			candidate.xinfo = parsed.xinfo;
			candidate.terminal_scn = parsed.scn;
			break;
		}
		case XLOG_XACT_PREPARE:
		{
			xl_xact_prepare *xlrec;
			xl_xact_parsed_prepare parsed;

			if (!side_xact_prepare_shape_valid(record))
				return false;
			xlrec = (xl_xact_prepare *) XLogRecGetData(record);
			ParsePrepareRecord(XLogRecGetInfo(record), xlrec, &parsed);
			xid = parsed.twophase_xid;
			if (!TransactionIdIsNormal(xid) || !OidIsValid(parsed.dbId) ||
				parsed.nsubxacts != 0 || parsed.nrels != 0 ||
				parsed.nabortrels != 0 || parsed.nstats != 0 ||
				parsed.nabortstats != 0 || parsed.nmsgs != 0 ||
				xlrec->initfileinval ||
				!cluster_remote_xact_prepare_digest_v2(system_identifier,
					origin_thread, xid, parsed.dbId, parsed.twophase_gid,
					candidate.prepare_binding))
				return false;
			candidate.kind = RF_SIDE_XACT_PREPARE;
			candidate.xid = xid;
			candidate.database = parsed.dbId;
			break;
		}
		case XLOG_XACT_COMMIT_PREPARED:
		{
			xl_xact_parsed_commit parsed;

			if (!side_xact_completion_shape_valid(record, true))
				return false;
			ParseCommitRecord(XLogRecGetInfo(record),
				(xl_xact_commit *) XLogRecGetData(record), &parsed);
			xid = parsed.twophase_xid;
			if (!TransactionIdIsNormal(xid) || !side_xact_no_commit_effects(&parsed) ||
				(parsed.xinfo & (XACT_XINFO_HAS_TWOPHASE | XACT_XINFO_HAS_GID |
					XACT_XINFO_HAS_DBINFO)) !=
					(XACT_XINFO_HAS_TWOPHASE | XACT_XINFO_HAS_GID |
					 XACT_XINFO_HAS_DBINFO) ||
				!SCN_VALID(parsed.scn) || side_xact_commit_timestamp(&parsed) == 0 ||
				parsed.has_tt_commit ||
				!cluster_remote_xact_prepare_digest_v2(system_identifier,
					origin_thread, xid, parsed.dbId, parsed.twophase_gid,
					candidate.prepare_binding))
				return false;
			candidate.kind = RF_SIDE_XACT_COMMIT_PREPARED;
			candidate.xid = xid;
			candidate.database = parsed.dbId;
			candidate.xinfo = parsed.xinfo;
			candidate.terminal_scn = parsed.scn;
			candidate.terminal_timestamp = side_xact_commit_timestamp(&parsed);
			break;
		}
		case XLOG_XACT_ABORT_PREPARED:
		{
			xl_xact_parsed_abort parsed;

			if (!side_xact_completion_shape_valid(record, false))
				return false;
			ParseAbortRecord(XLogRecGetInfo(record),
				(xl_xact_abort *) XLogRecGetData(record), &parsed);
			xid = parsed.twophase_xid;
			if (!TransactionIdIsNormal(xid) || !side_xact_no_abort_effects(&parsed) ||
				(parsed.xinfo & (XACT_XINFO_HAS_TWOPHASE | XACT_XINFO_HAS_GID |
					XACT_XINFO_HAS_DBINFO)) !=
					(XACT_XINFO_HAS_TWOPHASE | XACT_XINFO_HAS_GID |
					 XACT_XINFO_HAS_DBINFO) || !SCN_VALID(parsed.scn) ||
				!cluster_remote_xact_prepare_digest_v2(system_identifier,
					origin_thread, xid, parsed.dbId, parsed.twophase_gid,
					candidate.prepare_binding))
				return false;
			candidate.kind = RF_SIDE_XACT_ABORT_PREPARED;
			candidate.xid = xid;
			candidate.database = parsed.dbId;
			candidate.xinfo = parsed.xinfo;
			candidate.terminal_scn = parsed.scn;
			break;
		}
		default:
			return false;
	}
	if (!rf_side_xact_structural_preflight_v1(&candidate))
		return false;
	*out = candidate;
	return true;
}

RfSideXactApplyResultV1
rf_side_xact_apply_v1(const RfSideXactOperationV1 *operation)
{
	ClusterRemoteXactMutationV2 mutation;
	ClusterRemoteXactOutcome outcome;
	TimestampTz timestamp;
	SCN			post_scn;
	SCN			durable_scn;
	uint16		post_wrap;
	uint16		durable_segment;
	uint16		durable_slot;
	uint16		durable_wrap;
	bool		post_wrap_valid;

	if (!rf_side_xact_structural_preflight_v1(operation))
		return RF_SIDE_XACT_APPLY_BLOCKED;
	if (operation->kind != RF_SIDE_XACT_COMMIT)
		return RF_SIDE_XACT_APPLY_BLOCKED;

	/* Canonical TT truth first, using only the frozen typed delta. */
	cluster_tt_durable_redo_stamp_slot(operation->tt_delta.instance,
		operation->tt_delta.segment_id, operation->tt_delta.slot_offset,
		operation->tt_delta.wrap, operation->tt_delta.xid,
		operation->tt_delta.commit_scn);
	if (cluster_tt_slot_durable_resolve_by_xid_origin(
			operation->origin_thread - 1, operation->xid,
			operation->tt_delta.wrap, &durable_scn, &durable_segment,
			&durable_slot, &durable_wrap) != CLUSTER_TT_DURABLE_RESOLVED_SCN ||
		durable_scn != operation->terminal_scn ||
		durable_segment != operation->tt_delta.segment_id ||
		durable_slot != operation->tt_delta.slot_offset ||
		durable_wrap != operation->tt_delta.wrap)
		return RF_SIDE_XACT_APPLY_POST_READ_FAILED;

	cluster_scn_recovery_replay_observe(operation->terminal_scn);
	mutation = cluster_remote_xact_store_terminal_v2(
		operation->origin_thread - 1, operation->xid, false, NULL,
		CLUSTER_REMOTE_XACT_COMMITTED, operation->terminal_scn,
		operation->terminal_timestamp, true, operation->tt_delta.wrap);
	if (mutation == CLUSTER_REMOTE_XACT_MUTATION_CONFLICT)
		return RF_SIDE_XACT_APPLY_CONFLICT;
	if (mutation != CLUSTER_REMOTE_XACT_MUTATION_STORED &&
		mutation != CLUSTER_REMOTE_XACT_MUTATION_UNCHANGED)
		return RF_SIDE_XACT_APPLY_BLOCKED;

	outcome = cluster_remote_commit_outcome_ex(operation->origin_thread - 1,
		operation->xid, &post_scn, &post_wrap, &post_wrap_valid);
	if (outcome != CLUSTER_REMOTE_XACT_COMMITTED ||
		post_scn != operation->terminal_scn || !post_wrap_valid ||
		post_wrap != operation->tt_delta.wrap ||
		!cluster_remote_commit_timestamp(operation->origin_thread - 1,
			operation->xid, &timestamp) ||
		timestamp != operation->terminal_timestamp)
		return RF_SIDE_XACT_APPLY_POST_READ_FAILED;
	return RF_SIDE_XACT_APPLY_OK;
}

#endif
