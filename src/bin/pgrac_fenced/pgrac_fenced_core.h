/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_core.h
 *	  Provider-neutral pgrac-fenced peer and operation-state contract.
 *
 * Oracle documents failure isolation and Clusterware-owned fencing, but not
 * this split-process FSM.  These closed values implement the approved PGRAC
 * adaptation without selecting or registering a production provider.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_CORE_H
#define PGRAC_FENCED_CORE_H

#include "c.h"

#include <sys/types.h>

#define PGRAC_FENCED_MAX_CLIENTS UINT32_C(128)
#define PGRAC_FENCED_MAX_OPERATIONS UINT32_C(128)

typedef enum PgracFencedState
{
	PGRAC_FENCED_STATE_UNAVAILABLE = 0,
	PGRAC_FENCED_STATE_IDLE = 1,
	PGRAC_FENCED_STATE_QUEUED = 2,
	PGRAC_FENCED_STATE_RESOLVING = 3,
	PGRAC_FENCED_STATE_ACTUATING = 4,
	PGRAC_FENCED_STATE_VERIFYING = 5,
	PGRAC_FENCED_STATE_PROVEN_DURABLE = 6,
	PGRAC_FENCED_STATE_REENABLING = 7,
	PGRAC_FENCED_STATE_REJECTED = 8,
	PGRAC_FENCED_STATE_UNKNOWN = 9,
	PGRAC_FENCED_STATE_INVALIDATED = 10
} PgracFencedState;

typedef enum PgracFencedEvent
{
	PGRAC_FENCED_EVENT_CAPABILITY_READY = 0,
	PGRAC_FENCED_EVENT_REQUEST_TARGET_FREE = 1,
	PGRAC_FENCED_EVENT_REQUEST_JOINABLE = 2,
	PGRAC_FENCED_EVENT_REQUEST_NONJOINABLE = 3,
	PGRAC_FENCED_EVENT_CAPACITY_EXHAUSTED = 4,
	PGRAC_FENCED_EVENT_QUEUE_READY = 5,
	PGRAC_FENCED_EVENT_QUEUE_TIMEOUT = 6,
	PGRAC_FENCED_EVENT_QUEUE_INVALIDATED = 7,
	PGRAC_FENCED_EVENT_RESOLVE_EXACT = 8,
	PGRAC_FENCED_EVENT_RESOLVE_REJECTED = 9,
	PGRAC_FENCED_EVENT_RESOLVE_UNKNOWN = 10,
	PGRAC_FENCED_EVENT_RESOLVE_UNAVAILABLE = 11,
	PGRAC_FENCED_EVENT_ACTUATION_FINISHED = 12,
	PGRAC_FENCED_EVENT_READBACK_OFF_DRAINED = 13,
	PGRAC_FENCED_EVENT_READBACK_OFF_NOT_DRAINED = 14,
	PGRAC_FENCED_EVENT_READBACK_ON = 15,
	PGRAC_FENCED_EVENT_READBACK_UNKNOWN = 16,
	PGRAC_FENCED_EVENT_PROOF_INVALIDATED = 17,
	PGRAC_FENCED_EVENT_INTEGRITY_FAILURE = 18,
	PGRAC_FENCED_EVENT_ADMIN_PREPARE = 19,
	PGRAC_FENCED_EVENT_LMON_CLAIM_OFF_DRAINED = 20,
	PGRAC_FENCED_EVENT_AUTHORIZE_ON = 21,
	PGRAC_FENCED_EVENT_READBACK_ON_DRAINED = 22,
	PGRAC_FENCED_EVENT_REFRESH_ON_DRAINED = 23,
	PGRAC_FENCED_EVENT_REENABLE_FAILURE = 24
} PgracFencedEvent;

typedef enum PgracFencedOutcome
{
	PGRAC_FENCED_OUTCOME_NONE = 0,
	PGRAC_FENCED_OUTCOME_PENDING = 1,
	PGRAC_FENCED_OUTCOME_WRITE_EXCLUDED = 2,
	PGRAC_FENCED_OUTCOME_REJECTED = 3,
	PGRAC_FENCED_OUTCOME_UNKNOWN = 4,
	PGRAC_FENCED_OUTCOME_UNAVAILABLE = 5,
	PGRAC_FENCED_OUTCOME_ADMIN_OFFERED = 6,
	PGRAC_FENCED_OUTCOME_LMON_OFFERED = 7,
	PGRAC_FENCED_OUTCOME_WAITING_JOINER = 8,
	PGRAC_FENCED_OUTCOME_READY = 9
} PgracFencedOutcome;

typedef enum PgracFencedDenyReason
{
	PGRAC_FENCED_DENY_NONE = 0,
	PGRAC_FENCED_DENY_PROTOCOL = 7,
	PGRAC_FENCED_DENY_PROVIDER_REJECTED = 8,
	PGRAC_FENCED_DENY_PROVIDER_UNKNOWN = 9,
	PGRAC_FENCED_DENY_DAEMON_UNAVAILABLE = 10,
	PGRAC_FENCED_DENY_TIMEOUT = 11,
	PGRAC_FENCED_DENY_JOURNAL = 12,
	PGRAC_FENCED_DENY_MAPPING_CHANGED = 17,
	PGRAC_FENCED_DENY_REJOIN_INVALIDATED = 19,
	PGRAC_FENCED_DENY_IO_NOT_DRAINED = 30
} PgracFencedDenyReason;

typedef enum PgracFencedJournalMask
{
	PGRAC_FENCED_JOURNAL_NONE = 0,
	PGRAC_FENCED_JOURNAL_CONFIG_LOADED = 1U << 0,
	PGRAC_FENCED_JOURNAL_REQUEST_ACCEPTED = 1U << 1,
	PGRAC_FENCED_JOURNAL_ACTUATION_ISSUED = 1U << 2,
	PGRAC_FENCED_JOURNAL_ACTUATION_RESULT = 1U << 3,
	PGRAC_FENCED_JOURNAL_READBACK_RESULT = 1U << 4,
	PGRAC_FENCED_JOURNAL_PROOF_SERVED = 1U << 5,
	PGRAC_FENCED_JOURNAL_INVALIDATED = 1U << 6,
	PGRAC_FENCED_JOURNAL_REENABLE_REQUESTED = 1U << 7,
	PGRAC_FENCED_JOURNAL_REENABLE_RESULT = 1U << 8
} PgracFencedJournalMask;

typedef struct PgracFencedTransition
{
	PgracFencedState next_state;
	PgracFencedOutcome outcome;
	PgracFencedDenyReason deny_reason;
	uint32 journal_mask;
} PgracFencedTransition;

typedef struct PgracFencedPeerCredential
{
	uint64 uid;
	uint64 gid;
	int64 pid;
	bool pid_known;
} PgracFencedPeerCredential;

extern bool pgrac_fenced_peer_credential_get(
	int fd, PgracFencedPeerCredential *out);
extern bool pgrac_fenced_peer_is_root(
	const PgracFencedPeerCredential *peer);
extern bool pgrac_fenced_peer_is_db(
	const PgracFencedPeerCredential *peer,
	uint64 allowed_uid, uint64 allowed_gid);
extern bool pgrac_fenced_capacity_available(
	uint32 client_count, uint32 operation_count);
extern bool pgrac_fenced_fsm_step(
	PgracFencedState current, PgracFencedEvent event,
	PgracFencedTransition *out);

#endif /* PGRAC_FENCED_CORE_H */
