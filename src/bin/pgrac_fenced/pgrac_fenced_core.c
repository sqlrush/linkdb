/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_core.c
 *	  Provider-neutral peer authentication and closed daemon FSM.
 *
 *-------------------------------------------------------------------------
 */
#ifdef __linux__
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#endif

#include "postgres_fe.h"

#include <sys/socket.h>
#include <unistd.h>

#include "pgrac_fenced_core.h"

bool
pgrac_fenced_peer_credential_get(int fd, PgracFencedPeerCredential *out)
{
	if (fd < 0 || out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
#ifdef __linux__
	{
		struct ucred credential;
		socklen_t credential_len = sizeof(credential);

		if (getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &credential,
				   &credential_len) != 0 ||
			credential_len != sizeof(credential) || credential.pid <= 0)
			return false;
		out->uid = (uint64) credential.uid;
		out->gid = (uint64) credential.gid;
		out->pid = (int64) credential.pid;
		out->pid_known = true;
	}
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || \
	defined(__NetBSD__)
	{
		uid_t uid;
		gid_t gid;

		if (getpeereid(fd, &uid, &gid) != 0)
			return false;
		out->uid = (uint64) uid;
		out->gid = (uint64) gid;
		out->pid = 0;
		out->pid_known = false;
	}
#else
	return false;
#endif
	return true;
}

bool
pgrac_fenced_peer_is_root(const PgracFencedPeerCredential *peer)
{
	return peer != NULL && peer->uid == 0 &&
		(!peer->pid_known || peer->pid > 0);
}

bool
pgrac_fenced_peer_is_db(const PgracFencedPeerCredential *peer,
						uint64 allowed_uid, uint64 allowed_gid)
{
	return peer != NULL && peer->uid == allowed_uid &&
		peer->gid == allowed_gid && (!peer->pid_known || peer->pid > 0);
}

bool
pgrac_fenced_capacity_available(uint32 client_count, uint32 operation_count)
{
	return client_count < PGRAC_FENCED_MAX_CLIENTS &&
		operation_count < PGRAC_FENCED_MAX_OPERATIONS;
}

static bool
state_is_new_request_source(PgracFencedState state)
{
	return state == PGRAC_FENCED_STATE_IDLE ||
		state == PGRAC_FENCED_STATE_REJECTED ||
		state == PGRAC_FENCED_STATE_UNKNOWN ||
		state == PGRAC_FENCED_STATE_INVALIDATED;
}

static bool
state_allows_join(PgracFencedState state)
{
	return state == PGRAC_FENCED_STATE_RESOLVING ||
		state == PGRAC_FENCED_STATE_ACTUATING ||
		state == PGRAC_FENCED_STATE_VERIFYING;
}

static bool
state_requires_queue(PgracFencedState state)
{
	return state_allows_join(state) ||
		state == PGRAC_FENCED_STATE_PROVEN_DURABLE ||
		state == PGRAC_FENCED_STATE_REENABLING;
}

static void
set_transition(PgracFencedTransition *out, PgracFencedState state,
			   PgracFencedOutcome outcome, PgracFencedDenyReason reason,
			   uint32 journal_mask)
{
	out->next_state = state;
	out->outcome = outcome;
	out->deny_reason = reason;
	out->journal_mask = journal_mask;
}

bool
pgrac_fenced_fsm_step(PgracFencedState current, PgracFencedEvent event,
				  PgracFencedTransition *out)
{
	if (out == NULL)
		return false;
	set_transition(out, PGRAC_FENCED_STATE_UNAVAILABLE,
			   PGRAC_FENCED_OUTCOME_UNAVAILABLE,
			   PGRAC_FENCED_DENY_PROTOCOL, PGRAC_FENCED_JOURNAL_NONE);
	if (current < PGRAC_FENCED_STATE_UNAVAILABLE ||
		current > PGRAC_FENCED_STATE_INVALIDATED ||
		event < PGRAC_FENCED_EVENT_CAPABILITY_READY ||
		event > PGRAC_FENCED_EVENT_REENABLE_FAILURE)
		return false;

	if (event == PGRAC_FENCED_EVENT_INTEGRITY_FAILURE)
	{
		set_transition(out, PGRAC_FENCED_STATE_UNAVAILABLE,
				   PGRAC_FENCED_OUTCOME_UNAVAILABLE,
				   PGRAC_FENCED_DENY_JOURNAL,
				   PGRAC_FENCED_JOURNAL_NONE);
		return true;
	}
	if (event == PGRAC_FENCED_EVENT_CAPACITY_EXHAUSTED)
	{
		set_transition(out, current, PGRAC_FENCED_OUTCOME_UNAVAILABLE,
				   PGRAC_FENCED_DENY_DAEMON_UNAVAILABLE,
				   PGRAC_FENCED_JOURNAL_NONE);
		return true;
	}
	if (current == PGRAC_FENCED_STATE_UNAVAILABLE &&
		event == PGRAC_FENCED_EVENT_CAPABILITY_READY)
	{
		set_transition(out, PGRAC_FENCED_STATE_IDLE,
				   PGRAC_FENCED_OUTCOME_NONE, PGRAC_FENCED_DENY_NONE,
				   PGRAC_FENCED_JOURNAL_CONFIG_LOADED);
		return true;
	}
	if (state_is_new_request_source(current) &&
		event == PGRAC_FENCED_EVENT_REQUEST_TARGET_FREE)
	{
		set_transition(out, PGRAC_FENCED_STATE_RESOLVING,
				   PGRAC_FENCED_OUTCOME_PENDING, PGRAC_FENCED_DENY_NONE,
				   PGRAC_FENCED_JOURNAL_REQUEST_ACCEPTED);
		return true;
	}
	if (state_allows_join(current) &&
		event == PGRAC_FENCED_EVENT_REQUEST_JOINABLE)
	{
		set_transition(out, current, PGRAC_FENCED_OUTCOME_PENDING,
				   PGRAC_FENCED_DENY_NONE,
				   PGRAC_FENCED_JOURNAL_REQUEST_ACCEPTED);
		return true;
	}
	if (state_requires_queue(current) &&
		event == PGRAC_FENCED_EVENT_REQUEST_NONJOINABLE)
	{
		set_transition(out, PGRAC_FENCED_STATE_QUEUED,
				   PGRAC_FENCED_OUTCOME_PENDING, PGRAC_FENCED_DENY_NONE,
				   PGRAC_FENCED_JOURNAL_REQUEST_ACCEPTED);
		return true;
	}
	if (current == PGRAC_FENCED_STATE_QUEUED)
	{
		if (event == PGRAC_FENCED_EVENT_QUEUE_READY)
		{
			set_transition(out, PGRAC_FENCED_STATE_RESOLVING,
					   PGRAC_FENCED_OUTCOME_PENDING,
					   PGRAC_FENCED_DENY_NONE,
					   PGRAC_FENCED_JOURNAL_NONE);
			return true;
		}
		if (event == PGRAC_FENCED_EVENT_QUEUE_TIMEOUT)
		{
			set_transition(out, PGRAC_FENCED_STATE_INVALIDATED,
					   PGRAC_FENCED_OUTCOME_UNKNOWN,
					   PGRAC_FENCED_DENY_TIMEOUT,
					   PGRAC_FENCED_JOURNAL_INVALIDATED);
			return true;
		}
		if (event == PGRAC_FENCED_EVENT_QUEUE_INVALIDATED)
		{
			set_transition(out, PGRAC_FENCED_STATE_INVALIDATED,
					   PGRAC_FENCED_OUTCOME_UNAVAILABLE,
					   PGRAC_FENCED_DENY_MAPPING_CHANGED,
					   PGRAC_FENCED_JOURNAL_INVALIDATED);
			return true;
		}
	}
	if (current == PGRAC_FENCED_STATE_RESOLVING)
	{
		switch (event)
		{
			case PGRAC_FENCED_EVENT_RESOLVE_EXACT:
				set_transition(out, PGRAC_FENCED_STATE_ACTUATING,
						   PGRAC_FENCED_OUTCOME_PENDING,
						   PGRAC_FENCED_DENY_NONE,
						   PGRAC_FENCED_JOURNAL_ACTUATION_ISSUED);
				return true;
			case PGRAC_FENCED_EVENT_RESOLVE_REJECTED:
				set_transition(out, PGRAC_FENCED_STATE_REJECTED,
						   PGRAC_FENCED_OUTCOME_REJECTED,
						   PGRAC_FENCED_DENY_PROVIDER_REJECTED,
						   PGRAC_FENCED_JOURNAL_ACTUATION_RESULT);
				return true;
			case PGRAC_FENCED_EVENT_RESOLVE_UNKNOWN:
				set_transition(out, PGRAC_FENCED_STATE_UNKNOWN,
						   PGRAC_FENCED_OUTCOME_UNKNOWN,
						   PGRAC_FENCED_DENY_PROVIDER_UNKNOWN,
						   PGRAC_FENCED_JOURNAL_ACTUATION_RESULT);
				return true;
			case PGRAC_FENCED_EVENT_RESOLVE_UNAVAILABLE:
				set_transition(out, PGRAC_FENCED_STATE_UNAVAILABLE,
						   PGRAC_FENCED_OUTCOME_UNAVAILABLE,
						   PGRAC_FENCED_DENY_DAEMON_UNAVAILABLE,
						   PGRAC_FENCED_JOURNAL_NONE);
				return true;
			default:
				break;
		}
	}
	if (current == PGRAC_FENCED_STATE_ACTUATING &&
		event == PGRAC_FENCED_EVENT_ACTUATION_FINISHED)
	{
		set_transition(out, PGRAC_FENCED_STATE_VERIFYING,
				   PGRAC_FENCED_OUTCOME_PENDING, PGRAC_FENCED_DENY_NONE,
				   PGRAC_FENCED_JOURNAL_ACTUATION_RESULT);
		return true;
	}
	if (current == PGRAC_FENCED_STATE_VERIFYING)
	{
		switch (event)
		{
			case PGRAC_FENCED_EVENT_READBACK_OFF_DRAINED:
				set_transition(out, PGRAC_FENCED_STATE_PROVEN_DURABLE,
						   PGRAC_FENCED_OUTCOME_WRITE_EXCLUDED,
						   PGRAC_FENCED_DENY_NONE,
						   PGRAC_FENCED_JOURNAL_READBACK_RESULT |
						   PGRAC_FENCED_JOURNAL_PROOF_SERVED);
				return true;
			case PGRAC_FENCED_EVENT_READBACK_OFF_NOT_DRAINED:
				set_transition(out, PGRAC_FENCED_STATE_REJECTED,
						   PGRAC_FENCED_OUTCOME_REJECTED,
						   PGRAC_FENCED_DENY_IO_NOT_DRAINED,
						   PGRAC_FENCED_JOURNAL_READBACK_RESULT);
				return true;
			case PGRAC_FENCED_EVENT_READBACK_ON:
				set_transition(out, PGRAC_FENCED_STATE_REJECTED,
						   PGRAC_FENCED_OUTCOME_REJECTED,
						   PGRAC_FENCED_DENY_PROVIDER_REJECTED,
						   PGRAC_FENCED_JOURNAL_READBACK_RESULT);
				return true;
			case PGRAC_FENCED_EVENT_READBACK_UNKNOWN:
				set_transition(out, PGRAC_FENCED_STATE_UNKNOWN,
						   PGRAC_FENCED_OUTCOME_UNKNOWN,
						   PGRAC_FENCED_DENY_PROVIDER_UNKNOWN,
						   PGRAC_FENCED_JOURNAL_READBACK_RESULT);
				return true;
			default:
				break;
		}
	}
	if (current == PGRAC_FENCED_STATE_PROVEN_DURABLE &&
		event == PGRAC_FENCED_EVENT_PROOF_INVALIDATED)
	{
		set_transition(out, PGRAC_FENCED_STATE_INVALIDATED,
				   PGRAC_FENCED_OUTCOME_NONE, PGRAC_FENCED_DENY_NONE,
				   PGRAC_FENCED_JOURNAL_INVALIDATED);
		return true;
	}
	if ((current == PGRAC_FENCED_STATE_IDLE ||
		 current == PGRAC_FENCED_STATE_PROVEN_DURABLE) &&
		event == PGRAC_FENCED_EVENT_ADMIN_PREPARE)
	{
		set_transition(out, PGRAC_FENCED_STATE_REENABLING,
				   PGRAC_FENCED_OUTCOME_ADMIN_OFFERED,
				   PGRAC_FENCED_DENY_NONE,
				   PGRAC_FENCED_JOURNAL_REENABLE_REQUESTED);
		return true;
	}
	if (current == PGRAC_FENCED_STATE_REENABLING)
	{
		switch (event)
		{
			case PGRAC_FENCED_EVENT_LMON_CLAIM_OFF_DRAINED:
				set_transition(out, current,
						   PGRAC_FENCED_OUTCOME_LMON_OFFERED,
						   PGRAC_FENCED_DENY_NONE,
						   PGRAC_FENCED_JOURNAL_READBACK_RESULT);
				return true;
			case PGRAC_FENCED_EVENT_AUTHORIZE_ON:
				set_transition(out, current,
						   PGRAC_FENCED_OUTCOME_PENDING,
						   PGRAC_FENCED_DENY_NONE,
						   PGRAC_FENCED_JOURNAL_INVALIDATED);
				return true;
			case PGRAC_FENCED_EVENT_READBACK_ON_DRAINED:
				set_transition(out, current,
						   PGRAC_FENCED_OUTCOME_WAITING_JOINER,
						   PGRAC_FENCED_DENY_NONE,
						   PGRAC_FENCED_JOURNAL_REENABLE_RESULT);
				return true;
			case PGRAC_FENCED_EVENT_REFRESH_ON_DRAINED:
				set_transition(out, current, PGRAC_FENCED_OUTCOME_READY,
						   PGRAC_FENCED_DENY_NONE,
						   PGRAC_FENCED_JOURNAL_REENABLE_RESULT);
				return true;
			case PGRAC_FENCED_EVENT_REENABLE_FAILURE:
				set_transition(out, PGRAC_FENCED_STATE_UNKNOWN,
						   PGRAC_FENCED_OUTCOME_UNKNOWN,
						   PGRAC_FENCED_DENY_REJOIN_INVALIDATED,
						   PGRAC_FENCED_JOURNAL_NONE);
				return true;
			default:
				break;
		}
	}
	return false;
}
