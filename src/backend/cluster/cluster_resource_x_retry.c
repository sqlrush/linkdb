/*-------------------------------------------------------------------------
 *
 * cluster_resource_x_retry.c
 *    Resource-X bounded retry state -- spec-8.7 D7-01.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_resource_x_retry.h"

static bool
resource_x_retry_state_valid(const ResourceXRetryStateV1 *state)
{
	if (state == NULL || state->state_generation == 0 || state->flags != 0
		|| !resource_x_attempt_matches(&state->attempt, &state->attempt)
		|| state->first_submit_mono_us == 0
		|| state->next_retry_due_mono_us < state->first_submit_mono_us
		|| state->terminal_deadline_mono_us < state->next_retry_due_mono_us
		|| state->last_phase < RESOURCE_X_RETRY_PRE_NO_RETURN
		|| state->last_phase > RESOURCE_X_RETRY_PHASE_RECOVERY_BLOCKED)
		return false;
	if (state->last_phase == RESOURCE_X_RETRY_TERMINAL)
		return state->terminal_errcode != 0;
	return state->terminal_errcode == 0;
}

static bool
resource_x_retry_transport_valid(const ResourceXTransportWitness *transport)
{
	return transport != NULL && transport->cluster_epoch != 0
		&& transport->peer_session_incarnation != 0
		&& transport->connection_generation != 0 && transport->flags == 0;
}

bool
resource_x_retry_state_init(const ResourceXAttemptWitness *attempt,
							uint64 first_submit_mono_us,
							uint64 next_retry_due_mono_us,
							uint64 terminal_deadline_mono_us,
							uint32 state_generation,
							ResourceXRetryStateV1 *out)
{
	ResourceXRetryStateV1 candidate;

	if (attempt == NULL || out == NULL
		|| !resource_x_attempt_matches(attempt, attempt)
		|| first_submit_mono_us == 0
		|| next_retry_due_mono_us < first_submit_mono_us
		|| terminal_deadline_mono_us < next_retry_due_mono_us
		|| state_generation == 0)
		return false;
	memset(&candidate, 0, sizeof(candidate));
	candidate.attempt = *attempt;
	candidate.first_submit_mono_us = first_submit_mono_us;
	candidate.next_retry_due_mono_us = next_retry_due_mono_us;
	candidate.terminal_deadline_mono_us = terminal_deadline_mono_us;
	candidate.last_phase = RESOURCE_X_RETRY_PRE_NO_RETURN;
	candidate.state_generation = state_generation;
	*out = candidate;
	return true;
}

void
resource_x_retry_state_clear(ResourceXRetryStateV1 *state)
{
	if (state != NULL)
		memset(state, 0, sizeof(*state));
}

bool
resource_x_retry_state_is_clear(const ResourceXRetryStateV1 *state)
{
	ResourceXRetryStateV1 zero;

	if (state == NULL)
		return false;
	memset(&zero, 0, sizeof(zero));
	return memcmp(state, &zero, sizeof(zero)) == 0;
}

ResourceXRetryDecision
resource_x_retry_classify_exact(const ResourceXRetryStateV1 *state,
								const ResourceXAttemptWitness *current_attempt,
								const ResourceXTransportWitness *fresh_transport,
								uint64 now_mono_us,
								ResourceXRetryAction *out)
{
	ResourceXRetryAction action;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || !resource_x_retry_state_valid(state)
		|| !resource_x_retry_transport_valid(fresh_transport)
		|| !resource_x_attempt_matches(&state->attempt, current_attempt))
		return RESOURCE_X_RETRY_RECOVERY_BLOCKED;
	if (state->last_phase == RESOURCE_X_RETRY_PHASE_RECOVERY_BLOCKED)
		return RESOURCE_X_RETRY_RECOVERY_BLOCKED;
	if (state->last_phase == RESOURCE_X_RETRY_TERMINAL)
		return RESOURCE_X_RETRY_TERMINAL_DENIED;
	if (now_mono_us >= state->terminal_deadline_mono_us)
		return state->last_phase == RESOURCE_X_RETRY_PRE_NO_RETURN
			? RESOURCE_X_RETRY_TERMINAL_EXHAUSTED
			: RESOURCE_X_RETRY_ROLL_FORWARD;
	if (now_mono_us < state->next_retry_due_mono_us)
		return RESOURCE_X_RETRY_NOT_DUE;

	memset(&action, 0, sizeof(action));
	action.attempt = state->attempt;
	action.transport = *fresh_transport;
	action.expected_state_generation = state->state_generation;
	action.expected_retry_count = state->retry_count;
	*out = action;
	return RESOURCE_X_RETRY_STAGE_EXACT;
}
