/*-------------------------------------------------------------------------
 *
 * cluster_resource_x_retry.c
 *    Resource-X bounded retry state -- spec-8.7 D7-01.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_resource_x_retry.h"

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
