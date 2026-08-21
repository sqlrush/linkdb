/*-------------------------------------------------------------------------
 *
 * cluster_resource_x_retry.h
 *    Resource-X bounded retry state -- spec-8.7 D7-01.
 *
 * The state is local shared-memory state.  It is neither a wire record nor
 * part of Resource-X logical attempt identity.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RESOURCE_X_RETRY_H
#define CLUSTER_RESOURCE_X_RETRY_H

#include "cluster/cluster_resource_x_identity.h"

typedef enum ResourceXRetryPhase {
	RESOURCE_X_RETRY_PRE_NO_RETURN = 1,
	RESOURCE_X_RETRY_POST_NO_RETURN = 2,
	RESOURCE_X_RETRY_TERMINAL = 3,
	RESOURCE_X_RETRY_PHASE_RECOVERY_BLOCKED = 4
} ResourceXRetryPhase;

typedef struct ResourceXRetryStateV1 {
	ResourceXAttemptWitness attempt;
	uint64 first_submit_mono_us;
	uint64 next_retry_due_mono_us;
	uint64 terminal_deadline_mono_us;
	uint32 retry_count;
	uint32 terminal_errcode;
	uint16 last_phase;
	uint16 flags;
	uint32 state_generation;
} ResourceXRetryStateV1;

typedef enum ResourceXRetryDecision {
	RESOURCE_X_RETRY_NOT_DUE = 0,
	RESOURCE_X_RETRY_STAGE_EXACT,
	RESOURCE_X_RETRY_WAIT_SCHEDULER,
	RESOURCE_X_RETRY_TERMINAL_DENIED,
	RESOURCE_X_RETRY_TERMINAL_EXHAUSTED,
	RESOURCE_X_RETRY_ROLL_FORWARD,
	RESOURCE_X_RETRY_RECOVERY_BLOCKED
} ResourceXRetryDecision;

typedef struct ResourceXRetryAction {
	ResourceXAttemptWitness attempt;
	ResourceXTransportWitness transport;
	uint32 expected_state_generation;
	uint32 expected_retry_count;
} ResourceXRetryAction;

StaticAssertDecl(sizeof(ResourceXRetryStateV1) == 72,
				 "Resource-X retry state ABI");
StaticAssertDecl(offsetof(ResourceXRetryStateV1, attempt) == 0,
				 "Resource-X retry attempt offset");
StaticAssertDecl(offsetof(ResourceXRetryStateV1, first_submit_mono_us) == 32
					 && offsetof(ResourceXRetryStateV1, next_retry_due_mono_us) == 40
					 && offsetof(ResourceXRetryStateV1, terminal_deadline_mono_us) == 48,
				 "Resource-X retry deadline offsets");
StaticAssertDecl(offsetof(ResourceXRetryStateV1, retry_count) == 56
					 && offsetof(ResourceXRetryStateV1, terminal_errcode) == 60,
				 "Resource-X retry result offsets");
StaticAssertDecl(offsetof(ResourceXRetryStateV1, last_phase) == 64
					 && offsetof(ResourceXRetryStateV1, flags) == 66
					 && offsetof(ResourceXRetryStateV1, state_generation) == 68,
				 "Resource-X retry lifecycle offsets");
StaticAssertDecl(sizeof(ResourceXRetryAction) == 64,
				 "Resource-X retry action ABI");
StaticAssertDecl(offsetof(ResourceXRetryAction, attempt) == 0
					 && offsetof(ResourceXRetryAction, transport) == 32
					 && offsetof(ResourceXRetryAction, expected_state_generation) == 56
					 && offsetof(ResourceXRetryAction, expected_retry_count) == 60,
				 "Resource-X retry action offsets");

extern bool resource_x_retry_state_init(const ResourceXAttemptWitness *attempt,
										uint64 first_submit_mono_us,
										uint64 next_retry_due_mono_us,
										uint64 terminal_deadline_mono_us,
										uint32 state_generation,
										ResourceXRetryStateV1 *out);
extern void resource_x_retry_state_clear(ResourceXRetryStateV1 *state);
extern bool resource_x_retry_state_is_clear(const ResourceXRetryStateV1 *state);
extern ResourceXRetryDecision resource_x_retry_classify_exact(
	const ResourceXRetryStateV1 *state,
	const ResourceXAttemptWitness *current_attempt,
	const ResourceXTransportWitness *fresh_transport,
	uint64 now_mono_us,
	ResourceXRetryAction *out);

#endif /* CLUSTER_RESOURCE_X_RETRY_H */
