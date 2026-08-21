/*-------------------------------------------------------------------------
 *
 * test_cluster_resource_x_retry.c
 *    Resource-X bounded retry state -- spec-8.7 D7-01.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_pcm_x_convert.h"
#include "cluster/cluster_resource_x_retry.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name,
				 int line_number)
{
	printf("# Assert failed: %s at %s:%d\n", condition_name, file_name,
		   line_number);
	abort();
}

static ResourceXAttemptWitness
make_attempt(uint64 base_generation)
{
	BufferTag tag;
	ResourceXAssertion assertion;
	ResourceXAttemptWitness attempt;

	tag.spcOid = 1663;
	tag.dbOid = 5;
	tag.relNumber = 9001;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = 42;
	UT_ASSERT(resource_x_assertion_init(&tag, 3, &assertion));
	UT_ASSERT(resource_x_attempt_init(&assertion, base_generation, &attempt));
	return attempt;
}

static ResourceXTransportWitness
make_transport(uint32 connection_generation)
{
	ResourceXTransportWitness transport;

	MemSet(&transport, 0, sizeof(transport));
	transport.cluster_epoch = 41;
	transport.peer_session_incarnation = 73;
	transport.connection_generation = connection_generation;
	transport.lane_id = 1;
	return transport;
}

UT_TEST(test_retry_state_layout_and_slot_embedding)
{
	UT_ASSERT_EQ(RESOURCE_X_RETRY_PRE_NO_RETURN, 1);
	UT_ASSERT_EQ(RESOURCE_X_RETRY_POST_NO_RETURN, 2);
	UT_ASSERT_EQ(RESOURCE_X_RETRY_TERMINAL, 3);
	UT_ASSERT_EQ(RESOURCE_X_RETRY_PHASE_RECOVERY_BLOCKED, 4);
	UT_ASSERT_EQ(sizeof(ResourceXRetryStateV1), 72);
	UT_ASSERT_EQ(offsetof(ResourceXRetryStateV1, attempt), 0);
	UT_ASSERT_EQ(offsetof(ResourceXRetryStateV1, first_submit_mono_us), 32);
	UT_ASSERT_EQ(offsetof(ResourceXRetryStateV1, next_retry_due_mono_us), 40);
	UT_ASSERT_EQ(offsetof(ResourceXRetryStateV1, terminal_deadline_mono_us), 48);
	UT_ASSERT_EQ(offsetof(ResourceXRetryStateV1, retry_count), 56);
	UT_ASSERT_EQ(offsetof(ResourceXRetryStateV1, terminal_errcode), 60);
	UT_ASSERT_EQ(offsetof(ResourceXRetryStateV1, last_phase), 64);
	UT_ASSERT_EQ(offsetof(ResourceXRetryStateV1, flags), 66);
	UT_ASSERT_EQ(offsetof(ResourceXRetryStateV1, state_generation), 68);

	UT_ASSERT_EQ(offsetof(PcmXMasterTicketSlot, retry_state), 448);
	UT_ASSERT_EQ(sizeof(PcmXMasterTicketSlot), 520);
	UT_ASSERT_EQ(offsetof(PcmXLocalTagSlot, retry_state), 824);
	UT_ASSERT_EQ(sizeof(PcmXLocalTagSlot), 896);
}

UT_TEST(test_retry_state_initialization_publishes_exact_attempt)
{
	ResourceXAttemptWitness attempt = make_attempt(17);
	ResourceXRetryStateV1 state;

	memset(&state, 0xa5, sizeof(state));
	UT_ASSERT(resource_x_retry_state_init(&attempt, 1000, 1010, 6000, 7,
										  &state));
	UT_ASSERT(memcmp(&state.attempt, &attempt, sizeof(attempt)) == 0);
	UT_ASSERT_EQ(state.first_submit_mono_us, 1000);
	UT_ASSERT_EQ(state.next_retry_due_mono_us, 1010);
	UT_ASSERT_EQ(state.terminal_deadline_mono_us, 6000);
	UT_ASSERT_EQ(state.retry_count, 0);
	UT_ASSERT_EQ(state.terminal_errcode, 0);
	UT_ASSERT_EQ(state.last_phase, RESOURCE_X_RETRY_PRE_NO_RETURN);
	UT_ASSERT_EQ(state.flags, 0);
	UT_ASSERT_EQ(state.state_generation, 7);
	UT_ASSERT(!resource_x_retry_state_is_clear(&state));
}

UT_TEST(test_retry_state_initialization_rejects_unpublishable_state)
{
	ResourceXAttemptWitness attempt = make_attempt(17);
	ResourceXRetryStateV1 state;
	ResourceXRetryStateV1 before;

	memset(&state, 0x5a, sizeof(state));
	before = state;
	UT_ASSERT(!resource_x_retry_state_init(NULL, 1000, 1010, 6000, 7,
										   &state));
	UT_ASSERT(memcmp(&state, &before, sizeof(state)) == 0);
	UT_ASSERT(!resource_x_retry_state_init(&attempt, 0, 1010, 6000, 7,
										   &state));
	UT_ASSERT(!resource_x_retry_state_init(&attempt, 1000, 999, 6000, 7,
										   &state));
	UT_ASSERT(!resource_x_retry_state_init(&attempt, 1000, 6001, 6000, 7,
										   &state));
	UT_ASSERT(!resource_x_retry_state_init(&attempt, 1000, 1010, 6000, 0,
										   &state));
	UT_ASSERT(!resource_x_retry_state_init(&attempt, 1000, 1010, 6000, 7,
										   NULL));
	UT_ASSERT(memcmp(&state, &before, sizeof(state)) == 0);
}

UT_TEST(test_retry_state_clear_removes_all_attempt_state)
{
	ResourceXAttemptWitness attempt = make_attempt(17);
	ResourceXRetryStateV1 state;

	UT_ASSERT(resource_x_retry_state_init(&attempt, 1000, 1010, 6000, 7,
										  &state));
	resource_x_retry_state_clear(&state);
	UT_ASSERT(resource_x_retry_state_is_clear(&state));
	state.flags = 1;
	UT_ASSERT(!resource_x_retry_state_is_clear(&state));
	resource_x_retry_state_clear(NULL);
	UT_ASSERT(!resource_x_retry_state_is_clear(NULL));
}

UT_TEST(test_classifier_stages_same_attempt_with_fresh_transport)
{
	ResourceXAttemptWitness attempt = make_attempt(17);
	ResourceXTransportWitness transport = make_transport(9);
	ResourceXRetryStateV1 state;
	ResourceXRetryStateV1 before;
	ResourceXRetryAction action;

	UT_ASSERT(resource_x_retry_state_init(&attempt, 1000, 1010, 6000, 7,
										  &state));
	before = state;
	MemSet(&action, 0xa5, sizeof(action));
	UT_ASSERT_EQ(resource_x_retry_classify_exact(&state, &attempt, &transport,
											 1010, &action),
			 RESOURCE_X_RETRY_STAGE_EXACT);
	UT_ASSERT(memcmp(&action.attempt, &attempt, sizeof(attempt)) == 0);
	UT_ASSERT(memcmp(&action.transport, &transport, sizeof(transport)) == 0);
	UT_ASSERT_EQ(action.expected_state_generation, 7);
	UT_ASSERT_EQ(action.expected_retry_count, 0);
	UT_ASSERT(memcmp(&state, &before, sizeof(state)) == 0);

	transport.connection_generation++;
	UT_ASSERT_EQ(resource_x_retry_classify_exact(&state, &attempt, &transport,
											 1010, &action),
			 RESOURCE_X_RETRY_STAGE_EXACT);
	UT_ASSERT(memcmp(&action.attempt, &attempt, sizeof(attempt)) == 0);
	UT_ASSERT_EQ(action.transport.connection_generation, 10);
}

UT_TEST(test_classifier_never_retries_successor_attempt)
{
	ResourceXAttemptWitness attempt = make_attempt(17);
	ResourceXAttemptWitness successor = make_attempt(18);
	ResourceXTransportWitness transport = make_transport(9);
	ResourceXRetryStateV1 state;
	ResourceXRetryAction action;
	ResourceXRetryAction zero;

	UT_ASSERT(resource_x_retry_state_init(&attempt, 1000, 1010, 6000, 7,
										  &state));
	MemSet(&action, 0xa5, sizeof(action));
	MemSet(&zero, 0, sizeof(zero));
	UT_ASSERT_EQ(resource_x_retry_classify_exact(&state, &successor, &transport,
											 1010, &action),
			 RESOURCE_X_RETRY_RECOVERY_BLOCKED);
	UT_ASSERT(memcmp(&action, &zero, sizeof(action)) == 0);
}

UT_TEST(test_classifier_rejects_unbound_transport_without_staging)
{
	ResourceXAttemptWitness attempt = make_attempt(17);
	ResourceXTransportWitness transport = make_transport(9);
	ResourceXRetryStateV1 state;
	ResourceXRetryAction action;
	ResourceXRetryAction zero;

	UT_ASSERT(resource_x_retry_state_init(&attempt, 1000, 1010, 6000, 7,
										  &state));
	MemSet(&zero, 0, sizeof(zero));
	transport.connection_generation = 0;
	UT_ASSERT_EQ(resource_x_retry_classify_exact(&state, &attempt, &transport,
											 1010, &action),
			 RESOURCE_X_RETRY_RECOVERY_BLOCKED);
	UT_ASSERT(memcmp(&action, &zero, sizeof(action)) == 0);
	transport = make_transport(9);
	transport.flags = 1;
	UT_ASSERT_EQ(resource_x_retry_classify_exact(&state, &attempt, &transport,
											 1010, &action),
			 RESOURCE_X_RETRY_RECOVERY_BLOCKED);
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_retry_state_layout_and_slot_embedding);
	UT_RUN(test_retry_state_initialization_publishes_exact_attempt);
	UT_RUN(test_retry_state_initialization_rejects_unpublishable_state);
	UT_RUN(test_retry_state_clear_removes_all_attempt_state);
	UT_RUN(test_classifier_stages_same_attempt_with_fresh_transport);
	UT_RUN(test_classifier_never_retries_successor_attempt);
	UT_RUN(test_classifier_rejects_unbound_transport_without_staging);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
