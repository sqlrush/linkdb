/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_block0_current.c
 *	  Watched tests for the Candidate-2 cooperative block0 current guard.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "cluster/storage/cluster_undo_block0_current.h"

extern ClusterUndoBlock0Result cluster_undo_block0_current_pin_exclusive(
	ClusterUndoBlock0CurrentGuard *guard, const ClusterUndoBlock0ResolvedRoot *root,
	const ClusterUndoBlock0Generation *expected, ClusterUndoBlock0Pin *pin, char **page);

/*
 * Include the owner so this standalone fixture can inspect its private phase
 * and canonical key builder without widening the frozen public ABI.
 */
#include "../../backend/cluster/storage/cluster_undo_block0_current.c"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

bool cluster_enabled = true;
int cluster_node_id = 0;
int cluster_ges_request_timeout_ms = 1000;
int cluster_ges_retransmit_max_attempts = 5;
struct PGPROC *MyProc = NULL;
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

static TimestampTz fake_now;
static uint64 fake_epoch;
static int32 fake_master;
static uint64 fake_routing_generation;
static ClusterGrdShardPhase fake_shard_phase;
static bool fake_lms_ready;
static ClusterLmonStatus fake_lmon_status;
static bool fake_quorum;
static bool fake_member;
static ClusterSemanticAdmissionResult fake_admission_result;
static bool fake_admission_recheck;
static bool fake_post_pin_recheck_throws;
static ClusterGrdEntryResult fake_reserve_result;
static ClusterGrdGrantAction fake_grant_action;
static ClusterGrdEntryResult fake_promote_result;
static GesReplyWaitPollResult fake_poll_result;
static GesReplyWaitVerdict fake_poll_verdict;
static bool fake_poll_throws;
static bool fake_reply_insert_ok;
static bool fake_outbound_ok;
static bool fake_abandon_raced_grant;
static ClusterUndoBlock0Result fake_sample_result;
static ClusterUndoBlock0Generation fake_sample_generation;
static bool fake_sample_invalidates_authority;
static bool fake_sample_throws;
static ClusterUndoBlock0Result fake_copy_result;
static ClusterUndoBlock0Generation fake_copy_generation;
static bool fake_copy_invalidates_authority;
static bool fake_copy_throws;
static ClusterUndoBlock0Result fake_pin_result;
static bool fake_pin_invalidates_authority;
static bool fake_pin_throws;
static char fake_pin_page[BLCKSZ];
static pg_on_exit_callback fake_exit_callback;

static int event_sequence;
static int reserve_event;
static int insert_event;
static int outbound_event;
static int reply_delete_calls;
static int reply_poll_calls;
static int reservation_cancel_calls;
static int waiter_cancel_calls;
static int local_release_calls;
static int mirror_release_calls;
static int cancel_wait_calls;
static int cleanup_release_calls;
static int semantic_leave_calls;
static int sample_calls;
static int copy_calls;
static int pin_calls;
static int unpin_calls;
static int pin_error_local_cleanup_event;
static int unpin_event;
static int cleanup_release_event;
static int last_outbound_len;
static GesRequestPayload last_outbound;
static GesRequestPayload last_cleanup_release;
static GesReplyWaitKey last_insert_key;
static GesReplyWaitKey last_poll_key;
static ClusterGrdHolderId last_cancel_holder;
static ClusterResId last_cancel_resid;
static uint64 last_cancel_wait_seq;
static ClusterUndoBlock0LogicalKey last_pin_logical;
static ClusterUndoBlock0ResolvedRoot last_pin_root;
static ClusterUndoBlock0Generation last_pin_expected;
static ClusterUndoBlock0Mode last_pin_mode;
static ClusterUndoBlock0AuthorityProof last_pin_proof;

void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

TimestampTz
GetCurrentTimestamp(void)
{
	return fake_now;
}

void
before_shmem_exit(pg_on_exit_callback function, Datum arg pg_attribute_unused())
{
	/* The persistent guard hook and temporary PG_ENSURE hook may coexist. */
	if (function == current_backend_exit)
		fake_exit_callback = function;
}

void
cancel_before_shmem_exit(pg_on_exit_callback function pg_attribute_unused(),
					 Datum arg pg_attribute_unused())
{}

bool
cluster_lms_is_ready(void)
{
	return fake_lms_ready;
}

ClusterLmonStatus
cluster_lmon_status(void)
{
	return fake_lmon_status;
}

bool
cluster_qvotec_in_quorum(void)
{
	return fake_quorum;
}

bool
cluster_membership_is_member(int32 node_id)
{
	return fake_member && node_id == cluster_node_id;
}

uint64
cluster_epoch_get_current(void)
{
	return fake_epoch;
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter(uint64 feature_bit, ClusterSemanticAdmissionSide side,
								  ClusterSemanticAdmissionToken *token)
{
	memset(token, 0, sizeof(*token));
	if (fake_admission_result == CLUSTER_SEMANTIC_ADMISSION_OK) {
		token->feature_bit = feature_bit;
		token->formation_epoch = fake_epoch;
		token->side = side;
		token->entered = true;
	}
	return fake_admission_result;
}

bool
cluster_semantic_activation_recheck(const ClusterSemanticAdmissionToken *token)
{
	if (fake_post_pin_recheck_throws && pin_calls > 0) {
		UT_ASSERT_NOT_NULL(PG_exception_stack);
		siglongjmp(*PG_exception_stack, 1);
	}
	return fake_admission_recheck && token != NULL && token->entered
		   && token->formation_epoch == fake_epoch;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	semantic_leave_calls++;
	if (token != NULL)
		token->entered = false;
}

ClusterUndoBlock0Result
cluster_undo_block0_logical_slot(const ClusterUndoBlock0LogicalKey *logical, uint32 *slot)
{
	uint32 first;

	if (logical == NULL || logical->owner_instance == 0
		|| logical->owner_instance > UNDO_OWNER_INSTANCE_MAX)
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	first = ((uint32)logical->owner_instance - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	if (logical->segment_id < first
		|| logical->segment_id >= first + CLUSTER_UNDO_SEGS_PER_INSTANCE)
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	if (slot != NULL)
		*slot = logical->segment_id - 1;
	return CLUSTER_UNDO_BLOCK0_OK;
}

int32
cluster_grd_lookup_master_gen(const ClusterResId *resid pg_attribute_unused(),
							  uint64 *out_routing_generation)
{
	*out_routing_generation = fake_routing_generation;
	return fake_master;
}

uint32
cluster_grd_shard_for_resource(const ClusterResId *resid pg_attribute_unused())
{
	return 17;
}

ClusterGrdShardPhase
cluster_grd_shard_phase(uint32 shard_id)
{
	UT_ASSERT_EQ(shard_id, 17);
	return fake_shard_phase;
}

uint64
cluster_ges_reply_wait_next_request_id(void)
{
	static uint64 next_request_id = 40;

	return ++next_request_id;
}

ClusterGrdEntryResult
cluster_grd_try_reserve(const ClusterResId *resid pg_attribute_unused(),
						const ClusterGrdHolderId *holder pg_attribute_unused(), int mode,
						int32 self_node_id, bool *fast_path_out, uint64 *gen_snapshot_out)
{
	reserve_event = ++event_sequence;
	UT_ASSERT(mode == ShareLock || mode == ExclusiveLock);
	UT_ASSERT_EQ(self_node_id, cluster_node_id);
	if (fast_path_out != NULL)
		*fast_path_out = fake_master == cluster_node_id;
	if (gen_snapshot_out != NULL)
		*gen_snapshot_out = 77;
	return fake_reserve_result;
}

GesReplyWaitEntry *
cluster_ges_reply_wait_insert(const GesReplyWaitKey *key, TimestampTz deadline pg_attribute_unused())
{
	static GesReplyWaitEntry entry;

	insert_event = ++event_sequence;
	last_insert_key = *key;
	return fake_reply_insert_ok ? &entry : NULL;
}

void
cluster_ges_reply_wait_delete(const GesReplyWaitKey *key pg_attribute_unused())
{
	reply_delete_calls++;
}

GesReplyWaitPollResult
cluster_ges_reply_wait_poll_consume(const GesReplyWaitKey *key, GesReplyWaitVerdict *verdict_out)
{
	reply_poll_calls++;
	last_poll_key = *key;
	if (fake_poll_throws) {
		UT_ASSERT_NOT_NULL(PG_exception_stack);
		siglongjmp(*PG_exception_stack, 1);
	}
	if (fake_poll_result == GES_REPLY_WAIT_POLL_DELIVERED)
		*verdict_out = fake_poll_verdict;
	return fake_poll_result;
}

bool
cluster_ges_reply_wait_mark_abandoned(const GesReplyWaitKey *key pg_attribute_unused(),
									  TimestampTz tombstone_deadline pg_attribute_unused())
{
	return fake_abandon_raced_grant;
}

bool
cluster_grd_outbound_enqueue_backend_request(uint32 dest_node_id, const void *payload,
											 uint16 payload_len)
{
	outbound_event = ++event_sequence;
	UT_ASSERT_EQ(dest_node_id, fake_master);
	last_outbound_len = payload_len;
	memcpy(&last_outbound, payload, Min((Size)payload_len, sizeof(last_outbound)));
	return fake_outbound_ok;
}

void
cluster_grd_outbound_enqueue_cleanup_release(uint32 dest_node_id, const void *payload,
											 uint16 payload_len)
{
	cleanup_release_calls++;
	cleanup_release_event = ++event_sequence;
	UT_ASSERT_EQ(dest_node_id, fake_master);
	UT_ASSERT_EQ(payload_len, sizeof(GesRequestPayload));
	memcpy(&last_cleanup_release, payload, sizeof(last_cleanup_release));
}

ClusterGrdGrantAction
cluster_grd_entry_enqueue_or_grant(const ClusterResId *resid pg_attribute_unused(),
								   const ClusterGrdHolderId *holder pg_attribute_unused(),
								   int32 source_node_id pg_attribute_unused(),
								   uint64 request_id pg_attribute_unused(),
								   uint64 shard_master_generation pg_attribute_unused(),
								   uint32 request_opcode pg_attribute_unused(), int lockmode,
								   ClusterGrdConflictHolder *conflict_holders_out pg_attribute_unused(),
								   int *n_conflict_out)
{
	UT_ASSERT(insert_event != 0);
	UT_ASSERT(lockmode == ShareLock || lockmode == ExclusiveLock);
	*n_conflict_out = 0;
	return fake_grant_action;
}

void
cluster_ges_send_bast_targeted(const ClusterResId *resid pg_attribute_unused(),
							   int requested_mode pg_attribute_unused(),
							   const ClusterGrdConflictHolder *holders pg_attribute_unused(),
							   int n_holders pg_attribute_unused())
{}

ClusterGrdEntryResult
cluster_grd_revalidate_and_promote(const ClusterResId *resid pg_attribute_unused(),
								   const ClusterGrdHolderId *holder pg_attribute_unused(),
								   int32 self_node_id pg_attribute_unused(),
								   uint64 gen_snapshot)
{
	UT_ASSERT_EQ(gen_snapshot, 77);
	return fake_promote_result;
}

ClusterGrdEntryResult
cluster_grd_cancel_reservation_by_id(const ClusterResId *resid pg_attribute_unused(),
									 const ClusterGrdHolderId *holder pg_attribute_unused())
{
	reservation_cancel_calls++;
	return CLUSTER_GRD_ENTRY_OK;
}

ClusterGrdEntryResult
cluster_grd_cancel_waiter_by_id_seq(const ClusterResId *resid,
									const ClusterGrdHolderId *holder, uint64 wait_seq)
{
	waiter_cancel_calls++;
	last_cancel_resid = *resid;
	last_cancel_holder = *holder;
	last_cancel_wait_seq = wait_seq;
	return CLUSTER_GRD_ENTRY_OK;
}

void
cluster_ges_release_and_drain_local(const ClusterResId *resid pg_attribute_unused(),
									const ClusterGrdHolderId *holder pg_attribute_unused())
{
	local_release_calls++;
}

ClusterGrdEntryResult
cluster_grd_release_holder_by_id(const ClusterResId *resid pg_attribute_unused(),
								 const ClusterGrdHolderId *holder pg_attribute_unused())
{
	mirror_release_calls++;
	return CLUSTER_GRD_ENTRY_OK;
}

void
cluster_ges_send_cancel_wait(int32 master_node_id, const ClusterResId *resid,
							 const ClusterGrdHolderId *waiter, uint64 wait_seq,
							 uint64 cancel_id, uint8 kind)
{
	cancel_wait_calls++;
	UT_ASSERT_EQ(master_node_id, fake_master);
	UT_ASSERT_EQ(cancel_id, 0);
	UT_ASSERT_EQ(kind, GES_CANCEL_WAIT_KIND_REQUEST);
	last_cancel_resid = *resid;
	last_cancel_holder = *waiter;
	last_cancel_wait_seq = wait_seq;
}

ClusterUndoBlock0Result
cluster_undo_block0_sample_resident_generation(
	const ClusterUndoBlock0LogicalKey *logical pg_attribute_unused(),
	const ClusterUndoBlock0ResolvedRoot *expected_root pg_attribute_unused(),
	const ClusterUndoBlock0AuthorityProof *proof, ClusterUndoBlock0Generation *observed_generation)
{
	sample_calls++;
	UT_ASSERT_EQ(proof->kind, CLUSTER_UNDO_BLOCK0_LIVE_OWNER);
	UT_ASSERT_EQ(proof->cluster_epoch, fake_epoch);
	if (fake_sample_throws) {
		UT_ASSERT_NOT_NULL(PG_exception_stack);
		siglongjmp(*PG_exception_stack, 1);
	}
	if (fake_sample_result == CLUSTER_UNDO_BLOCK0_OK)
		*observed_generation = fake_sample_generation;
	if (fake_sample_invalidates_authority)
		fake_admission_recheck = false;
	return fake_sample_result;
}

ClusterUndoBlock0Result
cluster_undo_block0_copy_resident(const ClusterUndoBlock0LogicalKey *logical pg_attribute_unused(),
								  const ClusterUndoBlock0ResolvedRoot *expected_root pg_attribute_unused(),
								  const ClusterUndoBlock0Generation *expected pg_attribute_unused(),
								  const ClusterUndoBlock0AuthorityProof *proof,
								  char private_page[BLCKSZ],
								  ClusterUndoBlock0Generation *observed_generation)
{
	copy_calls++;
	UT_ASSERT_EQ(proof->kind, CLUSTER_UNDO_BLOCK0_LIVE_OWNER);
	if (fake_copy_throws) {
		UT_ASSERT_NOT_NULL(PG_exception_stack);
		siglongjmp(*PG_exception_stack, 1);
	}
	if (fake_copy_result == CLUSTER_UNDO_BLOCK0_OK) {
		memset(private_page, 0x5a, BLCKSZ);
		*observed_generation = fake_copy_generation;
	}
	if (fake_copy_invalidates_authority)
		fake_admission_recheck = false;
	return fake_copy_result;
}

ClusterUndoBlock0Result
cluster_undo_block0_pin(const ClusterUndoBlock0LogicalKey *logical,
						 const ClusterUndoBlock0ResolvedRoot *expected_root,
						 const ClusterUndoBlock0Generation *expected,
						 ClusterUndoBlock0Mode mode,
						 const ClusterUndoBlock0AuthorityProof *proof,
						 ClusterUndoBlock0Pin *pin, char **page)
{
	pin_calls++;
	last_pin_logical = *logical;
	last_pin_root = *expected_root;
	last_pin_expected = *expected;
	last_pin_mode = mode;
	last_pin_proof = *proof;
	if (fake_pin_throws) {
		/* The real resident primitive must unwind its local reservation first. */
		pin_error_local_cleanup_event = ++event_sequence;
		UT_ASSERT_NOT_NULL(PG_exception_stack);
		siglongjmp(*PG_exception_stack, 1);
	}
	if (fake_pin_result == CLUSTER_UNDO_BLOCK0_OK) {
		memset(pin, 0, sizeof(*pin));
		pin->slot = 7;
		pin->logical = *logical;
		pin->resolved_root = *expected_root;
		pin->observed_generation = *expected;
		pin->mode = mode;
		pin->proof = *proof;
		*page = fake_pin_page;
	}
	if (fake_pin_invalidates_authority)
		fake_admission_recheck = false;
	return fake_pin_result;
}

void
cluster_undo_block0_unpin(ClusterUndoBlock0Pin *pin)
{
	unpin_calls++;
	unpin_event = ++event_sequence;
	if (pin != NULL)
		pin->slot = -1;
}

static ClusterUndoBlock0LogicalKey
test_key(uint8 owner_instance)
{
	ClusterUndoBlock0LogicalKey key;

	memset(&key, 0, sizeof(key));
	key.owner_instance = owner_instance;
	key.segment_id = ((uint32)owner_instance - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	return key;
}

static ClusterUndoBlock0ResolvedRoot
test_root(void)
{
	ClusterUndoBlock0ResolvedRoot root;

	memset(&root, 0, sizeof(root));
	root.intent = CLUSTER_UNDO_PATH_RUNTIME_SHARED;
	root.root_id = 91;
	root.root_generation = 7;
	return root;
}

static void
reset_fixture(void)
{
	fake_now = UINT64_C(1000000);
	fake_epoch = 9;
	fake_master = 2;
	fake_routing_generation = UINT64_C(0x0000000900000004);
	fake_shard_phase = GRD_SHARD_NORMAL;
	fake_lms_ready = true;
	fake_lmon_status = CLUSTER_LMON_READY;
	fake_quorum = true;
	fake_member = true;
	fake_admission_result = CLUSTER_SEMANTIC_ADMISSION_OK;
	fake_admission_recheck = true;
	fake_post_pin_recheck_throws = false;
	fake_reserve_result = CLUSTER_GRD_ENTRY_OK;
	fake_grant_action = CLUSTER_GRD_ENQUEUED_WAITER;
	fake_promote_result = CLUSTER_GRD_ENTRY_OK;
	fake_poll_result = GES_REPLY_WAIT_POLL_PENDING;
	fake_poll_verdict = (GesReplyWaitVerdict){ GES_REPLY_OPCODE_GRANT,
											  GES_REJECT_REASON_NONE };
	fake_poll_throws = false;
	fake_reply_insert_ok = true;
	fake_outbound_ok = true;
	fake_abandon_raced_grant = false;
	fake_sample_result = CLUSTER_UNDO_BLOCK0_OK;
	fake_sample_generation = (ClusterUndoBlock0Generation){ true, 0 };
	fake_sample_invalidates_authority = false;
	fake_sample_throws = false;
	fake_copy_result = CLUSTER_UNDO_BLOCK0_OK;
	fake_copy_generation = (ClusterUndoBlock0Generation){ true, 0 };
	fake_copy_invalidates_authority = false;
	fake_copy_throws = false;
	fake_pin_result = CLUSTER_UNDO_BLOCK0_OK;
	fake_pin_invalidates_authority = false;
	fake_pin_throws = false;
	memset(fake_pin_page, 0x6b, sizeof(fake_pin_page));
	fake_exit_callback = current_exit_hook_registered ? current_backend_exit : NULL;
	event_sequence = reserve_event = insert_event = outbound_event = 0;
	reply_delete_calls = reply_poll_calls = 0;
	reservation_cancel_calls = waiter_cancel_calls = local_release_calls = 0;
	mirror_release_calls = cancel_wait_calls = cleanup_release_calls = 0;
	semantic_leave_calls = sample_calls = copy_calls = 0;
	pin_calls = unpin_calls = 0;
	pin_error_local_cleanup_event = unpin_event = cleanup_release_event = 0;
	last_outbound_len = 0;
	memset(&last_outbound, 0, sizeof(last_outbound));
	memset(&last_cleanup_release, 0, sizeof(last_cleanup_release));
	memset(&last_insert_key, 0, sizeof(last_insert_key));
	memset(&last_poll_key, 0, sizeof(last_poll_key));
	memset(&last_pin_logical, 0, sizeof(last_pin_logical));
	memset(&last_pin_root, 0, sizeof(last_pin_root));
	memset(&last_pin_expected, 0, sizeof(last_pin_expected));
	memset(&last_pin_proof, 0, sizeof(last_pin_proof));
}

static void
acquire_remote_held(ClusterUndoBlock0CurrentGuard *guard)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	memset(guard, 0, sizeof(*guard));
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
													 guard, &failure),
			 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	fake_poll_result = GES_REPLY_WAIT_POLL_DELIVERED;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_HELD);
}

static void
acquire_remote_xcur_held(ClusterUndoBlock0CurrentGuard *guard)
{
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	memset(guard, 0, sizeof(*guard));
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(
				 &key, CLUSTER_UNDO_BLOCK0_XCUR, 1000, guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	fake_poll_result = GES_REPLY_WAIT_POLL_DELIVERED;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_HELD);
}

UT_TEST(test_key_guard_and_phase_abi)
{
	ClusterUndoBlock0LogicalKey key = test_key(128);
	ClusterResId resid;

	reset_fixture();
	UT_ASSERT_EQ(sizeof(ClusterUndoBlock0CurrentGuard), 168);
	UT_ASSERT_EQ(sizeof(ClusterUndoBlock0CurrentGuardData), 168);
	UT_ASSERT_EQ(CLUSTER_UNDO_BLOCK0_CURRENT_UNUSED, 0);
	UT_ASSERT_EQ(CLUSTER_UNDO_BLOCK0_CURRENT_ACQUIRE_WAIT, 1);
	UT_ASSERT_EQ(CLUSTER_UNDO_BLOCK0_CURRENT_PHASE_HELD, 2);
	UT_ASSERT_EQ(CLUSTER_UNDO_BLOCK0_CURRENT_RELEASE_WAIT, 3);
	UT_ASSERT_EQ(CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP, 4);
	UT_ASSERT_EQ(CLUSTER_UNDO_BLOCK0_CURRENT_STARTUP_FENCED_XCUR, 5);
	UT_ASSERT(current_resid_build(&key, &resid));
	UT_ASSERT_EQ(resid.field1, key.segment_id);
	UT_ASSERT_EQ(resid.field2, 0);
	UT_ASSERT_EQ(resid.field3, 0);
	UT_ASSERT_EQ(resid.field4, key.owner_instance);
	UT_ASSERT_EQ(resid.type, 0xFB);
	UT_ASSERT_EQ(resid.lockmethodid, DEFAULT_LOCKMETHOD);
}

UT_TEST(test_startup_namespace_check_rejects_every_reserved_or_unfrozen_type)
{
	uint16 type;

	for (type = UINT8_C(0xF0); type <= UINT8_C(0xFA); type++)
		UT_ASSERT(!current_resid_namespace_valid((uint8)type));
	UT_ASSERT(current_resid_namespace_valid(UINT8_C(0xFB)));
	UT_ASSERT(!current_resid_namespace_valid(UINT8_C(0xFC)));
}

UT_TEST(test_startup_fenced_xcur_begin_end_owns_exact_private_phase)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0CurrentGuard other = { 0 };
	ClusterUndoBlock0CurrentGuardData *data = current_guard_data(&guard);

	reset_fixture();
	UT_ASSERT(cluster_undo_block0_current_startup_fenced_begin(&guard));
	UT_ASSERT_EQ(data->phase, CLUSTER_UNDO_BLOCK0_CURRENT_STARTUP_FENCED_XCUR);
	UT_ASSERT(data->active_linked);
	UT_ASSERT_NOT_NULL(fake_exit_callback);
	UT_ASSERT(cluster_undo_block0_current_startup_fenced_owned());
	UT_ASSERT(!cluster_undo_block0_current_startup_fenced_begin(&guard));
	UT_ASSERT(!cluster_undo_block0_current_startup_fenced_begin(&other));
	UT_ASSERT(cluster_undo_block0_current_startup_fenced_end(&guard));
	UT_ASSERT_EQ(memcmp(&guard, &(ClusterUndoBlock0CurrentGuard){ 0 }, sizeof(guard)), 0);
	UT_ASSERT(!cluster_undo_block0_current_startup_fenced_owned());
	UT_ASSERT(!cluster_undo_block0_current_startup_fenced_end(&guard));
}

UT_TEST(test_begin_installs_before_exact_72_byte_remote_send)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;
	ClusterUndoBlock0CurrentGuardData *data = current_guard_data(&guard);

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
													 &guard, &failure),
			 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT(reserve_event < insert_event && insert_event < outbound_event);
	UT_ASSERT_EQ(last_outbound_len, 72);
	UT_ASSERT_EQ(last_outbound.opcode, GES_REQ_OPCODE_REQUEST);
	UT_ASSERT_EQ(last_outbound.lockmode, ShareLock);
	UT_ASSERT_EQ(last_outbound.wait_seq, 0);
	UT_ASSERT_EQ(last_insert_key.request_opcode, GES_REQ_OPCODE_REQUEST);
	UT_ASSERT_EQ(last_insert_key.request_id, data->holder.request_id);
	UT_ASSERT_EQ(data->phase, CLUSTER_UNDO_BLOCK0_CURRENT_ACQUIRE_WAIT);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_pending_poll_is_pure_nonblocking_and_keeps_correlation)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
													 &guard, &failure),
			 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
			 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_EQ(reply_poll_calls, 1);
	UT_ASSERT_EQ(reply_delete_calls, 0);
	UT_ASSERT_EQ(failure, CLUSTER_UNDO_BLOCK0_NOT_FOUND);
	UT_ASSERT_EQ(last_poll_key.request_opcode, GES_REQ_OPCODE_REQUEST);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_delivered_grant_promotes_to_held)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };

	reset_fixture();
	acquire_remote_held(&guard);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase, CLUSTER_UNDO_BLOCK0_CURRENT_PHASE_HELD);
	UT_ASSERT(!current_guard_data(&guard)->reply_installed);
	UT_ASSERT(!current_guard_data(&guard)->reservation_held);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_remote_grant_with_failed_promote_stages_reliable_release)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(
				 &key, CLUSTER_UNDO_BLOCK0_SCUR, 1000, &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	fake_promote_result = CLUSTER_GRD_ENTRY_NOT_FOUND;
	fake_poll_result = GES_REPLY_WAIT_POLL_DELIVERED;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_FAILED);
	UT_ASSERT_EQ(failure, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ(cleanup_release_calls, 1);
	UT_ASSERT_EQ(last_cleanup_release.opcode, GES_REQ_OPCODE_RELEASE);
	UT_ASSERT_EQ(cancel_wait_calls, 1);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase,
				 CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP);
}

UT_TEST(test_local_grant_with_failed_promote_drains_local_holder)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	reset_fixture();
	fake_master = cluster_node_id;
	fake_grant_action = CLUSTER_GRD_GRANT_NOW;
	fake_promote_result = CLUSTER_GRD_ENTRY_NOT_FOUND;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(
				 &key, CLUSTER_UNDO_BLOCK0_XCUR, 1000, &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_FAILED);
	UT_ASSERT_EQ(failure, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ(local_release_calls, 1);
	UT_ASSERT_EQ(cleanup_release_calls, 0);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase,
				 CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP);
}

UT_TEST(test_cancel_tombstones_exact_pending_and_releases_raced_grant)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
													 &guard, &failure),
			 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	fake_abandon_raced_grant = true;
	cluster_undo_block0_current_cancel(&guard);
	UT_ASSERT_EQ(cancel_wait_calls, 1);
	UT_ASSERT_EQ(last_cancel_wait_seq, 0);
	UT_ASSERT_EQ(cleanup_release_calls, 1);
	UT_ASSERT_EQ(last_cleanup_release.opcode, GES_REQ_OPCODE_RELEASE);
	UT_ASSERT_EQ(reservation_cancel_calls, 1);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase, CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP);
}

UT_TEST(test_send_failure_cleans_only_created_local_obligations)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	reset_fixture();
	fake_outbound_ok = false;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
													 &guard, &failure),
			 CLUSTER_UNDO_BLOCK0_CURRENT_FAILED);
	UT_ASSERT_EQ(failure, CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE);
	UT_ASSERT_EQ(cancel_wait_calls, 0);
	UT_ASSERT_EQ(cleanup_release_calls, 0);
	UT_ASSERT_EQ(reservation_cancel_calls, 1);
	UT_ASSERT_EQ(reply_delete_calls, 1);
}

UT_TEST(test_nonzero_unused_guard_is_rejected_before_any_mutation)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	reset_fixture();
	guard.opaque[167] = 1;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
													 &guard, &failure),
			 CLUSTER_UNDO_BLOCK0_CURRENT_FAILED);
	UT_ASSERT_EQ(failure, CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH);
	UT_ASSERT_EQ(reserve_event, 0);
	UT_ASSERT_EQ(insert_event, 0);
	UT_ASSERT_EQ(outbound_event, 0);
}

UT_TEST(test_same_resid_nesting_refuses_second_guard_without_touching_first)
{
	ClusterUndoBlock0CurrentGuard first = { 0 };
	ClusterUndoBlock0CurrentGuard second = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;
	uint64 first_request_id;

	reset_fixture();
	acquire_remote_held(&first);
	first_request_id = current_guard_data(&first)->holder.request_id;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(
				 &key, CLUSTER_UNDO_BLOCK0_SCUR, 1000, &second, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_FAILED);
	UT_ASSERT_EQ(failure, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ(memcmp(second.opaque, (const uint8[168]){ 0 },
					 sizeof(second.opaque)), 0);
	UT_ASSERT_EQ(current_guard_data(&first)->phase,
				 CLUSTER_UNDO_BLOCK0_CURRENT_PHASE_HELD);
	UT_ASSERT_EQ(current_guard_data(&first)->holder.request_id, first_request_id);
	cluster_undo_block0_current_cancel(&first);
}

UT_TEST(test_preflight_failure_restores_reusable_zero_guard)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	reset_fixture();
	fake_lms_ready = false;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
													 &guard, &failure),
			 CLUSTER_UNDO_BLOCK0_CURRENT_FAILED);
	UT_ASSERT_EQ(failure, CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ(memcmp(guard.opaque, (const uint8[168]){ 0 }, sizeof(guard.opaque)), 0);
	fake_lms_ready = true;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
													 &guard, &failure),
			 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_release_retains_mirror_until_exact_ack_is_consumed)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	reset_fixture();
	acquire_remote_held(&guard);
	fake_poll_result = GES_REPLY_WAIT_POLL_PENDING;
	UT_ASSERT_EQ(cluster_undo_block0_current_release_begin(&guard, &failure),
			 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_EQ(last_outbound.opcode, GES_REQ_OPCODE_RELEASE);
	UT_ASSERT_EQ(mirror_release_calls, 0);
	UT_ASSERT_EQ(cluster_undo_block0_current_release_poll(&guard, &failure),
			 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_EQ(mirror_release_calls, 0);
	fake_poll_result = GES_REPLY_WAIT_POLL_DELIVERED;
	UT_ASSERT_EQ(cluster_undo_block0_current_release_poll(&guard, &failure),
			 CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED);
	UT_ASSERT_EQ(last_poll_key.request_opcode, GES_REQ_OPCODE_RELEASE);
	UT_ASSERT_EQ(mirror_release_calls, 1);
}

UT_TEST(test_release_reuses_exact_canonical_72_byte_ges_shape)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;
	ClusterUndoBlock0CurrentGuardData *data = current_guard_data(&guard);

	reset_fixture();
	acquire_remote_held(&guard);
	fake_poll_result = GES_REPLY_WAIT_POLL_PENDING;
	UT_ASSERT_EQ(cluster_undo_block0_current_release_begin(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_EQ(last_outbound_len, sizeof(GesRequestPayload));
	UT_ASSERT_EQ(last_outbound.opcode, GES_REQ_OPCODE_RELEASE);
	UT_ASSERT_EQ(last_outbound.lockmode, NoLock);
	UT_ASSERT_EQ(last_outbound.waiter_xid, InvalidTransactionId);
	UT_ASSERT_EQ(last_outbound.wait_seq, 0);
	UT_ASSERT_EQ(last_outbound.shard_master_generation_lo,
				 (uint32)(data->routing_generation & UINT64_C(0xffffffff)));
	UT_ASSERT_EQ(last_outbound.shard_master_generation_hi,
				 (uint32)(data->routing_generation >> 32));
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_explicit_perpetual_timeout_survives_acquire_and_release)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0CurrentGuardData *data = current_guard_data(&guard);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(
				 &key, CLUSTER_UNDO_BLOCK0_SCUR, -1, &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_EQ(data->timeout_ms, -1);
	UT_ASSERT_EQ(data->deadline, 0);
	fake_poll_result = GES_REPLY_WAIT_POLL_DELIVERED;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_HELD);
	UT_ASSERT_EQ(cluster_undo_block0_current_release_begin(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_EQ(data->timeout_ms, -1);
	UT_ASSERT_EQ(data->deadline, 0);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_perpetual_acquire_retransmits_past_attempt_threshold)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0CurrentGuardData *data = current_guard_data(&guard);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(
				 &key, CLUSTER_UNDO_BLOCK0_SCUR, -1, &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	data->retry_attempt = (uint16)cluster_ges_retransmit_max_attempts;
	fake_now = data->next_retry_at;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_EQ(data->retry_attempt,
				 (uint16)cluster_ges_retransmit_max_attempts + 1);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_perpetual_release_retransmits_past_attempt_threshold)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0CurrentGuardData *data = current_guard_data(&guard);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(
				 &key, CLUSTER_UNDO_BLOCK0_SCUR, -1, &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	fake_poll_result = GES_REPLY_WAIT_POLL_DELIVERED;
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_HELD);
	fake_poll_result = GES_REPLY_WAIT_POLL_PENDING;
	UT_ASSERT_EQ(cluster_undo_block0_current_release_begin(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	data->retry_attempt = (uint16)cluster_ges_retransmit_max_attempts;
	fake_now = data->next_retry_at;
	UT_ASSERT_EQ(cluster_undo_block0_current_release_poll(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_EQ(data->retry_attempt,
				 (uint16)cluster_ges_retransmit_max_attempts + 1);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_generation_zero_is_valid_and_max_is_exhausted)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0ResolvedRoot root = test_root();
	ClusterUndoBlock0Generation observed = { false, 77 };
	ClusterUndoBlock0Generation expected = { true, 0 };
	char page[BLCKSZ];

	reset_fixture();
	acquire_remote_held(&guard);
	UT_ASSERT_EQ(cluster_undo_block0_current_sample_generation(&guard, &root, &observed),
			 CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT(observed.known);
	UT_ASSERT_EQ(observed.value, 0);
	UT_ASSERT_EQ(cluster_undo_block0_current_copy_resident(&guard, &root, &expected, page),
			 CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ((unsigned char)page[0], 0x5a);
	fake_sample_generation.value = UINT32_MAX;
	observed = (ClusterUndoBlock0Generation){ false, 99 };
	UT_ASSERT_EQ(cluster_undo_block0_current_sample_generation(&guard, &root, &observed),
			 CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH);
	UT_ASSERT(!observed.known);
	UT_ASSERT_EQ(observed.value, 99);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_xcur_guard_cannot_use_scur_sampling_or_copy_surface)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0ResolvedRoot root = test_root();
	ClusterUndoBlock0Generation observed = { false, 77 };
	ClusterUndoBlock0Generation expected = { true, 0 };
	char page[BLCKSZ];

	reset_fixture();
	acquire_remote_xcur_held(&guard);
	memset(page, 0xa5, sizeof(page));
	UT_ASSERT_EQ(cluster_undo_block0_current_sample_generation(
				 &guard, &root, &observed),
				 CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT(!observed.known);
	UT_ASSERT_EQ(observed.value, 77);
	UT_ASSERT_EQ(sample_calls, 0);
	UT_ASSERT_EQ(cluster_undo_block0_current_copy_resident(
				 &guard, &root, &expected, page),
				 CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ(copy_calls, 0);
	UT_ASSERT_EQ((unsigned char)page[0], 0xa5);
	UT_ASSERT_EQ((unsigned char)page[BLCKSZ - 1], 0xa5);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_copy_generation_drift_does_not_publish_private_bytes)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0ResolvedRoot root = test_root();
	ClusterUndoBlock0Generation expected = { true, 0 };
	char page[BLCKSZ];

	reset_fixture();
	acquire_remote_held(&guard);
	fake_copy_generation = (ClusterUndoBlock0Generation){ true, 1 };
	memset(page, 0xa5, sizeof(page));
	UT_ASSERT_EQ(cluster_undo_block0_current_copy_resident(
				 &guard, &root, &expected, page),
				 CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH);
	UT_ASSERT_EQ((unsigned char)page[0], 0xa5);
	UT_ASSERT_EQ((unsigned char)page[BLCKSZ - 1], 0xa5);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_sample_authority_drift_does_not_publish_generation)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0ResolvedRoot root = test_root();
	ClusterUndoBlock0Generation observed = { false, 77 };

	reset_fixture();
	acquire_remote_held(&guard);
	fake_sample_generation = (ClusterUndoBlock0Generation){ true, 9 };
	fake_sample_invalidates_authority = true;
	UT_ASSERT_EQ(cluster_undo_block0_current_sample_generation(
				 &guard, &root, &observed),
				 CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT(!observed.known);
	UT_ASSERT_EQ(observed.value, 77);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_copy_authority_drift_does_not_publish_private_bytes)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0ResolvedRoot root = test_root();
	ClusterUndoBlock0Generation expected = { true, 0 };
	char page[BLCKSZ];

	reset_fixture();
	acquire_remote_held(&guard);
	fake_copy_invalidates_authority = true;
	memset(page, 0xa5, sizeof(page));
	UT_ASSERT_EQ(cluster_undo_block0_current_copy_resident(
				 &guard, &root, &expected, page),
				 CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ((unsigned char)page[0], 0xa5);
	UT_ASSERT_EQ((unsigned char)page[BLCKSZ - 1], 0xa5);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_xcur_exclusive_pin_binds_guard_root_generation_and_live_proof)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0ResolvedRoot root = test_root();
	ClusterUndoBlock0Generation expected = { true, 0 };
	ClusterUndoBlock0Pin pin;
	char *page = NULL;

	reset_fixture();
	acquire_remote_xcur_held(&guard);
	memset(&pin, 0xa5, sizeof(pin));
	UT_ASSERT_EQ(cluster_undo_block0_current_pin_exclusive(
				 &guard, &root, &expected, &pin, &page), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(pin_calls, 1);
	UT_ASSERT_EQ(last_pin_logical.segment_id, test_key(1).segment_id);
	UT_ASSERT_EQ(last_pin_logical.owner_instance, 1);
	UT_ASSERT_EQ(last_pin_root.root_id, root.root_id);
	UT_ASSERT_EQ(last_pin_root.root_generation, root.root_generation);
	UT_ASSERT(last_pin_expected.known);
	UT_ASSERT_EQ(last_pin_expected.value, 0);
	UT_ASSERT_EQ(last_pin_mode, CLUSTER_UNDO_BLOCK0_EXCLUSIVE);
	UT_ASSERT_EQ(last_pin_proof.kind, CLUSTER_UNDO_BLOCK0_LIVE_OWNER);
	UT_ASSERT_EQ(last_pin_proof.owner_instance, 1);
	UT_ASSERT(last_pin_proof.cluster_epoch_present);
	UT_ASSERT_EQ(last_pin_proof.cluster_epoch, fake_epoch);
	UT_ASSERT_EQ(pin.slot, 7);
	UT_ASSERT_EQ(page, fake_pin_page);
	cluster_undo_block0_unpin(&pin);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_exclusive_pin_refuses_scur_and_nonheld_without_publishing_outputs)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0ResolvedRoot root = test_root();
	ClusterUndoBlock0Generation expected = { true, 0 };
	ClusterUndoBlock0Pin pin;
	ClusterUndoBlock0Pin sentinel;
	char *page = (char *)(uintptr_t)0x1234;

	reset_fixture();
	memset(&pin, 0xa5, sizeof(pin));
	sentinel = pin;
	UT_ASSERT_EQ(cluster_undo_block0_current_pin_exclusive(
				 &guard, &root, &expected, &pin, &page),
				 CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ(memcmp(&pin, &sentinel, sizeof(pin)), 0);
	UT_ASSERT_EQ(page, (char *)(uintptr_t)0x1234);
	UT_ASSERT_EQ(pin_calls, 0);

	acquire_remote_held(&guard);
	UT_ASSERT_EQ(cluster_undo_block0_current_pin_exclusive(
				 &guard, &root, &expected, &pin, &page),
				 CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ(memcmp(&pin, &sentinel, sizeof(pin)), 0);
	UT_ASSERT_EQ(page, (char *)(uintptr_t)0x1234);
	UT_ASSERT_EQ(pin_calls, 0);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_exclusive_pin_post_pin_drift_unpins_before_refusal_and_keeps_outputs_private)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0ResolvedRoot root = test_root();
	ClusterUndoBlock0Generation expected = { true, 0 };
	ClusterUndoBlock0Pin pin;
	ClusterUndoBlock0Pin sentinel;
	char *page = (char *)(uintptr_t)0x1234;

	reset_fixture();
	acquire_remote_xcur_held(&guard);
	fake_pin_invalidates_authority = true;
	memset(&pin, 0xa5, sizeof(pin));
	sentinel = pin;
	UT_ASSERT_EQ(cluster_undo_block0_current_pin_exclusive(
				 &guard, &root, &expected, &pin, &page),
				 CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED);
	UT_ASSERT_EQ(pin_calls, 1);
	UT_ASSERT_EQ(unpin_calls, 1);
	UT_ASSERT(unpin_event > 0);
	UT_ASSERT_EQ(memcmp(&pin, &sentinel, sizeof(pin)), 0);
	UT_ASSERT_EQ(page, (char *)(uintptr_t)0x1234);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_exclusive_pin_error_unwinds_local_before_staging_xcur_cleanup)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0ResolvedRoot root = test_root();
	ClusterUndoBlock0Generation expected = { true, 0 };
	ClusterUndoBlock0Pin pin;
	char *page = NULL;
	volatile bool caught = false;

	reset_fixture();
	acquire_remote_xcur_held(&guard);
	fake_pin_throws = true;
	PG_TRY();
	{
		(void)cluster_undo_block0_current_pin_exclusive(
			&guard, &root, &expected, &pin, &page);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(pin_error_local_cleanup_event > 0);
	UT_ASSERT(cleanup_release_event > pin_error_local_cleanup_event);
	UT_ASSERT_EQ(cleanup_release_calls, 1);
	UT_ASSERT_EQ(semantic_leave_calls, 1);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase,
				 CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_exclusive_pin_post_pin_error_unpins_before_staging_xcur_cleanup)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0ResolvedRoot root = test_root();
	ClusterUndoBlock0Generation expected = { true, 0 };
	ClusterUndoBlock0Pin pin;
	char *page = NULL;
	volatile bool caught = false;

	reset_fixture();
	acquire_remote_xcur_held(&guard);
	fake_post_pin_recheck_throws = true;
	PG_TRY();
	{
		(void)cluster_undo_block0_current_pin_exclusive(
			&guard, &root, &expected, &pin, &page);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(pin_calls, 1);
	UT_ASSERT_EQ(unpin_calls, 1);
	UT_ASSERT(unpin_event > 0);
	UT_ASSERT(cleanup_release_event > unpin_event);
	UT_ASSERT_EQ(cleanup_release_calls, 1);
	UT_ASSERT_EQ(semantic_leave_calls, 1);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase,
				 CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_sample_error_stages_held_guard_cleanup_before_rethrow)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0ResolvedRoot root = test_root();
	ClusterUndoBlock0Generation observed = { false, 77 };
	volatile bool caught = false;

	reset_fixture();
	acquire_remote_held(&guard);
	fake_sample_throws = true;
	PG_TRY();
	{
		(void)cluster_undo_block0_current_sample_generation(&guard, &root, &observed);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(cleanup_release_calls, 1);
	UT_ASSERT_EQ(semantic_leave_calls, 1);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase,
				 CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_copy_error_stages_held_guard_cleanup_before_rethrow)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0ResolvedRoot root = test_root();
	ClusterUndoBlock0Generation expected = { true, 0 };
	char page[BLCKSZ];
	volatile bool caught = false;

	reset_fixture();
	acquire_remote_held(&guard);
	fake_copy_throws = true;
	PG_TRY();
	{
		(void)cluster_undo_block0_current_copy_resident(
			&guard, &root, &expected, page);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(cleanup_release_calls, 1);
	UT_ASSERT_EQ(semantic_leave_calls, 1);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase,
				 CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_acquire_poll_error_stages_pending_guard_cleanup_before_rethrow)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;
	volatile bool caught = false;

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(
				 &key, CLUSTER_UNDO_BLOCK0_SCUR, 1000, &guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	fake_poll_throws = true;
	PG_TRY();
	{
		(void)cluster_undo_block0_current_acquire_poll(&guard, &failure);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(cancel_wait_calls, 1);
	UT_ASSERT_EQ(cleanup_release_calls, 1);
	UT_ASSERT_EQ(semantic_leave_calls, 1);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase,
				 CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_release_poll_error_stages_release_guard_cleanup_before_rethrow)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;
	volatile bool caught = false;

	reset_fixture();
	acquire_remote_held(&guard);
	fake_poll_result = GES_REPLY_WAIT_POLL_PENDING;
	UT_ASSERT_EQ(cluster_undo_block0_current_release_begin(&guard, &failure),
				 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	fake_poll_throws = true;
	PG_TRY();
	{
		(void)cluster_undo_block0_current_release_poll(&guard, &failure);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(reply_delete_calls, 1);
	UT_ASSERT_EQ(cleanup_release_calls, 1);
	UT_ASSERT_EQ(semantic_leave_calls, 1);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase,
				 CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP);
	cluster_undo_block0_current_cancel(&guard);
}

UT_TEST(test_backend_exit_stages_exact_cleanup_without_poll_or_wait)
{
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterUndoBlock0LogicalKey key = test_key(1);
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_NOT_FOUND;

	reset_fixture();
	UT_ASSERT_EQ(cluster_undo_block0_current_acquire_begin(&key, CLUSTER_UNDO_BLOCK0_SCUR, 1000,
													 &guard, &failure),
			 CLUSTER_UNDO_BLOCK0_CURRENT_PENDING);
	UT_ASSERT_NOT_NULL(fake_exit_callback);
	fake_exit_callback(0, (Datum)0);
	UT_ASSERT_EQ(cancel_wait_calls, 1);
	UT_ASSERT_EQ(last_cancel_wait_seq, 0);
	UT_ASSERT_EQ(cleanup_release_calls, 1);
	UT_ASSERT_EQ(reply_poll_calls, 0);
	UT_ASSERT_EQ(current_guard_data(&guard)->phase, CLUSTER_UNDO_BLOCK0_CURRENT_CLEANUP);
}

int
main(void)
{
	UT_PLAN(33);
	UT_RUN(test_key_guard_and_phase_abi);
	UT_RUN(test_startup_namespace_check_rejects_every_reserved_or_unfrozen_type);
	UT_RUN(test_startup_fenced_xcur_begin_end_owns_exact_private_phase);
	UT_RUN(test_begin_installs_before_exact_72_byte_remote_send);
	UT_RUN(test_pending_poll_is_pure_nonblocking_and_keeps_correlation);
	UT_RUN(test_delivered_grant_promotes_to_held);
	UT_RUN(test_remote_grant_with_failed_promote_stages_reliable_release);
	UT_RUN(test_local_grant_with_failed_promote_drains_local_holder);
	UT_RUN(test_cancel_tombstones_exact_pending_and_releases_raced_grant);
	UT_RUN(test_send_failure_cleans_only_created_local_obligations);
	UT_RUN(test_nonzero_unused_guard_is_rejected_before_any_mutation);
	UT_RUN(test_same_resid_nesting_refuses_second_guard_without_touching_first);
	UT_RUN(test_preflight_failure_restores_reusable_zero_guard);
	UT_RUN(test_release_retains_mirror_until_exact_ack_is_consumed);
	UT_RUN(test_release_reuses_exact_canonical_72_byte_ges_shape);
	UT_RUN(test_explicit_perpetual_timeout_survives_acquire_and_release);
	UT_RUN(test_perpetual_acquire_retransmits_past_attempt_threshold);
	UT_RUN(test_perpetual_release_retransmits_past_attempt_threshold);
	UT_RUN(test_generation_zero_is_valid_and_max_is_exhausted);
	UT_RUN(test_xcur_guard_cannot_use_scur_sampling_or_copy_surface);
	UT_RUN(test_sample_authority_drift_does_not_publish_generation);
	UT_RUN(test_copy_generation_drift_does_not_publish_private_bytes);
	UT_RUN(test_copy_authority_drift_does_not_publish_private_bytes);
	UT_RUN(test_xcur_exclusive_pin_binds_guard_root_generation_and_live_proof);
	UT_RUN(test_exclusive_pin_refuses_scur_and_nonheld_without_publishing_outputs);
	UT_RUN(test_exclusive_pin_post_pin_drift_unpins_before_refusal_and_keeps_outputs_private);
	UT_RUN(test_exclusive_pin_error_unwinds_local_before_staging_xcur_cleanup);
	UT_RUN(test_exclusive_pin_post_pin_error_unpins_before_staging_xcur_cleanup);
	UT_RUN(test_sample_error_stages_held_guard_cleanup_before_rethrow);
	UT_RUN(test_copy_error_stages_held_guard_cleanup_before_rethrow);
	UT_RUN(test_acquire_poll_error_stages_pending_guard_cleanup_before_rethrow);
	UT_RUN(test_release_poll_error_stages_release_guard_cleanup_before_rethrow);
	UT_RUN(test_backend_exit_stages_exact_cleanup_without_poll_or_wait);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
