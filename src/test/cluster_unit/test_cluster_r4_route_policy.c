/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_route_policy.c
 *	  Stage 8 R4 canonical current-holder route policy.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>

#include "cluster/cluster_conf.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_cssd.h"
#include "cluster/cluster_gcs.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_gcs_block_dedup.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_tier1.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_lms_shard.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_sf_dep.h"
#include "miscadmin.h"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* The two symbols exist only in the USE_CLUSTER_UNIT special object.  They
 * call the same static exact-length branches used by the production envelope
 * handlers; neither symbol is present in a production build. */
extern bool cluster_gcs_block_test_r4_request80(const ClusterICEnvelope *env,
											 const void *payload);
extern bool cluster_gcs_block_test_r4_forward96(const ClusterICEnvelope *env,
											 const void *payload);
extern bool cluster_gcs_block_test_r4_refusal_status(ClusterCrBuildResult result,
											  ClusterCrBuildReason reason,
											  bool admitted_forward,
											  GcsBlockReplyStatus *status_out);
extern bool cluster_gcs_block_test_decode_r4_reply(
	const ClusterICEnvelope *env, const void *payload, uint64 expected_request_id,
	uint64 expected_epoch, int32 expected_requester_backend_id, uint8 expected_transition_id,
	int32 expected_sender_node);

/* Backend globals reached by the narrow production route section. */
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;
int MaxBackends = 32;
bool cluster_enabled = true;
int cluster_node_id = 1;
int cluster_pcm_grd_max_entries = 0;
bool cluster_recmerge_window_active = false;
bool cluster_online_join = true;
int cluster_lms_workers = 4;
ClusterConf *ClusterConfShmem = NULL;

#define UT_FORMATION_EPOCH UINT64_C(9)
#define UT_ACTIVATION_GENERATION UINT64_C(12)
#define UT_REQUESTER_CAPABILITY_GENERATION UINT32_C(42)
#define UT_MASTER_CAPABILITY_GENERATION UINT32_C(43)
#define UT_HOLDER_CAPABILITY_GENERATION UINT32_C(44)
#define UT_REQUESTER_NODE 2
#define UT_MASTER_NODE 1
#define UT_HOLDER_NODE 3
#define UT_REQUESTER_BACKEND 7
#define UT_REQUEST_ID UINT64_C(0x0102030405060708)
#define UT_READ_SCN ((SCN)UINT64_C(0x1234))
#define UT_EXPECTED_PAGE_SCN ((SCN)UINT64_C(0x2222))
#define UT_MASTER_GENERATION ((UT_FORMATION_EPOCH << 32) | UINT64_C(4))
#define UT_MASTER_TRANSITION UINT64_C(7)
#define UT_R4_REQUIRED_CAPABILITIES                                                          \
	(PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1 | PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1)

typedef struct RouteSeamCapture {
	ClusterSemanticAdmissionResult admission_result;
	bool capability_ok;
	bool peer_open_ok;
	bool recheck_ok;
	bool snapshot_ok;
	GcsBlockR4RouteArmResult arm_result;
	bool enqueue_ok;
	bool refusal_enqueue_ok;
	GcsBlockR4RouteSendResult finish_result;
	bool holder_submit_ok;
	int lookup_master_node;

	int enter_calls;
	int leave_calls;
	int capability_calls;
	int peer_open_calls;
	int snapshot_calls;
	int arm_calls;
	int enqueue_calls;
	int refusal_enqueue_calls;
	int finish_calls;
	int recheck_calls;
	int holder_submit_calls;

	int sequence;
	int snapshot_sequence;
	int arm_sequence;
	int enqueue_sequence;
	int refusal_enqueue_sequence;
	int finish_sequence;
	int recheck_sequence;
	int leave_sequence;

	int32 capability_peers[4];
	uint32 capability_required[4];
	uint32 capability_optional[4];
	int32 peer_open_peers[4];
	uint32 peer_open_required[4];
	uint32 peer_open_generation[4];

	GcsBlockR4RouteIdentity armed_identity;
	ClusterR4CrRouteProof armed_proof;
	uint8 armed_transition;
	uint32 armed_lifetime_hint_ms;
	bool armed_lifetime_hint_trusted;

	GcsBlockR4RouteIdentity finished_identity;
	ClusterR4CrRouteProof finished_proof;
	uint8 finished_transition;
	bool finished_outbound_admitted;

	int enqueue_worker;
	uint8 enqueue_msg_type;
	uint32 enqueue_dest;
	uint16 enqueue_payload_len;
	uint32 enqueue_required_capability;
	uint32 enqueue_connection_generation;
	uint8 enqueue_payload[128];

	int refusal_enqueue_worker;
	uint32 refusal_enqueue_dest;
	uint32 refusal_enqueue_required_capability;
	uint32 refusal_enqueue_connection_generation;
	GcsBlockReplyHeader refusal_header;

	ClusterR4CrForwardPayload submitted_forward;
} RouteSeamCapture;

static RouteSeamCapture route_seam;

static uint32
route_test_capability_generation(int32 peer_id)
{
	if (peer_id == UT_REQUESTER_NODE)
		return UT_REQUESTER_CAPABILITY_GENERATION;
	if (peer_id == UT_MASTER_NODE)
		return UT_MASTER_CAPABILITY_GENERATION;
	if (peer_id == UT_HOLDER_NODE)
		return UT_HOLDER_CAPABILITY_GENERATION;
	return 0;
}

static void
route_seam_reset(void)
{
	memset(&route_seam, 0, sizeof(route_seam));
	route_seam.admission_result = CLUSTER_SEMANTIC_ADMISSION_OK;
	route_seam.capability_ok = true;
	route_seam.peer_open_ok = true;
	route_seam.recheck_ok = true;
	route_seam.snapshot_ok = true;
	route_seam.arm_result = GCS_BLOCK_R4_ROUTE_ARM_NEW;
	route_seam.enqueue_ok = true;
	route_seam.refusal_enqueue_ok = true;
	route_seam.finish_result = GCS_BLOCK_R4_ROUTE_SEND_FORWARDED;
	route_seam.holder_submit_ok = true;
	route_seam.lookup_master_node = UT_MASTER_NODE;
}

static BufferTag
route_test_tag(void)
{
	BufferTag tag;

	memset(&tag, 0, sizeof(tag));
	tag.spcOid = 1663;
	tag.dbOid = 5;
	tag.relNumber = 20000;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = 37;
	return tag;
}

static ClusterICEnvelope
route_test_envelope(uint8 msg_type, uint32 source, uint32 dest, uint32 payload_length)
{
	ClusterICEnvelope env;

	memset(&env, 0, sizeof(env));
	env.magic = PGRAC_IC_ENVELOPE_MAGIC;
	env.version = PGRAC_IC_ENVELOPE_VERSION_V1;
	env.msg_type = msg_type;
	env.source_node_id = source;
	env.dest_node_id = dest;
	env.epoch = UT_FORMATION_EPOCH;
	env.payload_length = payload_length;
	return env;
}

/* Build wire fixtures from literal bytes, independently of the production
 * encoder under test. */
static ClusterR4CrRequestPayload
route_test_request80(void)
{
	ClusterR4CrRequestPayload request;
	uint8 *extension;

	memset(&request, 0, sizeof(request));
	request.base.request_id = UT_REQUEST_ID;
	request.base.epoch = UT_FORMATION_EPOCH;
	request.base.tag = route_test_tag();
	request.base.sender_node = UT_REQUESTER_NODE;
	request.base.requester_backend_id = UT_REQUESTER_BACKEND;
	request.base.transition_id = PCM_TRANS_N_TO_S;
	/* A negotiated requester supplies a legal 1000 ms inherited lifetime. */
	request.base.reserved_0[2] = 0xe8;
	request.base.reserved_0[3] = 0x03;
	extension = (uint8 *)&request.extension;
	extension[0] = 1;
	extension[1] = 1;
	extension[4] = 0x34;
	extension[5] = 0x12;
	return request;
}

static ClusterR4CrForwardPayload
route_test_forward96(void)
{
	static const uint8 extension_bytes[32] = {
		0x01, 0x01, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
		0x09, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x22, 0x22, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
	};
	ClusterR4CrForwardPayload forward;

	memset(&forward, 0, sizeof(forward));
	forward.base.request_id = UT_REQUEST_ID;
	forward.base.epoch = UT_FORMATION_EPOCH;
	forward.base.tag = route_test_tag();
	forward.base.original_requester_node = UT_REQUESTER_NODE;
	forward.base.requester_backend_id = UT_REQUESTER_BACKEND;
	forward.base.master_node = UT_MASTER_NODE;
	forward.base.transition_id = PCM_TRANS_N_TO_S;
	forward.base.expected_pi_watermark_scn_bytes[0] = 0x34;
	forward.base.expected_pi_watermark_scn_bytes[1] = 0x12;
	forward.base.reserved_0[4] = 1;
	memcpy(&forward.extension, extension_bytes, sizeof(extension_bytes));
	return forward;
}

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(), int lineNumber pg_attribute_unused())
{
	abort();
}

void
pg_re_throw(void)
{
	abort();
}

uint64
cluster_epoch_get_current(void)
{
	return UT_FORMATION_EPOCH;
}

int
cluster_gcs_lookup_master(BufferTag tag pg_attribute_unused())
{
	return route_seam.lookup_master_node;
}

int
cluster_gcs_lookup_master_static(BufferTag tag pg_attribute_unused())
{
	return route_seam.lookup_master_node;
}

int
cluster_conf_node_count(void)
{
	return 1;
}

bool
cluster_grd_join_remaster_active_for_shard(BufferTag tag pg_attribute_unused())
{
	return false;
}

bool
cluster_grd_block_view_rebuilt(BufferTag tag pg_attribute_unused())
{
	return true;
}

void
cluster_grd_inc_join_block_failclosed(void)
{}

bool
cluster_grd_offpath_boot_decided(void)
{
	return true;
}

bool
cluster_grd_recovery_in_progress(void)
{
	return false;
}

ClusterCssdPeerState
cluster_cssd_get_peer_state(int node_id pg_attribute_unused())
{
	return CLUSTER_CSSD_PEER_ALIVE;
}

bool
cluster_merged_instance_is_materialized(int origin_node pg_attribute_unused())
{
	return true;
}

uint64
cluster_merged_instance_recovered_through(int origin_node pg_attribute_unused())
{
	return UINT64_MAX;
}

XLogRecPtr
cluster_pcm_lock_pi_watermark_lsn_query(BufferTag tag pg_attribute_unused())
{
	return InvalidXLogRecPtr;
}

int
cluster_ic_tier1_my_data_channel(void)
{
	return 0;
}

int
cluster_lms_shard_for_tag(const BufferTag *tag pg_attribute_unused(), int n_workers)
{
	return n_workers > 0 ? 0 : -1;
}

int
cluster_gcs_block_payload_shard(uint8 msg_type, const void *payload, uint16 payload_len,
								int n_workers)
{
	if (payload == NULL || n_workers <= 0)
		return -1;
	if (msg_type == PGRAC_IC_MSG_GCS_BLOCK_FORWARD
		&& payload_len == sizeof(ClusterR4CrForwardPayload))
		return 0;
	return -1;
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter(uint64 feature_bit, ClusterSemanticAdmissionSide side,
								  ClusterSemanticAdmissionToken *token)
{
	route_seam.enter_calls++;
	if (token != NULL)
		memset(token, 0, sizeof(*token));
	if (route_seam.admission_result != CLUSTER_SEMANTIC_ADMISSION_OK)
		return route_seam.admission_result;
	if (feature_bit != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		|| side != CLUSTER_SEMANTIC_TARGET_SIDE || token == NULL)
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	token->feature_bit = feature_bit;
	token->record_generation = UT_ACTIVATION_GENERATION;
	token->formation_epoch = UT_FORMATION_EPOCH;
	token->side = (uint8)side;
	token->entered = true;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

bool
cluster_semantic_activation_recheck(const ClusterSemanticAdmissionToken *token)
{
	route_seam.recheck_calls++;
	route_seam.recheck_sequence = ++route_seam.sequence;
	return route_seam.recheck_ok && token != NULL && token->entered;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	if (token == NULL || !token->entered)
		return;
	route_seam.leave_calls++;
	route_seam.leave_sequence = ++route_seam.sequence;
	memset(token, 0, sizeof(*token));
}

bool
cluster_sf_peer_capability_family_sample(int32 peer_id, uint32 required_bits,
									 uint32 optional_bits, bool *optional_out,
									 uint32 *generation_out)
{
	int slot = route_seam.capability_calls++;

	if (optional_out != NULL)
		*optional_out = false;
	if (generation_out != NULL)
		*generation_out = 0;
	if (slot < lengthof(route_seam.capability_peers)) {
		route_seam.capability_peers[slot] = peer_id;
		route_seam.capability_required[slot] = required_bits;
		route_seam.capability_optional[slot] = optional_bits;
	}
	if (!route_seam.capability_ok || generation_out == NULL)
		return false;
	if (optional_out != NULL)
		*optional_out = true;
	*generation_out = route_test_capability_generation(peer_id);
	return *generation_out != 0;
}

bool
cluster_semantic_activation_peer_open_matches(const ClusterSemanticAdmissionToken *token,
										  int32 authenticated_peer,
										  uint32 required_capabilities,
										  uint32 sampled_generation)
{
	int slot = route_seam.peer_open_calls++;

	if (slot < lengthof(route_seam.peer_open_peers)) {
		route_seam.peer_open_peers[slot] = authenticated_peer;
		route_seam.peer_open_required[slot] = required_capabilities;
		route_seam.peer_open_generation[slot] = sampled_generation;
	}
	return route_seam.peer_open_ok && token != NULL && token->entered
		   && required_capabilities == UT_R4_REQUIRED_CAPABILITIES
		   && sampled_generation == route_test_capability_generation(authenticated_peer);
}

bool
cluster_pcm_lock_r4_route_snapshot(BufferTag tag, PcmAuthoritySnapshot *authority_out,
								   uint64 *master_authority_generation_out,
								   SCN *expected_page_scn_out)
{
	route_seam.snapshot_calls++;
	route_seam.snapshot_sequence = ++route_seam.sequence;
	if (!route_seam.snapshot_ok || authority_out == NULL
		|| master_authority_generation_out == NULL || expected_page_scn_out == NULL)
		return false;
	memset(authority_out, 0, sizeof(*authority_out));
	authority_out->state = PCM_STATE_S;
	authority_out->x_holder_node = -1;
	authority_out->pending_x_requester_node = -1;
	authority_out->transition_count = UT_MASTER_TRANSITION;
	authority_out->master_holder.node_id = UT_HOLDER_NODE;
	authority_out->s_holders_bitmap = UINT32_C(1) << UT_HOLDER_NODE;
	*master_authority_generation_out = UT_MASTER_GENERATION;
	*expected_page_scn_out = UT_EXPECTED_PAGE_SCN;
	(void)tag;
	return true;
}

GcsBlockR4RouteArmResult
cluster_gcs_block_dedup_r4_route_arm_or_match(
	int worker_id, const GcsBlockR4RouteIdentity *identity, uint8 transition_id,
	const ClusterR4CrRouteProof *fresh_proof, uint32 requester_lifetime_hint_ms,
	bool lifetime_hint_trusted, GcsBlockR4RouteRecord *record_out)
{
	(void)worker_id;
	route_seam.arm_calls++;
	route_seam.arm_sequence = ++route_seam.sequence;
	if (identity != NULL)
		route_seam.armed_identity = *identity;
	if (fresh_proof != NULL)
		route_seam.armed_proof = *fresh_proof;
	route_seam.armed_transition = transition_id;
	route_seam.armed_lifetime_hint_ms = requester_lifetime_hint_ms;
	route_seam.armed_lifetime_hint_trusted = lifetime_hint_trusted;
	if ((route_seam.arm_result == GCS_BLOCK_R4_ROUTE_ARM_NEW
		 || route_seam.arm_result == GCS_BLOCK_R4_ROUTE_ARM_REPLAY)
		&& fresh_proof != NULL && record_out != NULL) {
		memset(record_out, 0, sizeof(*record_out));
		record_out->proof = *fresh_proof;
		record_out->state = GCS_BLOCK_R4_ROUTE_ROUTING;
	}
	return route_seam.arm_result;
}

GcsBlockR4RouteSendResult
cluster_gcs_block_dedup_r4_route_finish_send(
	int worker_id, const GcsBlockR4RouteIdentity *identity, uint8 transition_id,
	const ClusterR4CrRouteProof *armed_proof, bool outbound_admitted)
{
	(void)worker_id;
	route_seam.finish_calls++;
	route_seam.finish_sequence = ++route_seam.sequence;
	if (identity != NULL)
		route_seam.finished_identity = *identity;
	if (armed_proof != NULL)
		route_seam.finished_proof = *armed_proof;
	route_seam.finished_transition = transition_id;
	route_seam.finished_outbound_admitted = outbound_admitted;
	return route_seam.finish_result;
}

bool
cluster_lms_outbound_enqueue_cap_bound(int worker_id, uint8 msg_type, uint32 dest_node_id,
									   const void *payload, uint16 payload_len,
									   uint32 required_capability,
									   uint32 connection_generation)
{
	route_seam.enqueue_calls++;
	route_seam.enqueue_sequence = ++route_seam.sequence;
	route_seam.enqueue_worker = worker_id;
	route_seam.enqueue_msg_type = msg_type;
	route_seam.enqueue_dest = dest_node_id;
	route_seam.enqueue_payload_len = payload_len;
	route_seam.enqueue_required_capability = required_capability;
	route_seam.enqueue_connection_generation = connection_generation;
	if (payload != NULL && payload_len <= sizeof(route_seam.enqueue_payload))
		memcpy(route_seam.enqueue_payload, payload, payload_len);
	return route_seam.enqueue_ok;
}

bool
cluster_lms_outbound_enqueue_zero_block_reply_cap_bound(
	int worker_id, uint32 dest_node_id, const GcsBlockReplyHeader *header,
	uint32 required_capability, uint32 connection_generation)
{
	route_seam.refusal_enqueue_calls++;
	route_seam.refusal_enqueue_sequence = ++route_seam.sequence;
	route_seam.refusal_enqueue_worker = worker_id;
	route_seam.refusal_enqueue_dest = dest_node_id;
	route_seam.refusal_enqueue_required_capability = required_capability;
	route_seam.refusal_enqueue_connection_generation = connection_generation;
	if (header != NULL)
		route_seam.refusal_header = *header;
	return route_seam.refusal_enqueue_ok;
}

static void
route_assert_refusal(GcsBlockReplyStatus status, uint64 page_lsn)
{
	int i;

	UT_ASSERT_EQ(route_seam.refusal_enqueue_calls, 1);
	UT_ASSERT_EQ(route_seam.refusal_enqueue_worker, 0);
	UT_ASSERT_EQ(route_seam.refusal_enqueue_dest, UT_REQUESTER_NODE);
	UT_ASSERT_EQ(route_seam.refusal_enqueue_required_capability, UT_R4_REQUIRED_CAPABILITIES);
	UT_ASSERT_EQ(route_seam.refusal_enqueue_connection_generation,
				 UT_REQUESTER_CAPABILITY_GENERATION);
	UT_ASSERT_EQ(route_seam.refusal_header.request_id, UT_REQUEST_ID);
	UT_ASSERT_EQ(route_seam.refusal_header.page_lsn, page_lsn);
	UT_ASSERT_EQ(route_seam.refusal_header.checksum, 0);
	UT_ASSERT_EQ(route_seam.refusal_header.sender_node, UT_MASTER_NODE);
	UT_ASSERT_EQ(route_seam.refusal_header.requester_backend_id, UT_REQUESTER_BACKEND);
	UT_ASSERT_EQ(route_seam.refusal_header.transition_id, PCM_TRANS_N_TO_S);
	UT_ASSERT_EQ(route_seam.refusal_header.status, status);
	UT_ASSERT_EQ(GcsBlockReplyHeaderGetForwardingMasterNode(&route_seam.refusal_header),
				 GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	for (i = 0; i < (int)sizeof(route_seam.refusal_header.reserved_0); i++)
		UT_ASSERT_EQ(route_seam.refusal_header.reserved_0[i], 0);
}

bool
cluster_lms_cr_submit_r4(const ClusterR4CrForwardPayload *forward)
{
	route_seam.holder_submit_calls++;
	if (forward != NULL)
		route_seam.submitted_forward = *forward;
	return route_seam.holder_submit_ok;
}

static PcmAuthoritySnapshot
route_snapshot(PcmState state)
{
	PcmAuthoritySnapshot authority;

	memset(&authority, 0, sizeof(authority));
	authority.master_holder.node_id = UINT32_MAX;
	authority.transition_count = 7;
	authority.state = state;
	authority.x_holder_node = -1;
	authority.pending_x_requester_node = -1;
	return authority;
}

static ClusterCrBuildReason
classify(const PcmAuthoritySnapshot *authority, uint64 epoch, uint64 generation,
		 int32 *holder_out)
{
	return cluster_r4_route_policy_classify(authority, epoch, generation, holder_out);
}

#define DEFINE_REASON_TEST(test_name, setup_code, expected_reason, expected_holder)                \
	UT_TEST(test_name)                                                                             \
	{                                                                                              \
		PcmAuthoritySnapshot authority = route_snapshot(PCM_STATE_N);                               \
		PcmAuthoritySnapshot *authority_ptr = &authority;                                           \
		uint64 epoch = 9;                                                                           \
		uint64 generation = (epoch << 32) | 3;                                                      \
		int32 holder = -1;                                                                          \
		setup_code;                                                                                  \
		UT_ASSERT_EQ(classify(authority_ptr, epoch, generation, &holder), (expected_reason));        \
		UT_ASSERT_EQ(holder, (expected_holder));                                                     \
	}

DEFINE_REASON_TEST(test_01_null_authority_is_protocol, authority_ptr = NULL,
			   CLUSTER_CR_BUILD_PROTOCOL, -1)

UT_TEST(test_02_null_output_is_protocol)
{
	PcmAuthoritySnapshot authority = route_snapshot(PCM_STATE_N);

	UT_ASSERT_EQ(classify(&authority, 9, (UINT64_C(9) << 32) | 3, NULL),
				 CLUSTER_CR_BUILD_PROTOCOL);
}

DEFINE_REASON_TEST(test_03_canonical_n_has_no_holder, (void)0, CLUSTER_CR_BUILD_NO_HOLDER, -1)
DEFINE_REASON_TEST(test_04_n_with_x_is_ambiguous, authority.x_holder_node = 1,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_05_n_with_s_is_ambiguous, authority.s_holders_bitmap = 1,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_06_n_with_master_is_ambiguous, authority.master_holder.node_id = 1,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_07_x_node_zero_is_selected,
			   authority.state = PCM_STATE_X; authority.x_holder_node = 0;
			   authority.master_holder.node_id = 0,
			   CLUSTER_CR_BUILD_NONE, 0)
DEFINE_REASON_TEST(test_08_x_node_31_is_selected,
			   authority.state = PCM_STATE_X; authority.x_holder_node = 31;
			   authority.master_holder.node_id = 31,
			   CLUSTER_CR_BUILD_NONE, 31)
DEFINE_REASON_TEST(test_09_x_without_holder_is_ambiguous, authority.state = PCM_STATE_X,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_10_x_negative_holder_is_ambiguous,
			   authority.state = PCM_STATE_X; authority.x_holder_node = -2,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_11_x_out_of_range_is_ambiguous,
			   authority.state = PCM_STATE_X; authority.x_holder_node = 32;
			   authority.master_holder.node_id = 32,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_12_x_master_mismatch_is_ambiguous,
			   authority.state = PCM_STATE_X; authority.x_holder_node = 4;
			   authority.master_holder.node_id = 5,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_13_x_with_s_bitmap_is_ambiguous,
			   authority.state = PCM_STATE_X; authority.x_holder_node = 4;
			   authority.master_holder.node_id = 4; authority.s_holders_bitmap = 1,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_14_s_node_zero_is_selected,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 0;
			   authority.s_holders_bitmap = 1,
			   CLUSTER_CR_BUILD_NONE, 0)
DEFINE_REASON_TEST(test_15_s_node_31_is_selected,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 31;
			   authority.s_holders_bitmap = UINT32_C(1) << 31,
			   CLUSTER_CR_BUILD_NONE, 31)
DEFINE_REASON_TEST(test_16_s_multiple_selects_canonical,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 3;
			   authority.s_holders_bitmap = (UINT32_C(1) << 2) | (UINT32_C(1) << 3),
			   CLUSTER_CR_BUILD_NONE, 3)
DEFINE_REASON_TEST(test_17_s_zero_bitmap_is_ambiguous,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 3,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_18_s_with_x_is_ambiguous,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 3;
			   authority.s_holders_bitmap = UINT32_C(1) << 3; authority.x_holder_node = 3,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_19_s_without_master_is_ambiguous,
			   authority.state = PCM_STATE_S; authority.s_holders_bitmap = 1,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_20_s_out_of_range_master_is_ambiguous,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 33;
			   authority.s_holders_bitmap = 1,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_21_s_missing_canonical_bit_is_ambiguous,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 3;
			   authority.s_holders_bitmap = UINT32_C(1) << 2,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_22_unknown_state_is_ambiguous, authority.state = (PcmState)99,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_23_reserved_zero_is_required, authority.reserved[0] = 1,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_24_reserved_one_is_required, authority.reserved[1] = 1,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_25_pending_destructive_convert_is_recovering,
			   authority.pending_x_requester_node = 4,
			   CLUSTER_CR_BUILD_RECOVERING, -1)
DEFINE_REASON_TEST(test_26_zero_master_generation_is_rejected,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 0;
			   authority.s_holders_bitmap = 1; generation = 0,
			   CLUSTER_CR_BUILD_GENERATION_MISMATCH, -1)
DEFINE_REASON_TEST(test_27_zero_restart_half_is_rejected,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 0;
			   authority.s_holders_bitmap = 1; generation = epoch << 32,
			   CLUSTER_CR_BUILD_GENERATION_MISMATCH, -1)
DEFINE_REASON_TEST(test_28_wrong_epoch_half_is_rejected,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 0;
			   authority.s_holders_bitmap = 1; generation = ((epoch + 1) << 32) | 3,
			   CLUSTER_CR_BUILD_GENERATION_MISMATCH, -1)
DEFINE_REASON_TEST(test_29_zero_transition_count_is_rejected,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 0;
			   authority.s_holders_bitmap = 1; authority.transition_count = 0,
			   CLUSTER_CR_BUILD_GENERATION_MISMATCH, -1)
DEFINE_REASON_TEST(test_30_exhausted_transition_count_is_rejected,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 0;
			   authority.s_holders_bitmap = 1; authority.transition_count = UINT64_MAX,
			   CLUSTER_CR_BUILD_GENERATION_MISMATCH, -1)

static ClusterR4CrRouteProof
route_proof(void)
{
	ClusterR4CrRouteProof proof;

	memset(&proof, 0, sizeof(proof));
	proof.formation_epoch = 9;
	proof.master_authority_generation = (UINT64_C(9) << 32) | 3;
	proof.master_resource_transition_count = 7;
	proof.expected_page_scn = (SCN)11;
	proof.selected_holder_node = 4;
	return proof;
}

UT_TEST(test_31_exact_duplicate_route_matches)
{
	ClusterR4CrRouteProof proof = route_proof();

	UT_ASSERT(cluster_r4_route_proof_matches(&proof, 9, (UINT64_C(9) << 32) | 3, 4, 7,
										   (SCN)11));
}

UT_TEST(test_32_transition_drift_closes_duplicate)
{
	ClusterR4CrRouteProof proof = route_proof();

	UT_ASSERT(!cluster_r4_route_proof_matches(&proof, 9, (UINT64_C(9) << 32) | 3, 4, 8,
											(SCN)11));
}

static const ClusterCrBuildResult reason_result[18] = {
	[CLUSTER_CR_BUILD_NONE] = CLUSTER_CR_BUILD_FULL,
	[CLUSTER_CR_BUILD_TARGET_DISABLED] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_RF_DEFERRED] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_WRONG_MASTER] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_NO_HOLDER] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_HOLDER_MOVED] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_RECOVERING] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_GENERATION_MISMATCH] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_CAPACITY] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_BAD_LOCATOR] = CLUSTER_CR_BUILD_FAIL_CLOSED,
	[CLUSTER_CR_BUILD_BAD_UNDO] = CLUSTER_CR_BUILD_FAIL_CLOSED,
	[CLUSTER_CR_BUILD_CHAIN_LIMIT] = CLUSTER_CR_BUILD_FAIL_CLOSED,
	[CLUSTER_CR_BUILD_SNAPSHOT_TOO_OLD] = CLUSTER_CR_BUILD_FAIL_CLOSED,
	[CLUSTER_CR_BUILD_EPOCH_MISMATCH] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_CANCELLED] = CLUSTER_CR_BUILD_FAIL_CLOSED,
	[CLUSTER_CR_BUILD_IO_ERROR] = CLUSTER_CR_BUILD_FAIL_CLOSED,
	[CLUSTER_CR_BUILD_PROTOCOL] = CLUSTER_CR_BUILD_FAIL_CLOSED,
};

static void
run_reason_polarity(int reason)
{
	UT_ASSERT_EQ(cluster_cr_build_result_for_reason((ClusterCrBuildReason)reason),
				 reason_result[reason]);
}

#define DEFINE_POLARITY_TEST(n) \
	UT_TEST(test_reason_polarity_##n) { run_reason_polarity(n); }

DEFINE_POLARITY_TEST(0)
DEFINE_POLARITY_TEST(1)
DEFINE_POLARITY_TEST(2)
DEFINE_POLARITY_TEST(3)
DEFINE_POLARITY_TEST(4)
DEFINE_POLARITY_TEST(5)
DEFINE_POLARITY_TEST(6)
DEFINE_POLARITY_TEST(7)
DEFINE_POLARITY_TEST(8)
DEFINE_POLARITY_TEST(9)
DEFINE_POLARITY_TEST(10)
DEFINE_POLARITY_TEST(11)
DEFINE_POLARITY_TEST(12)
DEFINE_POLARITY_TEST(13)
DEFINE_POLARITY_TEST(14)
DEFINE_POLARITY_TEST(15)
DEFINE_POLARITY_TEST(16)
DEFINE_POLARITY_TEST(17)

UT_TEST(test_unknown_reason_fails_closed)
{
	UT_ASSERT_EQ(cluster_cr_build_result_for_reason((ClusterCrBuildReason)-1),
				 CLUSTER_CR_BUILD_FAIL_CLOSED);
	UT_ASSERT_EQ(cluster_cr_build_result_for_reason((ClusterCrBuildReason)18),
				 CLUSTER_CR_BUILD_FAIL_CLOSED);
}

UT_TEST(test_d3_result_reason_mapping_is_closed)
{
	static const GcsBlockReplyStatus expected[18] = {
		GCS_BLOCK_REPLY_R4_DENIED,
		GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED,
		GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED,
		GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED,
		GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED,
		GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED,
		GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED,
		GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED,
		GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED,
		GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED,
		GCS_BLOCK_REPLY_R4_DENIED,
		GCS_BLOCK_REPLY_R4_DENIED,
		GCS_BLOCK_REPLY_R4_DENIED,
		GCS_BLOCK_REPLY_R4_DENIED,
		GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED,
		GCS_BLOCK_REPLY_R4_DENIED,
		GCS_BLOCK_REPLY_R4_DENIED,
		GCS_BLOCK_REPLY_R4_DENIED
	};
	int reason;

	for (reason = CLUSTER_CR_BUILD_TARGET_DISABLED; reason <= CLUSTER_CR_BUILD_PROTOCOL;
		 reason++) {
		ClusterCrBuildResult result
			= cluster_cr_build_result_for_reason((ClusterCrBuildReason)reason);
		GcsBlockReplyStatus status = GCS_BLOCK_REPLY_GRANTED;

		UT_ASSERT(cluster_gcs_block_test_r4_refusal_status(
			result, (ClusterCrBuildReason)reason, false, &status));
		UT_ASSERT_EQ(status, expected[reason]);
	}
	{
		GcsBlockReplyStatus status = GCS_BLOCK_REPLY_GRANTED;

		UT_ASSERT(!cluster_gcs_block_test_r4_refusal_status(
			CLUSTER_CR_BUILD_FULL, CLUSTER_CR_BUILD_NONE, true, &status));
		UT_ASSERT(cluster_gcs_block_test_r4_refusal_status(
			CLUSTER_CR_BUILD_FULL, CLUSTER_CR_BUILD_NONE, false, &status));
		UT_ASSERT_EQ(status, GCS_BLOCK_REPLY_R4_DENIED);
		UT_ASSERT(cluster_gcs_block_test_r4_refusal_status(
			CLUSTER_CR_BUILD_FAIL_CLOSED, CLUSTER_CR_BUILD_HOLDER_MOVED, false, &status));
		UT_ASSERT_EQ(status, GCS_BLOCK_REPLY_R4_DENIED);
		UT_ASSERT(cluster_gcs_block_test_r4_refusal_status(
			(ClusterCrBuildResult)99, (ClusterCrBuildReason)99, false, &status));
		UT_ASSERT_EQ(status, GCS_BLOCK_REPLY_R4_DENIED);
	}
}

UT_TEST(test_r4_refusal_decoder_requires_exact_domain_identity_and_zero_body)
{
	typedef struct TestR4Reply {
		GcsBlockReplyHeader header;
		char block_data[GCS_BLOCK_DATA_SIZE];
	} TestR4Reply;
	TestR4Reply reply;
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY, UT_REQUESTER_NODE, UT_MASTER_NODE,
							  sizeof(reply));

	memset(&reply, 0, sizeof(reply));
	reply.header.request_id = UT_REQUEST_ID;
	reply.header.epoch = UT_FORMATION_EPOCH;
	reply.header.sender_node = UT_REQUESTER_NODE;
	reply.header.requester_backend_id = UT_REQUESTER_BACKEND;
	reply.header.transition_id = PCM_TRANS_N_TO_S;
	reply.header.status = GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED;
	reply.header.page_lsn = 1;
	GcsBlockReplyHeaderSetForwardingMasterNode(
		&reply.header, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	reply.header.checksum = cluster_gcs_block_compute_checksum(reply.block_data);
	UT_ASSERT(cluster_gcs_block_test_decode_r4_reply(
		&env, &reply, UT_REQUEST_ID, UT_FORMATION_EPOCH, UT_REQUESTER_BACKEND,
		PCM_TRANS_N_TO_S, UT_REQUESTER_NODE));

	reply.header.status = GCS_BLOCK_REPLY_DENIED_PENDING_X;
	UT_ASSERT(!cluster_gcs_block_test_decode_r4_reply(
		&env, &reply, UT_REQUEST_ID, UT_FORMATION_EPOCH, UT_REQUESTER_BACKEND,
		PCM_TRANS_N_TO_S, UT_REQUESTER_NODE));
	reply.header.status = GCS_BLOCK_REPLY_R4_DENIED;
	reply.header.page_lsn = 1;
	UT_ASSERT(!cluster_gcs_block_test_decode_r4_reply(
		&env, &reply, UT_REQUEST_ID, UT_FORMATION_EPOCH, UT_REQUESTER_BACKEND,
		PCM_TRANS_N_TO_S, UT_REQUESTER_NODE));
	reply.header.page_lsn = 0;
	reply.header.reserved_0[0] = 1;
	UT_ASSERT(!cluster_gcs_block_test_decode_r4_reply(
		&env, &reply, UT_REQUEST_ID, UT_FORMATION_EPOCH, UT_REQUESTER_BACKEND,
		PCM_TRANS_N_TO_S, UT_REQUESTER_NODE));
	reply.header.reserved_0[0] = 0;
	reply.block_data[0] = 1;
	reply.header.checksum = cluster_gcs_block_compute_checksum(reply.block_data);
	UT_ASSERT(!cluster_gcs_block_test_decode_r4_reply(
		&env, &reply, UT_REQUEST_ID, UT_FORMATION_EPOCH, UT_REQUESTER_BACKEND,
		PCM_TRANS_N_TO_S, UT_REQUESTER_NODE));
}

/* Removing the real request80 branch, taking a second TARGET admission,
 * re-snapshotting PCM, encoding fresh rather than stored proof bytes, using an
 * unbound enqueue, or failing to feed the exact forward96 branch all break
 * this one request -> holder behavior test. */
UT_TEST(test_request80_routes_stored_proof_to_real_forward96_handler)
{
	static const uint8 expected_extension[32] = {
		0x01, 0x01, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
		0x09, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x22, 0x22, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
	};
	ClusterR4CrRequestPayload request = route_test_request80();
	ClusterICEnvelope request_env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, UT_REQUESTER_NODE, UT_MASTER_NODE,
							  sizeof(request));
	ClusterR4CrForwardPayload forwarded;
	ClusterICEnvelope forward_env;

	route_seam_reset();
	UT_ASSERT(cluster_gcs_block_test_r4_request80(&request_env, &request));
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT_EQ(route_seam.capability_calls, 2);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 2);
	UT_ASSERT_EQ(route_seam.capability_peers[0], UT_REQUESTER_NODE);
	UT_ASSERT_EQ(route_seam.capability_peers[1], UT_HOLDER_NODE);
	UT_ASSERT_EQ(route_seam.capability_required[0], UT_R4_REQUIRED_CAPABILITIES);
	UT_ASSERT_EQ(route_seam.capability_required[1], UT_R4_REQUIRED_CAPABILITIES);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 1);
	UT_ASSERT_EQ(route_seam.arm_calls, 1);
	UT_ASSERT_EQ(route_seam.armed_lifetime_hint_ms, 1000);
	UT_ASSERT(route_seam.armed_lifetime_hint_trusted);
	UT_ASSERT_EQ(route_seam.armed_identity.legacy_key.origin_node_id, UT_REQUESTER_NODE);
	UT_ASSERT_EQ(route_seam.armed_identity.legacy_key.requester_backend_id,
				 UT_REQUESTER_BACKEND);
	UT_ASSERT_EQ(route_seam.armed_identity.legacy_key.request_id, UT_REQUEST_ID);
	UT_ASSERT_EQ(route_seam.armed_identity.legacy_key.cluster_epoch, UT_FORMATION_EPOCH);
	UT_ASSERT_EQ(route_seam.armed_identity.read_scn, UT_READ_SCN);
	UT_ASSERT_EQ(route_seam.armed_identity.activation_generation, UT_ACTIVATION_GENERATION);
	UT_ASSERT_EQ(route_seam.armed_proof.formation_epoch, UT_FORMATION_EPOCH);
	UT_ASSERT_EQ(route_seam.armed_proof.activation_generation, UT_ACTIVATION_GENERATION);
	UT_ASSERT_EQ(route_seam.armed_proof.master_authority_generation, UT_MASTER_GENERATION);
	UT_ASSERT_EQ(route_seam.armed_proof.master_resource_transition_count, UT_MASTER_TRANSITION);
	UT_ASSERT_EQ(route_seam.armed_proof.expected_page_scn, UT_EXPECTED_PAGE_SCN);
	UT_ASSERT_EQ(route_seam.armed_proof.real_master_node, UT_MASTER_NODE);
	UT_ASSERT_EQ(route_seam.armed_proof.selected_holder_node, UT_HOLDER_NODE);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 1);
	UT_ASSERT_EQ(route_seam.finish_calls, 1);
	UT_ASSERT_EQ(route_seam.refusal_enqueue_calls, 0);
	UT_ASSERT(route_seam.finished_outbound_admitted);
	UT_ASSERT_EQ(memcmp(&route_seam.finished_identity, &route_seam.armed_identity,
					 sizeof(route_seam.armed_identity)), 0);
	UT_ASSERT_EQ(memcmp(&route_seam.finished_proof, &route_seam.armed_proof,
					 sizeof(route_seam.armed_proof)), 0);
	UT_ASSERT_EQ(route_seam.recheck_calls, 1);
	UT_ASSERT(route_seam.snapshot_sequence < route_seam.arm_sequence);
	UT_ASSERT(route_seam.arm_sequence < route_seam.enqueue_sequence);
	UT_ASSERT(route_seam.enqueue_sequence < route_seam.finish_sequence);
	UT_ASSERT(route_seam.finish_sequence < route_seam.recheck_sequence);
	UT_ASSERT(route_seam.recheck_sequence < route_seam.leave_sequence);

	UT_ASSERT_EQ(route_seam.enqueue_msg_type, PGRAC_IC_MSG_GCS_BLOCK_FORWARD);
	UT_ASSERT_EQ(route_seam.enqueue_dest, UT_HOLDER_NODE);
	UT_ASSERT_EQ(route_seam.enqueue_payload_len, sizeof(ClusterR4CrForwardPayload));
	UT_ASSERT_EQ(route_seam.enqueue_required_capability, UT_R4_REQUIRED_CAPABILITIES);
	UT_ASSERT_EQ(route_seam.enqueue_connection_generation, UT_HOLDER_CAPABILITY_GENERATION);
	memcpy(&forwarded, route_seam.enqueue_payload, sizeof(forwarded));
	UT_ASSERT_EQ(forwarded.base.request_id, UT_REQUEST_ID);
	UT_ASSERT_EQ(forwarded.base.epoch, UT_FORMATION_EPOCH);
	UT_ASSERT_EQ(memcmp(&forwarded.base.tag, &request.base.tag, sizeof(BufferTag)), 0);
	UT_ASSERT_EQ(forwarded.base.original_requester_node, UT_REQUESTER_NODE);
	UT_ASSERT_EQ(forwarded.base.requester_backend_id, UT_REQUESTER_BACKEND);
	UT_ASSERT_EQ(forwarded.base.master_node, UT_MASTER_NODE);
	UT_ASSERT_EQ(forwarded.base.transition_id, PCM_TRANS_N_TO_S);
	UT_ASSERT_EQ(forwarded.base.expected_pi_watermark_scn_bytes[0], 0x34);
	UT_ASSERT_EQ(forwarded.base.expected_pi_watermark_scn_bytes[1], 0x12);
	UT_ASSERT_EQ(forwarded.base.reserved_0[4], 1);
	UT_ASSERT_EQ(memcmp(&forwarded.extension, expected_extension, sizeof(expected_extension)), 0);
	/* Absolute bytes 76..83 are the master-resource transition count. */
	UT_ASSERT_EQ(memcmp(((const uint8 *)&forwarded) + 76, expected_extension + 12, 8), 0);

	forward_env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, UT_MASTER_NODE,
								  UT_HOLDER_NODE, sizeof(forwarded));
	route_seam_reset();
	UT_ASSERT(cluster_gcs_block_test_r4_forward96(&forward_env, &forwarded));
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT_EQ(route_seam.capability_calls, 1);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 1);
	UT_ASSERT_EQ(route_seam.capability_peers[0], UT_MASTER_NODE);
	UT_ASSERT_EQ(route_seam.capability_required[0], UT_R4_REQUIRED_CAPABILITIES);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 0);
	UT_ASSERT_EQ(route_seam.arm_calls, 0);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 1);
	UT_ASSERT_EQ(memcmp(&route_seam.submitted_forward, &forwarded, sizeof(forwarded)), 0);
}

/* These hooks are exact-extended-shape classifiers: an exact R4 frame is
 * consumed even when its capability gate refuses it, while legacy and
 * malformed lengths remain for the outer dispatcher/drop path. */
UT_TEST(test_r4_try_handlers_consume_only_exact_extended_lengths)
{
	uint8 bytes[sizeof(ClusterR4CrForwardPayload)];
	ClusterICEnvelope env;

	memset(bytes, 0, sizeof(bytes));
	route_seam_reset();
	route_seam.capability_ok = false;

	env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, UT_REQUESTER_NODE, UT_MASTER_NODE,
						  sizeof(ClusterR4CrRequestPayload));
	UT_ASSERT(cluster_gcs_block_test_r4_request80(&env, bytes));
	UT_ASSERT_EQ(route_seam.refusal_enqueue_calls, 0);
	env.payload_length = sizeof(GcsBlockRequestPayload);
	UT_ASSERT(!cluster_gcs_block_test_r4_request80(&env, bytes));
	env.payload_length = sizeof(ClusterR4CrRequestPayload) - 1;
	UT_ASSERT(!cluster_gcs_block_test_r4_request80(&env, bytes));
	env.payload_length = sizeof(ClusterR4CrRequestPayload) + 1;
	UT_ASSERT(!cluster_gcs_block_test_r4_request80(&env, bytes));

	env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, UT_MASTER_NODE, UT_HOLDER_NODE,
						  sizeof(ClusterR4CrForwardPayload));
	UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, bytes));
	env.payload_length = sizeof(GcsBlockForwardPayload);
	UT_ASSERT(!cluster_gcs_block_test_r4_forward96(&env, bytes));
	env.payload_length = sizeof(ClusterR4CrForwardPayload) - 1;
	UT_ASSERT(!cluster_gcs_block_test_r4_forward96(&env, bytes));
	env.payload_length = sizeof(ClusterR4CrForwardPayload) + 1;
	UT_ASSERT(!cluster_gcs_block_test_r4_forward96(&env, bytes));
}

/* A negotiated-capability sample that cannot be joined to the same committed
 * OPEN generation is consumed fail-closed before PCM/dedup/ring/holder state.
 * Deleting either matcher call or moving mutation ahead of it breaks this. */
UT_TEST(test_r4_same_open_mismatch_has_zero_route_or_holder_mutation)
{
	ClusterR4CrRequestPayload request = route_test_request80();
	ClusterR4CrForwardPayload forward = route_test_forward96();
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, UT_REQUESTER_NODE, UT_MASTER_NODE,
							  sizeof(request));

	route_seam_reset();
	route_seam.peer_open_ok = false;
	UT_ASSERT(cluster_gcs_block_test_r4_request80(&env, &request));
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 1);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 0);
	UT_ASSERT_EQ(route_seam.arm_calls, 0);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.finish_calls, 0);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
	UT_ASSERT_EQ(route_seam.recheck_calls, 1);
	route_assert_refusal(GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED, 0);

	env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, UT_MASTER_NODE, UT_HOLDER_NODE,
						  sizeof(forward));
	route_seam_reset();
	route_seam.peer_open_ok = false;
	UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 1);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 0);
	UT_ASSERT_EQ(route_seam.arm_calls, 0);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.finish_calls, 0);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
	UT_ASSERT_EQ(route_seam.recheck_calls, 0);
	UT_ASSERT_EQ(route_seam.refusal_enqueue_calls, 0);
}

/* Once the requester capability generation has been sampled, admission
 * refusal is an authenticated pre-token status-25 outcome.  It must not
 * acquire/recheck/leave a token or touch PCM, dedup, forwarding, or holder
 * state. */
UT_TEST(test_request80_admission_refusals_publish_without_token)
{
	static const ClusterSemanticAdmissionResult refusals[] = {
		CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED,
		CLUSTER_SEMANTIC_ADMISSION_CLOSED,
		CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED
	};
	ClusterR4CrRequestPayload request = route_test_request80();
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, UT_REQUESTER_NODE, UT_MASTER_NODE,
							  sizeof(request));
	int i;

	for (i = 0; i < lengthof(refusals); i++) {
		route_seam_reset();
		route_seam.admission_result = refusals[i];
		UT_ASSERT(cluster_gcs_block_test_r4_request80(&env, &request));
		UT_ASSERT_EQ(route_seam.capability_calls, 1);
		UT_ASSERT_EQ(route_seam.capability_peers[0], UT_REQUESTER_NODE);
		UT_ASSERT_EQ(route_seam.enter_calls, 1);
		UT_ASSERT_EQ(route_seam.peer_open_calls, 0);
		UT_ASSERT_EQ(route_seam.leave_calls, 0);
		UT_ASSERT_EQ(route_seam.recheck_calls, 0);
		UT_ASSERT_EQ(route_seam.snapshot_calls, 0);
		UT_ASSERT_EQ(route_seam.arm_calls, 0);
		UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
		UT_ASSERT_EQ(route_seam.finish_calls, 0);
		UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
		route_assert_refusal(GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED, 0);
	}
}

UT_TEST(test_request80_sender_mismatch_is_consumed_without_route_mutation)
{
	ClusterR4CrRequestPayload request = route_test_request80();
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, UT_REQUESTER_NODE, UT_MASTER_NODE,
							  sizeof(request));

	request.base.sender_node = UT_HOLDER_NODE;
	route_seam_reset();
	UT_ASSERT_EQ(env.payload_length, 80);
	UT_ASSERT(cluster_gcs_block_test_r4_request80(&env, &request));
	UT_ASSERT_EQ(route_seam.capability_calls, 1);
	UT_ASSERT_EQ(route_seam.capability_peers[0], UT_REQUESTER_NODE);
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 1);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 0);
	UT_ASSERT_EQ(route_seam.arm_calls, 0);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.finish_calls, 0);
	UT_ASSERT_EQ(route_seam.recheck_calls, 1);
	route_assert_refusal(GCS_BLOCK_REPLY_R4_DENIED, 0);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
}

UT_TEST(test_request80_reserved_extension_is_consumed_without_route_mutation)
{
	ClusterR4CrRequestPayload request = route_test_request80();
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, UT_REQUESTER_NODE, UT_MASTER_NODE,
							  sizeof(request));

	request.extension.reserved[0] = 1;
	route_seam_reset();
	UT_ASSERT_EQ(env.payload_length, 80);
	UT_ASSERT(cluster_gcs_block_test_r4_request80(&env, &request));
	UT_ASSERT_EQ(route_seam.capability_calls, 1);
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 1);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 0);
	UT_ASSERT_EQ(route_seam.arm_calls, 0);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.finish_calls, 0);
	UT_ASSERT_EQ(route_seam.recheck_calls, 1);
	route_assert_refusal(GCS_BLOCK_REPLY_R4_DENIED, 0);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
}

UT_TEST(test_request80_epoch_mismatch_is_consumed_without_route_mutation)
{
	ClusterR4CrRequestPayload request = route_test_request80();
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, UT_REQUESTER_NODE, UT_MASTER_NODE,
							  sizeof(request));

	request.base.epoch = UT_FORMATION_EPOCH + 1;
	route_seam_reset();
	UT_ASSERT_EQ(env.payload_length, 80);
	UT_ASSERT(cluster_gcs_block_test_r4_request80(&env, &request));
	UT_ASSERT_EQ(route_seam.capability_calls, 1);
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 1);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 0);
	UT_ASSERT_EQ(route_seam.arm_calls, 0);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.finish_calls, 0);
	UT_ASSERT_EQ(route_seam.recheck_calls, 1);
	route_assert_refusal(GCS_BLOCK_REPLY_R4_DENIED, 0);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
}

UT_TEST(test_request80_wrong_master_is_consumed_before_snapshot)
{
	ClusterR4CrRequestPayload request = route_test_request80();
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, UT_REQUESTER_NODE, UT_MASTER_NODE,
							  sizeof(request));

	route_seam_reset();
	route_seam.lookup_master_node = 0;
	UT_ASSERT_EQ(env.payload_length, 80);
	UT_ASSERT(cluster_gcs_block_test_r4_request80(&env, &request));
	UT_ASSERT_EQ(route_seam.capability_calls, 1);
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 1);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 0);
	UT_ASSERT_EQ(route_seam.arm_calls, 0);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.finish_calls, 0);
	UT_ASSERT_EQ(route_seam.recheck_calls, 1);
	route_assert_refusal(GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED, 1);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
}

UT_TEST(test_request80_snapshot_failure_stops_before_route_arm)
{
	ClusterR4CrRequestPayload request = route_test_request80();
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, UT_REQUESTER_NODE, UT_MASTER_NODE,
							  sizeof(request));

	route_seam_reset();
	route_seam.snapshot_ok = false;
	UT_ASSERT_EQ(env.payload_length, 80);
	UT_ASSERT(cluster_gcs_block_test_r4_request80(&env, &request));
	UT_ASSERT_EQ(route_seam.capability_calls, 1);
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 1);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 1);
	UT_ASSERT_EQ(route_seam.arm_calls, 0);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.finish_calls, 0);
	UT_ASSERT_EQ(route_seam.recheck_calls, 1);
	route_assert_refusal(GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED, 0);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT(route_seam.snapshot_sequence < route_seam.leave_sequence);
}

UT_TEST(test_request80_arm_full_stops_before_publication)
{
	ClusterR4CrRequestPayload request = route_test_request80();
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, UT_REQUESTER_NODE, UT_MASTER_NODE,
							  sizeof(request));

	route_seam_reset();
	route_seam.arm_result = GCS_BLOCK_R4_ROUTE_ARM_FULL;
	UT_ASSERT_EQ(env.payload_length, 80);
	UT_ASSERT(cluster_gcs_block_test_r4_request80(&env, &request));
	UT_ASSERT_EQ(route_seam.capability_calls, 2);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 2);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 1);
	UT_ASSERT_EQ(route_seam.arm_calls, 1);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.finish_calls, 0);
	UT_ASSERT_EQ(route_seam.recheck_calls, 1);
	route_assert_refusal(GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED, 0);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT(route_seam.snapshot_sequence < route_seam.arm_sequence);
	UT_ASSERT(route_seam.arm_sequence < route_seam.leave_sequence);
}

UT_TEST(test_request80_enqueue_refusal_finishes_unadmitted_once)
{
	ClusterR4CrRequestPayload request = route_test_request80();
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, UT_REQUESTER_NODE, UT_MASTER_NODE,
							  sizeof(request));

	route_seam_reset();
	route_seam.enqueue_ok = false;
	route_seam.finish_result = GCS_BLOCK_R4_ROUTE_SEND_RETRYABLE;
	UT_ASSERT_EQ(env.payload_length, 80);
	UT_ASSERT(cluster_gcs_block_test_r4_request80(&env, &request));
	UT_ASSERT_EQ(route_seam.capability_calls, 2);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 2);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 1);
	UT_ASSERT_EQ(route_seam.arm_calls, 1);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 1);
	UT_ASSERT_EQ(route_seam.finish_calls, 1);
	UT_ASSERT(!route_seam.finished_outbound_admitted);
	UT_ASSERT_EQ(route_seam.recheck_calls, 1);
	route_assert_refusal(GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED, 0);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT(route_seam.arm_sequence < route_seam.enqueue_sequence);
	UT_ASSERT(route_seam.enqueue_sequence < route_seam.finish_sequence);
	UT_ASSERT(route_seam.finish_sequence < route_seam.recheck_sequence);
	UT_ASSERT(route_seam.recheck_sequence < route_seam.refusal_enqueue_sequence);
	UT_ASSERT(route_seam.refusal_enqueue_sequence < route_seam.leave_sequence);
	UT_ASSERT(route_seam.recheck_sequence < route_seam.leave_sequence);
}

UT_TEST(test_request80_final_recheck_failure_leaves_after_one_publication)
{
	ClusterR4CrRequestPayload request = route_test_request80();
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, UT_REQUESTER_NODE, UT_MASTER_NODE,
							  sizeof(request));

	route_seam_reset();
	route_seam.recheck_ok = false;
	UT_ASSERT_EQ(env.payload_length, 80);
	UT_ASSERT(cluster_gcs_block_test_r4_request80(&env, &request));
	UT_ASSERT_EQ(route_seam.capability_calls, 2);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 2);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 1);
	UT_ASSERT_EQ(route_seam.arm_calls, 1);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 1);
	UT_ASSERT_EQ(route_seam.finish_calls, 1);
	UT_ASSERT(route_seam.finished_outbound_admitted);
	UT_ASSERT_EQ(route_seam.recheck_calls, 1);
	route_assert_refusal(GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED, 0);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
	UT_ASSERT(route_seam.enqueue_sequence < route_seam.finish_sequence);
	UT_ASSERT(route_seam.finish_sequence < route_seam.recheck_sequence);
	UT_ASSERT(route_seam.recheck_sequence < route_seam.refusal_enqueue_sequence);
	UT_ASSERT(route_seam.refusal_enqueue_sequence < route_seam.leave_sequence);
	UT_ASSERT(route_seam.recheck_sequence < route_seam.leave_sequence);
}

UT_TEST(test_forward96_master_mismatch_is_consumed_without_holder_submit)
{
	ClusterR4CrForwardPayload forward = route_test_forward96();
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, UT_MASTER_NODE, UT_HOLDER_NODE,
							  sizeof(forward));

	forward.base.master_node = UT_HOLDER_NODE;
	route_seam_reset();
	UT_ASSERT_EQ(env.payload_length, 96);
	UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
	UT_ASSERT_EQ(route_seam.capability_calls, 1);
	UT_ASSERT_EQ(route_seam.capability_peers[0], UT_MASTER_NODE);
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 1);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
	UT_ASSERT_EQ(route_seam.recheck_calls, 0);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 0);
	UT_ASSERT_EQ(route_seam.arm_calls, 0);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.finish_calls, 0);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
}

UT_TEST(test_forward96_proof_epoch_mismatch_is_consumed_without_holder_submit)
{
	ClusterR4CrForwardPayload forward = route_test_forward96();
	ClusterICEnvelope env
		= route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, UT_MASTER_NODE, UT_HOLDER_NODE,
							  sizeof(forward));

	/* Corrupt the proof's embedded formation-epoch half while keeping the base
	 * and authenticated envelope epochs valid. */
	forward.extension.kind.cr.master_authority_generation_le[4]
		= (uint8)(UT_FORMATION_EPOCH + 1);
	route_seam_reset();
	UT_ASSERT_EQ(env.payload_length, 96);
	UT_ASSERT(cluster_gcs_block_test_r4_forward96(&env, &forward));
	UT_ASSERT_EQ(route_seam.capability_calls, 1);
	UT_ASSERT_EQ(route_seam.capability_peers[0], UT_MASTER_NODE);
	UT_ASSERT_EQ(route_seam.enter_calls, 1);
	UT_ASSERT_EQ(route_seam.peer_open_calls, 1);
	UT_ASSERT_EQ(route_seam.holder_submit_calls, 0);
	UT_ASSERT_EQ(route_seam.recheck_calls, 0);
	UT_ASSERT_EQ(route_seam.snapshot_calls, 0);
	UT_ASSERT_EQ(route_seam.arm_calls, 0);
	UT_ASSERT_EQ(route_seam.enqueue_calls, 0);
	UT_ASSERT_EQ(route_seam.finish_calls, 0);
	UT_ASSERT_EQ(route_seam.leave_calls, 1);
}

int
main(void)
{
	UT_PLAN(67);
	UT_RUN(test_01_null_authority_is_protocol);
	UT_RUN(test_02_null_output_is_protocol);
	UT_RUN(test_03_canonical_n_has_no_holder);
	UT_RUN(test_04_n_with_x_is_ambiguous);
	UT_RUN(test_05_n_with_s_is_ambiguous);
	UT_RUN(test_06_n_with_master_is_ambiguous);
	UT_RUN(test_07_x_node_zero_is_selected);
	UT_RUN(test_08_x_node_31_is_selected);
	UT_RUN(test_09_x_without_holder_is_ambiguous);
	UT_RUN(test_10_x_negative_holder_is_ambiguous);
	UT_RUN(test_11_x_out_of_range_is_ambiguous);
	UT_RUN(test_12_x_master_mismatch_is_ambiguous);
	UT_RUN(test_13_x_with_s_bitmap_is_ambiguous);
	UT_RUN(test_14_s_node_zero_is_selected);
	UT_RUN(test_15_s_node_31_is_selected);
	UT_RUN(test_16_s_multiple_selects_canonical);
	UT_RUN(test_17_s_zero_bitmap_is_ambiguous);
	UT_RUN(test_18_s_with_x_is_ambiguous);
	UT_RUN(test_19_s_without_master_is_ambiguous);
	UT_RUN(test_20_s_out_of_range_master_is_ambiguous);
	UT_RUN(test_21_s_missing_canonical_bit_is_ambiguous);
	UT_RUN(test_22_unknown_state_is_ambiguous);
	UT_RUN(test_23_reserved_zero_is_required);
	UT_RUN(test_24_reserved_one_is_required);
	UT_RUN(test_25_pending_destructive_convert_is_recovering);
	UT_RUN(test_26_zero_master_generation_is_rejected);
	UT_RUN(test_27_zero_restart_half_is_rejected);
	UT_RUN(test_28_wrong_epoch_half_is_rejected);
	UT_RUN(test_29_zero_transition_count_is_rejected);
	UT_RUN(test_30_exhausted_transition_count_is_rejected);
	UT_RUN(test_31_exact_duplicate_route_matches);
	UT_RUN(test_32_transition_drift_closes_duplicate);
	UT_RUN(test_reason_polarity_0);
	UT_RUN(test_reason_polarity_1);
	UT_RUN(test_reason_polarity_2);
	UT_RUN(test_reason_polarity_3);
	UT_RUN(test_reason_polarity_4);
	UT_RUN(test_reason_polarity_5);
	UT_RUN(test_reason_polarity_6);
	UT_RUN(test_reason_polarity_7);
	UT_RUN(test_reason_polarity_8);
	UT_RUN(test_reason_polarity_9);
	UT_RUN(test_reason_polarity_10);
	UT_RUN(test_reason_polarity_11);
	UT_RUN(test_reason_polarity_12);
	UT_RUN(test_reason_polarity_13);
	UT_RUN(test_reason_polarity_14);
	UT_RUN(test_reason_polarity_15);
	UT_RUN(test_reason_polarity_16);
	UT_RUN(test_reason_polarity_17);
	UT_RUN(test_unknown_reason_fails_closed);
	UT_RUN(test_d3_result_reason_mapping_is_closed);
	UT_RUN(test_r4_refusal_decoder_requires_exact_domain_identity_and_zero_body);
	UT_RUN(test_request80_routes_stored_proof_to_real_forward96_handler);
	UT_RUN(test_r4_try_handlers_consume_only_exact_extended_lengths);
	UT_RUN(test_r4_same_open_mismatch_has_zero_route_or_holder_mutation);
	UT_RUN(test_request80_admission_refusals_publish_without_token);
	UT_RUN(test_request80_sender_mismatch_is_consumed_without_route_mutation);
	UT_RUN(test_request80_reserved_extension_is_consumed_without_route_mutation);
	UT_RUN(test_request80_epoch_mismatch_is_consumed_without_route_mutation);
	UT_RUN(test_request80_wrong_master_is_consumed_before_snapshot);
	UT_RUN(test_request80_snapshot_failure_stops_before_route_arm);
	UT_RUN(test_request80_arm_full_stops_before_publication);
	UT_RUN(test_request80_enqueue_refusal_finishes_unadmitted_once);
	UT_RUN(test_request80_final_recheck_failure_leaves_after_one_publication);
	UT_RUN(test_forward96_master_mismatch_is_consumed_without_holder_submit);
	UT_RUN(test_forward96_proof_epoch_mismatch_is_consumed_without_holder_submit);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
