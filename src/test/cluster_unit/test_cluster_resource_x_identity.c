/*-------------------------------------------------------------------------
 *
 * test_cluster_resource_x_identity.c
 *    Resource-X logical assertion and attempt identity — spec-8.6 D6-01.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_pcm_x_convert.h"
#include "cluster/cluster_resource_x_identity.h"

#include "cluster_resource_x_wire_f076.h"

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

static BufferTag
make_tag(void)
{
	BufferTag tag;

	tag.spcOid = 1663;
	tag.dbOid = 5;
	tag.relNumber = 9001;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = 42;
	return tag;
}

static void
put_wire_value(unsigned char *bytes, Size offset, const void *value, Size len)
{
	memcpy(bytes + offset, value, len);
}

static void
put_f076_buffer_tag(unsigned char *bytes, Size offset, const BufferTag *tag)
{
	put_wire_value(bytes, offset, &tag->spcOid, sizeof(tag->spcOid));
	put_wire_value(bytes, offset + 4, &tag->dbOid, sizeof(tag->dbOid));
	put_wire_value(bytes, offset + 8, &tag->relNumber, sizeof(tag->relNumber));
	put_wire_value(bytes, offset + 12, &tag->forkNum, sizeof(tag->forkNum));
	put_wire_value(bytes, offset + 16, &tag->blockNum, sizeof(tag->blockNum));
}

static void
put_f076_wait_identity(unsigned char *bytes, Size offset,
						const PcmXWaitIdentity *identity)
{
	put_f076_buffer_tag(bytes, offset, &identity->tag);
	put_wire_value(bytes, offset + F076_WAIT_NODE_OFF, &identity->node_id,
				   sizeof(identity->node_id));
	put_wire_value(bytes, offset + F076_WAIT_PROCNO_OFF, &identity->procno,
				   sizeof(identity->procno));
	put_wire_value(bytes, offset + F076_WAIT_XID_OFF, &identity->xid,
				   sizeof(identity->xid));
	put_wire_value(bytes, offset + F076_WAIT_CLUSTER_EPOCH_OFF,
				   &identity->cluster_epoch, sizeof(identity->cluster_epoch));
	put_wire_value(bytes, offset + F076_WAIT_REQUEST_ID_OFF,
				   &identity->request_id, sizeof(identity->request_id));
	put_wire_value(bytes, offset + F076_WAIT_SEQ_OFF, &identity->wait_seq,
				   sizeof(identity->wait_seq));
	put_wire_value(bytes, offset + F076_WAIT_BASE_OWN_GENERATION_OFF,
				   &identity->base_own_generation,
				   sizeof(identity->base_own_generation));
}

static void
put_f076_prehandle(unsigned char *bytes, Size offset,
				   const PcmXPrehandleKey *prehandle)
{
	put_wire_value(bytes, offset, &prehandle->sender_session_incarnation,
				   sizeof(prehandle->sender_session_incarnation));
	put_wire_value(bytes, offset + F076_PREHANDLE_SEQUENCE_OFF,
				   &prehandle->prehandle_sequence,
				   sizeof(prehandle->prehandle_sequence));
}

static void
put_f076_ticket_handle(unsigned char *bytes, Size offset,
					   const PcmXTicketHandle *handle)
{
	put_wire_value(bytes, offset, &handle->ticket_id, sizeof(handle->ticket_id));
	put_wire_value(bytes, offset + F076_TICKET_QUEUE_GENERATION_OFF,
				   &handle->queue_generation, sizeof(handle->queue_generation));
}

static void
put_f076_ticket_ref(unsigned char *bytes, Size offset, const PcmXTicketRef *ref)
{
	put_f076_wait_identity(bytes, offset, &ref->identity);
	put_f076_ticket_handle(bytes, offset + F076_TICKET_REF_HANDLE_OFF,
					   &ref->handle);
	put_wire_value(bytes, offset + F076_TICKET_REF_GRANT_GENERATION_OFF,
				   &ref->grant_generation, sizeof(ref->grant_generation));
}

static void
put_f076_image_token(unsigned char *bytes, Size offset,
					 const PcmXImageToken *image)
{
	put_wire_value(bytes, offset, &image->image_id, sizeof(image->image_id));
	put_wire_value(bytes, offset + F076_IMAGE_SOURCE_OWN_GENERATION_OFF,
				   &image->source_own_generation,
				   sizeof(image->source_own_generation));
	put_wire_value(bytes, offset + F076_IMAGE_PAGE_SCN_OFF, &image->page_scn,
				   sizeof(image->page_scn));
	put_wire_value(bytes, offset + F076_IMAGE_PAGE_LSN_OFF, &image->page_lsn,
				   sizeof(image->page_lsn));
	put_wire_value(bytes, offset + F076_IMAGE_SOURCE_NODE_OFF, &image->source_node,
				   sizeof(image->source_node));
	put_wire_value(bytes, offset + F076_IMAGE_PAGE_CHECKSUM_OFF,
				   &image->page_checksum, sizeof(image->page_checksum));
}

static void
put_f076_lmd_vertex(unsigned char *bytes, Size offset,
					const ClusterLmdVertex *vertex)
{
	put_wire_value(bytes, offset, &vertex->node_id, sizeof(vertex->node_id));
	put_wire_value(bytes, offset + F076_LMD_VERTEX_PROCNO_OFF, &vertex->procno,
				   sizeof(vertex->procno));
	put_wire_value(bytes, offset + F076_LMD_VERTEX_CLUSTER_EPOCH_OFF,
				   &vertex->cluster_epoch, sizeof(vertex->cluster_epoch));
	put_wire_value(bytes, offset + F076_LMD_VERTEX_REQUEST_ID_OFF,
				   &vertex->request_id, sizeof(vertex->request_id));
	put_wire_value(bytes, offset + F076_LMD_VERTEX_XID_OFF, &vertex->xid,
				   sizeof(vertex->xid));
	put_wire_value(bytes, offset + F076_LMD_VERTEX_LOCAL_START_OFF,
				   &vertex->local_start_ts_ms,
				   sizeof(vertex->local_start_ts_ms));
	put_wire_value(bytes, offset + F076_LMD_VERTEX_WAIT_SEQ_OFF,
				   &vertex->wait_seq, sizeof(vertex->wait_seq));
}

#define ASSERT_F076_BYTES(actual, expected) \
	UT_ASSERT(memcmp(&(actual), (expected), sizeof(actual)) == 0)

UT_TEST(test_canonical_layout)
{
	UT_ASSERT_EQ(sizeof(BufferTag), 20);
	UT_ASSERT_EQ(sizeof(ResourceXAssertion), 24);
	UT_ASSERT_EQ(offsetof(ResourceXAssertion, resource), 0);
	UT_ASSERT_EQ(offsetof(ResourceXAssertion, requester_node), 20);
	UT_ASSERT_EQ(sizeof(ResourceXAttemptWitness), 32);
	UT_ASSERT_EQ(offsetof(ResourceXAttemptWitness, assertion), 0);
	UT_ASSERT_EQ(offsetof(ResourceXAttemptWitness, base_authority_generation), 24);
	UT_ASSERT_EQ(sizeof(ResourceXTransportWitness), 24);
	UT_ASSERT_EQ(offsetof(ResourceXTransportWitness, cluster_epoch), 0);
	UT_ASSERT_EQ(offsetof(ResourceXTransportWitness, peer_session_incarnation), 8);
	UT_ASSERT_EQ(offsetof(ResourceXTransportWitness, connection_generation), 16);
	UT_ASSERT_EQ(offsetof(ResourceXTransportWitness, lane_id), 20);
	UT_ASSERT_EQ(offsetof(ResourceXTransportWitness, flags), 22);
}

UT_TEST(test_init_accepts_shared_catalog_identity)
{
	BufferTag tag = make_tag();
	ResourceXAssertion assertion;

	tag.dbOid = InvalidOid;
	UT_ASSERT(resource_x_assertion_init(&tag, 0, &assertion));
	UT_ASSERT(resource_x_assertion_valid(&assertion));
	UT_ASSERT(BufferTagsEqual(&assertion.resource, &tag));
	UT_ASSERT_EQ(assertion.requester_node, 0);
}

UT_TEST(test_validation_rejects_invalid_native_tag_fields)
{
	BufferTag tag = make_tag();
	ResourceXAssertion assertion;

	UT_ASSERT(resource_x_assertion_init(&tag, RESOURCE_X_PROTOCOL_NODE_LIMIT - 1,
		&assertion));
	assertion.resource.relNumber = InvalidRelFileNumber;
	UT_ASSERT(!resource_x_assertion_valid(&assertion));
	assertion.resource = tag;
	assertion.resource.blockNum = InvalidBlockNumber;
	UT_ASSERT(!resource_x_assertion_valid(&assertion));
	assertion.resource = tag;
	assertion.resource.forkNum = InvalidForkNumber;
	UT_ASSERT(!resource_x_assertion_valid(&assertion));
	assertion.resource.forkNum = (ForkNumber)(MAX_FORKNUM + 1);
	UT_ASSERT(!resource_x_assertion_valid(&assertion));
	assertion.resource = tag;
	assertion.requester_node = -1;
	UT_ASSERT(!resource_x_assertion_valid(&assertion));
	assertion.requester_node = RESOURCE_X_PROTOCOL_NODE_LIMIT;
	UT_ASSERT(!resource_x_assertion_valid(&assertion));
}

UT_TEST(test_init_rejects_null_and_invalid_inputs)
{
	BufferTag tag = make_tag();
	ResourceXAssertion assertion;

	UT_ASSERT(!resource_x_assertion_init(NULL, 1, &assertion));
	UT_ASSERT(!resource_x_assertion_init(&tag, 1, NULL));
	tag.relNumber = InvalidRelFileNumber;
	UT_ASSERT(!resource_x_assertion_init(&tag, 1, &assertion));
}

UT_TEST(test_equality_is_exactly_resource_and_requester_node)
{
	BufferTag tag = make_tag();
	ResourceXAssertion left;
	ResourceXAssertion right;

	UT_ASSERT(resource_x_assertion_init(&tag, 7, &left));
	UT_ASSERT(resource_x_assertion_init(&tag, 7, &right));
	UT_ASSERT(resource_x_assertion_equal(&left, &right));
	UT_ASSERT_EQ(resource_x_assertion_hash(&left),
		resource_x_assertion_hash(&right));
	right.requester_node++;
	UT_ASSERT(!resource_x_assertion_equal(&left, &right));
	right = left;
	right.resource.blockNum++;
	UT_ASSERT(!resource_x_assertion_equal(&left, &right));
}

UT_TEST(test_attempt_match_adds_only_base_generation)
{
	BufferTag tag = make_tag();
	ResourceXAttemptWitness left;
	ResourceXAttemptWitness right;

	UT_ASSERT(resource_x_assertion_init(&tag, 7, &left.assertion));
	left.base_authority_generation = 100;
	right = left;
	UT_ASSERT(resource_x_attempt_matches(&left, &right));
	right.base_authority_generation++;
	UT_ASSERT(!resource_x_attempt_matches(&left, &right));
	right = left;
	right.assertion.requester_node++;
	UT_ASSERT(!resource_x_attempt_matches(&left, &right));
}

UT_TEST(test_null_comparisons_fail_closed)
{
	BufferTag tag = make_tag();
	ResourceXAttemptWitness attempt;

	UT_ASSERT(resource_x_assertion_init(&tag, 1, &attempt.assertion));
	attempt.base_authority_generation = 1;
	UT_ASSERT(!resource_x_assertion_valid(NULL));
	UT_ASSERT(!resource_x_assertion_equal(NULL, &attempt.assertion));
	UT_ASSERT(!resource_x_assertion_equal(&attempt.assertion, NULL));
	UT_ASSERT(!resource_x_attempt_matches(NULL, &attempt));
	UT_ASSERT(!resource_x_attempt_matches(&attempt, NULL));
}

UT_TEST(test_f076_wire_opcode_and_length_manifest)
{
	static const uint8 current_opcodes[] = {
		PGRAC_IC_MSG_PCM_X_ENQUEUE,
		PGRAC_IC_MSG_PCM_X_ADMIT_ACK,
		PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM,
		PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM_ACK,
		PGRAC_IC_MSG_PCM_X_BLOCKER_SET_BEGIN,
		PGRAC_IC_MSG_PCM_X_BLOCKER_SET_EDGE,
		PGRAC_IC_MSG_PCM_X_BLOCKER_SET_COMMIT,
		PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK,
		PGRAC_IC_MSG_PCM_X_REVOKE,
		PGRAC_IC_MSG_PCM_X_IMAGE_READY,
		PGRAC_IC_MSG_PCM_X_PREPARE_GRANT,
		PGRAC_IC_MSG_PCM_X_INSTALL_READY,
		PGRAC_IC_MSG_PCM_X_COMMIT_X,
		PGRAC_IC_MSG_PCM_X_FINAL_ACK,
		PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK,
		PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM,
		PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL,
		PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL_ACK,
		PGRAC_IC_MSG_PCM_X_CANCEL,
		PGRAC_IC_MSG_PCM_X_CANCEL_ACK,
		PGRAC_IC_MSG_PCM_X_DRAIN_POLL,
		PGRAC_IC_MSG_PCM_X_DRAIN_ACK,
		PGRAC_IC_MSG_PCM_X_RETIRE_UP_TO,
		PGRAC_IC_MSG_PCM_X_RETIRE_ACK
	};
	static const Size current_primary_lengths[] = {
		sizeof(PcmXEnqueuePayload),
		sizeof(PcmXAdmitAckPayload),
		sizeof(PcmXPhasePayload),
		sizeof(PcmXPhasePayload),
		sizeof(PcmXBlockerSetHeaderPayload),
		sizeof(PcmXBlockerChunkPayload),
		sizeof(PcmXBlockerSetHeaderPayload),
		sizeof(PcmXPhasePayload),
		sizeof(PcmXRevokePayload),
		sizeof(PcmXGrantPayload),
		sizeof(PcmXGrantPayload),
		PCM_X_INSTALL_READY_V1_LEN,
		sizeof(PcmXPhasePayload),
		sizeof(PcmXFinalAckPayload),
		sizeof(PcmXPhasePayload),
		sizeof(PcmXPhasePayload),
		sizeof(PcmXPrehandleCancelPayload),
		sizeof(PcmXAdmitAckPayload),
		sizeof(PcmXPhasePayload),
		sizeof(PcmXPhasePayload),
		sizeof(PcmXDrainPollPayload),
		sizeof(PcmXPhasePayload),
		sizeof(PcmXRetirePayload),
		sizeof(PcmXRetirePayload)
	};
	static const Size current_alternate_lengths[] = {
		0, 0, 0, 0, 0, 0, 0, 0,
		sizeof(PcmXRevokePayloadV2),
		0, 0,
		sizeof(PcmXInstallReadyPayload),
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
	};
	Size i;

	UT_ASSERT_EQ(lengthof(current_opcodes),
		lengthof(resource_x_wire_f076_message_abi));
	UT_ASSERT_EQ(lengthof(current_primary_lengths), lengthof(current_opcodes));
	UT_ASSERT_EQ(lengthof(current_alternate_lengths), lengthof(current_opcodes));
	for (i = 0; i < lengthof(current_opcodes); i++)
	{
		UT_ASSERT_EQ(current_opcodes[i], resource_x_wire_f076_message_abi[i].opcode);
		UT_ASSERT_EQ(current_primary_lengths[i],
			resource_x_wire_f076_message_abi[i].primary_len);
		UT_ASSERT_EQ(current_alternate_lengths[i],
			resource_x_wire_f076_message_abi[i].alternate_len);
	}
}

UT_TEST(test_f076_wire_layout_offsets_are_exact)
{
	UT_ASSERT_EQ(sizeof(BufferTag), F076_BUFFER_TAG_LEN);
	UT_ASSERT_EQ(sizeof(PcmXWaitIdentity), F076_WAIT_IDENTITY_LEN);
	UT_ASSERT_EQ(offsetof(PcmXWaitIdentity, node_id), F076_WAIT_NODE_OFF);
	UT_ASSERT_EQ(offsetof(PcmXWaitIdentity, procno), F076_WAIT_PROCNO_OFF);
	UT_ASSERT_EQ(offsetof(PcmXWaitIdentity, xid), F076_WAIT_XID_OFF);
	UT_ASSERT_EQ(offsetof(PcmXWaitIdentity, cluster_epoch),
		F076_WAIT_CLUSTER_EPOCH_OFF);
	UT_ASSERT_EQ(offsetof(PcmXWaitIdentity, request_id), F076_WAIT_REQUEST_ID_OFF);
	UT_ASSERT_EQ(offsetof(PcmXWaitIdentity, wait_seq), F076_WAIT_SEQ_OFF);
	UT_ASSERT_EQ(offsetof(PcmXWaitIdentity, base_own_generation),
		F076_WAIT_BASE_OWN_GENERATION_OFF);
	UT_ASSERT_EQ(sizeof(PcmXPrehandleKey), F076_PREHANDLE_LEN);
	UT_ASSERT_EQ(offsetof(PcmXPrehandleKey, prehandle_sequence),
		F076_PREHANDLE_SEQUENCE_OFF);
	UT_ASSERT_EQ(sizeof(PcmXTicketHandle), F076_TICKET_HANDLE_LEN);
	UT_ASSERT_EQ(offsetof(PcmXTicketHandle, queue_generation),
		F076_TICKET_QUEUE_GENERATION_OFF);
	UT_ASSERT_EQ(sizeof(PcmXTicketRef), F076_TICKET_REF_LEN);
	UT_ASSERT_EQ(offsetof(PcmXTicketRef, handle), F076_TICKET_REF_HANDLE_OFF);
	UT_ASSERT_EQ(offsetof(PcmXTicketRef, grant_generation),
		F076_TICKET_REF_GRANT_GENERATION_OFF);
	UT_ASSERT_EQ(sizeof(PcmXImageToken), F076_IMAGE_TOKEN_LEN);
	UT_ASSERT_EQ(offsetof(PcmXImageToken, source_own_generation),
		F076_IMAGE_SOURCE_OWN_GENERATION_OFF);
	UT_ASSERT_EQ(offsetof(PcmXImageToken, page_scn), F076_IMAGE_PAGE_SCN_OFF);
	UT_ASSERT_EQ(offsetof(PcmXImageToken, page_lsn), F076_IMAGE_PAGE_LSN_OFF);
	UT_ASSERT_EQ(offsetof(PcmXImageToken, source_node), F076_IMAGE_SOURCE_NODE_OFF);
	UT_ASSERT_EQ(offsetof(PcmXImageToken, page_checksum),
		F076_IMAGE_PAGE_CHECKSUM_OFF);

	UT_ASSERT_EQ(sizeof(PcmXEnqueuePayload), F076_ENQUEUE_LEN);
	UT_ASSERT_EQ(offsetof(PcmXEnqueuePayload, prehandle),
		F076_ENQUEUE_PREHANDLE_OFF);
	UT_ASSERT_EQ(sizeof(PcmXPrehandleCancelPayload), F076_ENQUEUE_LEN);
	UT_ASSERT_EQ(offsetof(PcmXPrehandleCancelPayload, prehandle),
		F076_ENQUEUE_PREHANDLE_OFF);
	UT_ASSERT_EQ(sizeof(PcmXAdmitAckPayload), F076_ADMIT_ACK_LEN);
	UT_ASSERT_EQ(offsetof(PcmXAdmitAckPayload, prehandle),
		F076_ADMIT_ACK_PREHANDLE_OFF);
	UT_ASSERT_EQ(offsetof(PcmXAdmitAckPayload, result), F076_ADMIT_ACK_RESULT_OFF);
	UT_ASSERT_EQ(offsetof(PcmXAdmitAckPayload, phase), F076_ADMIT_ACK_PHASE_OFF);
	UT_ASSERT_EQ(offsetof(PcmXAdmitAckPayload, flags), F076_ADMIT_ACK_FLAGS_OFF);
	UT_ASSERT_EQ(sizeof(PcmXPhasePayload), F076_PHASE_LEN);
	UT_ASSERT_EQ(offsetof(PcmXPhasePayload, reason), F076_PHASE_REASON_OFF);
	UT_ASSERT_EQ(offsetof(PcmXPhasePayload, phase), F076_PHASE_PHASE_OFF);
	UT_ASSERT_EQ(offsetof(PcmXPhasePayload, flags), F076_PHASE_FLAGS_OFF);
	UT_ASSERT_EQ(sizeof(PcmXRevokePayload), F076_REVOKE_LEN);
	UT_ASSERT_EQ(offsetof(PcmXRevokePayload, image_id), F076_REVOKE_IMAGE_ID_OFF);
	UT_ASSERT_EQ(sizeof(PcmXRevokePayloadV2), F076_REVOKE_V2_LEN);
	UT_ASSERT_EQ(offsetof(PcmXRevokePayloadV2, required_page_scn),
		F076_REVOKE_V2_REQUIRED_PAGE_SCN_OFF);
	UT_ASSERT_EQ(sizeof(PcmXGrantPayload), F076_GRANT_LEN);
	UT_ASSERT_EQ(offsetof(PcmXGrantPayload, image), F076_GRANT_IMAGE_OFF);
	UT_ASSERT_EQ(PCM_X_INSTALL_READY_V1_LEN, F076_INSTALL_READY_V1_LEN);
	UT_ASSERT_EQ(sizeof(PcmXInstallReadyPayload), F076_INSTALL_READY_V2_LEN);
	UT_ASSERT_EQ(offsetof(PcmXInstallReadyPayload, image_id),
		F076_INSTALL_READY_IMAGE_ID_OFF);
	UT_ASSERT_EQ(offsetof(PcmXInstallReadyPayload, result),
		F076_INSTALL_READY_RESULT_OFF);
	UT_ASSERT_EQ(offsetof(PcmXInstallReadyPayload, phase),
		F076_INSTALL_READY_PHASE_OFF);
	UT_ASSERT_EQ(offsetof(PcmXInstallReadyPayload, flags),
		F076_INSTALL_READY_FLAGS_OFF);
	UT_ASSERT_EQ(offsetof(PcmXInstallReadyPayload, rebased_own_generation),
		F076_INSTALL_READY_REBASE_OFF);
	UT_ASSERT_EQ(sizeof(PcmXFinalAckPayload), F076_FINAL_ACK_LEN);
	UT_ASSERT_EQ(offsetof(PcmXFinalAckPayload, image_id),
		F076_FINAL_ACK_IMAGE_ID_OFF);
	UT_ASSERT_EQ(offsetof(PcmXFinalAckPayload, committed_own_generation),
		F076_FINAL_ACK_COMMITTED_GENERATION_OFF);
	UT_ASSERT_EQ(sizeof(PcmXBlockerSetHeaderPayload), F076_BLOCKER_HEADER_LEN);
	UT_ASSERT_EQ(offsetof(PcmXBlockerSetHeaderPayload, set_generation),
		F076_BLOCKER_HEADER_SET_GENERATION_OFF);
	UT_ASSERT_EQ(offsetof(PcmXBlockerSetHeaderPayload, nblockers),
		F076_BLOCKER_HEADER_NBLOCKERS_OFF);
	UT_ASSERT_EQ(offsetof(PcmXBlockerSetHeaderPayload, set_crc32c),
		F076_BLOCKER_HEADER_CRC_OFF);
	UT_ASSERT_EQ(sizeof(PcmXBlockerChunkPayload), F076_BLOCKER_CHUNK_LEN);
	UT_ASSERT_EQ(offsetof(PcmXBlockerChunkPayload, requester_node),
		F076_BLOCKER_CHUNK_REQUESTER_NODE_OFF);
	UT_ASSERT_EQ(offsetof(PcmXBlockerChunkPayload, requester_procno),
		F076_BLOCKER_CHUNK_REQUESTER_PROCNO_OFF);
	UT_ASSERT_EQ(offsetof(PcmXBlockerChunkPayload, chunk_no),
		F076_BLOCKER_CHUNK_NO_OFF);
	UT_ASSERT_EQ(offsetof(PcmXBlockerChunkPayload, cluster_epoch),
		F076_BLOCKER_CHUNK_CLUSTER_EPOCH_OFF);
	UT_ASSERT_EQ(offsetof(PcmXBlockerChunkPayload, request_id),
		F076_BLOCKER_CHUNK_REQUEST_ID_OFF);
	UT_ASSERT_EQ(offsetof(PcmXBlockerChunkPayload, handle),
		F076_BLOCKER_CHUNK_HANDLE_OFF);
	UT_ASSERT_EQ(offsetof(PcmXBlockerChunkPayload, grant_generation),
		F076_BLOCKER_CHUNK_GRANT_GENERATION_OFF);
	UT_ASSERT_EQ(offsetof(PcmXBlockerChunkPayload, set_generation),
		F076_BLOCKER_CHUNK_SET_GENERATION_OFF);
	UT_ASSERT_EQ(offsetof(PcmXBlockerChunkPayload, blocker),
		F076_BLOCKER_CHUNK_BLOCKER_OFF);
	UT_ASSERT_EQ(sizeof(ClusterLmdVertex), F076_LMD_VERTEX_LEN);
	UT_ASSERT_EQ(offsetof(ClusterLmdVertex, procno), F076_LMD_VERTEX_PROCNO_OFF);
	UT_ASSERT_EQ(offsetof(ClusterLmdVertex, cluster_epoch),
		F076_LMD_VERTEX_CLUSTER_EPOCH_OFF);
	UT_ASSERT_EQ(offsetof(ClusterLmdVertex, request_id),
		F076_LMD_VERTEX_REQUEST_ID_OFF);
	UT_ASSERT_EQ(offsetof(ClusterLmdVertex, xid), F076_LMD_VERTEX_XID_OFF);
	UT_ASSERT_EQ(offsetof(ClusterLmdVertex, local_start_ts_ms),
		F076_LMD_VERTEX_LOCAL_START_OFF);
	UT_ASSERT_EQ(offsetof(ClusterLmdVertex, wait_seq),
		F076_LMD_VERTEX_WAIT_SEQ_OFF);
	UT_ASSERT_EQ(sizeof(PcmXRetirePayload), F076_RETIRE_LEN);
	UT_ASSERT_EQ(offsetof(PcmXRetirePayload, master_session_incarnation),
		F076_RETIRE_MASTER_SESSION_OFF);
	UT_ASSERT_EQ(offsetof(PcmXRetirePayload, retire_through_ticket_id),
		F076_RETIRE_TICKET_ID_OFF);
	UT_ASSERT_EQ(offsetof(PcmXRetirePayload, sender_node),
		F076_RETIRE_SENDER_NODE_OFF);
	UT_ASSERT_EQ(offsetof(PcmXRetirePayload, flags), F076_RETIRE_FLAGS_OFF);
	UT_ASSERT_EQ(sizeof(PcmXDrainPollPayload), F076_DRAIN_POLL_LEN);
	UT_ASSERT_EQ(offsetof(PcmXDrainPollPayload, drain_generation),
		F076_DRAIN_POLL_GENERATION_OFF);
}

UT_TEST(test_f076_wire_payload_bytes_are_exact)
{
	PcmXWaitIdentity identity;
	PcmXPrehandleKey prehandle;
	PcmXTicketRef ref;
	PcmXImageToken image;
	PcmXEnqueuePayload enqueue;
	PcmXPrehandleCancelPayload prehandle_cancel;
	PcmXAdmitAckPayload admit_ack;
	PcmXPhasePayload phase;
	PcmXRevokePayload revoke;
	PcmXRevokePayloadV2 revoke_v2;
	PcmXGrantPayload grant;
	PcmXInstallReadyPayload install_ready;
	PcmXFinalAckPayload final_ack;
	PcmXBlockerSetHeaderPayload blocker_header;
	PcmXBlockerChunkPayload blocker_chunk;
	PcmXRetirePayload retire;
	PcmXDrainPollPayload drain_poll;
	unsigned char expected[F076_BLOCKER_CHUNK_LEN];

	MemSet(&identity, 0, sizeof(identity));
	identity.tag = make_tag();
	identity.node_id = 0x1020304;
	identity.procno = UINT32_C(0x11223344);
	identity.xid = UINT32_C(0x55667788);
	identity.cluster_epoch = UINT64CONST(0x0102030405060708);
	identity.request_id = UINT64CONST(0x1112131415161718);
	identity.wait_seq = UINT64CONST(0x2122232425262728);
	identity.base_own_generation = UINT64CONST(0x3132333435363738);
	MemSet(expected, 0, sizeof(expected));
	put_f076_wait_identity(expected, 0, &identity);
	ASSERT_F076_BYTES(identity, expected);

	MemSet(&prehandle, 0, sizeof(prehandle));
	prehandle.sender_session_incarnation = UINT64CONST(0x4142434445464748);
	prehandle.prehandle_sequence = UINT64CONST(0x5152535455565758);
	MemSet(expected, 0, sizeof(expected));
	put_f076_prehandle(expected, 0, &prehandle);
	ASSERT_F076_BYTES(prehandle, expected);

	MemSet(&ref, 0, sizeof(ref));
	ref.identity = identity;
	ref.handle.ticket_id = UINT64CONST(0x6162636465666768);
	ref.handle.queue_generation = UINT64CONST(0x7172737475767778);
	ref.grant_generation = UINT64CONST(0x8182838485868788);
	MemSet(expected, 0, sizeof(expected));
	put_f076_ticket_ref(expected, 0, &ref);
	ASSERT_F076_BYTES(ref, expected);

	MemSet(&image, 0, sizeof(image));
	image.image_id = UINT64CONST(0x9192939495969798);
	image.source_own_generation = UINT64CONST(0xa1a2a3a4a5a6a7a8);
	image.page_scn = UINT64CONST(0xb1b2b3b4b5b6b7b8);
	image.page_lsn = UINT64CONST(0xc1c2c3c4c5c6c7c8);
	image.source_node = UINT32_C(0xd1d2d3d4);
	image.page_checksum = UINT32_C(0xe1e2e3e4);
	MemSet(expected, 0, sizeof(expected));
	put_f076_image_token(expected, 0, &image);
	ASSERT_F076_BYTES(image, expected);

	MemSet(&enqueue, 0, sizeof(enqueue));
	enqueue.identity = identity;
	enqueue.prehandle = prehandle;
	MemSet(expected, 0, sizeof(expected));
	put_f076_wait_identity(expected, 0, &identity);
	put_f076_prehandle(expected, F076_ENQUEUE_PREHANDLE_OFF, &prehandle);
	ASSERT_F076_BYTES(enqueue, expected);

	MemSet(&prehandle_cancel, 0, sizeof(prehandle_cancel));
	prehandle_cancel.identity = identity;
	prehandle_cancel.prehandle = prehandle;
	ASSERT_F076_BYTES(prehandle_cancel, expected);

	MemSet(&admit_ack, 0, sizeof(admit_ack));
	admit_ack.ref = ref;
	admit_ack.prehandle = prehandle;
	admit_ack.result = UINT32_C(0x10293847);
	admit_ack.phase = UINT16_C(0x5968);
	admit_ack.flags = UINT16_C(0x7a8b);
	MemSet(expected, 0, sizeof(expected));
	put_f076_ticket_ref(expected, 0, &ref);
	put_f076_prehandle(expected, F076_ADMIT_ACK_PREHANDLE_OFF, &prehandle);
	put_wire_value(expected, F076_ADMIT_ACK_RESULT_OFF, &admit_ack.result,
				   sizeof(admit_ack.result));
	put_wire_value(expected, F076_ADMIT_ACK_PHASE_OFF, &admit_ack.phase,
				   sizeof(admit_ack.phase));
	put_wire_value(expected, F076_ADMIT_ACK_FLAGS_OFF, &admit_ack.flags,
				   sizeof(admit_ack.flags));
	ASSERT_F076_BYTES(admit_ack, expected);

	MemSet(&phase, 0, sizeof(phase));
	phase.ref = ref;
	phase.reason = UINT32_C(0x8c9daebf);
	phase.phase = UINT16_C(0xc0d1);
	phase.flags = UINT16_C(0xe2f3);
	MemSet(expected, 0, sizeof(expected));
	put_f076_ticket_ref(expected, 0, &ref);
	put_wire_value(expected, F076_PHASE_REASON_OFF, &phase.reason,
				   sizeof(phase.reason));
	put_wire_value(expected, F076_PHASE_PHASE_OFF, &phase.phase,
				   sizeof(phase.phase));
	put_wire_value(expected, F076_PHASE_FLAGS_OFF, &phase.flags,
				   sizeof(phase.flags));
	ASSERT_F076_BYTES(phase, expected);

	MemSet(&revoke, 0, sizeof(revoke));
	revoke.ref = ref;
	revoke.image_id = image.image_id;
	MemSet(expected, 0, sizeof(expected));
	put_f076_ticket_ref(expected, 0, &ref);
	put_wire_value(expected, F076_REVOKE_IMAGE_ID_OFF, &revoke.image_id,
				   sizeof(revoke.image_id));
	ASSERT_F076_BYTES(revoke, expected);

	MemSet(&revoke_v2, 0, sizeof(revoke_v2));
	revoke_v2.v1 = revoke;
	revoke_v2.required_page_scn = UINT64CONST(0xf1f2f3f4f5f6f7f8);
	put_wire_value(expected, F076_REVOKE_V2_REQUIRED_PAGE_SCN_OFF,
				   &revoke_v2.required_page_scn,
				   sizeof(revoke_v2.required_page_scn));
	ASSERT_F076_BYTES(revoke_v2, expected);
	UT_ASSERT(memcmp(&revoke_v2, &revoke, F076_REVOKE_LEN) == 0);

	MemSet(&grant, 0, sizeof(grant));
	grant.ref = ref;
	grant.image = image;
	MemSet(expected, 0, sizeof(expected));
	put_f076_ticket_ref(expected, 0, &ref);
	put_f076_image_token(expected, F076_GRANT_IMAGE_OFF, &image);
	ASSERT_F076_BYTES(grant, expected);

	MemSet(&install_ready, 0, sizeof(install_ready));
	install_ready.ref = ref;
	install_ready.image_id = image.image_id;
	install_ready.result = UINT32_C(0x13572468);
	install_ready.phase = UINT16_C(0x2468);
	install_ready.flags = UINT16_C(0x369c);
	install_ready.rebased_own_generation = UINT64CONST(0x0a1b2c3d4e5f6071);
	MemSet(expected, 0, sizeof(expected));
	put_f076_ticket_ref(expected, 0, &ref);
	put_wire_value(expected, F076_INSTALL_READY_IMAGE_ID_OFF,
				   &install_ready.image_id, sizeof(install_ready.image_id));
	put_wire_value(expected, F076_INSTALL_READY_RESULT_OFF,
				   &install_ready.result, sizeof(install_ready.result));
	put_wire_value(expected, F076_INSTALL_READY_PHASE_OFF,
				   &install_ready.phase, sizeof(install_ready.phase));
	put_wire_value(expected, F076_INSTALL_READY_FLAGS_OFF,
				   &install_ready.flags, sizeof(install_ready.flags));
	put_wire_value(expected, F076_INSTALL_READY_REBASE_OFF,
				   &install_ready.rebased_own_generation,
				   sizeof(install_ready.rebased_own_generation));
	ASSERT_F076_BYTES(install_ready, expected);
	UT_ASSERT(memcmp(&install_ready, expected, F076_INSTALL_READY_V1_LEN) == 0);

	MemSet(&final_ack, 0, sizeof(final_ack));
	final_ack.ref = ref;
	final_ack.image_id = image.image_id;
	final_ack.committed_own_generation = UINT64CONST(0x123456789abcdef0);
	MemSet(expected, 0, sizeof(expected));
	put_f076_ticket_ref(expected, 0, &ref);
	put_wire_value(expected, F076_FINAL_ACK_IMAGE_ID_OFF, &final_ack.image_id,
				   sizeof(final_ack.image_id));
	put_wire_value(expected, F076_FINAL_ACK_COMMITTED_GENERATION_OFF,
				   &final_ack.committed_own_generation,
				   sizeof(final_ack.committed_own_generation));
	ASSERT_F076_BYTES(final_ack, expected);

	MemSet(&blocker_header, 0, sizeof(blocker_header));
	blocker_header.ref = ref;
	blocker_header.set_generation = UINT64CONST(0x2233445566778899);
	blocker_header.nblockers = UINT32_C(0xaabbccdd);
	blocker_header.set_crc32c = UINT32_C(0xeeff1020);
	MemSet(expected, 0, sizeof(expected));
	put_f076_ticket_ref(expected, 0, &ref);
	put_wire_value(expected, F076_BLOCKER_HEADER_SET_GENERATION_OFF,
				   &blocker_header.set_generation,
				   sizeof(blocker_header.set_generation));
	put_wire_value(expected, F076_BLOCKER_HEADER_NBLOCKERS_OFF,
				   &blocker_header.nblockers, sizeof(blocker_header.nblockers));
	put_wire_value(expected, F076_BLOCKER_HEADER_CRC_OFF,
				   &blocker_header.set_crc32c,
				   sizeof(blocker_header.set_crc32c));
	ASSERT_F076_BYTES(blocker_header, expected);

	MemSet(&blocker_chunk, 0, sizeof(blocker_chunk));
	blocker_chunk.tag = identity.tag;
	blocker_chunk.requester_node = identity.node_id;
	blocker_chunk.requester_procno = identity.procno;
	blocker_chunk.chunk_no = UINT32_C(0x31415926);
	blocker_chunk.cluster_epoch = identity.cluster_epoch;
	blocker_chunk.request_id = identity.request_id;
	blocker_chunk.handle = ref.handle;
	blocker_chunk.grant_generation = ref.grant_generation;
	blocker_chunk.set_generation = blocker_header.set_generation;
	blocker_chunk.blocker.node_id = 8;
	blocker_chunk.blocker.procno = UINT32_C(0x27182818);
	blocker_chunk.blocker.cluster_epoch = UINT64CONST(0x33445566778899aa);
	blocker_chunk.blocker.request_id = UINT64CONST(0x445566778899aabb);
	blocker_chunk.blocker.xid = UINT32_C(0x55667788);
	blocker_chunk.blocker.local_start_ts_ms = INT64CONST(0x1234567890abcdef);
	blocker_chunk.blocker.wait_seq = UINT64CONST(0x66778899aabbccdd);
	MemSet(expected, 0, sizeof(expected));
	put_f076_buffer_tag(expected, 0, &blocker_chunk.tag);
	put_wire_value(expected, F076_BLOCKER_CHUNK_REQUESTER_NODE_OFF,
				   &blocker_chunk.requester_node,
				   sizeof(blocker_chunk.requester_node));
	put_wire_value(expected, F076_BLOCKER_CHUNK_REQUESTER_PROCNO_OFF,
				   &blocker_chunk.requester_procno,
				   sizeof(blocker_chunk.requester_procno));
	put_wire_value(expected, F076_BLOCKER_CHUNK_NO_OFF, &blocker_chunk.chunk_no,
				   sizeof(blocker_chunk.chunk_no));
	put_wire_value(expected, F076_BLOCKER_CHUNK_CLUSTER_EPOCH_OFF,
				   &blocker_chunk.cluster_epoch,
				   sizeof(blocker_chunk.cluster_epoch));
	put_wire_value(expected, F076_BLOCKER_CHUNK_REQUEST_ID_OFF,
				   &blocker_chunk.request_id, sizeof(blocker_chunk.request_id));
	put_f076_ticket_handle(expected, F076_BLOCKER_CHUNK_HANDLE_OFF,
					   &blocker_chunk.handle);
	put_wire_value(expected, F076_BLOCKER_CHUNK_GRANT_GENERATION_OFF,
				   &blocker_chunk.grant_generation,
				   sizeof(blocker_chunk.grant_generation));
	put_wire_value(expected, F076_BLOCKER_CHUNK_SET_GENERATION_OFF,
				   &blocker_chunk.set_generation,
				   sizeof(blocker_chunk.set_generation));
	put_f076_lmd_vertex(expected, F076_BLOCKER_CHUNK_BLOCKER_OFF,
					&blocker_chunk.blocker);
	ASSERT_F076_BYTES(blocker_chunk, expected);

	MemSet(&retire, 0, sizeof(retire));
	retire.cluster_epoch = UINT64CONST(0x1020304050607080);
	retire.master_session_incarnation = UINT64CONST(0x1122334455667788);
	retire.retire_through_ticket_id = UINT64CONST(0x2132435465768798);
	retire.sender_node = 9;
	retire.flags = UINT32_C(0xa1b2c3d4);
	MemSet(expected, 0, sizeof(expected));
	put_wire_value(expected, 0, &retire.cluster_epoch,
				   sizeof(retire.cluster_epoch));
	put_wire_value(expected, F076_RETIRE_MASTER_SESSION_OFF,
				   &retire.master_session_incarnation,
				   sizeof(retire.master_session_incarnation));
	put_wire_value(expected, F076_RETIRE_TICKET_ID_OFF,
				   &retire.retire_through_ticket_id,
				   sizeof(retire.retire_through_ticket_id));
	put_wire_value(expected, F076_RETIRE_SENDER_NODE_OFF, &retire.sender_node,
				   sizeof(retire.sender_node));
	put_wire_value(expected, F076_RETIRE_FLAGS_OFF, &retire.flags,
				   sizeof(retire.flags));
	ASSERT_F076_BYTES(retire, expected);

	MemSet(&drain_poll, 0, sizeof(drain_poll));
	drain_poll.ref = ref;
	drain_poll.drain_generation = UINT64CONST(0xb1c2d3e4f5061728);
	MemSet(expected, 0, sizeof(expected));
	put_f076_ticket_ref(expected, 0, &ref);
	put_wire_value(expected, F076_DRAIN_POLL_GENERATION_OFF,
				   &drain_poll.drain_generation,
				   sizeof(drain_poll.drain_generation));
	ASSERT_F076_BYTES(drain_poll, expected);
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_canonical_layout);
	UT_RUN(test_init_accepts_shared_catalog_identity);
	UT_RUN(test_validation_rejects_invalid_native_tag_fields);
	UT_RUN(test_init_rejects_null_and_invalid_inputs);
	UT_RUN(test_equality_is_exactly_resource_and_requester_node);
	UT_RUN(test_attempt_match_adds_only_base_generation);
	UT_RUN(test_null_comparisons_fail_closed);
	UT_RUN(test_f076_wire_opcode_and_length_manifest);
	UT_RUN(test_f076_wire_layout_offsets_are_exact);
	UT_RUN(test_f076_wire_payload_bytes_are_exact);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
