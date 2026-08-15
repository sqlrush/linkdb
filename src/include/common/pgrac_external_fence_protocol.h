/*-------------------------------------------------------------------------
 *
 * pgrac_external_fence_protocol.h
 *	  Frontend-safe provider-neutral external-fence wire codec.
 *
 * These are semantic values, not wire structs.  Callers must use the manual
 * codec; no value declared here may be cast to or written as a wire frame.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_EXTERNAL_FENCE_PROTOCOL_H
#define PGRAC_EXTERNAL_FENCE_PROTOCOL_H

#include "c.h"

#define PGRAC_EXTERNAL_FENCE_PROTOCOL_V1 UINT16_C(1)
#define PGRAC_EXTERNAL_FENCE_MESSAGE_ACQUIRE_V1 UINT16_C(1)
#define PGRAC_EXTERNAL_FENCE_MESSAGE_ACQUIRE_RESULT_V1 UINT16_C(2)
#define PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES 160
#define PGRAC_EXTERNAL_FENCE_RESPONSE_V1_BYTES 256
#define PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES 256
#define PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES 16
#define PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES 32
#define PGRAC_EXTERNAL_FENCE_TIMEOUT_MIN_MS UINT32_C(1)
#define PGRAC_EXTERNAL_FENCE_TIMEOUT_MAX_MS UINT32_C(600000)

#define PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE UINT16_C(1)
#define PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE_RESULT UINT16_C(2)
#define PGRAC_EXTERNAL_FENCE_REJOIN_LMON_CLAIM_NEXT UINT16_C(3)
#define PGRAC_EXTERNAL_FENCE_REJOIN_LMON_OFFER UINT16_C(4)
#define PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON UINT16_C(5)
#define PGRAC_EXTERNAL_FENCE_REJOIN_LMON_ON_RESULT UINT16_C(6)
#define PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON UINT16_C(7)
#define PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_RESULT UINT16_C(8)
#define PGRAC_EXTERNAL_FENCE_REJOIN_LMON_CANCEL UINT16_C(9)

typedef struct PgracExternalFenceProtocolNeedV1
{
	uint64 system_identifier;
	uint8 canonical_duty_digest[PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES];
	int32 victim_node_id;
	uint64 victim_incarnation;
	uint8 protected_set_digest[PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES];
	uint32 predicate_id;
	uint32 predicate_version;
} PgracExternalFenceProtocolNeedV1;

typedef struct PgracExternalFenceProtocolRequestV1
{
	uint8 request_nonce[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES];
	PgracExternalFenceProtocolNeedV1 need;
	uint32 timeout_ms;
} PgracExternalFenceProtocolRequestV1;

typedef struct PgracExternalFenceProtocolBindingV1
{
	uint64 system_identifier;
	uint8 canonical_duty_digest[PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES];
	int32 victim_node_id;
	uint64 victim_incarnation;
	uint64 target_mapping_generation;
	uint8 protected_set_digest[PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES];
	uint32 predicate_id;
	uint32 predicate_version;
} PgracExternalFenceProtocolBindingV1;

typedef struct PgracExternalFenceProtocolRejoinBindingV1
{
	uint64 system_identifier;
	uint8 rejoin_gate_digest[PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES];
	int32 old_node_id;
	uint64 old_incarnation;
	uint64 candidate_incarnation;
	uint64 target_mapping_generation;
	uint8 protected_set_digest[PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES];
	uint32 predicate_id;
	uint32 predicate_version;
} PgracExternalFenceProtocolRejoinBindingV1;

typedef struct PgracExternalFenceProtocolResponseV1
{
	uint32 verdict;
	uint8 request_nonce[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES];
	PgracExternalFenceProtocolBindingV1 binding;
	uint8 daemon_boot_id[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES];
	uint64 journal_seq;
	uint64 verified_mono_ns;
	uint64 fresh_until_mono_ns;
	uint64 proof_generation;
	uint16 provider_id;
	uint16 provider_abi_version;
	uint32 provider_result;
	uint8 target_state_digest[PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES];
	int32 provider_native_status;
	uint32 deny_reason;
} PgracExternalFenceProtocolResponseV1;

typedef struct PgracExternalFenceProtocolRejoinFrameV1
{
	uint16 opcode;
	uint8 transport_nonce[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES];
	uint8 operation_id[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES];
	uint64 system_identifier;
	uint8 rejoin_gate_digest[PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES];
	uint8 protected_set_digest[PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES];
	int32 old_node_id;
	uint64 old_incarnation;
	uint64 candidate_incarnation;
	uint32 timeout_ms;
	uint16 provider_id;
	uint16 provider_abi_version;
	uint64 target_mapping_generation;
	uint8 daemon_boot_id[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES];
	uint64 journal_seq;
	uint64 verified_mono_ns;
	uint64 fresh_until_mono_ns;
	uint64 proof_generation;
	uint8 target_state_digest[PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES];
	uint32 status;
	uint32 deny_reason;
} PgracExternalFenceProtocolRejoinFrameV1;

extern bool pgrac_external_fence_request_v1_encode(
	const PgracExternalFenceProtocolRequestV1 *request,
	uint8 frame[PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES]);
extern bool pgrac_external_fence_request_v1_decode(
	const uint8 *frame, size_t frame_len,
	PgracExternalFenceProtocolRequestV1 *request);
extern bool pgrac_external_fence_response_v1_encode(
	const PgracExternalFenceProtocolResponseV1 *response,
	uint8 frame[PGRAC_EXTERNAL_FENCE_RESPONSE_V1_BYTES]);
extern bool pgrac_external_fence_response_v1_decode(
	const uint8 *frame, size_t frame_len,
	PgracExternalFenceProtocolResponseV1 *response);
extern bool pgrac_external_fence_rejoin_v1_encode(
	const PgracExternalFenceProtocolRejoinFrameV1 *rejoin,
	uint8 frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES]);
extern bool pgrac_external_fence_rejoin_v1_decode(
	const uint8 *frame, size_t frame_len,
	PgracExternalFenceProtocolRejoinFrameV1 *rejoin);
extern bool pgrac_external_fence_need_v1_valid(
	const PgracExternalFenceProtocolNeedV1 *need);
extern bool pgrac_external_fence_binding_from_request_v1(
	const PgracExternalFenceProtocolNeedV1 *need,
	uint64 mapping_generation,
	PgracExternalFenceProtocolBindingV1 *binding);
extern bool pgrac_external_fence_affirmative_response_matches_request_v1(
	const PgracExternalFenceProtocolRequestV1 *request,
	const PgracExternalFenceProtocolResponseV1 *response);
extern bool pgrac_external_fence_protected_set_digest_v1(
	uint32 backend_id,
	const uint8 storage_uuid[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES],
	uint8 digest[PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES]);
extern bool pgrac_external_fence_target_state_digest_v1(
	const uint8 target_uuid[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES],
	uint32 target_state, uint32 io_drain_state,
	uint64 mapping_generation, uint64 proof_generation,
	uint8 digest[PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES]);
extern bool pgrac_external_fence_binding_digest_v1(
	const PgracExternalFenceProtocolBindingV1 *binding,
	uint8 digest[PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES]);
extern bool pgrac_external_fence_rejoin_binding_digest_v1(
	const PgracExternalFenceProtocolRejoinBindingV1 *binding,
	uint8 digest[PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES]);

#endif /* PGRAC_EXTERNAL_FENCE_PROTOCOL_H */
