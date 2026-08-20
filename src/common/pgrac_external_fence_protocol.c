/*-------------------------------------------------------------------------
 *
 * pgrac_external_fence_protocol.c
 *	  Frontend-safe provider-neutral external-fence wire codec.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "common/cryptohash.h"
#include "common/pgrac_external_fence_protocol.h"
#include "port/pg_crc32c.h"

#define PFRQ_CRC_OFFSET 156
#define PFRS_CRC_OFFSET 252
#define PFRJ_CRC_OFFSET 252
#define PGRAC_EXTERNAL_FENCE_MAX_FRESHNESS_NS UINT64_C(5000000000)

static const uint8 protected_set_domain[] = "PGRAC-PROTECTED-SET-V1";
static const uint8 target_state_domain[] = "PGRAC-TARGET-STATE-V1";
static const uint8 binding_domain[] = "PGRAC-FENCE-BINDING-V1";
static const uint8 rejoin_binding_domain[] = "PGRAC-REJOIN-BINDING-V1";

static bool response_semantics_valid(
	const PgracExternalFenceProtocolResponseV1 *response);

StaticAssertDecl(sizeof(protected_set_domain) == 23,
				 "protected-set digest domain changed");
StaticAssertDecl(sizeof(target_state_domain) == 22,
				 "target-state digest domain changed");
StaticAssertDecl(sizeof(binding_domain) == 23,
				 "binding digest domain changed");
StaticAssertDecl(sizeof(rejoin_binding_domain) == 24,
				 "rejoin-binding digest domain changed");

static void
put_u16_le(uint8 *out, uint16 value)
{
	out[0] = (uint8) value;
	out[1] = (uint8) (value >> 8);
}

static void
put_u32_le(uint8 *out, uint32 value)
{
	out[0] = (uint8) value;
	out[1] = (uint8) (value >> 8);
	out[2] = (uint8) (value >> 16);
	out[3] = (uint8) (value >> 24);
}

static void
put_u64_le(uint8 *out, uint64 value)
{
	int i;

	for (i = 0; i < 8; i++)
		out[i] = (uint8) (value >> (i * 8));
}

static uint16
get_u16_le(const uint8 *in)
{
	return ((uint16) in[0]) | ((uint16) in[1] << 8);
}

static uint32
get_u32_le(const uint8 *in)
{
	return ((uint32) in[0]) |
		((uint32) in[1] << 8) |
		((uint32) in[2] << 16) |
		((uint32) in[3] << 24);
}

static uint64
get_u64_le(const uint8 *in)
{
	uint64 value = 0;
	int i;

	for (i = 0; i < 8; i++)
		value |= ((uint64) in[i]) << (i * 8);
	return value;
}

static bool
bytes_all_zero(const uint8 *bytes, size_t len)
{
	size_t i;

	for (i = 0; i < len; i++)
	{
		if (bytes[i] != 0)
			return false;
	}
	return true;
}

bool
pgrac_external_fence_need_v1_valid(
	const PgracExternalFenceProtocolNeedV1 *need)
{
	return need != NULL && need->system_identifier != 0 &&
		!bytes_all_zero(need->canonical_duty_digest,
						sizeof(need->canonical_duty_digest)) &&
		need->victim_node_id >= 0 && need->victim_node_id < 128 &&
		need->victim_incarnation != 0 &&
		!bytes_all_zero(need->protected_set_digest,
						sizeof(need->protected_set_digest)) &&
		need->predicate_id == 1 && need->predicate_version == 1;
}

bool
pgrac_external_fence_binding_from_request_v1(
	const PgracExternalFenceProtocolNeedV1 *need,
	uint64 mapping_generation,
	PgracExternalFenceProtocolBindingV1 *binding)
{
	if (!pgrac_external_fence_need_v1_valid(need) ||
		mapping_generation == 0 || binding == NULL)
		return false;
	memset(binding, 0, sizeof(*binding));
	binding->system_identifier = need->system_identifier;
	memcpy(binding->canonical_duty_digest, need->canonical_duty_digest,
		   sizeof(binding->canonical_duty_digest));
	binding->victim_node_id = need->victim_node_id;
	binding->victim_incarnation = need->victim_incarnation;
	binding->target_mapping_generation = mapping_generation;
	memcpy(binding->protected_set_digest, need->protected_set_digest,
		   sizeof(binding->protected_set_digest));
	binding->predicate_id = need->predicate_id;
	binding->predicate_version = need->predicate_version;
	return true;
}

bool
pgrac_external_fence_affirmative_response_matches_request_v1(
	const PgracExternalFenceProtocolRequestV1 *request,
	const PgracExternalFenceProtocolResponseV1 *response)
{
	const PgracExternalFenceProtocolNeedV1 *need;
	const PgracExternalFenceProtocolBindingV1 *binding;

	if (request == NULL || response == NULL ||
		!pgrac_external_fence_need_v1_valid(&request->need) ||
		bytes_all_zero(request->request_nonce,
					   sizeof(request->request_nonce)) ||
		request->timeout_ms < PGRAC_EXTERNAL_FENCE_TIMEOUT_MIN_MS ||
		request->timeout_ms > PGRAC_EXTERNAL_FENCE_TIMEOUT_MAX_MS ||
		!response_semantics_valid(response) || response->verdict != 1 ||
		memcmp(request->request_nonce, response->request_nonce,
			   sizeof(request->request_nonce)) != 0)
		return false;
	need = &request->need;
	binding = &response->binding;
	return binding->system_identifier == need->system_identifier &&
		memcmp(binding->canonical_duty_digest, need->canonical_duty_digest,
			   sizeof(binding->canonical_duty_digest)) == 0 &&
		binding->victim_node_id == need->victim_node_id &&
		binding->victim_incarnation == need->victim_incarnation &&
		binding->target_mapping_generation != 0 &&
		memcmp(binding->protected_set_digest, need->protected_set_digest,
			   sizeof(binding->protected_set_digest)) == 0 &&
		binding->predicate_id == need->predicate_id &&
		binding->predicate_version == need->predicate_version;
}

static uint32
frame_crc32c(const uint8 *frame, size_t len)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, frame, len);
	FIN_CRC32C(crc);
	return (uint32) crc;
}

static bool
sha256_bytes(const uint8 *bytes, size_t len,
			 uint8 digest[PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES])
{
	pg_cryptohash_ctx *ctx;
	bool ok;

	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL)
		return false;
	ok = pg_cryptohash_init(ctx) >= 0 &&
		pg_cryptohash_update(ctx, bytes, len) >= 0 &&
		pg_cryptohash_final(ctx, digest,
						PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES) >= 0;
	pg_cryptohash_free(ctx);
	if (!ok)
		memset(digest, 0, PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES);
	return ok;
}

static void
encode_binding(const PgracExternalFenceProtocolBindingV1 *binding,
			   uint8 *out)
{
	put_u64_le(out, binding->system_identifier);
	memcpy(out + 8, binding->canonical_duty_digest,
		   PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES);
	put_u32_le(out + 40, (uint32) binding->victim_node_id);
	put_u64_le(out + 48, binding->victim_incarnation);
	put_u64_le(out + 56, binding->target_mapping_generation);
	memcpy(out + 64, binding->protected_set_digest,
		   PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES);
	put_u32_le(out + 96, binding->predicate_id);
	put_u32_le(out + 100, binding->predicate_version);
}

static void
decode_binding(const uint8 *in, PgracExternalFenceProtocolBindingV1 *binding)
{
	binding->system_identifier = get_u64_le(in);
	memcpy(binding->canonical_duty_digest, in + 8,
		   PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES);
	binding->victim_node_id = (int32) get_u32_le(in + 40);
	binding->victim_incarnation = get_u64_le(in + 48);
	binding->target_mapping_generation = get_u64_le(in + 56);
	memcpy(binding->protected_set_digest, in + 64,
		   PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES);
	binding->predicate_id = get_u32_le(in + 96);
	binding->predicate_version = get_u32_le(in + 100);
}

static void
encode_rejoin_binding(
	const PgracExternalFenceProtocolRejoinBindingV1 *binding, uint8 *out)
{
	put_u64_le(out, binding->system_identifier);
	memcpy(out + 8, binding->rejoin_gate_digest,
		   PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES);
	put_u32_le(out + 40, (uint32) binding->old_node_id);
	put_u64_le(out + 48, binding->old_incarnation);
	put_u64_le(out + 56, binding->candidate_incarnation);
	put_u64_le(out + 64, binding->target_mapping_generation);
	memcpy(out + 72, binding->protected_set_digest,
		   PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES);
	put_u32_le(out + 104, binding->predicate_id);
	put_u32_le(out + 108, binding->predicate_version);
}

static bool
response_semantics_valid(const PgracExternalFenceProtocolResponseV1 *response)
{
	bool affirmative;

	if (response->verdict < 1 || response->verdict > 4 ||
		bytes_all_zero(response->request_nonce,
					   sizeof(response->request_nonce)) ||
		response->provider_result > 6 || response->deny_reason > 31)
		return false;

	affirmative = response->verdict == 1;
	if (affirmative)
	{
		if (response->deny_reason != 0 || response->journal_seq == 0 ||
			response->proof_generation == 0 || response->provider_id == 0 ||
			response->provider_abi_version != 1 ||
			response->provider_result != 0 ||
			response->binding.target_mapping_generation == 0 ||
			bytes_all_zero(response->daemon_boot_id,
						   sizeof(response->daemon_boot_id)) ||
			bytes_all_zero(response->target_state_digest,
						   sizeof(response->target_state_digest)) ||
			response->fresh_until_mono_ns <= response->verified_mono_ns ||
			response->fresh_until_mono_ns - response->verified_mono_ns >
			PGRAC_EXTERNAL_FENCE_MAX_FRESHNESS_NS)
			return false;
	}
	else
	{
		PgracExternalFenceProtocolBindingV1 zero_binding;

		memset(&zero_binding, 0, sizeof(zero_binding));
		if (response->deny_reason == 0 ||
			memcmp(&response->binding, &zero_binding,
				   sizeof(zero_binding)) != 0 ||
			response->verified_mono_ns != 0 ||
			response->fresh_until_mono_ns != 0 ||
			response->proof_generation != 0 ||
			!bytes_all_zero(response->target_state_digest,
							sizeof(response->target_state_digest)))
			return false;
	}

	return true;
}

static bool
rejoin_status_valid(const PgracExternalFenceProtocolRejoinFrameV1 *rejoin)
{
	bool negative = rejoin->status >= 5 && rejoin->status <= 8;

	switch (rejoin->opcode)
	{
		case PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE:
		case PGRAC_EXTERNAL_FENCE_REJOIN_LMON_CLAIM_NEXT:
		case PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON:
		case PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON:
		case PGRAC_EXTERNAL_FENCE_REJOIN_LMON_CANCEL:
			return rejoin->status == 0 && rejoin->deny_reason == 0;
		case PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE_RESULT:
		case PGRAC_EXTERNAL_FENCE_REJOIN_LMON_OFFER:
			return (rejoin->status == 1 && rejoin->deny_reason == 0) ||
				(negative && rejoin->deny_reason >= 1 &&
				 rejoin->deny_reason <= 31);
		case PGRAC_EXTERNAL_FENCE_REJOIN_LMON_ON_RESULT:
			return (rejoin->status == 3 && rejoin->deny_reason == 0) ||
				(negative && rejoin->deny_reason >= 1 &&
				 rejoin->deny_reason <= 31);
		case PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_RESULT:
			return (rejoin->status == 4 && rejoin->deny_reason == 0) ||
				(negative && rejoin->deny_reason >= 1 &&
				 rejoin->deny_reason <= 31);
		default:
			return false;
	}
}

static bool
rejoin_identity_zero(const PgracExternalFenceProtocolRejoinFrameV1 *rejoin)
{
	return rejoin->system_identifier == 0 &&
		bytes_all_zero(rejoin->rejoin_gate_digest,
					   sizeof(rejoin->rejoin_gate_digest)) &&
		bytes_all_zero(rejoin->protected_set_digest,
					   sizeof(rejoin->protected_set_digest)) &&
		rejoin->old_node_id == 0 && rejoin->old_incarnation == 0 &&
		rejoin->candidate_incarnation == 0;
}

static bool
rejoin_proof_zero(const PgracExternalFenceProtocolRejoinFrameV1 *rejoin)
{
	return rejoin->provider_id == 0 && rejoin->provider_abi_version == 0 &&
		rejoin->target_mapping_generation == 0 &&
		bytes_all_zero(rejoin->daemon_boot_id,
					   sizeof(rejoin->daemon_boot_id)) &&
		rejoin->journal_seq == 0 && rejoin->verified_mono_ns == 0 &&
		rejoin->fresh_until_mono_ns == 0 && rejoin->proof_generation == 0 &&
		bytes_all_zero(rejoin->target_state_digest,
					   sizeof(rejoin->target_state_digest));
}

static bool
rejoin_positive_proof_valid(
	const PgracExternalFenceProtocolRejoinFrameV1 *rejoin)
{
	return rejoin->system_identifier != 0 &&
		!bytes_all_zero(rejoin->protected_set_digest,
						sizeof(rejoin->protected_set_digest)) &&
		rejoin->old_node_id >= 0 && rejoin->old_node_id < 128 &&
		rejoin->old_incarnation != 0 &&
		rejoin->candidate_incarnation > rejoin->old_incarnation &&
		rejoin->provider_id != 0 && rejoin->provider_id != UINT16_MAX &&
		rejoin->provider_abi_version == 1 &&
		rejoin->target_mapping_generation != 0 &&
		!bytes_all_zero(rejoin->daemon_boot_id,
						sizeof(rejoin->daemon_boot_id)) &&
		rejoin->journal_seq != 0 && rejoin->verified_mono_ns != 0 &&
		rejoin->fresh_until_mono_ns > rejoin->verified_mono_ns &&
		rejoin->fresh_until_mono_ns - rejoin->verified_mono_ns <=
		PGRAC_EXTERNAL_FENCE_MAX_FRESHNESS_NS &&
		rejoin->proof_generation != 0 &&
		!bytes_all_zero(rejoin->target_state_digest,
						sizeof(rejoin->target_state_digest));
}

static bool
rejoin_frame_semantics_valid(
	const PgracExternalFenceProtocolRejoinFrameV1 *rejoin)
{
	bool operation_must_be_zero;
	bool timeout_must_be_set;

	if (rejoin->opcode < PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE ||
		rejoin->opcode > PGRAC_EXTERNAL_FENCE_REJOIN_LMON_CANCEL ||
		bytes_all_zero(rejoin->transport_nonce,
					   sizeof(rejoin->transport_nonce)) ||
		!rejoin_status_valid(rejoin))
		return false;

	operation_must_be_zero =
		rejoin->opcode == PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE ||
		rejoin->opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_CLAIM_NEXT;
	if (operation_must_be_zero !=
		bytes_all_zero(rejoin->operation_id, sizeof(rejoin->operation_id)))
		return false;

	timeout_must_be_set =
		rejoin->opcode == PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE ||
		rejoin->opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON ||
		rejoin->opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON;
	if (timeout_must_be_set)
	{
		if (rejoin->timeout_ms < PGRAC_EXTERNAL_FENCE_TIMEOUT_MIN_MS ||
			rejoin->timeout_ms > PGRAC_EXTERNAL_FENCE_TIMEOUT_MAX_MS)
			return false;
	}
	else if (rejoin->timeout_ms != 0)
		return false;

	switch (rejoin->opcode)
	{
		case PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE:
			return rejoin->system_identifier == 0 &&
				bytes_all_zero(rejoin->rejoin_gate_digest,
					sizeof(rejoin->rejoin_gate_digest)) &&
				bytes_all_zero(rejoin->protected_set_digest,
					sizeof(rejoin->protected_set_digest)) &&
				rejoin_proof_zero(rejoin);
		case PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE_RESULT:
			return rejoin->system_identifier == 0 &&
				bytes_all_zero(rejoin->rejoin_gate_digest,
					sizeof(rejoin->rejoin_gate_digest)) &&
				bytes_all_zero(rejoin->protected_set_digest,
					sizeof(rejoin->protected_set_digest)) &&
				rejoin_proof_zero(rejoin);
		case PGRAC_EXTERNAL_FENCE_REJOIN_LMON_CLAIM_NEXT:
		case PGRAC_EXTERNAL_FENCE_REJOIN_LMON_CANCEL:
			return rejoin_identity_zero(rejoin) &&
				rejoin_proof_zero(rejoin);
		case PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON:
		case PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON:
			return rejoin->system_identifier != 0 &&
				!bytes_all_zero(rejoin->rejoin_gate_digest,
					sizeof(rejoin->rejoin_gate_digest)) &&
				!bytes_all_zero(rejoin->protected_set_digest,
					sizeof(rejoin->protected_set_digest)) &&
				rejoin->old_node_id >= 0 && rejoin->old_node_id < 128 &&
				rejoin->old_incarnation != 0 &&
				rejoin->candidate_incarnation > rejoin->old_incarnation &&
				rejoin_proof_zero(rejoin);
		case PGRAC_EXTERNAL_FENCE_REJOIN_LMON_OFFER:
			if (rejoin->status != 1)
				return bytes_all_zero(rejoin->rejoin_gate_digest,
					sizeof(rejoin->rejoin_gate_digest)) &&
					rejoin_proof_zero(rejoin);
			return bytes_all_zero(rejoin->rejoin_gate_digest,
					sizeof(rejoin->rejoin_gate_digest)) &&
				rejoin_positive_proof_valid(rejoin);
		case PGRAC_EXTERNAL_FENCE_REJOIN_LMON_ON_RESULT:
		case PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_RESULT:
			if (rejoin->status != 3 && rejoin->status != 4)
				return rejoin_proof_zero(rejoin);
			return !bytes_all_zero(rejoin->rejoin_gate_digest,
					sizeof(rejoin->rejoin_gate_digest)) &&
				rejoin_positive_proof_valid(rejoin);
		default:
			return false;
	}

}

bool
pgrac_external_fence_request_v1_encode(
	const PgracExternalFenceProtocolRequestV1 *request,
	uint8 frame[PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES])
{
	if (request == NULL || frame == NULL ||
		bytes_all_zero(request->request_nonce,
					   sizeof(request->request_nonce)) ||
		request->timeout_ms < PGRAC_EXTERNAL_FENCE_TIMEOUT_MIN_MS ||
		request->timeout_ms > PGRAC_EXTERNAL_FENCE_TIMEOUT_MAX_MS)
		return false;

	memset(frame, 0, PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES);
	memcpy(frame, "PFRQ", 4);
	put_u16_le(frame + 4, PGRAC_EXTERNAL_FENCE_PROTOCOL_V1);
	put_u16_le(frame + 6, PGRAC_EXTERNAL_FENCE_MESSAGE_ACQUIRE_V1);
	put_u32_le(frame + 8, PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES);
	memcpy(frame + 16, request->request_nonce,
		   sizeof(request->request_nonce));
	put_u64_le(frame + 32, request->need.system_identifier);
	memcpy(frame + 40, request->need.canonical_duty_digest,
		   PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES);
	put_u32_le(frame + 72, (uint32) request->need.victim_node_id);
	put_u64_le(frame + 80, request->need.victim_incarnation);
	memcpy(frame + 88, request->need.protected_set_digest,
		   PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES);
	put_u32_le(frame + 120, request->need.predicate_id);
	put_u32_le(frame + 124, request->need.predicate_version);
	put_u32_le(frame + 128, request->timeout_ms);
	put_u32_le(frame + PFRQ_CRC_OFFSET,
			   frame_crc32c(frame, PFRQ_CRC_OFFSET));
	return true;
}

bool
pgrac_external_fence_request_v1_decode(
	const uint8 *frame, size_t frame_len,
	PgracExternalFenceProtocolRequestV1 *request)
{
	if (frame == NULL || request == NULL ||
		frame_len != PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES ||
		memcmp(frame, "PFRQ", 4) != 0 ||
		get_u16_le(frame + 4) != PGRAC_EXTERNAL_FENCE_PROTOCOL_V1 ||
		get_u16_le(frame + 6) != PGRAC_EXTERNAL_FENCE_MESSAGE_ACQUIRE_V1 ||
		get_u32_le(frame + 8) != PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES ||
		get_u32_le(frame + 12) != 0 ||
		bytes_all_zero(frame + 16, PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES) ||
		get_u32_le(frame + 76) != 0 ||
		!bytes_all_zero(frame + 132, 24) ||
		get_u32_le(frame + PFRQ_CRC_OFFSET) !=
		frame_crc32c(frame, PFRQ_CRC_OFFSET))
		return false;

	memset(request, 0, sizeof(*request));
	memcpy(request->request_nonce, frame + 16,
		   sizeof(request->request_nonce));
	request->need.system_identifier = get_u64_le(frame + 32);
	memcpy(request->need.canonical_duty_digest, frame + 40,
		   PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES);
	request->need.victim_node_id = (int32) get_u32_le(frame + 72);
	request->need.victim_incarnation = get_u64_le(frame + 80);
	memcpy(request->need.protected_set_digest, frame + 88,
		   PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES);
	request->need.predicate_id = get_u32_le(frame + 120);
	request->need.predicate_version = get_u32_le(frame + 124);
	request->timeout_ms = get_u32_le(frame + 128);
	if (request->timeout_ms < PGRAC_EXTERNAL_FENCE_TIMEOUT_MIN_MS ||
		request->timeout_ms > PGRAC_EXTERNAL_FENCE_TIMEOUT_MAX_MS)
	{
		memset(request, 0, sizeof(*request));
		return false;
	}

	return true;
}

bool
pgrac_external_fence_response_v1_encode(
	const PgracExternalFenceProtocolResponseV1 *response,
	uint8 frame[PGRAC_EXTERNAL_FENCE_RESPONSE_V1_BYTES])
{
	if (response == NULL || frame == NULL ||
		!response_semantics_valid(response))
		return false;

	memset(frame, 0, PGRAC_EXTERNAL_FENCE_RESPONSE_V1_BYTES);
	memcpy(frame, "PFRS", 4);
	put_u16_le(frame + 4, PGRAC_EXTERNAL_FENCE_PROTOCOL_V1);
	put_u16_le(frame + 6,
			   PGRAC_EXTERNAL_FENCE_MESSAGE_ACQUIRE_RESULT_V1);
	put_u32_le(frame + 8, PGRAC_EXTERNAL_FENCE_RESPONSE_V1_BYTES);
	put_u32_le(frame + 12, response->verdict);
	memcpy(frame + 16, response->request_nonce,
		   sizeof(response->request_nonce));
	encode_binding(&response->binding, frame + 32);
	memcpy(frame + 136, response->daemon_boot_id,
		   sizeof(response->daemon_boot_id));
	put_u64_le(frame + 152, response->journal_seq);
	put_u64_le(frame + 160, response->verified_mono_ns);
	put_u64_le(frame + 168, response->fresh_until_mono_ns);
	put_u64_le(frame + 176, response->proof_generation);
	put_u16_le(frame + 184, response->provider_id);
	put_u16_le(frame + 186, response->provider_abi_version);
	put_u32_le(frame + 188, response->provider_result);
	memcpy(frame + 192, response->target_state_digest,
		   sizeof(response->target_state_digest));
	put_u32_le(frame + 224, (uint32) response->provider_native_status);
	put_u32_le(frame + 228, response->deny_reason);
	memcpy(frame + 248, "PFRE", 4);
	put_u32_le(frame + PFRS_CRC_OFFSET,
			   frame_crc32c(frame, PFRS_CRC_OFFSET));
	return true;
}

bool
pgrac_external_fence_response_v1_decode(
	const uint8 *frame, size_t frame_len,
	PgracExternalFenceProtocolResponseV1 *response)
{
	if (frame == NULL || response == NULL ||
		frame_len != PGRAC_EXTERNAL_FENCE_RESPONSE_V1_BYTES ||
		memcmp(frame, "PFRS", 4) != 0 ||
		get_u16_le(frame + 4) != PGRAC_EXTERNAL_FENCE_PROTOCOL_V1 ||
		get_u16_le(frame + 6) !=
		PGRAC_EXTERNAL_FENCE_MESSAGE_ACQUIRE_RESULT_V1 ||
		get_u32_le(frame + 8) != PGRAC_EXTERNAL_FENCE_RESPONSE_V1_BYTES ||
		get_u32_le(frame + 76) != 0 ||
		!bytes_all_zero(frame + 232, 16) ||
		memcmp(frame + 248, "PFRE", 4) != 0 ||
		get_u32_le(frame + PFRS_CRC_OFFSET) !=
		frame_crc32c(frame, PFRS_CRC_OFFSET))
		return false;

	memset(response, 0, sizeof(*response));
	response->verdict = get_u32_le(frame + 12);
	memcpy(response->request_nonce, frame + 16,
		   sizeof(response->request_nonce));
	decode_binding(frame + 32, &response->binding);
	memcpy(response->daemon_boot_id, frame + 136,
		   sizeof(response->daemon_boot_id));
	response->journal_seq = get_u64_le(frame + 152);
	response->verified_mono_ns = get_u64_le(frame + 160);
	response->fresh_until_mono_ns = get_u64_le(frame + 168);
	response->proof_generation = get_u64_le(frame + 176);
	response->provider_id = get_u16_le(frame + 184);
	response->provider_abi_version = get_u16_le(frame + 186);
	response->provider_result = get_u32_le(frame + 188);
	memcpy(response->target_state_digest, frame + 192,
		   sizeof(response->target_state_digest));
	response->provider_native_status = (int32) get_u32_le(frame + 224);
	response->deny_reason = get_u32_le(frame + 228);
	if (!response_semantics_valid(response))
	{
		memset(response, 0, sizeof(*response));
		return false;
	}

	return true;
}

bool
pgrac_external_fence_rejoin_v1_encode(
	const PgracExternalFenceProtocolRejoinFrameV1 *rejoin,
	uint8 frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES])
{
	if (rejoin == NULL || frame == NULL ||
		!rejoin_frame_semantics_valid(rejoin))
		return false;

	memset(frame, 0, PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES);
	memcpy(frame, "PFRJ", 4);
	put_u16_le(frame + 4, PGRAC_EXTERNAL_FENCE_PROTOCOL_V1);
	put_u16_le(frame + 6, rejoin->opcode);
	put_u32_le(frame + 8, PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES);
	memcpy(frame + 16, rejoin->transport_nonce,
		   sizeof(rejoin->transport_nonce));
	memcpy(frame + 32, rejoin->operation_id,
		   sizeof(rejoin->operation_id));
	put_u64_le(frame + 48, rejoin->system_identifier);
	memcpy(frame + 56, rejoin->rejoin_gate_digest,
		   sizeof(rejoin->rejoin_gate_digest));
	memcpy(frame + 88, rejoin->protected_set_digest,
		   sizeof(rejoin->protected_set_digest));
	put_u32_le(frame + 120, (uint32) rejoin->old_node_id);
	put_u64_le(frame + 128, rejoin->old_incarnation);
	put_u64_le(frame + 136, rejoin->candidate_incarnation);
	put_u32_le(frame + 144, rejoin->timeout_ms);
	put_u16_le(frame + 148, rejoin->provider_id);
	put_u16_le(frame + 150, rejoin->provider_abi_version);
	put_u64_le(frame + 152, rejoin->target_mapping_generation);
	memcpy(frame + 160, rejoin->daemon_boot_id,
		   sizeof(rejoin->daemon_boot_id));
	put_u64_le(frame + 176, rejoin->journal_seq);
	put_u64_le(frame + 184, rejoin->verified_mono_ns);
	put_u64_le(frame + 192, rejoin->fresh_until_mono_ns);
	put_u64_le(frame + 200, rejoin->proof_generation);
	memcpy(frame + 208, rejoin->target_state_digest,
		   sizeof(rejoin->target_state_digest));
	put_u32_le(frame + 240, rejoin->status);
	put_u32_le(frame + 244, rejoin->deny_reason);
	memcpy(frame + 248, "PFRZ", 4);
	put_u32_le(frame + PFRJ_CRC_OFFSET,
			   frame_crc32c(frame, PFRJ_CRC_OFFSET));
	return true;
}

bool
pgrac_external_fence_rejoin_v1_decode(
	const uint8 *frame, size_t frame_len,
	PgracExternalFenceProtocolRejoinFrameV1 *rejoin)
{
	if (frame == NULL || rejoin == NULL ||
		frame_len != PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES ||
		memcmp(frame, "PFRJ", 4) != 0 ||
		get_u16_le(frame + 4) != PGRAC_EXTERNAL_FENCE_PROTOCOL_V1 ||
		get_u32_le(frame + 8) != PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES ||
		get_u32_le(frame + 12) != 0 || get_u32_le(frame + 124) != 0 ||
		memcmp(frame + 248, "PFRZ", 4) != 0 ||
		get_u32_le(frame + PFRJ_CRC_OFFSET) !=
		frame_crc32c(frame, PFRJ_CRC_OFFSET))
		return false;

	memset(rejoin, 0, sizeof(*rejoin));
	rejoin->opcode = get_u16_le(frame + 6);
	memcpy(rejoin->transport_nonce, frame + 16,
		   sizeof(rejoin->transport_nonce));
	memcpy(rejoin->operation_id, frame + 32,
		   sizeof(rejoin->operation_id));
	rejoin->system_identifier = get_u64_le(frame + 48);
	memcpy(rejoin->rejoin_gate_digest, frame + 56,
		   sizeof(rejoin->rejoin_gate_digest));
	memcpy(rejoin->protected_set_digest, frame + 88,
		   sizeof(rejoin->protected_set_digest));
	rejoin->old_node_id = (int32) get_u32_le(frame + 120);
	rejoin->old_incarnation = get_u64_le(frame + 128);
	rejoin->candidate_incarnation = get_u64_le(frame + 136);
	rejoin->timeout_ms = get_u32_le(frame + 144);
	rejoin->provider_id = get_u16_le(frame + 148);
	rejoin->provider_abi_version = get_u16_le(frame + 150);
	rejoin->target_mapping_generation = get_u64_le(frame + 152);
	memcpy(rejoin->daemon_boot_id, frame + 160,
		   sizeof(rejoin->daemon_boot_id));
	rejoin->journal_seq = get_u64_le(frame + 176);
	rejoin->verified_mono_ns = get_u64_le(frame + 184);
	rejoin->fresh_until_mono_ns = get_u64_le(frame + 192);
	rejoin->proof_generation = get_u64_le(frame + 200);
	memcpy(rejoin->target_state_digest, frame + 208,
		   sizeof(rejoin->target_state_digest));
	rejoin->status = get_u32_le(frame + 240);
	rejoin->deny_reason = get_u32_le(frame + 244);
	if (!rejoin_frame_semantics_valid(rejoin))
	{
		memset(rejoin, 0, sizeof(*rejoin));
		return false;
	}

	return true;
}

bool
pgrac_external_fence_protected_set_digest_v1(
	uint32 backend_id,
	const uint8 storage_uuid[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES],
	uint8 digest[PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES])
{
	uint8 preimage[45];

	if (storage_uuid == NULL || digest == NULL ||
		(backend_id != 2 && backend_id != 3) ||
		bytes_all_zero(storage_uuid, PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES))
		return false;
	memcpy(preimage, protected_set_domain, sizeof(protected_set_domain));
	put_u32_le(preimage + 23, backend_id);
	put_u16_le(preimage + 27, PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES);
	memcpy(preimage + 29, storage_uuid,
		   PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES);
	return sha256_bytes(preimage, sizeof(preimage), digest);
}

bool
pgrac_external_fence_target_state_digest_v1(
	const uint8 target_uuid[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES],
	uint32 target_state, uint32 io_drain_state,
	uint64 mapping_generation, uint64 proof_generation,
	uint8 digest[PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES])
{
	uint8 preimage[62];

	if (target_uuid == NULL || digest == NULL ||
		(target_state != 1 && target_state != 2) || io_drain_state != 1 ||
		mapping_generation == 0 || proof_generation == 0 ||
		bytes_all_zero(target_uuid, PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES))
		return false;
	memcpy(preimage, target_state_domain, sizeof(target_state_domain));
	memcpy(preimage + 22, target_uuid,
		   PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES);
	put_u32_le(preimage + 38, target_state);
	put_u32_le(preimage + 42, io_drain_state);
	put_u64_le(preimage + 46, mapping_generation);
	put_u64_le(preimage + 54, proof_generation);
	return sha256_bytes(preimage, sizeof(preimage), digest);
}

bool
pgrac_external_fence_binding_digest_v1(
	const PgracExternalFenceProtocolBindingV1 *binding,
	uint8 digest[PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES])
{
	uint8 preimage[127];

	if (binding == NULL || digest == NULL)
		return false;
	memcpy(preimage, binding_domain, sizeof(binding_domain));
	memset(preimage + 23, 0, 104);
	encode_binding(binding, preimage + 23);
	return sha256_bytes(preimage, sizeof(preimage), digest);
}

bool
pgrac_external_fence_rejoin_binding_digest_v1(
	const PgracExternalFenceProtocolRejoinBindingV1 *binding,
	uint8 digest[PGRAC_EXTERNAL_FENCE_PROTOCOL_DIGEST_BYTES])
{
	uint8 preimage[136];

	if (binding == NULL || digest == NULL)
		return false;
	memcpy(preimage, rejoin_binding_domain, sizeof(rejoin_binding_domain));
	memset(preimage + 24, 0, 112);
	encode_rejoin_binding(binding, preimage + 24);
	return sha256_bytes(preimage, sizeof(preimage), digest);
}
