/*-------------------------------------------------------------------------
 *
 * test_cluster_rf_route.c
 *    Stage 8 JIT Task 4 exhaustive redo-route tests.
 *
 *    The immutable RED build has no future route header or object.  It runs
 *    through the existing common record/apply decision and names the route
 *    information that boundary cannot represent.  Once the production
 *    route API exists, the same unit switches to its real declarations and
 *    exercises the generated authority directly; no substitute route table
 *    or test-owned parser is provided here.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_rf_route.c
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "cluster/cluster_block_apply.h"

#if defined(__has_include)
#if __has_include("cluster/cluster_rf_route.h")
#define HAVE_CLUSTER_RF_ROUTE 1
#include "cluster/cluster_rf_route.h"
#endif
#endif

#include "unit_test.h"

#ifdef HAVE_CLUSTER_RF_ROUTE
#include "access/rmgr.h"
#include "access/xact.h"
#include "access/xlogrecord.h"
#include "common/cryptohash.h"
#include "common/relpath.h"
#include "common/sha2.h"
#endif

UT_DEFINE_GLOBALS();


void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# unexpected Assert: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}


UT_TEST(test_existing_apply_boundary_control)
{
	UT_ASSERT_EQ((int)cluster_block_apply_decide(true, true, true), (int)CLUSTER_BLKAPPLY_ACT_FPI);
}

#ifndef HAVE_CLUSTER_RF_ROUTE

UT_TEST(test_red_page_zero_block_must_not_be_noop)
{
	printf("# JIT_SEMANTIC_RED:T4-PAGE-ZERO-BLOCK\n");
	UT_ASSERT_NE((int)cluster_block_apply_decide(false, false, false),
				 (int)CLUSTER_BLKAPPLY_ACT_NOOP);
}

UT_TEST(test_red_adg_block_must_not_enter_delta_apply)
{
	printf("# JIT_SEMANTIC_RED:T4-ADG-BLOCK-NO-ORDINARY\n");
	UT_ASSERT_NE((int)cluster_block_apply_decide(true, false, false),
				 (int)CLUSTER_BLKAPPLY_ACT_DELTA);
}

UT_TEST(test_red_xid_stripe_requires_typed_route)
{
	printf("# JIT_SEMANTIC_RED:T4-XID-STRIPE-TYPED-OWNER\n");
	UT_ASSERT_NE((int)cluster_block_apply_decide(false, false, false),
				 (int)CLUSTER_BLKAPPLY_ACT_NOOP);
}

#endif

#ifdef HAVE_CLUSTER_RF_ROUTE

#define T4_ROUTE_KEY_STREAM_LEN 959
#define T4_ROUTE_SHA256 "06e7a83faedc112989f226ba30e3e6c9421b22e93014a5893ec9e0b984da0636"

enum {
	T4_PAGE_CLASS_INVALID = 0,
	T4_PAGE_CLASS_ORDINARY = 1,
	T4_PAGE_CLASS_REBUILDABLE_FSM = 2,
	T4_PAGE_CLASS_ROUTED_SPACE = 3,
	T4_PAGE_CLASS_ROUTED_HEADER = 4,
	T4_PAGE_CLASS_ROUTED_SIDE = 5,
	T4_PAGE_CLASS_TEMP_LOCAL = 6
};

static RfOpcodeRouteV1
t4_route_sentinel(void)
{
	RfOpcodeRouteV1 route;

	memset(&route, 0xA5, sizeof(route));
	return route;
}

static RfRouteComponentV1
t4_component(uint8 page_class, uint8 forknum, uint8 owner_count, uint8 declared_owner)
{
	RfRouteComponentV1 component;

	component.page_class = page_class;
	component.forknum = forknum;
	component.owner_count = owner_count;
	component.declared_owner = declared_owner;
	return component;
}

static void
t4_assert_lookup_failure_untouched(uint16 rmid, uint8 raw_info, uint8 block_count,
								   RfOpcodeRouteLookupResultV1 expected)
{
	RfOpcodeRouteV1 route = t4_route_sentinel();
	RfOpcodeRouteV1 before = route;

	UT_ASSERT_EQ((int)rf_opcode_route_lookup_v1(rmid, raw_info, block_count, &route),
				 (int)expected);
	UT_ASSERT_EQ(memcmp(&route, &before, sizeof(route)), 0);
}

static bool
t4_manifest_find(uint8 rmid, uint8 info, RfOpcodeRouteV1 *route, bool *active, const char **name)
{
	uint16 i;

	for (i = 0; i < rf_opcode_route_manifest_count_v1(); i++) {
		RfOpcodeRouteV1 candidate;
		bool candidate_active;
		const char *candidate_name;

		if (!rf_opcode_route_manifest_at_v1(i, &candidate, &candidate_active, &candidate_name))
			return false;
		if (candidate.rmid == rmid && candidate.normalized_info == info) {
			*route = candidate;
			*active = candidate_active;
			*name = candidate_name;
			return true;
		}
	}
	return false;
}

static bool
t4_sha256_hex(const uint8 *input, size_t len, char output[PG_SHA256_DIGEST_STRING_LENGTH])
{
	static const char hex[] = "0123456789abcdef";
	pg_cryptohash_ctx *ctx;
	uint8 digest[PG_SHA256_DIGEST_LENGTH];
	int i;

	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL)
		return false;
	if (pg_cryptohash_init(ctx) < 0 || pg_cryptohash_update(ctx, input, len) < 0
		|| pg_cryptohash_final(ctx, digest, sizeof(digest)) < 0) {
		pg_cryptohash_free(ctx);
		return false;
	}
	pg_cryptohash_free(ctx);

	for (i = 0; i < PG_SHA256_DIGEST_LENGTH; i++) {
		output[i * 2] = hex[digest[i] >> 4];
		output[i * 2 + 1] = hex[digest[i] & 0x0F];
	}
	output[PG_SHA256_DIGEST_LENGTH * 2] = '\0';
	return true;
}

UT_TEST(test_route_abi_and_counts)
{
	UT_ASSERT_EQ(sizeof(RfOpcodeRouteV1), 8);
	UT_ASSERT_EQ(offsetof(RfOpcodeRouteV1, reserved_zero), 6);
	UT_ASSERT_EQ(rf_opcode_route_manifest_count_v1(), 137);
	UT_ASSERT_EQ(rf_opcode_route_live_count_v1(), 136);
	UT_ASSERT(rf_opcode_route_manifest_collision_free_v1());
}

UT_TEST(test_canonical_manifest_stream)
{
	char stream[T4_ROUTE_KEY_STREAM_LEN + 1];
	char digest[PG_SHA256_DIGEST_STRING_LENGTH];
	size_t used = 0;
	uint16 previous_key = 0;
	uint16 i;

	for (i = 0; i < rf_opcode_route_manifest_count_v1(); i++) {
		RfOpcodeRouteV1 route;
		bool active;
		const char *name;
		uint16 key;
		int written;

		UT_ASSERT(rf_opcode_route_manifest_at_v1(i, &route, &active, &name));
		UT_ASSERT_NOT_NULL(name);
		UT_ASSERT_EQ(route.reserved_zero, 0);
		key = ((uint16)route.rmid << 8) | route.normalized_info;
		if (i > 0)
			UT_ASSERT(key > previous_key);
		previous_key = key;
		written = snprintf(stream + used, sizeof(stream) - used, "%03u:%02X\n",
						   (unsigned int)route.rmid, (unsigned int)route.normalized_info);
		UT_ASSERT_EQ(written, 7);
		if (written == 7)
			used += 7;
	}

	UT_ASSERT_EQ(used, T4_ROUTE_KEY_STREAM_LEN);
	UT_ASSERT(t4_sha256_hex((const uint8 *)stream, used, digest));
	UT_ASSERT_STR_EQ(digest, T4_ROUTE_SHA256);
}

UT_TEST(test_every_manifest_row_lookup)
{
	uint16 live_count = 0;
	uint16 inactive_count = 0;
	uint16 i;

	for (i = 0; i < rf_opcode_route_manifest_count_v1(); i++) {
		RfOpcodeRouteV1 expected;
		RfOpcodeRouteV1 actual = t4_route_sentinel();
		bool active;
		const char *name;
		uint8 block_count;
		uint8 raw_info;

		UT_ASSERT(rf_opcode_route_manifest_at_v1(i, &expected, &active, &name));
		block_count = expected.block_policy == RF_ROUTE_BLOCKS_REQUIRED ? 1 : 0;
		raw_info = expected.normalized_info | UINT8_C(0x05);
		if (active) {
			UT_ASSERT_EQ(
				(int)rf_opcode_route_lookup_v1(expected.rmid, raw_info, block_count, &actual),
				(int)RF_OPCODE_ROUTE_OK);
			UT_ASSERT_EQ(memcmp(&actual, &expected, sizeof(actual)), 0);
			live_count++;
		} else {
			RfOpcodeRouteV1 before = actual;

			UT_ASSERT_EQ(
				(int)rf_opcode_route_lookup_v1(expected.rmid, raw_info, block_count, &actual),
				(int)RF_OPCODE_ROUTE_OPCODE_UNSUPPORTED);
			UT_ASSERT_EQ(memcmp(&actual, &before, sizeof(actual)), 0);
			inactive_count++;
		}
	}
	UT_ASSERT_EQ(live_count, 136);
	UT_ASSERT_EQ(inactive_count, 1);
}

UT_TEST(test_every_unused_nibble_fails_closed)
{
	bool assigned[26][16] = { { false } };
	bool active[26][16] = { { false } };
	uint16 i;
	uint8 rmid;
	uint8 nibble;

	for (i = 0; i < rf_opcode_route_manifest_count_v1(); i++) {
		RfOpcodeRouteV1 route;
		bool row_active;
		const char *name;

		UT_ASSERT(rf_opcode_route_manifest_at_v1(i, &route, &row_active, &name));
		assigned[route.rmid][route.normalized_info >> 4] = true;
		active[route.rmid][route.normalized_info >> 4] = row_active;
	}

	for (rmid = 0; rmid < 26; rmid++) {
		for (nibble = 0; nibble < 16; nibble++) {
			uint8 raw_info = (uint8)((nibble << 4) | UINT8_C(0x0F));

			if (rmid == RM_XACT_ID) {
				uint8 base = nibble & UINT8_C(0x07);
				bool has_info = (nibble & UINT8_C(0x08)) != 0;

				if (!assigned[rmid][base])
					t4_assert_lookup_failure_untouched(rmid, raw_info, 0,
													   RF_OPCODE_ROUTE_OPCODE_UNSUPPORTED);
				else if (has_info && base != 0 && base != 2 && base != 3 && base != 4)
					t4_assert_lookup_failure_untouched(rmid, raw_info, 0,
													   RF_OPCODE_ROUTE_FLAG_ILLEGAL);
				else {
					RfOpcodeRouteV1 route;

					UT_ASSERT_EQ((int)rf_opcode_route_lookup_v1(rmid, raw_info, 0, &route),
								 (int)RF_OPCODE_ROUTE_OK);
				}
			} else if (!assigned[rmid][nibble] || !active[rmid][nibble])
				t4_assert_lookup_failure_untouched(rmid, raw_info, active[rmid][nibble] ? 0 : 1,
												   RF_OPCODE_ROUTE_OPCODE_UNSUPPORTED);
		}
	}
}

UT_TEST(test_xact_flags_and_init_combinations)
{
	uint8 low_bits;

	for (low_bits = 0; low_bits <= XLR_INFO_MASK; low_bits++) {
		RfOpcodeRouteV1 route;

		UT_ASSERT_EQ((int)rf_opcode_route_lookup_v1(
						 RM_XACT_ID, XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO | low_bits, 0, &route),
					 (int)RF_OPCODE_ROUTE_OK);
		t4_assert_lookup_failure_untouched(RM_XACT_ID,
										   XLOG_XACT_PREPARE | XLOG_XACT_HAS_INFO | low_bits, 0,
										   RF_OPCODE_ROUTE_FLAG_ILLEGAL);
	}

	{
		RfOpcodeRouteV1 route;

		UT_ASSERT_EQ((int)rf_opcode_route_lookup_v1(RM_HEAP_ID, 0x80, 1, &route),
					 (int)RF_OPCODE_ROUTE_OK);
		UT_ASSERT_EQ((int)rf_opcode_route_lookup_v1(RM_HEAP_ID, 0xA0, 1, &route),
					 (int)RF_OPCODE_ROUTE_OK);
		UT_ASSERT_EQ((int)rf_opcode_route_lookup_v1(RM_HEAP_ID, 0xC0, 1, &route),
					 (int)RF_OPCODE_ROUTE_OK);
		UT_ASSERT_EQ((int)rf_opcode_route_lookup_v1(RM_BRIN_ID, 0x90, 1, &route),
					 (int)RF_OPCODE_ROUTE_OK);
		UT_ASSERT_EQ((int)rf_opcode_route_lookup_v1(RM_BRIN_ID, 0xA0, 1, &route),
					 (int)RF_OPCODE_ROUTE_OK);
	}
	t4_assert_lookup_failure_untouched(RM_HEAP_ID, 0x90, 1, RF_OPCODE_ROUTE_OPCODE_UNSUPPORTED);
	t4_assert_lookup_failure_untouched(RM_HEAP_ID, 0xB0, 1, RF_OPCODE_ROUTE_OPCODE_UNSUPPORTED);
	t4_assert_lookup_failure_untouched(RM_BRIN_ID, 0x80, 1, RF_OPCODE_ROUTE_OPCODE_UNSUPPORTED);
}

UT_TEST(test_gin_nested_values_fail_closed)
{
	uint8 action;

	for (action = 1; action <= 4; action++)
		UT_ASSERT_EQ((int)rf_opcode_route_validate_gin_segment_action_v1(action),
					 (int)RF_OPCODE_ROUTE_OK);
	UT_ASSERT_EQ((int)rf_opcode_route_validate_gin_segment_action_v1(0),
				 (int)RF_OPCODE_ROUTE_NESTED_VALUE_INVALID);
	UT_ASSERT_EQ((int)rf_opcode_route_validate_gin_segment_action_v1(5),
				 (int)RF_OPCODE_ROUTE_NESTED_VALUE_INVALID);
	UT_ASSERT_EQ((int)rf_opcode_route_validate_gin_segment_action_v1(255),
				 (int)RF_OPCODE_ROUTE_NESTED_VALUE_INVALID);
}

UT_TEST(test_unknown_rmgr_and_block_shapes_fail_closed)
{
	uint16 rmid;

	for (rmid = 26; rmid <= 255; rmid++)
		t4_assert_lookup_failure_untouched(rmid, 0, 0, RF_OPCODE_ROUTE_RMID_UNSUPPORTED);
	t4_assert_lookup_failure_untouched(256, 0, 0, RF_OPCODE_ROUTE_RMID_UNSUPPORTED);
	t4_assert_lookup_failure_untouched(UINT16_MAX, 0, 0, RF_OPCODE_ROUTE_RMID_UNSUPPORTED);
	t4_assert_lookup_failure_untouched(RM_HEAP_ID, 0x00, 0, RF_OPCODE_ROUTE_BLOCK_SHAPE_INVALID);
	t4_assert_lookup_failure_untouched(RM_CLUSTER_ADG_ID, 0x10, 1,
									   RF_OPCODE_ROUTE_BLOCK_SHAPE_INVALID);
	t4_assert_lookup_failure_untouched(RM_CLUSTER_XID_STRIPE_ID, 0x00, 1,
									   RF_OPCODE_ROUTE_BLOCK_SHAPE_INVALID);
}

UT_TEST(test_typed_no_ordinary_routes)
{
	RfOpcodeRouteV1 route;
	bool active;
	const char *name;

	UT_ASSERT(t4_manifest_find(RM_CLUSTER_ADG_ID, 0x10, &route, &active, &name));
	UT_ASSERT(active);
	UT_ASSERT_EQ(route.record_owner, RF_ROUTE_OWNER_SIDE_TYPED);
	UT_ASSERT_EQ(route.block_policy, RF_ROUTE_BLOCKS_FORBIDDEN);
	UT_ASSERT_EQ(route.codec_id, RF_ROUTE_CODEC_SIDE_ADG_BARRIER);
	UT_ASSERT_STR_CONTAINS(name, "THREAD_BARRIER/NO_ORDINARY");

	UT_ASSERT(t4_manifest_find(RM_CLUSTER_XID_STRIPE_ID, 0x00, &route, &active, &name));
	UT_ASSERT(active);
	UT_ASSERT_EQ(route.record_owner, RF_ROUTE_OWNER_SIDE_TYPED);
	UT_ASSERT_EQ(route.block_policy, RF_ROUTE_BLOCKS_FORBIDDEN);
	UT_ASSERT_EQ(route.codec_id, RF_ROUTE_CODEC_SIDE_XID_STRIPE_SHMEM);
	UT_ASSERT_STR_CONTAINS(name, "JOIN/NO_ORDINARY");

	UT_ASSERT(t4_manifest_find(RM_CLUSTER_XID_STRIPE_ID, 0x10, &route, &active, &name));
	UT_ASSERT(active);
	UT_ASSERT_EQ(route.record_owner, RF_ROUTE_OWNER_SIDE_TYPED);
	UT_ASSERT_EQ(route.block_policy, RF_ROUTE_BLOCKS_FORBIDDEN);
	UT_ASSERT_EQ(route.codec_id, RF_ROUTE_CODEC_SIDE_XID_STRIPE_SHMEM);
	UT_ASSERT_STR_CONTAINS(name, "RETIRE/NO_ORDINARY");
}

UT_TEST(test_stop07_row_present_but_inactive)
{
	RfOpcodeRouteV1 route;
	bool active;
	const char *name;

	UT_ASSERT(t4_manifest_find(RM_CLUSTER_UNDO_ID, 0xA0, &route, &active, &name));
	UT_ASSERT(!active);
	UT_ASSERT_EQ(route.record_owner, RF_ROUTE_OWNER_SIDE_TYPED);
	UT_ASSERT_EQ(route.block_policy, RF_ROUTE_BLOCKS_REQUIRED);
	UT_ASSERT_EQ(route.codec_id, RF_ROUTE_CODEC_SIDE_CLUSTER_UNDO);
	UT_ASSERT_STR_CONTAINS(name, "INACTIVE_STOP07");
	t4_assert_lookup_failure_untouched(RM_CLUSTER_UNDO_ID, 0xA0, 1,
									   RF_OPCODE_ROUTE_OPCODE_UNSUPPORTED);
}

UT_TEST(test_component_classes_and_unique_owners)
{
	RfOpcodeRouteV1 route;
	RfRouteComponentV1 components[2];
	RfRouteComponentV1 invalid;

	UT_ASSERT_EQ((int)rf_opcode_route_lookup_v1(RM_HEAP_ID, 0x00, 1, &route),
				 (int)RF_OPCODE_ROUTE_OK);
	components[0]
		= t4_component(T4_PAGE_CLASS_ORDINARY, MAIN_FORKNUM, 1, RF_ROUTE_OWNER_PAGE_CODEC);
	components[1]
		= t4_component(T4_PAGE_CLASS_ROUTED_SIDE, MAIN_FORKNUM, 1, RF_ROUTE_OWNER_SIDE_TYPED);
	UT_ASSERT_EQ((int)rf_opcode_route_validate_components_v1(&route, components, 2),
				 (int)RF_OPCODE_ROUTE_OK);

	components[0].forknum = VISIBILITYMAP_FORKNUM;
	UT_ASSERT_EQ((int)rf_opcode_route_validate_components_v1(&route, components, 2),
				 (int)RF_OPCODE_ROUTE_OK);
	components[0].forknum = INIT_FORKNUM;
	UT_ASSERT_EQ((int)rf_opcode_route_validate_components_v1(&route, components, 2),
				 (int)RF_OPCODE_ROUTE_OK);

	invalid
		= t4_component(T4_PAGE_CLASS_REBUILDABLE_FSM, FSM_FORKNUM, 1, RF_ROUTE_OWNER_PAGE_CODEC);
	UT_ASSERT_EQ((int)rf_opcode_route_validate_components_v1(&route, &invalid, 1),
				 (int)RF_OPCODE_ROUTE_OK);

	invalid = t4_component(T4_PAGE_CLASS_ORDINARY, 4, 1, RF_ROUTE_OWNER_PAGE_CODEC);
	UT_ASSERT_EQ((int)rf_opcode_route_validate_components_v1(&route, &invalid, 1),
				 (int)RF_OPCODE_ROUTE_COMPONENT_CLASS_INVALID);
	invalid = t4_component(T4_PAGE_CLASS_ROUTED_SPACE, 4, 1, RF_ROUTE_OWNER_SIDE_TYPED);
	UT_ASSERT_EQ((int)rf_opcode_route_validate_components_v1(&route, &invalid, 1),
				 (int)RF_OPCODE_ROUTE_COMPONENT_CLASS_INVALID);
	invalid = t4_component(T4_PAGE_CLASS_TEMP_LOCAL, MAIN_FORKNUM, 1, RF_ROUTE_OWNER_PAGE_CODEC);
	UT_ASSERT_EQ((int)rf_opcode_route_validate_components_v1(&route, &invalid, 1),
				 (int)RF_OPCODE_ROUTE_COMPONENT_CLASS_INVALID);
	invalid = t4_component(99, MAIN_FORKNUM, 1, RF_ROUTE_OWNER_PAGE_CODEC);
	UT_ASSERT_EQ((int)rf_opcode_route_validate_components_v1(&route, &invalid, 1),
				 (int)RF_OPCODE_ROUTE_COMPONENT_CLASS_INVALID);
	invalid = t4_component(T4_PAGE_CLASS_ORDINARY, MAIN_FORKNUM, 2, RF_ROUTE_OWNER_PAGE_CODEC);
	UT_ASSERT_EQ((int)rf_opcode_route_validate_components_v1(&route, &invalid, 1),
				 (int)RF_OPCODE_ROUTE_COMPONENT_CLASS_INVALID);
	invalid.owner_count = 0;
	UT_ASSERT_EQ((int)rf_opcode_route_validate_components_v1(&route, &invalid, 1),
				 (int)RF_OPCODE_ROUTE_COMPONENT_CLASS_INVALID);
	invalid.owner_count = 1;
	invalid.declared_owner = RF_ROUTE_OWNER_SIDE_TYPED;
	UT_ASSERT_EQ((int)rf_opcode_route_validate_components_v1(&route, &invalid, 1),
				 (int)RF_OPCODE_ROUTE_COMPONENT_CLASS_INVALID);
}

UT_TEST(test_invalid_api_inputs_leave_outputs_untouched)
{
	RfOpcodeRouteV1 route = t4_route_sentinel();
	RfOpcodeRouteV1 before = route;
	bool active = false;
	const char *name = "sentinel";

	UT_ASSERT(!rf_opcode_route_manifest_at_v1(137, &route, &active, &name));
	UT_ASSERT_EQ(memcmp(&route, &before, sizeof(route)), 0);
	UT_ASSERT(!active);
	UT_ASSERT_STR_EQ(name, "sentinel");
	UT_ASSERT(!rf_opcode_route_manifest_at_v1(0, NULL, &active, &name));
	UT_ASSERT(!rf_opcode_route_manifest_at_v1(0, &route, NULL, &name));
	UT_ASSERT(!rf_opcode_route_manifest_at_v1(0, &route, &active, NULL));
	UT_ASSERT_EQ((int)rf_opcode_route_lookup_v1(RM_HEAP_ID, 0x00, 1, NULL),
				 (int)RF_OPCODE_ROUTE_COMPONENT_CLASS_INVALID);
	UT_ASSERT_EQ((int)rf_opcode_route_validate_components_v1(&before, NULL, 0),
				 (int)RF_OPCODE_ROUTE_COMPONENT_CLASS_INVALID);
}

#endif

int
main(void)
{
#ifdef HAVE_CLUSTER_RF_ROUTE
	UT_PLAN(12);
#else
	UT_PLAN(4);
#endif
	UT_RUN(test_existing_apply_boundary_control);
#ifndef HAVE_CLUSTER_RF_ROUTE
	UT_RUN(test_red_page_zero_block_must_not_be_noop);
	UT_RUN(test_red_adg_block_must_not_enter_delta_apply);
	UT_RUN(test_red_xid_stripe_requires_typed_route);
#else
	UT_RUN(test_route_abi_and_counts);
	UT_RUN(test_canonical_manifest_stream);
	UT_RUN(test_every_manifest_row_lookup);
	UT_RUN(test_every_unused_nibble_fails_closed);
	UT_RUN(test_xact_flags_and_init_combinations);
	UT_RUN(test_gin_nested_values_fail_closed);
	UT_RUN(test_unknown_rmgr_and_block_shapes_fail_closed);
	UT_RUN(test_typed_no_ordinary_routes);
	UT_RUN(test_stop07_row_present_but_inactive);
	UT_RUN(test_component_classes_and_unique_owners);
	UT_RUN(test_invalid_api_inputs_leave_outputs_untouched);
#endif
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
