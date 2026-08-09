/*-------------------------------------------------------------------------
 *
 * cluster_rf_route.c
 *    Exhaustive Stage 8 failed-origin redo route authority.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/backend/cluster/cluster_rf_route.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/rmgr.h"
#include "access/xact.h"
#include "access/xlogrecord.h"
#include "cluster/cluster_rf_route.h"
#include "common/relpath.h"


/* The manifest is invalid until the complete built-in rmgr order is exact. */
StaticAssertDecl(RM_XLOG_ID == 0, "RM_XLOG_ID changed");
StaticAssertDecl(RM_XACT_ID == 1, "RM_XACT_ID changed");
StaticAssertDecl(RM_SMGR_ID == 2, "RM_SMGR_ID changed");
StaticAssertDecl(RM_CLOG_ID == 3, "RM_CLOG_ID changed");
StaticAssertDecl(RM_DBASE_ID == 4, "RM_DBASE_ID changed");
StaticAssertDecl(RM_TBLSPC_ID == 5, "RM_TBLSPC_ID changed");
StaticAssertDecl(RM_MULTIXACT_ID == 6, "RM_MULTIXACT_ID changed");
StaticAssertDecl(RM_RELMAP_ID == 7, "RM_RELMAP_ID changed");
StaticAssertDecl(RM_STANDBY_ID == 8, "RM_STANDBY_ID changed");
StaticAssertDecl(RM_HEAP2_ID == 9, "RM_HEAP2_ID changed");
StaticAssertDecl(RM_HEAP_ID == 10, "RM_HEAP_ID changed");
StaticAssertDecl(RM_BTREE_ID == 11, "RM_BTREE_ID changed");
StaticAssertDecl(RM_HASH_ID == 12, "RM_HASH_ID changed");
StaticAssertDecl(RM_GIN_ID == 13, "RM_GIN_ID changed");
StaticAssertDecl(RM_GIST_ID == 14, "RM_GIST_ID changed");
StaticAssertDecl(RM_SEQ_ID == 15, "RM_SEQ_ID changed");
StaticAssertDecl(RM_SPGIST_ID == 16, "RM_SPGIST_ID changed");
StaticAssertDecl(RM_BRIN_ID == 17, "RM_BRIN_ID changed");
StaticAssertDecl(RM_COMMIT_TS_ID == 18, "RM_COMMIT_TS_ID changed");
StaticAssertDecl(RM_REPLORIGIN_ID == 19, "RM_REPLORIGIN_ID changed");
StaticAssertDecl(RM_GENERIC_ID == 20, "RM_GENERIC_ID changed");
StaticAssertDecl(RM_LOGICALMSG_ID == 21, "RM_LOGICALMSG_ID changed");
StaticAssertDecl(RM_CLUSTER_UNDO_ID == 22, "RM_CLUSTER_UNDO_ID changed");
StaticAssertDecl(RM_CLUSTER_RAW_LAYOUT_ID == 23, "RM_CLUSTER_RAW_LAYOUT_ID changed");
StaticAssertDecl(RM_CLUSTER_ADG_ID == 24, "RM_CLUSTER_ADG_ID changed");
StaticAssertDecl(RM_CLUSTER_XID_STRIPE_ID == 25, "RM_CLUSTER_XID_STRIPE_ID changed");
StaticAssertDecl(RM_NEXT_ID == 26, "built-in rmgr count changed");
StaticAssertDecl(XLR_INFO_MASK == 0x0F, "generic info mask changed");
StaticAssertDecl(XLOG_XACT_OPMASK == 0x70, "XACT opcode mask changed");
StaticAssertDecl(XLOG_XACT_HAS_INFO == 0x80, "XACT info flag changed");

typedef struct RfOpcodeRouteManifestRowV1 {
	RfOpcodeRouteV1 route;
	bool active;
	const char *diagnostic_name;
} RfOpcodeRouteManifestRowV1;

static const RfOpcodeRouteManifestRowV1 rf_opcode_route_manifest_v1[] = {
#define RF_OPCODE_ROUTE_ROW(rmid, info, legal_flags, owner, policy, codec, active, name)           \
	{ { (uint8)(rmid), (uint8)(info), (uint8)(legal_flags), (uint8)(owner), (uint8)(policy),       \
		(uint8)(codec), UINT16_C(0) },                                                             \
	  (active) != 0,                                                                               \
	  (name) },
#include "cluster/cluster_rf_route_manifest.def"
#undef RF_OPCODE_ROUTE_ROW
};

enum {
	RF_OPCODE_ROUTE_COMPILED_COUNT_V1 = 0
#define RF_OPCODE_ROUTE_ROW(rmid, info, legal_flags, owner, policy, codec, active, name) +1
#include "cluster/cluster_rf_route_manifest.def"
#undef RF_OPCODE_ROUTE_ROW
};

enum {
	RF_OPCODE_ROUTE_COMPILED_LIVE_COUNT_V1 = 0
#define RF_OPCODE_ROUTE_ROW(rmid, info, legal_flags, owner, policy, codec, active, name) +(active)
#include "cluster/cluster_rf_route_manifest.def"
#undef RF_OPCODE_ROUTE_ROW
};

StaticAssertDecl(lengthof(rf_opcode_route_manifest_v1) == RF_OPCODE_ROUTE_MANIFEST_COUNT_V1,
				 "opcode route manifest row count");
StaticAssertDecl(RF_OPCODE_ROUTE_COMPILED_COUNT_V1 == RF_OPCODE_ROUTE_MANIFEST_COUNT_V1,
				 "opcode route generated count");
StaticAssertDecl(RF_OPCODE_ROUTE_COMPILED_LIVE_COUNT_V1 == RF_OPCODE_ROUTE_LIVE_COUNT_V1,
				 "opcode route live count");

/* Duplicate generated keys cause a compile-time duplicate-case error. */
static inline void
rf_opcode_route_compile_collision_check_v1(void)
{
	switch (0) {
#define RF_OPCODE_ROUTE_ROW(rmid, info, legal_flags, owner, policy, codec, active, name)           \
	case (((rmid) << 8) | (info)):                                                                 \
		break;
#include "cluster/cluster_rf_route_manifest.def"
#undef RF_OPCODE_ROUTE_ROW
	}
}

uint16
rf_opcode_route_manifest_count_v1(void)
{
	return RF_OPCODE_ROUTE_MANIFEST_COUNT_V1;
}

uint16
rf_opcode_route_live_count_v1(void)
{
	return RF_OPCODE_ROUTE_LIVE_COUNT_V1;
}

bool
rf_opcode_route_manifest_collision_free_v1(void)
{
	uint16 live_count = 0;
	uint32 previous_key = 0;
	uint16 i;

	rf_opcode_route_compile_collision_check_v1();

	for (i = 0; i < lengthof(rf_opcode_route_manifest_v1); i++) {
		const RfOpcodeRouteManifestRowV1 *row = &rf_opcode_route_manifest_v1[i];
		uint32 key = ((uint32)row->route.rmid << 8) | row->route.normalized_info;

		if (i > 0 && key <= previous_key)
			return false;
		if ((row->route.normalized_info & XLR_INFO_MASK) != 0 || row->route.reserved_zero != 0
			|| row->diagnostic_name == NULL || row->diagnostic_name[0] == '\0')
			return false;
		if (row->active)
			live_count++;
		previous_key = key;
	}

	return live_count == RF_OPCODE_ROUTE_LIVE_COUNT_V1;
}

bool
rf_opcode_route_manifest_at_v1(uint16 index, RfOpcodeRouteV1 *route, bool *active,
							   const char **diagnostic_name)
{
	const RfOpcodeRouteManifestRowV1 *row;

	if (index >= lengthof(rf_opcode_route_manifest_v1) || route == NULL || active == NULL
		|| diagnostic_name == NULL)
		return false;

	row = &rf_opcode_route_manifest_v1[index];
	*route = row->route;
	*active = row->active;
	*diagnostic_name = row->diagnostic_name;
	return true;
}

RfOpcodeRouteLookupResultV1
rf_opcode_route_lookup_v1(uint16 rmid, uint8 raw_info, uint8 registered_block_count,
						  RfOpcodeRouteV1 *route)
{
	const RfOpcodeRouteManifestRowV1 *row = NULL;
	uint8 normalized_info;
	uint8 info_flags;
	uint32 target_key;
	int low;
	int high;

	if (route == NULL)
		return RF_OPCODE_ROUTE_COMPONENT_CLASS_INVALID;
	if (rmid >= RM_NEXT_ID)
		return RF_OPCODE_ROUTE_RMID_UNSUPPORTED;

	if (rmid == RM_XACT_ID) {
		normalized_info = raw_info & XLOG_XACT_OPMASK;
		info_flags = raw_info & XLOG_XACT_HAS_INFO;
	} else {
		normalized_info = raw_info & ~XLR_INFO_MASK;
		info_flags = 0;
	}

	target_key = ((uint32)rmid << 8) | normalized_info;
	low = 0;
	high = lengthof(rf_opcode_route_manifest_v1) - 1;
	while (low <= high) {
		int middle = low + (high - low) / 2;
		const RfOpcodeRouteManifestRowV1 *candidate = &rf_opcode_route_manifest_v1[middle];
		uint32 candidate_key
			= ((uint32)candidate->route.rmid << 8) | candidate->route.normalized_info;

		if (candidate_key == target_key) {
			row = candidate;
			break;
		}
		if (candidate_key < target_key)
			low = middle + 1;
		else
			high = middle - 1;
	}

	if (row == NULL || !row->active)
		return RF_OPCODE_ROUTE_OPCODE_UNSUPPORTED;
	if ((info_flags & ~row->route.legal_info_flags) != 0)
		return RF_OPCODE_ROUTE_FLAG_ILLEGAL;
	if ((row->route.block_policy == RF_ROUTE_BLOCKS_REQUIRED && registered_block_count == 0)
		|| (row->route.block_policy == RF_ROUTE_BLOCKS_FORBIDDEN && registered_block_count != 0))
		return RF_OPCODE_ROUTE_BLOCK_SHAPE_INVALID;

	*route = row->route;
	return RF_OPCODE_ROUTE_OK;
}

/* Values are the frozen RfPageClassV1 ABI owned by xlogrecord.h in Task 3. */
#define RF_ROUTE_PAGE_CLASS_INVALID_V1 UINT8_C(0)
#define RF_ROUTE_PAGE_CLASS_ORDINARY_V1 UINT8_C(1)
#define RF_ROUTE_PAGE_CLASS_REBUILDABLE_FSM_V1 UINT8_C(2)
#define RF_ROUTE_PAGE_CLASS_ROUTED_SPACE_V1 UINT8_C(3)
#define RF_ROUTE_PAGE_CLASS_ROUTED_HEADER_V1 UINT8_C(4)
#define RF_ROUTE_PAGE_CLASS_ROUTED_SIDE_V1 UINT8_C(5)
#define RF_ROUTE_PAGE_CLASS_TEMP_LOCAL_V1 UINT8_C(6)

RfOpcodeRouteLookupResultV1
rf_opcode_route_validate_components_v1(const RfOpcodeRouteV1 *route,
									   const RfRouteComponentV1 *components, uint16 component_count)
{
	uint16 i;

	if (route == NULL || (component_count > 0 && components == NULL))
		return RF_OPCODE_ROUTE_COMPONENT_CLASS_INVALID;
	switch (route->record_owner) {
	case RF_ROUTE_OWNER_PAGE_CODEC:
		break;
	case RF_ROUTE_OWNER_SIDE_TYPED:
	case RF_ROUTE_OWNER_LOGICAL_NOOP:
		return component_count == 0 ? RF_OPCODE_ROUTE_OK : RF_OPCODE_ROUTE_COMPONENT_CLASS_INVALID;
	case RF_ROUTE_OWNER_INVALID:
	default:
		return RF_OPCODE_ROUTE_COMPONENT_CLASS_INVALID;
	}
	if (component_count == 0)
		return RF_OPCODE_ROUTE_COMPONENT_CLASS_INVALID;

	for (i = 0; i < component_count; i++) {
		const RfRouteComponentV1 *component = &components[i];
		uint8 expected_owner;

		if (component->owner_count != 1)
			return RF_OPCODE_ROUTE_COMPONENT_CLASS_INVALID;

		switch (component->page_class) {
		case RF_ROUTE_PAGE_CLASS_ORDINARY_V1:
			if (component->forknum != MAIN_FORKNUM && component->forknum != VISIBILITYMAP_FORKNUM
				&& component->forknum != INIT_FORKNUM)
				return RF_OPCODE_ROUTE_COMPONENT_CLASS_INVALID;
			expected_owner = RF_ROUTE_OWNER_PAGE_CODEC;
			break;
		case RF_ROUTE_PAGE_CLASS_REBUILDABLE_FSM_V1:
			if (component->forknum != FSM_FORKNUM)
				return RF_OPCODE_ROUTE_COMPONENT_CLASS_INVALID;
			expected_owner = RF_ROUTE_OWNER_PAGE_CODEC;
			break;
		case RF_ROUTE_PAGE_CLASS_ROUTED_HEADER_V1:
		case RF_ROUTE_PAGE_CLASS_ROUTED_SIDE_V1:
			expected_owner = RF_ROUTE_OWNER_SIDE_TYPED;
			break;
		case RF_ROUTE_PAGE_CLASS_INVALID_V1:
		case RF_ROUTE_PAGE_CLASS_ROUTED_SPACE_V1:
		case RF_ROUTE_PAGE_CLASS_TEMP_LOCAL_V1:
		default:
			return RF_OPCODE_ROUTE_COMPONENT_CLASS_INVALID;
		}

		if (component->declared_owner != expected_owner)
			return RF_OPCODE_ROUTE_COMPONENT_CLASS_INVALID;
	}

	return RF_OPCODE_ROUTE_OK;
}

RfOpcodeRouteLookupResultV1
rf_opcode_route_validate_gin_segment_action_v1(uint8 action)
{
	return action >= 1 && action <= 4 ? RF_OPCODE_ROUTE_OK : RF_OPCODE_ROUTE_NESTED_VALUE_INVALID;
}
