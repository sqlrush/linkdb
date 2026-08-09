/*-------------------------------------------------------------------------
 *
 * cluster_rf_route.h
 *    Exhaustive Stage 8 failed-origin redo route authority.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/include/cluster/cluster_rf_route.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RF_ROUTE_H
#define CLUSTER_RF_ROUTE_H

#include "c.h"


#define PGRAC_JIT_RF_ROUTE_ABI_V1 1

#define RF_OPCODE_ROUTE_MANIFEST_COUNT_V1 UINT16_C(137)
#define RF_OPCODE_ROUTE_LIVE_COUNT_V1 UINT16_C(136)

typedef enum RfRecordRouteOwnerV1 {
	RF_ROUTE_OWNER_INVALID = 0,
	RF_ROUTE_OWNER_PAGE_CODEC = 1,
	RF_ROUTE_OWNER_SIDE_TYPED = 2,
	RF_ROUTE_OWNER_LOGICAL_NOOP = 3
} RfRecordRouteOwnerV1;

typedef enum RfRouteBlockPolicyV1 {
	RF_ROUTE_BLOCKS_FORBIDDEN = 0,
	RF_ROUTE_BLOCKS_REQUIRED = 1,
	RF_ROUTE_BLOCKS_OPTIONAL_TYPED = 2
} RfRouteBlockPolicyV1;

typedef enum RfRouteCodecIdV1 {
	RF_ROUTE_CODEC_NONE = 0,
	RF_ROUTE_CODEC_XLOG_FPI = 1,
	RF_ROUTE_CODEC_HEAP2 = 2,
	RF_ROUTE_CODEC_HEAP = 3,
	RF_ROUTE_CODEC_BTREE = 4,
	RF_ROUTE_CODEC_HASH = 5,
	RF_ROUTE_CODEC_GIN = 6,
	RF_ROUTE_CODEC_GIST = 7,
	RF_ROUTE_CODEC_SEQ = 8,
	RF_ROUTE_CODEC_SPGIST = 9,
	RF_ROUTE_CODEC_BRIN = 10,
	RF_ROUTE_CODEC_GENERIC = 11,
	RF_ROUTE_CODEC_SIDE_STANDARD = 12,
	RF_ROUTE_CODEC_SIDE_CLUSTER_UNDO = 13,
	RF_ROUTE_CODEC_SIDE_RAW_LAYOUT = 14,
	RF_ROUTE_CODEC_LOGICAL_NOOP = 15,
	RF_ROUTE_CODEC_SIDE_ADG_BARRIER = 16,
	RF_ROUTE_CODEC_SIDE_XID_STRIPE_SHMEM = 17
} RfRouteCodecIdV1;

typedef enum RfOpcodeRouteLookupResultV1 {
	RF_OPCODE_ROUTE_OK = 0,
	RF_OPCODE_ROUTE_RMID_UNSUPPORTED = 1,
	RF_OPCODE_ROUTE_OPCODE_UNSUPPORTED = 2,
	RF_OPCODE_ROUTE_FLAG_ILLEGAL = 3,
	RF_OPCODE_ROUTE_BLOCK_SHAPE_INVALID = 4,
	RF_OPCODE_ROUTE_COMPONENT_CLASS_INVALID = 5,
	RF_OPCODE_ROUTE_NESTED_VALUE_INVALID = 6
} RfOpcodeRouteLookupResultV1;

typedef struct RfOpcodeRouteV1 {
	uint8 rmid;
	uint8 normalized_info;
	uint8 legal_info_flags;
	uint8 record_owner;
	uint8 block_policy;
	uint8 codec_id;
	uint16 reserved_zero;
} RfOpcodeRouteV1;

StaticAssertDecl(sizeof(RfOpcodeRouteV1) == 8, "RfOpcodeRouteV1 size");
StaticAssertDecl(offsetof(RfOpcodeRouteV1, reserved_zero) == 6, "RfOpcodeRouteV1 reserved offset");

/*
 * A decoded component supplies the already-decoded page class, fork and the
 * number of semantic owners that claimed it.  owner_count must be exactly
 * one.  This is a preflight value only; it is not persistent or wire ABI.
 */
typedef struct RfRouteComponentV1 {
	uint8 page_class;
	uint8 forknum;
	uint8 owner_count;
	uint8 declared_owner;
} RfRouteComponentV1;

extern uint16 rf_opcode_route_manifest_count_v1(void);
extern uint16 rf_opcode_route_live_count_v1(void);
extern bool rf_opcode_route_manifest_collision_free_v1(void);
extern bool rf_opcode_route_manifest_at_v1(uint16 index, RfOpcodeRouteV1 *route, bool *active,
										   const char **diagnostic_name);
extern RfOpcodeRouteLookupResultV1 rf_opcode_route_lookup_v1(uint16 rmid, uint8 raw_info,
															 uint8 registered_block_count,
															 RfOpcodeRouteV1 *route);
extern RfOpcodeRouteLookupResultV1 rf_opcode_route_validate_components_v1(
	const RfOpcodeRouteV1 *route, const RfRouteComponentV1 *components, uint16 component_count);
extern RfOpcodeRouteLookupResultV1 rf_opcode_route_validate_gin_segment_action_v1(uint8 action);

#endif /* CLUSTER_RF_ROUTE_H */
