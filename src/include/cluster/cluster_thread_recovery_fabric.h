/*-------------------------------------------------------------------------
 * cluster_thread_recovery_fabric.h
 *    Immutable PAGE/SIDE plan assembled before online recovery mutation.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_THREAD_RECOVERY_FABRIC_H
#define CLUSTER_THREAD_RECOVERY_FABRIC_H

#include "cluster/cluster_side_online_plan.h"

#define CLUSTER_THREAD_RECOVERY_FABRIC_INTERFACE_V1 1

typedef struct ClusterThreadRecoveryFabricPlanV1
	ClusterThreadRecoveryFabricPlanV1;

typedef struct ClusterThreadRecoveryFabricPlanRequestV1
{
	uint64		system_identifier;
	uint8		storage_uuid[16];
	const RfContributorStreamCutV1 *physical_cuts;
	uint32		participant_count;
	uint64		retention_binding_cookie;
	Size		page_memory_budget;
	Size		side_memory_budget;
	bool		space_active;
	uint8		reserved_zero[7];
} ClusterThreadRecoveryFabricPlanRequestV1;

extern RfPageProofDetailV1 cluster_thread_recovery_fabric_plan_create_v1(
	const ClusterThreadRecoveryFabricPlanRequestV1 *request,
	ClusterThreadRecoveryFabricPlanV1 **out_plan);
extern RfPageProofDetailV1 cluster_thread_recovery_fabric_plan_feed_record_v1(
	ClusterThreadRecoveryFabricPlanV1 *plan, XLogReaderState *record,
	uint16 participant_index);
extern RfPageProofDetailV1 cluster_thread_recovery_fabric_plan_seal_v1(
	ClusterThreadRecoveryFabricPlanV1 *plan);
extern const RfPageOnlinePlanV1 *cluster_thread_recovery_fabric_page_plan_v1(
	const ClusterThreadRecoveryFabricPlanV1 *plan);
extern const RfSideOnlinePlanV1 *cluster_thread_recovery_fabric_side_plan_v1(
	const ClusterThreadRecoveryFabricPlanV1 *plan);
extern void cluster_thread_recovery_fabric_plan_destroy_v1(
	ClusterThreadRecoveryFabricPlanV1 **plan);

#endif
