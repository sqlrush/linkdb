/*-------------------------------------------------------------------------
 *
 * cluster_r4_observe.h
 *	Observation-only Stage 8 R4 event domain.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_R4_OBSERVE_H
#define CLUSTER_R4_OBSERVE_H

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_tx_resolve.h"

#ifdef USE_PGRAC_CLUSTER

typedef enum ClusterR4Event {
	CLUSTER_R4_EVENT_CR_ROUTE_STARTED = 0,
	CLUSTER_R4_EVENT_CR_HOLDER_FULL = 1,
	CLUSTER_R4_EVENT_CR_HOLDER_RETRY = 2,
	CLUSTER_R4_EVENT_CR_HOLDER_FAIL_CLOSED = 3,
	CLUSTER_R4_EVENT_UNDO_FETCH_SERVED = 4,
	CLUSTER_R4_EVENT_UNDO_FETCH_DENIED = 5,
	CLUSTER_R4_EVENT_TX_UNKNOWN = 6,
	CLUSTER_R4_EVENT_TX_IN_PROGRESS = 7,
	CLUSTER_R4_EVENT_TX_PREPARED = 8,
	CLUSTER_R4_EVENT_TX_COMMITTED = 9,
	CLUSTER_R4_EVENT_TX_ABORTED = 10,
	CLUSTER_R4_EVENT_MULTI_SERVED = 11,
	CLUSTER_R4_EVENT_MULTI_UNKNOWN = 12,
	CLUSTER_R4_EVENT_SLOT_CAPACITY_RETRY = 13
} ClusterR4Event;

#define CLUSTER_R4_OBSERVATION_EVENT_COUNT 14

extern void cluster_r4_observe(ClusterR4Event event, ClusterTxResolveReason tx_reason,
							  ClusterCrBuildReason cr_reason);

#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_R4_OBSERVE_H */
