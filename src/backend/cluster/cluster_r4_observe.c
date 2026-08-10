/*-------------------------------------------------------------------------
 *
 * cluster_r4_observe.c
 *	Thin observation-only adapter for Stage 8 R4 events.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_cr.h"
#include "cluster/cluster_r4_observe.h"

void
cluster_r4_observe(ClusterR4Event event, ClusterTxResolveReason tx_reason,
				   ClusterCrBuildReason cr_reason)
{
	/* Reasons remain typed at the producer boundary.  The current R1 carrier
	 * is the monotonic event counter surface, never a decision input. */
	(void)tx_reason;
	(void)cr_reason;
	if ((uint32)event >= CLUSTER_R4_OBSERVATION_EVENT_COUNT)
		return;
	cluster_cr_r4_event_bump((uint32)event);
}

#endif /* USE_PGRAC_CLUSTER */
