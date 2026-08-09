/*-------------------------------------------------------------------------
 *
 * cluster_space_codec.c
 *	  Direct-built bounded STOP07 SPACE value codec.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_space_codec.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "cluster/cluster_space.h"

/* T3-I strong interface: behavior remains fail-closed until T3-B. */
bool
cluster_space_identity_equal(const ClusterSpaceIdentityV1 *left,
							 const ClusterSpaceIdentityV1 *right)
{
	(void) left;
	(void) right;
	return false;
}
