/*-------------------------------------------------------------------------
 *
 * cluster_space_codec.c
 *	  Direct-built bounded STOP07 SPACE value codec.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_space_codec.c
 *
 * NOTES
 *	  This is a pgrac-original, direct-built value-codec implementation.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "cluster/cluster_space.h"

bool
cluster_space_identity_equal(const ClusterSpaceIdentityV1 *left,
							 const ClusterSpaceIdentityV1 *right)
{
	if (left == NULL || right == NULL ||
		left->system_identifier != right->system_identifier ||
		left->spc_oid != right->spc_oid ||
		left->db_oid != right->db_oid ||
		left->rel_number != right->rel_number ||
		left->target_fork != right->target_fork)
		return false;

	for (int i = 0; i < 16; i++)
	{
		if (left->space_incarnation[i] != right->space_incarnation[i])
			return false;
	}

	return true;
}
