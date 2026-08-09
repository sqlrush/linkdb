/*-------------------------------------------------------------------------
 *
 * cluster_space.h
 *	  Bounded pre-activation STOP07 SPACE value interfaces.
 *
 * This is a value-only pre-activation interface.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_space.h
 *
 * NOTES
 *	  This is a pgrac-original, value-only pre-activation interface.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SPACE_H
#define CLUSTER_SPACE_H

#include "storage/relfilelocator.h"

typedef enum ClusterSpaceResultV1
{
	CLUSTER_SPACE_OK = 0,
	CLUSTER_SPACE_NOT_APPLICABLE = 1,
	CLUSTER_SPACE_RETRY = 2,
	CLUSTER_SPACE_WOULD_BLOCK = 3,
	CLUSTER_SPACE_ROOT_STALE = 4,
	CLUSTER_SPACE_DUTY_TAIL_PENDING = 5,
	CLUSTER_SPACE_DUTY_STALE = 6,
	CLUSTER_SPACE_SERIAL_NOT_HELD = 7,
	CLUSTER_SPACE_FENCE_STALE = 8,
	CLUSTER_SPACE_RETENTION_STALE = 9,
	CLUSTER_SPACE_PAGE_AUTHORITY_BLOCKED = 10,
	CLUSTER_SPACE_SOURCE_INCOMPLETE = 11,
	CLUSTER_SPACE_IDENTITY_MISMATCH = 12,
	CLUSTER_SPACE_INCARNATION_MISMATCH = 13,
	CLUSTER_SPACE_FORMAT_UNSUPPORTED = 14,
	CLUSTER_SPACE_INTEGRITY_FAILED = 15,
	CLUSTER_SPACE_COVERAGE_CONFLICT = 16,
	CLUSTER_SPACE_EXPECTED_MISMATCH = 17,
	CLUSTER_SPACE_TOMBSTONED = 18,
	CLUSTER_SPACE_MIXED_VERSION = 19,
	CLUSTER_SPACE_IO_FAILED = 20,
	CLUSTER_SPACE_OOM = 21,
	CLUSTER_SPACE_CANCELLED = 22,
	CLUSTER_SPACE_INVALID_ARGUMENT = 23,
	CLUSTER_SPACE_INTERNAL = 24
} ClusterSpaceResultV1;

typedef struct ClusterSpaceLocatorV1
{
	uint64		system_identifier;
	Oid			spc_oid;
	Oid			db_oid;
	RelFileNumber rel_number;
	uint32		target_fork;
} ClusterSpaceLocatorV1;

typedef struct ClusterSpaceIdentityV1
{
	uint64		system_identifier;
	Oid			spc_oid;
	Oid			db_oid;
	RelFileNumber rel_number;
	uint32		target_fork;
	uint8		space_incarnation[16];
} ClusterSpaceIdentityV1;

typedef struct ClusterSpaceReservationV1
{
	ClusterSpaceIdentityV1 identity;
	uint64		reservation_id;
	uint64		operation_id;
	uint64		first_block;
	uint64		owner_incarnation;
	uint64		catalog_sequence;
	uint32		block_count;
	uint32		owner_node_id;
	uint32		header_page_crc32c;
	uint32		reserved_zero;
} ClusterSpaceReservationV1;

StaticAssertDecl(sizeof(ClusterSpaceLocatorV1) == 24,
				 "ClusterSpaceLocatorV1 size");
StaticAssertDecl(offsetof(ClusterSpaceLocatorV1, system_identifier) == 0,
				 "ClusterSpaceLocatorV1 system identifier offset");
StaticAssertDecl(offsetof(ClusterSpaceLocatorV1, spc_oid) == 8,
				 "ClusterSpaceLocatorV1 tablespace offset");
StaticAssertDecl(offsetof(ClusterSpaceLocatorV1, db_oid) == 12,
				 "ClusterSpaceLocatorV1 database offset");
StaticAssertDecl(offsetof(ClusterSpaceLocatorV1, rel_number) == 16,
				 "ClusterSpaceLocatorV1 relation offset");
StaticAssertDecl(offsetof(ClusterSpaceLocatorV1, target_fork) == 20,
				 "ClusterSpaceLocatorV1 fork offset");

StaticAssertDecl(sizeof(ClusterSpaceIdentityV1) == 40,
				 "ClusterSpaceIdentityV1 size");
StaticAssertDecl(offsetof(ClusterSpaceIdentityV1, space_incarnation) == 24,
				 "ClusterSpaceIdentityV1 incarnation offset");

StaticAssertDecl(sizeof(ClusterSpaceReservationV1) == 96,
				 "ClusterSpaceReservationV1 size");
StaticAssertDecl(offsetof(ClusterSpaceReservationV1, reservation_id) == 40,
				 "ClusterSpaceReservationV1 reservation offset");
StaticAssertDecl(offsetof(ClusterSpaceReservationV1, operation_id) == 48,
				 "ClusterSpaceReservationV1 operation offset");
StaticAssertDecl(offsetof(ClusterSpaceReservationV1, first_block) == 56,
				 "ClusterSpaceReservationV1 first-block offset");
StaticAssertDecl(offsetof(ClusterSpaceReservationV1, owner_incarnation) == 64,
				 "ClusterSpaceReservationV1 owner offset");
StaticAssertDecl(offsetof(ClusterSpaceReservationV1, catalog_sequence) == 72,
				 "ClusterSpaceReservationV1 catalog offset");
StaticAssertDecl(offsetof(ClusterSpaceReservationV1, block_count) == 80,
				 "ClusterSpaceReservationV1 block-count offset");
StaticAssertDecl(offsetof(ClusterSpaceReservationV1, owner_node_id) == 84,
				 "ClusterSpaceReservationV1 owner-node offset");
StaticAssertDecl(offsetof(ClusterSpaceReservationV1, header_page_crc32c) == 88,
				 "ClusterSpaceReservationV1 crc offset");
StaticAssertDecl(offsetof(ClusterSpaceReservationV1, reserved_zero) == 92,
				 "ClusterSpaceReservationV1 reserved offset");

#define CLUSTER_SPACE_CODEC_INTERFACE_V1 1
extern bool cluster_space_identity_equal(const ClusterSpaceIdentityV1 *left,
										 const ClusterSpaceIdentityV1 *right);

#endif							/* CLUSTER_SPACE_H */
