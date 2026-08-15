/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_config.h
 *	  Strict root-owned pgrac-fenced configuration ABI.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_CONFIG_H
#define PGRAC_FENCED_CONFIG_H

#include "c.h"

#include <sys/stat.h>

#define PGRAC_FENCED_CONFIG_PATH "/etc/pgrac/pgrac-fenced.conf"
#define PGRAC_FENCED_DB_SOCKET_PATH "/var/run/pgrac/pgrac-fenced.sock"
#define PGRAC_FENCED_ADMIN_SOCKET_PATH "/var/run/pgrac/pgrac-fenced-admin.sock"
#define PGRAC_FENCED_JOURNAL_DIR "/var/lib/pgrac-fenced/journal"
#define PGRAC_FENCED_CONFIG_MAX_BYTES (64 * 1024)
#define PGRAC_FENCED_MAX_NODES 128
#define PGRAC_FENCED_UUID_BYTES 16
#define PGRAC_FENCED_ADAPTER_DATA_MAX_BYTES 4096

typedef enum PgracFencedConfigResult
{
	PGRAC_FENCED_CONFIG_OK = 0,
	PGRAC_FENCED_CONFIG_BAD_ARGUMENT = 1,
	PGRAC_FENCED_CONFIG_TOO_LARGE = 2,
	PGRAC_FENCED_CONFIG_NONCANONICAL = 3,
	PGRAC_FENCED_CONFIG_VALUE_INVALID = 4
} PgracFencedConfigResult;

typedef struct PgracFencedNodeConfigV1
{
	bool present;
	uint8 target_uuid[PGRAC_FENCED_UUID_BYTES];
	uint16 adapter_data_len;
	uint8 adapter_data[PGRAC_FENCED_ADAPTER_DATA_MAX_BYTES];
} PgracFencedNodeConfigV1;

typedef struct PgracFencedConfigV1
{
	uint32 format_version;
	uint64 mapping_generation;
	uint64 system_identifier;
	uint32 storage_backend_id;
	uint8 storage_uuid[PGRAC_FENCED_UUID_BYTES];
	uint64 allowed_db_uid;
	uint64 allowed_db_gid;
	uint16 provider_id;
	uint16 provider_abi;
	uint16 node_count;
	PgracFencedNodeConfigV1 nodes[PGRAC_FENCED_MAX_NODES];
} PgracFencedConfigV1;

extern PgracFencedConfigResult pgrac_fenced_config_parse_v1(
	const uint8 *bytes, size_t len, PgracFencedConfigV1 *out);
extern bool pgrac_fenced_config_stat_secure(const struct stat *st);
extern int pgrac_fenced_config_open_secure(void);

#endif /* PGRAC_FENCED_CONFIG_H */
