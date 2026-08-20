/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_ipmi.h
 *    Fixed IPMI LAN+ provider entry point.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_IPMI_H
#define PGRAC_FENCED_IPMI_H

#include <sys/stat.h>

#include "pgrac_fenced_provider.h"

#define PGRAC_FENCED_IPMI_ADAPTER_MAGIC "PFIP"
#define PGRAC_FENCED_IPMI_ADAPTER_VERSION UINT16_C(1)
#define PGRAC_FENCED_IPMI_ADAPTER_FIXED_BYTES 64
#define PGRAC_FENCED_IPMI_ADAPTER_PATH_MAX 255
#define PGRAC_FENCED_IPMI_ADAPTER_MAX_BYTES \
	(PGRAC_FENCED_IPMI_ADAPTER_FIXED_BYTES + \
	 PGRAC_FENCED_IPMI_ADAPTER_PATH_MAX)
#define PGRAC_FENCED_IPMI_USERNAME_MAX 16
#define PGRAC_FENCED_IPMI_PASSWORD_MAX 20
#define PGRAC_FENCED_IPMI_CREDENTIAL_PATH_MAX 96
#define PGRAC_FENCED_IPMI_INVOCATION_ARGV_MAX 17
#define PGRAC_FENCED_IPMI_ACTION_OUTPUT_MAX 4096

typedef struct PgracFencedIpmiAdapterV1
{
	uint16_t address_family;
	uint16_t port;
	uint8_t address[16];
	uint8_t cipher_suite;
	uint16_t executable_path_len;
	uint8_t executable_sha256[32];
	char executable_path[PGRAC_FENCED_IPMI_ADAPTER_PATH_MAX + 1];
} PgracFencedIpmiAdapterV1;

typedef enum PgracFencedIpmiCommand
{
	PGRAC_FENCED_IPMI_COMMAND_GUID = 1,
	PGRAC_FENCED_IPMI_COMMAND_OFF = 2,
	PGRAC_FENCED_IPMI_COMMAND_STATUS = 3,
	PGRAC_FENCED_IPMI_COMMAND_ON = 4
} PgracFencedIpmiCommand;

typedef struct PgracFencedIpmiInvocationV1
{
	char address[46];
	char port[6];
	char cipher_suite[4];
	char username[PGRAC_FENCED_IPMI_USERNAME_MAX + 1];
	char password_path[PGRAC_FENCED_IPMI_CREDENTIAL_PATH_MAX];
	char *argv[PGRAC_FENCED_IPMI_INVOCATION_ARGV_MAX];
	size_t argc;
} PgracFencedIpmiInvocationV1;

typedef struct PgracFencedIpmiCommandOutputV1
{
	uint8 stdout_bytes[PGRAC_FENCED_IPMI_ACTION_OUTPUT_MAX];
	uint8 stderr_bytes[PGRAC_FENCED_IPMI_ACTION_OUTPUT_MAX];
	size_t stdout_len;
	size_t stderr_len;
	int exit_code;
} PgracFencedIpmiCommandOutputV1;

typedef struct PgracFencedIpmiCredentialsV1
{
	char username[PGRAC_FENCED_IPMI_USERNAME_MAX + 1];
	char user_path[PGRAC_FENCED_IPMI_CREDENTIAL_PATH_MAX];
	char password_path[PGRAC_FENCED_IPMI_CREDENTIAL_PATH_MAX];
} PgracFencedIpmiCredentialsV1;

extern bool pgrac_fenced_ipmi_adapter_parse(
	const uint8 *bytes, size_t len, PgracFencedIpmiAdapterV1 *out);
extern bool pgrac_fenced_ipmi_executable_stat_secure(const struct stat *st);
extern bool pgrac_fenced_ipmi_credential_stat_secure(const struct stat *st);
extern bool pgrac_fenced_ipmi_username_parse(
	const uint8 *bytes, size_t len,
	char out[PGRAC_FENCED_IPMI_USERNAME_MAX + 1]);
extern bool pgrac_fenced_ipmi_password_validate(
	const uint8 *bytes, size_t len);
extern bool pgrac_fenced_ipmi_credential_paths(
	const uint8 target_uuid[16], char *user_path, size_t user_path_size,
	char *password_path, size_t password_path_size);
extern bool pgrac_fenced_ipmi_credentials_load(
	const uint8 target_uuid[16], PgracFencedIpmiCredentialsV1 *out);
extern bool pgrac_fenced_ipmi_sha256_fd(int fd, uint8 out[32]);
extern int pgrac_fenced_ipmi_executable_open(
	const PgracFencedIpmiAdapterV1 *adapter);
extern bool pgrac_fenced_ipmi_invocation_build(
	const PgracFencedIpmiAdapterV1 *adapter, const char *username,
	const char *password_path, PgracFencedIpmiCommand command,
	PgracFencedIpmiInvocationV1 *out);
extern bool pgrac_fenced_ipmi_guid_result_parse(
	int exit_code, const uint8 *stdout_bytes, size_t stdout_len,
	const uint8 *stderr_bytes, size_t stderr_len, uint8 out_uuid[16]);
extern bool pgrac_fenced_ipmi_power_result_parse(
	int exit_code, const uint8 *stdout_bytes, size_t stdout_len,
	const uint8 *stderr_bytes, size_t stderr_len,
	PgracFencedTargetState *out_state);
extern bool pgrac_fenced_ipmi_action_result_validate(
	int exit_code, const uint8 *stdout_bytes, size_t stdout_len,
	const uint8 *stderr_bytes, size_t stderr_len);
extern bool pgrac_fenced_ipmi_uncertified_readback(
	const uint8 observed_uuid[16], PgracFencedTargetState state,
	PgracFencedReadbackV1 *out);
extern PgracFencedProviderResult pgrac_fenced_ipmi_resolve_result(
	const PgracFencedTargetV1 *configured,
	const PgracFencedIpmiCommandOutputV1 *guid_output,
	PgracFencedTargetV1 *resolved, int32 *native_status);
extern PgracFencedProviderResult pgrac_fenced_ipmi_readback_results(
	const PgracFencedTargetV1 *configured,
	const PgracFencedIpmiCommandOutputV1 *guid_output,
	const PgracFencedIpmiCommandOutputV1 *status_output,
	PgracFencedReadbackV1 *out);
extern PgracFencedProviderResult pgrac_fenced_ipmi_action_result(
	const PgracFencedIpmiCommandOutputV1 *action_output,
	int32 *native_status);
extern bool pgrac_fenced_ipmi_command_run_fd(
	int executable_fd, const PgracFencedIpmiInvocationV1 *invocation,
	uint64 deadline_mono_ns, PgracFencedIpmiCommandOutputV1 *out);
extern PgracFencedProviderResult pgrac_fenced_ipmi_execute_prevalidated(
	const PgracFencedTargetV1 *target, const char *username,
	const char *password_path, PgracFencedIpmiCommand command,
	uint64 deadline_mono_ns, PgracFencedIpmiCommandOutputV1 *out);
extern const PgracFencedProviderOpsV1 *pgrac_fenced_ipmi_provider_ops(void);

#endif /* PGRAC_FENCED_IPMI_H */
