/*-------------------------------------------------------------------------
 *
 * pgrac-fencedctl.c
 *	  Root-only provider-neutral pgrac-fenced administration client.
 *
 * The current provider-0 package has no operational daemon socket.  This
 * client therefore preserves UNAVAILABLE while still fixing the journal
 * verifier and ADMIN_PREPARE ABI used by a future certified deployment.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/un.h>
#include <unistd.h>

#include "pgrac_fenced_config.h"
#include "pgrac_fenced_core.h"
#include "pgrac_fenced_ctl.h"

#define PGRAC_FENCED_CTL_DEFAULT_TIMEOUT_MS UINT32_C(120000)
#define PGRAC_FENCED_CTL_STATUS_TIMEOUT_MS UINT32_C(1000)
#define PGRAC_FENCED_CTL_REJOIN_OFFERED UINT32_C(1)

static const char *progname;

static void
usage(void)
{
	printf("%s status --json\n", progname);
	printf("%s verify-journal PATH\n", progname);
	printf("%s prepare-rejoin NODE OLD_INCARNATION CANDIDATE_INCARNATION "
		   "[--timeout-ms MILLISECONDS]\n", progname);
}

static bool
parse_uint64_decimal(const char *text, uint64 maximum, uint64 *value)
{
	uint64 parsed = 0;
	const unsigned char *cursor = (const unsigned char *) text;

	if (text == NULL || value == NULL || *cursor == '\0')
		return false;
	for (; *cursor != '\0'; cursor++)
	{
		uint32 digit;

		if (*cursor < '0' || *cursor > '9')
			return false;
		digit = (uint32) (*cursor - '0');
		if (parsed > (maximum - digit) / 10)
			return false;
		parsed = parsed * 10 + digit;
	}
	*value = parsed;
	return true;
}

static bool
bytes_all_zero(const uint8 *bytes, size_t len)
{
	size_t i;

	for (i = 0; i < len; i++)
		if (bytes[i] != 0)
			return false;
	return true;
}

static bool
set_socket_timeout(int fd, uint32 timeout_ms)
{
	struct timeval timeout;

	timeout.tv_sec = (time_t) (timeout_ms / 1000);
	timeout.tv_usec = (suseconds_t) ((timeout_ms % 1000) * 1000);
	return setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
					  sizeof(timeout)) == 0 &&
		setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout,
				   sizeof(timeout)) == 0;
}

static int
connect_admin_socket(uint32 timeout_ms)
{
	struct sockaddr_un address;
	PgracFencedPeerCredential peer;
	size_t path_len = strlen(PGRAC_FENCED_ADMIN_SOCKET_PATH);
	int fd;

	if (path_len >= sizeof(address.sun_path))
		return -1;
	fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (fd < 0)
		return -1;
	memset(&address, 0, sizeof(address));
	address.sun_family = AF_UNIX;
	memcpy(address.sun_path, PGRAC_FENCED_ADMIN_SOCKET_PATH, path_len + 1);
	if (!set_socket_timeout(fd, timeout_ms) ||
		connect(fd, (struct sockaddr *) &address, sizeof(address)) != 0 ||
		!pgrac_fenced_peer_credential_get(fd, &peer) ||
		!pgrac_fenced_peer_is_root(&peer))
	{
		(void) close(fd);
		return -1;
	}
	return fd;
}

static bool
write_all(int fd, const uint8 *bytes, size_t len)
{
	size_t used = 0;

	while (used < len)
	{
		ssize_t written = write(fd, bytes + used, len - used);

		if (written < 0 && errno == EINTR)
			continue;
		if (written <= 0)
			return false;
		used += (size_t) written;
	}
	return true;
}

static bool
read_all(int fd, uint8 *bytes, size_t len)
{
	size_t used = 0;

	while (used < len)
	{
		ssize_t got = read(fd, bytes + used, len - used);

		if (got < 0 && errno == EINTR)
			continue;
		if (got <= 0)
			return false;
		used += (size_t) got;
	}
	return true;
}

static bool
socket_has_no_buffered_extra(int fd)
{
	uint8 extra;
	ssize_t got;

	do
		got = recv(fd, &extra, 1, MSG_PEEK | MSG_DONTWAIT);
	while (got < 0 && errno == EINTR);
	return got == 0 || (got < 0 && (errno == EAGAIN || errno == EWOULDBLOCK));
}

static void
hex_encode(const uint8 *bytes, size_t len, char *out)
{
	static const char digits[] = "0123456789abcdef";
	size_t i;

	for (i = 0; i < len; i++)
	{
		out[i * 2] = digits[bytes[i] >> 4];
		out[i * 2 + 1] = digits[bytes[i] & 0x0f];
	}
	out[len * 2] = '\0';
}

static int
status_command(void)
{
	int fd = connect_admin_socket(PGRAC_FENCED_CTL_STATUS_TIMEOUT_MS);

	if (fd < 0)
	{
		printf("{\"status\":\"UNAVAILABLE\","
			   "\"reason\":\"admin_socket_unavailable\"}\n");
		return PGRAC_FENCED_CTL_EXIT_UNAVAILABLE;
	}
	(void) close(fd);
	/* The frozen v1 ABI defines ADMIN_PREPARE but no status request frame. */
	printf("{\"status\":\"UNAVAILABLE\","
		   "\"reason\":\"status_protocol_unavailable\"}\n");
	return PGRAC_FENCED_CTL_EXIT_UNAVAILABLE;
}

static int
verify_journal_command(const char *path)
{
	PgracFencedCtlJournalSummaryV1 summary;
	struct stat before;
	struct stat after;
	uint8 *bytes = NULL;
	char tail_hex[PGRAC_FENCED_JOURNAL_DIGEST_BYTES * 2 + 1];
	size_t size;
	int fd = -1;
	int rc = PGRAC_FENCED_CTL_EXIT_UNAVAILABLE;

	fd = open(path, O_RDONLY | O_NOFOLLOW | PG_BINARY);
	if (fd < 0 || fstat(fd, &before) != 0 ||
		!pgrac_fenced_ctl_journal_stat_secure(&before))
		goto done;
	size = (size_t) before.st_size;
	if (size > 0)
	{
		bytes = (uint8 *) malloc(size);
		if (bytes == NULL || !read_all(fd, bytes, size))
			goto done;
	}
	if (fstat(fd, &after) != 0 ||
		before.st_dev != after.st_dev || before.st_ino != after.st_ino ||
		before.st_size != after.st_size || before.st_mode != after.st_mode ||
		before.st_uid != after.st_uid || before.st_gid != after.st_gid ||
		!pgrac_fenced_ctl_journal_scan(bytes, size, &summary))
		goto done;
	hex_encode(summary.tail_digest, sizeof(summary.tail_digest), tail_hex);
	printf("{\"status\":\"OK\",\"records\":%u,"
		   "\"first_seq\":" UINT64_FORMAT ","
		   "\"last_seq\":" UINT64_FORMAT ","
		   "\"tail_digest\":\"%s\"}\n",
		   summary.record_count, summary.first_seq, summary.last_seq,
		   tail_hex);
	rc = PGRAC_FENCED_CTL_EXIT_OK;

done:
	if (bytes != NULL)
	{
		memset(bytes, 0, size);
		free(bytes);
	}
	if (fd >= 0)
		(void) close(fd);
	if (rc != PGRAC_FENCED_CTL_EXIT_OK)
		fprintf(stderr, "%s: journal is unavailable or invalid\n", progname);
	return rc;
}

static int
prepare_rejoin_command(int argc, char **argv)
{
	PgracFencedCtlPrepareRejoinV1 request;
	PgracExternalFenceProtocolRejoinFrameV1 response;
	uint8 nonce[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES];
	uint8 request_frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];
	uint8 response_frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];
	char operation_hex[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES * 2 + 1];
	uint64 parsed;
	int fd = -1;
	int rc = PGRAC_FENCED_CTL_EXIT_UNAVAILABLE;

	memset(&request, 0, sizeof(request));
	if ((argc != 5 && argc != 7) ||
		!parse_uint64_decimal(argv[2], PGRAC_FENCED_MAX_NODES - 1, &parsed))
		return PGRAC_FENCED_CTL_EXIT_USAGE;
	request.node_id = (int32) parsed;
	if (!parse_uint64_decimal(argv[3], UINT64_MAX, &request.old_incarnation) ||
		!parse_uint64_decimal(argv[4], UINT64_MAX,
			&request.candidate_incarnation))
		return PGRAC_FENCED_CTL_EXIT_USAGE;
	request.timeout_ms = PGRAC_FENCED_CTL_DEFAULT_TIMEOUT_MS;
	if (argc == 7)
	{
		if (strcmp(argv[5], "--timeout-ms") != 0 ||
			!parse_uint64_decimal(argv[6],
				PGRAC_EXTERNAL_FENCE_TIMEOUT_MAX_MS, &parsed))
			return PGRAC_FENCED_CTL_EXIT_USAGE;
		request.timeout_ms = (uint32) parsed;
	}
	if (!pgrac_fenced_ctl_prepare_rejoin_valid(&request) ||
		!pg_strong_random(nonce, sizeof(nonce)) ||
		bytes_all_zero(nonce, sizeof(nonce)) ||
		!pgrac_fenced_ctl_prepare_rejoin_frame(&request, nonce,
			request_frame))
		goto done;
	fd = connect_admin_socket(request.timeout_ms);
	if (fd < 0 || !write_all(fd, request_frame, sizeof(request_frame)) ||
		!read_all(fd, response_frame, sizeof(response_frame)) ||
		!socket_has_no_buffered_extra(fd) ||
		!pgrac_fenced_ctl_prepare_rejoin_response(&request, nonce,
			response_frame, &response))
		goto done;
	if (response.status != PGRAC_FENCED_CTL_REJOIN_OFFERED)
	{
		fprintf(stderr, "%s: prepare-rejoin unavailable "
				"(status=%u reason=%u)\n", progname,
				response.status, response.deny_reason);
		goto done;
	}
	hex_encode(response.operation_id, sizeof(response.operation_id),
		operation_hex);
	printf("%s\n", operation_hex);
	rc = PGRAC_FENCED_CTL_EXIT_OK;

done:
	if (fd >= 0)
		(void) close(fd);
	memset(nonce, 0, sizeof(nonce));
	memset(request_frame, 0, sizeof(request_frame));
	memset(response_frame, 0, sizeof(response_frame));
	memset(&response, 0, sizeof(response));
	if (rc != PGRAC_FENCED_CTL_EXIT_OK && fd < 0)
		fprintf(stderr, "%s: admin socket is unavailable\n", progname);
	return rc;
}

int
main(int argc, char **argv)
{
	int rc;

	progname = get_progname(argv[0]);
	if (argc == 2 && (strcmp(argv[1], "--help") == 0 ||
					strcmp(argv[1], "-?") == 0))
	{
		usage();
		return PGRAC_FENCED_CTL_EXIT_OK;
	}
	if (geteuid() != 0)
	{
		fprintf(stderr, "%s: must run as root\n", progname);
		return PGRAC_FENCED_CTL_EXIT_UNAVAILABLE;
	}
	(void) signal(SIGPIPE, SIG_IGN);
	if (argc == 3 && strcmp(argv[1], "status") == 0 &&
		strcmp(argv[2], "--json") == 0)
		return status_command();
	if (argc == 3 && strcmp(argv[1], "verify-journal") == 0)
		return verify_journal_command(argv[2]);
	if (argc >= 2 && strcmp(argv[1], "prepare-rejoin") == 0)
	{
		rc = prepare_rejoin_command(argc, argv);
		if (rc == PGRAC_FENCED_CTL_EXIT_USAGE)
			usage();
		return rc;
	}
	usage();
	return PGRAC_FENCED_CTL_EXIT_USAGE;
}
