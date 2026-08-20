/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_session.c
 *    Authenticated exact-frame DB session boundary for pgrac-fenced.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "pgrac_fenced_core.h"
#include "pgrac_fenced_session.h"

static bool
monotonic_now_ns(uint64 *out)
{
	struct timespec now;

	if (out == NULL || clock_gettime(CLOCK_MONOTONIC, &now) != 0 ||
		now.tv_sec < 0)
		return false;
	*out = (uint64) now.tv_sec * UINT64_C(1000000000) +
		(uint64) now.tv_nsec;
	return *out != 0;
}

static bool
wait_ready(int fd, short events, uint64 deadline_mono_ns)
{
	struct pollfd descriptor;
	uint64 now;
	uint64 remaining_ns;
	int timeout_ms;
	int rc;

	for (;;)
	{
		if (!monotonic_now_ns(&now) || now >= deadline_mono_ns)
			return false;
		remaining_ns = deadline_mono_ns - now;
		timeout_ms = (int) Min(UINT64_C(2147483647),
			(remaining_ns + UINT64_C(999999)) / UINT64_C(1000000));
		descriptor.fd = fd;
		descriptor.events = events;
		descriptor.revents = 0;
		rc = poll(&descriptor, 1, timeout_ms);
		if (rc < 0 && errno == EINTR)
			continue;
		if (rc <= 0 || (descriptor.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0)
			return false;
		return (descriptor.revents & events) != 0;
	}
}

static bool
write_exact(int fd, const uint8 *bytes, size_t len, uint64 deadline_mono_ns)
{
	size_t used = 0;

	while (used < len)
	{
		int flags = MSG_DONTWAIT;
		ssize_t written;

#ifdef MSG_NOSIGNAL
		flags |= MSG_NOSIGNAL;
#endif
		written = send(fd, bytes + used, len - used, flags);
		if (written > 0)
		{
			used += (size_t) written;
			continue;
		}
		if (written < 0 && errno == EINTR)
			continue;
		if (written >= 0 || (errno != EAGAIN && errno != EWOULDBLOCK))
			return false;
		if (!wait_ready(fd, POLLOUT, deadline_mono_ns))
			return false;
	}
	return true;
}

static bool
configure_client_fd(int fd)
{
	int descriptor_flags;
	int status_flags;

	status_flags = fcntl(fd, F_GETFL, 0);
	descriptor_flags = fcntl(fd, F_GETFD, 0);
	if (status_flags < 0 || descriptor_flags < 0 ||
		fcntl(fd, F_SETFL, status_flags | O_NONBLOCK) != 0 ||
		fcntl(fd, F_SETFD, descriptor_flags | FD_CLOEXEC) != 0)
		return false;
#ifdef SO_NOSIGPIPE
	{
		int enabled = 1;

		if (setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled,
				sizeof(enabled)) != 0)
			return false;
	}
#endif
	return true;
}

bool
pgrac_fenced_session_prepare_client(
	PgracFencedOperationContextV1 *context,
	int client_fd)
{
	PgracFencedPeerCredential peer;

	return context != NULL && context->config != NULL && client_fd >= 0 &&
		configure_client_fd(client_fd) &&
		pgrac_fenced_peer_credential_get(client_fd, &peer) &&
		pgrac_fenced_peer_is_db(&peer, context->config->allowed_db_uid,
			context->config->allowed_db_gid);
}

static bool
frame_has_no_immediate_suffix(int fd)
{
	uint8 extra;
	ssize_t got;

	do
	{
		got = recv(fd, &extra, 1, MSG_DONTWAIT | MSG_PEEK);
	} while (got < 0 && errno == EINTR);
	if (got > 0 || got == 0)
		return false;
	return errno == EAGAIN || errno == EWOULDBLOCK;
}

PgracFencedSessionDisposition
pgrac_fenced_session_exchange(
	PgracFencedOperationContextV1 *context,
	int client_fd,
	bool capacity_available,
	uint64 transport_deadline_mono_ns,
	PgracExternalFenceProtocolResponseV1 *response)
{
	PgracExternalFenceProtocolRequestV1 request;
	uint64 operation_deadline;

	if (response == NULL ||
		!pgrac_fenced_session_receive_request(context, client_fd,
			transport_deadline_mono_ns, &request, &operation_deadline))
		return PGRAC_FENCED_SESSION_ERROR;
	if (!capacity_available)
	{
		memset(response, 0, sizeof(*response));
		response->verdict = 4;
		memcpy(response->request_nonce, request.request_nonce,
			sizeof(response->request_nonce));
		memcpy(response->daemon_boot_id, context->daemon_boot_id,
			sizeof(response->daemon_boot_id));
		response->provider_id = context->provider->provider_id;
		response->provider_abi_version = PGRAC_FENCED_PROVIDER_ABI_V1;
		response->provider_result = PGRAC_FENCED_PROVIDER_UNAVAILABLE;
		response->deny_reason = 10;
	}
	else if (!pgrac_fenced_operation_acquire(context, &request,
			operation_deadline, response) ||
		response->verdict == 0)
		return PGRAC_FENCED_SESSION_ERROR;
	if (!pgrac_fenced_session_send_response(client_fd, response,
			operation_deadline))
		return PGRAC_FENCED_SESSION_ERROR;
	return response->verdict == 1 ? PGRAC_FENCED_SESSION_RETAINED :
		PGRAC_FENCED_SESSION_CLOSED;
}

bool
pgrac_fenced_session_receive_request(
	PgracFencedOperationContextV1 *context,
	int client_fd,
	uint64 transport_deadline_mono_ns,
	PgracExternalFenceProtocolRequestV1 *request,
	uint64 *operation_deadline_mono_ns)
{
	uint8 request_frame[PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES];
	PgracFencedSessionReceiveProgress progress;
	size_t used = 0;

	if (!pgrac_fenced_session_prepare_client(context, client_fd))
		return false;
	for (;;)
	{
		progress = pgrac_fenced_session_receive_progress(context, client_fd,
			transport_deadline_mono_ns, request_frame, &used, request,
			operation_deadline_mono_ns);
		if (progress == PGRAC_FENCED_SESSION_RECEIVE_COMPLETE)
			return true;
		if (progress == PGRAC_FENCED_SESSION_RECEIVE_ERROR ||
			!wait_ready(client_fd, POLLIN, transport_deadline_mono_ns))
			return false;
	}
}

PgracFencedSessionReceiveProgress
pgrac_fenced_session_receive_progress(
	PgracFencedOperationContextV1 *context,
	int client_fd,
	uint64 transport_deadline_mono_ns,
	uint8 request_frame[PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES],
	size_t *request_frame_used,
	PgracExternalFenceProtocolRequestV1 *request,
	uint64 *operation_deadline_mono_ns)
{
	uint64 request_timeout_ns;
	uint64 now;

	if (context == NULL || context->config == NULL || client_fd < 0 ||
		request_frame == NULL || request_frame_used == NULL ||
		*request_frame_used > PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES ||
		request == NULL || operation_deadline_mono_ns == NULL ||
		!monotonic_now_ns(&now) || now >= transport_deadline_mono_ns)
		return PGRAC_FENCED_SESSION_RECEIVE_ERROR;
	while (*request_frame_used < PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES)
	{
		ssize_t got = recv(client_fd, request_frame + *request_frame_used,
			PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES - *request_frame_used,
			MSG_DONTWAIT);

		if (got > 0)
		{
			*request_frame_used += (size_t) got;
			continue;
		}
		if (got == 0)
			return PGRAC_FENCED_SESSION_RECEIVE_ERROR;
		if (errno == EINTR)
			continue;
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			return PGRAC_FENCED_SESSION_RECEIVE_PENDING;
		return PGRAC_FENCED_SESSION_RECEIVE_ERROR;
	}
	if (!frame_has_no_immediate_suffix(client_fd) ||
		!pgrac_external_fence_request_v1_decode(request_frame,
			PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES, request) ||
		!monotonic_now_ns(&now))
		return PGRAC_FENCED_SESSION_RECEIVE_ERROR;
	request_timeout_ns = (uint64) request->timeout_ms * UINT64_C(1000000);
	if (UINT64_MAX - now < request_timeout_ns)
		return PGRAC_FENCED_SESSION_RECEIVE_ERROR;
	*operation_deadline_mono_ns = now + request_timeout_ns;
	if (*operation_deadline_mono_ns > transport_deadline_mono_ns)
		*operation_deadline_mono_ns = transport_deadline_mono_ns;
	return now < *operation_deadline_mono_ns ?
		PGRAC_FENCED_SESSION_RECEIVE_COMPLETE :
		PGRAC_FENCED_SESSION_RECEIVE_ERROR;
}

bool
pgrac_fenced_session_send_response(
	int client_fd,
	const PgracExternalFenceProtocolResponseV1 *response,
	uint64 deadline_mono_ns)
{
	uint8 response_frame[PGRAC_EXTERNAL_FENCE_RESPONSE_V1_BYTES];

	return client_fd >= 0 && response != NULL &&
		pgrac_external_fence_response_v1_encode(response, response_frame) &&
		write_exact(client_fd, response_frame, sizeof(response_frame),
			deadline_mono_ns);
}

bool
pgrac_fenced_session_retention_event(
	int client_fd,
	const PgracExternalFenceProtocolResponseV1 *response,
	uint64 now_mono_ns,
	uint32 *deny_reason)
{
	struct pollfd descriptor;
	int rc;

	if (deny_reason == NULL)
		return true;
	*deny_reason = 0;
	if (client_fd < 0 || response == NULL || response->verdict != 1 ||
		response->verified_mono_ns == 0 ||
		response->fresh_until_mono_ns <= response->verified_mono_ns ||
		now_mono_ns < response->verified_mono_ns ||
		now_mono_ns >= response->fresh_until_mono_ns)
	{
		*deny_reason = 14;
		return true;
	}
	for (;;)
	{
		descriptor.fd = client_fd;
		descriptor.events = POLLIN | POLLHUP | POLLERR;
		descriptor.revents = 0;
		rc = poll(&descriptor, 1, 0);
		if (rc < 0 && errno == EINTR)
			continue;
		break;
	}
	if (rc == 0)
		return false;
	*deny_reason = 16;
	return true;
}
