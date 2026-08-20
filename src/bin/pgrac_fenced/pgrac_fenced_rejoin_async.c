/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_rejoin_async.c
 *    Forked PFRJ phases with parent-owned state and durable sequencing.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/wait.h>
#ifdef __linux__
#include <sys/syscall.h>
#endif
#include <unistd.h>

#include "pgrac_fenced_rejoin_async.h"

typedef enum PgracFencedRejoinAsyncMessageKind
{
	PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_APPEND = 1,
	PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_APPEND_RESULT = 2,
	PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_RESERVE_PROOF = 3,
	PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_RESERVE_PROOF_RESULT = 4,
	PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_COMPLETE = 5
} PgracFencedRejoinAsyncMessageKind;

typedef struct PgracFencedRejoinAsyncMessageV1
{
	uint32 kind;
	uint32 success;
	uint32 changed_slot;
	uint32 operation_count;
	uint64 proof_generation;
	PgracFencedJournalRecordV1 record;
	PgracFencedRejoinOperationV1 operation;
	PgracExternalFenceProtocolRejoinFrameV1 response;
} PgracFencedRejoinAsyncMessageV1;

static bool
bytes_nonzero(const uint8 *bytes, size_t len)
{
	size_t i;

	if (bytes == NULL)
		return false;
	for (i = 0; i < len; i++)
	{
		if (bytes[i] != 0)
			return true;
	}
	return false;
}

static bool
send_message(int fd, const PgracFencedRejoinAsyncMessageV1 *message)
{
	int flags = 0;
	ssize_t written;

#ifdef MSG_NOSIGNAL
	flags |= MSG_NOSIGNAL;
#endif
	do
	{
		written = send(fd, message, sizeof(*message), flags);
	} while (written < 0 && errno == EINTR);
	return written == sizeof(*message);
}

static bool
receive_message(int fd, PgracFencedRejoinAsyncMessageV1 *message)
{
	ssize_t got;

	do
	{
		got = recv(fd, message, sizeof(*message), 0);
	} while (got < 0 && errno == EINTR);
	return got == sizeof(*message);
}

static bool
child_append(void *argument, PgracFencedJournalRecordV1 *record)
{
	PgracFencedRejoinAsyncMessageV1 message;
	int fd = *(int *) argument;

	memset(&message, 0, sizeof(message));
	message.kind = PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_APPEND;
	message.record = *record;
	if (!send_message(fd, &message) || !receive_message(fd, &message) ||
		message.kind != PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_APPEND_RESULT ||
		message.success != 1)
		return false;
	*record = message.record;
	return true;
}

static bool
child_reserve_proof(void *argument, uint64 *proof_generation)
{
	PgracFencedRejoinAsyncMessageV1 message;
	int fd = *(int *) argument;

	memset(&message, 0, sizeof(message));
	message.kind = PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_RESERVE_PROOF;
	if (!send_message(fd, &message) || !receive_message(fd, &message) ||
		message.kind !=
		PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_RESERVE_PROOF_RESULT ||
		message.success != 1 || message.proof_generation == 0)
		return false;
	*proof_generation = message.proof_generation;
	return true;
}

static int
child_keep_only_ipc(int fd)
{
	struct rlimit limit;
	rlim_t upper;
	int keep = 3;
	int current;

	if (fd != keep)
	{
		if (dup2(fd, keep) < 0)
			return -1;
		(void) close(fd);
	}
	if (fcntl(keep, F_SETFD, FD_CLOEXEC) != 0 ||
		getrlimit(RLIMIT_NOFILE, &limit) != 0)
		return -1;
#if defined(__linux__) && defined(SYS_close_range)
	if (syscall(SYS_close_range, (unsigned int) (keep + 1), ~0U, 0) == 0)
		return keep;
	if (errno != ENOSYS && errno != EINVAL)
		return -1;
#endif
	upper = limit.rlim_cur;
	if (upper == RLIM_INFINITY || upper > INT_MAX)
		upper = INT_MAX;
	for (current = keep + 1; current < (int) upper; current++)
		(void) close(current);
	return keep;
}

static bool
execute_action(PgracFencedRejoinContextV1 *context,
		   PgracFencedRejoinAsyncAction action,
		   const PgracExternalFenceProtocolRejoinFrameV1 *request,
		   const uint8 operation_id[16],
		   bool target_admissions_invalidated, uint64 deadline_mono_ns,
		   PgracExternalFenceProtocolRejoinFrameV1 *response)
{
	switch (action)
	{
		case PGRAC_FENCED_REJOIN_ASYNC_ADMIN_PREPARE:
			return pgrac_fenced_rejoin_admin_prepare(context, request,
				operation_id, deadline_mono_ns, response);
		case PGRAC_FENCED_REJOIN_ASYNC_CLAIM_NEXT:
			return pgrac_fenced_rejoin_claim(context, request,
				deadline_mono_ns, response);
		case PGRAC_FENCED_REJOIN_ASYNC_AUTHORIZE_ON:
			return pgrac_fenced_rejoin_authorize_on(context, request,
				target_admissions_invalidated, deadline_mono_ns, response);
		case PGRAC_FENCED_REJOIN_ASYNC_REFRESH_ON:
			return pgrac_fenced_rejoin_refresh_on(context, request,
				deadline_mono_ns, response);
	}
	return false;
}

static void
child_main(int fd, PgracFencedOperationContextV1 operation_context,
	   PgracFencedRejoinContextV1 rejoin_context,
	   PgracFencedRejoinAsyncAction action,
	   const PgracExternalFenceProtocolRejoinFrameV1 *request,
	   const uint8 operation_id[16], bool target_admissions_invalidated,
	   uint64 deadline_mono_ns)
{
	PgracFencedRejoinAsyncMessageV1 message;
	PgracFencedRejoinContextV1 before;
	PgracExternalFenceProtocolRejoinFrameV1 response;
	uint8 frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];
	uint32 changed_slot = UINT32_MAX;
	uint32 i;

	fd = child_keep_only_ipc(fd);
	if (fd < 0)
		_exit(120);
	operation_context.journal_fd = -1;
	operation_context.journal_directory_fd = -1;
	operation_context.journal_append_hook = NULL;
	operation_context.journal_append_argument = NULL;
	operation_context.proof_reserve_hook = NULL;
	operation_context.proof_reserve_argument = NULL;
	rejoin_context.operation_context = &operation_context;
	before = rejoin_context;
	if (!pgrac_fenced_operation_set_journal_append_hook(&operation_context,
			child_append, &fd) ||
		!pgrac_fenced_operation_set_proof_reserve_hook(&operation_context,
			child_reserve_proof, &fd) ||
		!execute_action(&rejoin_context, action, request, operation_id,
			target_admissions_invalidated, deadline_mono_ns, &response) ||
		!operation_context.available ||
		!pgrac_external_fence_rejoin_v1_encode(&response, frame))
		_exit(121);
	for (i = 0; i < PGRAC_FENCED_REJOIN_MAX_OPERATIONS; i++)
	{
		if (memcmp(&before.operations[i], &rejoin_context.operations[i],
				sizeof(before.operations[i])) == 0)
			continue;
		if (changed_slot != UINT32_MAX)
			_exit(122);
		changed_slot = i;
	}
	memset(&message, 0, sizeof(message));
	message.kind = PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_COMPLETE;
	message.success = 1;
	message.changed_slot = changed_slot;
	message.operation_count = rejoin_context.operation_count;
	if (changed_slot != UINT32_MAX)
		message.operation = rejoin_context.operations[changed_slot];
	message.response = response;
	if (!send_message(fd, &message))
		_exit(123);
	(void) close(fd);
	_exit(0);
}

static bool
set_parent_fd_flags(int fd)
{
	int descriptor_flags = fcntl(fd, F_GETFD, 0);
	int status_flags = fcntl(fd, F_GETFL, 0);

	return descriptor_flags >= 0 && status_flags >= 0 &&
		fcntl(fd, F_SETFD, descriptor_flags | FD_CLOEXEC) == 0 &&
		fcntl(fd, F_SETFL, status_flags | O_NONBLOCK) == 0;
}

static bool
action_matches_request(PgracFencedRejoinAsyncAction action,
			   const PgracExternalFenceProtocolRejoinFrameV1 *request)
{
	if (request == NULL)
		return false;
	return (action == PGRAC_FENCED_REJOIN_ASYNC_ADMIN_PREPARE &&
			request->opcode == PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE) ||
		(action == PGRAC_FENCED_REJOIN_ASYNC_CLAIM_NEXT &&
			request->opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_CLAIM_NEXT) ||
		(action == PGRAC_FENCED_REJOIN_ASYNC_AUTHORIZE_ON &&
			request->opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON) ||
		(action == PGRAC_FENCED_REJOIN_ASYNC_REFRESH_ON &&
			request->opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON);
}

bool
pgrac_fenced_rejoin_async_start(
	PgracFencedOperationContextV1 *operation_context,
	const PgracFencedRejoinContextV1 *rejoin_context,
	PgracFencedRejoinAsyncAction action,
	const PgracExternalFenceProtocolRejoinFrameV1 *request,
	const uint8 operation_id[16], bool target_admissions_invalidated,
	uint64 deadline_mono_ns, PgracFencedRejoinAsyncWorkerV1 *worker)
{
	int sockets[2];
	pid_t pid;

	if (worker == NULL)
		return false;
	memset(worker, 0, sizeof(*worker));
	worker->fd = -1;
	if (operation_context == NULL || rejoin_context == NULL ||
		rejoin_context->operation_context != operation_context ||
		!operation_context->available || deadline_mono_ns == 0 ||
		!action_matches_request(action, request) ||
		(action == PGRAC_FENCED_REJOIN_ASYNC_ADMIN_PREPARE ?
			(operation_id == NULL || !bytes_nonzero(operation_id, 16)) :
			operation_id != NULL) ||
		(action == PGRAC_FENCED_REJOIN_ASYNC_AUTHORIZE_ON ?
			!target_admissions_invalidated : target_admissions_invalidated) ||
		socketpair(AF_UNIX, SOCK_DGRAM, 0, sockets) != 0)
		return false;
	pid = fork();
	if (pid < 0)
	{
		(void) close(sockets[0]);
		(void) close(sockets[1]);
		return false;
	}
	if (pid == 0)
	{
		(void) close(sockets[0]);
		child_main(sockets[1], *operation_context, *rejoin_context, action,
			request, operation_id, target_admissions_invalidated,
			deadline_mono_ns);
	}
	(void) close(sockets[1]);
	if (!set_parent_fd_flags(sockets[0]))
	{
		(void) close(sockets[0]);
		(void) kill(pid, SIGKILL);
		while (waitpid(pid, NULL, 0) < 0 && errno == EINTR)
			;
		return false;
	}
	worker->pid = pid;
	worker->fd = sockets[0];
	worker->action = action;
	worker->active = true;
	return true;
}

int
pgrac_fenced_rejoin_async_fd(
	const PgracFencedRejoinAsyncWorkerV1 *worker)
{
	return worker == NULL || !worker->active ? -1 : worker->fd;
}

static bool
reap_exact_child(PgracFencedRejoinAsyncWorkerV1 *worker)
{
	pid_t waited;

	do
	{
		waited = waitpid(worker->pid, &worker->wait_status, 0);
	} while (waited < 0 && errno == EINTR);
	return waited == worker->pid && WIFEXITED(worker->wait_status) &&
		WEXITSTATUS(worker->wait_status) == 0;
}

bool
pgrac_fenced_rejoin_async_service(
	PgracFencedOperationContextV1 *operation_context,
	PgracFencedRejoinContextV1 *rejoin_context,
	PgracFencedRejoinAsyncWorkerV1 *worker,
	PgracFencedRejoinAsyncEvent *event,
	PgracExternalFenceProtocolRejoinFrameV1 *response)
{
	PgracFencedRejoinAsyncMessageV1 message;
	bool reserved;

	if (event != NULL)
		*event = PGRAC_FENCED_REJOIN_ASYNC_NONE;
	if (operation_context == NULL || rejoin_context == NULL ||
		worker == NULL || event == NULL || response == NULL ||
		!worker->active || worker->fd < 0)
		return false;
	memset(&message, 0, sizeof(message));
	if (!receive_message(worker->fd, &message))
	{
		(void) close(worker->fd);
		worker->fd = -1;
		worker->active = false;
		(void) reap_exact_child(worker);
		return false;
	}
	if (message.kind == PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_APPEND)
	{
		reserved = pgrac_fenced_operation_append_journal(operation_context,
			&message.record);
		message.kind =
			PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_APPEND_RESULT;
		message.success = reserved ? 1 : 0;
		if (!send_message(worker->fd, &message))
			return false;
		*event = PGRAC_FENCED_REJOIN_ASYNC_JOURNAL;
		return reserved;
	}
	if (message.kind ==
		PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_RESERVE_PROOF)
	{
		reserved = pgrac_fenced_operation_reserve_proof_generation(
			operation_context, &message.proof_generation);
		message.kind =
			PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_RESERVE_PROOF_RESULT;
		message.success = reserved ? 1 : 0;
		if (!send_message(worker->fd, &message))
			return false;
		*event = PGRAC_FENCED_REJOIN_ASYNC_PROOF;
		return reserved;
	}
	if (message.kind != PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_COMPLETE ||
		message.success != 1 ||
		message.operation_count > PGRAC_FENCED_REJOIN_MAX_OPERATIONS ||
		(message.changed_slot != UINT32_MAX &&
		 message.changed_slot >= PGRAC_FENCED_REJOIN_MAX_OPERATIONS))
		return false;
	if (message.changed_slot != UINT32_MAX)
		rejoin_context->operations[message.changed_slot] = message.operation;
	rejoin_context->operation_count = message.operation_count;
	*response = message.response;
	(void) close(worker->fd);
	worker->fd = -1;
	worker->active = false;
	if (!reap_exact_child(worker))
		return false;
	worker->pid = 0;
	*event = PGRAC_FENCED_REJOIN_ASYNC_COMPLETE;
	return true;
}
