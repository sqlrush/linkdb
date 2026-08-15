/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_provider.c
 *	  Empty production registry and exact terminal verifier.
 *
 * No production provider is selected in the current approved package.  The
 * test-only id is deliberately absent from this binary's registry.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <signal.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "pgrac_fenced_provider.h"

#define PGRAC_FENCED_WORKER_MAGIC UINT32_C(0x50465731)
#define PGRAC_FENCED_WORKER_ACTUATE UINT32_C(1)
#define PGRAC_FENCED_WORKER_READBACK UINT32_C(2)
#define PGRAC_FENCED_WORKER_TERM_GRACE_NS UINT64_C(1000000000)

typedef struct PgracFencedWorkerMessage
{
	uint32 magic;
	uint32 call_type;
	uint32 provider_result;
	int32 native_status;
	PgracFencedReadbackV1 readback;
} PgracFencedWorkerMessage;

StaticAssertDecl(sizeof(PgracFencedWorkerMessage) == 48,
				 "provider worker message size changed");

const PgracFencedProviderOpsV1 *
pgrac_fenced_provider_lookup(uint16 provider_id)
{
	(void) provider_id;
	return NULL;
}

bool
pgrac_fenced_provider_ops_valid(const PgracFencedProviderOpsV1 *ops,
							bool allow_test_only)
{
	size_t name_len;
	size_t i;

	if (ops == NULL || ops->abi_version != PGRAC_FENCED_PROVIDER_ABI_V1 ||
		ops->struct_size != sizeof(*ops) || ops->reserved0 != 0 ||
		ops->provider_id == PGRAC_FENCED_PROVIDER_ID_UNAVAILABLE ||
		ops->provider_id == UINT16_MAX ||
		(ops->provider_id == PGRAC_FENCED_PROVIDER_ID_TEST_ONLY &&
		 !allow_test_only) ||
		ops->provider_name == NULL || ops->resolve == NULL ||
		ops->actuate_off == NULL || ops->readback == NULL ||
		ops->actuate_on == NULL || ops->shutdown == NULL)
		return false;
	name_len = strlen(ops->provider_name);
	if (name_len == 0 || name_len > 31)
		return false;
	for (i = 0; i < name_len; i++)
	{
		unsigned char ch = (unsigned char) ops->provider_name[i];

		if (ch < 0x21 || ch > 0x7e)
			return false;
	}
	return true;
}

static PgracFencedProviderTerminal
provider_result_terminal(PgracFencedProviderResult result)
{
	switch (result)
	{
		case PGRAC_FENCED_PROVIDER_OK:
			return PGRAC_FENCED_PROVIDER_TERMINAL_PROVEN;
		case PGRAC_FENCED_PROVIDER_REJECTED:
			return PGRAC_FENCED_PROVIDER_TERMINAL_REJECTED;
		case PGRAC_FENCED_PROVIDER_UNAVAILABLE:
		case PGRAC_FENCED_PROVIDER_CONFIG_ERROR:
			return PGRAC_FENCED_PROVIDER_TERMINAL_UNAVAILABLE;
		case PGRAC_FENCED_PROVIDER_PENDING:
		case PGRAC_FENCED_PROVIDER_UNKNOWN:
		case PGRAC_FENCED_PROVIDER_IO_ERROR:
		default:
			return PGRAC_FENCED_PROVIDER_TERMINAL_UNKNOWN;
	}
}

static PgracFencedProviderTerminal
classify_common(PgracFencedProviderResult result,
				const uint8 expected_target_uuid[16],
				const PgracFencedReadbackV1 *readback)
{
	PgracFencedProviderTerminal terminal = provider_result_terminal(result);

	if (terminal != PGRAC_FENCED_PROVIDER_TERMINAL_PROVEN)
		return terminal;
	if (expected_target_uuid == NULL || readback == NULL)
		return PGRAC_FENCED_PROVIDER_TERMINAL_UNAVAILABLE;
	if (readback->reserved0 != 0 ||
		readback->state < PGRAC_FENCED_TARGET_OFF ||
		readback->state > PGRAC_FENCED_TARGET_UNKNOWN ||
		readback->io_drain_state > PGRAC_FENCED_IO_DRAIN_NOT_DRAINED)
		return PGRAC_FENCED_PROVIDER_TERMINAL_UNKNOWN;
	if (memcmp(readback->observed_target_uuid, expected_target_uuid, 16) != 0)
		return PGRAC_FENCED_PROVIDER_TERMINAL_REJECTED;
	return PGRAC_FENCED_PROVIDER_TERMINAL_PROVEN;
}

PgracFencedProviderTerminal
pgrac_fenced_provider_classify_recovery(
	PgracFencedProviderResult result, const uint8 expected_target_uuid[16],
	const PgracFencedReadbackV1 *readback)
{
	PgracFencedProviderTerminal terminal =
		classify_common(result, expected_target_uuid, readback);

	if (terminal != PGRAC_FENCED_PROVIDER_TERMINAL_PROVEN)
		return terminal;
	if (readback->state == PGRAC_FENCED_TARGET_OFF)
	{
		if (readback->io_drain_state == PGRAC_FENCED_IO_DRAIN_DRAINED)
			return PGRAC_FENCED_PROVIDER_TERMINAL_PROVEN;
		if (readback->io_drain_state ==
			PGRAC_FENCED_IO_DRAIN_NOT_DRAINED)
			return PGRAC_FENCED_PROVIDER_TERMINAL_REJECTED;
		return PGRAC_FENCED_PROVIDER_TERMINAL_UNKNOWN;
	}
	if (readback->state == PGRAC_FENCED_TARGET_ON)
		return PGRAC_FENCED_PROVIDER_TERMINAL_REJECTED;
	return PGRAC_FENCED_PROVIDER_TERMINAL_UNKNOWN;
}

PgracFencedProviderTerminal
pgrac_fenced_provider_classify_rejoin_on(
	PgracFencedProviderResult result, const uint8 expected_target_uuid[16],
	const PgracFencedReadbackV1 *readback)
{
	PgracFencedProviderTerminal terminal =
		classify_common(result, expected_target_uuid, readback);

	if (terminal != PGRAC_FENCED_PROVIDER_TERMINAL_PROVEN)
		return terminal;
	if (readback->state == PGRAC_FENCED_TARGET_ON)
	{
		if (readback->io_drain_state == PGRAC_FENCED_IO_DRAIN_DRAINED)
			return PGRAC_FENCED_PROVIDER_TERMINAL_PROVEN;
		if (readback->io_drain_state ==
			PGRAC_FENCED_IO_DRAIN_NOT_DRAINED)
			return PGRAC_FENCED_PROVIDER_TERMINAL_REJECTED;
		return PGRAC_FENCED_PROVIDER_TERMINAL_UNKNOWN;
	}
	if (readback->state == PGRAC_FENCED_TARGET_OFF)
		return PGRAC_FENCED_PROVIDER_TERMINAL_REJECTED;
	return PGRAC_FENCED_PROVIDER_TERMINAL_UNKNOWN;
}

static bool
monotonic_now_ns(uint64_t *out)
{
	struct timespec now;

	if (out == NULL || clock_gettime(CLOCK_MONOTONIC, &now) != 0 ||
		now.tv_sec < 0)
		return false;
	*out = (uint64_t) now.tv_sec * UINT64_C(1000000000) +
		(uint64_t) now.tv_nsec;
	return true;
}

static bool
target_valid(const PgracFencedTargetV1 *target)
{
	size_t i;
	bool nonzero = false;

	if (target == NULL || target->reserved0 != 0 ||
		target->victim_node_id < 0 || target->victim_node_id >= 128 ||
		target->mapping_generation == 0 || target->adapter_config_len > 4096 ||
		(target->adapter_config_len > 0 && target->adapter_config == NULL))
		return false;
	for (i = 0; i < sizeof(target->target_uuid); i++)
		nonzero = nonzero || target->target_uuid[i] != 0;
	return nonzero;
}

static int
deadline_poll_timeout_ms(uint64_t deadline_mono_ns)
{
	uint64_t now;
	uint64_t remaining;
	uint64_t milliseconds;

	if (!monotonic_now_ns(&now) || now >= deadline_mono_ns)
		return 0;
	remaining = deadline_mono_ns - now;
	milliseconds = (remaining + UINT64_C(999999)) / UINT64_C(1000000);
	if (milliseconds > INT_MAX)
		return INT_MAX;
	return (int) milliseconds;
}

static void
wait_worker_exit(pid_t pid)
{
	int status;
	uint64_t stop_deadline;
	uint64_t now;
	struct timespec pause = {0, 10000000};
	pid_t waited;

	if (kill(-pid, SIGTERM) != 0 && errno == ESRCH)
		(void) kill(pid, SIGTERM);
	if (!monotonic_now_ns(&now) ||
		now > UINT64_MAX - PGRAC_FENCED_WORKER_TERM_GRACE_NS)
		stop_deadline = UINT64_MAX;
	else
		stop_deadline = now + PGRAC_FENCED_WORKER_TERM_GRACE_NS;
	for (;;)
	{
		waited = waitpid(pid, &status, WNOHANG);
		if (waited == pid || (waited < 0 && errno == ECHILD))
			return;
		if (waited < 0 && errno != EINTR)
			break;
		if (!monotonic_now_ns(&now) || now >= stop_deadline)
			break;
		(void) nanosleep(&pause, NULL);
	}
	if (kill(-pid, SIGKILL) != 0 && errno == ESRCH)
		(void) kill(pid, SIGKILL);
	do
	{
		waited = waitpid(pid, &status, 0);
	} while (waited < 0 && errno == EINTR);
}

static PgracFencedProviderWorkerResult
run_worker(const PgracFencedProviderOpsV1 *ops, bool allow_test_only,
		   uint32 call_type, bool turn_on,
		   const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns,
		   PgracFencedWorkerMessage *message)
{
	struct pollfd poll_fd;
	uint64_t now;
	int pipe_fds[2] = {-1, -1};
	int status;
	int poll_rc;
	ssize_t got;
	pid_t pid;
	pid_t waited;

	if (message == NULL)
		return PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE;
	memset(message, 0, sizeof(*message));
	message->provider_result = PGRAC_FENCED_PROVIDER_UNAVAILABLE;
	if (!pgrac_fenced_provider_ops_valid(ops, allow_test_only) ||
		!target_valid(target) ||
		(call_type != PGRAC_FENCED_WORKER_ACTUATE &&
		 call_type != PGRAC_FENCED_WORKER_READBACK) ||
		!monotonic_now_ns(&now) || now >= deadline_mono_ns ||
		pipe(pipe_fds) != 0)
		return PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE;
	(void) fcntl(pipe_fds[0], F_SETFD, FD_CLOEXEC);
	(void) fcntl(pipe_fds[1], F_SETFD, FD_CLOEXEC);
	pid = fork();
	if (pid < 0)
	{
		(void) close(pipe_fds[0]);
		(void) close(pipe_fds[1]);
		return PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE;
	}
	if (pid == 0)
	{
		PgracFencedWorkerMessage child_message;
		PgracFencedProviderResult provider_result;

		(void) close(pipe_fds[0]);
		if (setpgid(0, 0) != 0)
			_exit(125);
		memset(&child_message, 0, sizeof(child_message));
		child_message.magic = PGRAC_FENCED_WORKER_MAGIC;
		child_message.call_type = call_type;
		if (call_type == PGRAC_FENCED_WORKER_ACTUATE)
		{
			provider_result = turn_on ?
				ops->actuate_on(target, deadline_mono_ns,
							&child_message.native_status) :
				ops->actuate_off(target, deadline_mono_ns,
							 &child_message.native_status);
		}
		else
			provider_result = ops->readback(target, deadline_mono_ns,
										&child_message.readback);
		child_message.provider_result = (uint32) provider_result;
		got = write(pipe_fds[1], &child_message, sizeof(child_message));
		(void) close(pipe_fds[1]);
		_exit(got == sizeof(child_message) ? 0 : 125);
	}
	(void) close(pipe_fds[1]);
	pipe_fds[1] = -1;
	if (setpgid(pid, pid) != 0 && errno != EACCES && errno != ESRCH)
	{
		(void) close(pipe_fds[0]);
		wait_worker_exit(pid);
		return PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE;
	}
	poll_fd.fd = pipe_fds[0];
	poll_fd.events = POLLIN | POLLHUP | POLLERR;
	for (;;)
	{
		poll_rc = poll(&poll_fd, 1, deadline_poll_timeout_ms(deadline_mono_ns));
		if (poll_rc >= 0 || errno != EINTR)
			break;
	}
	if (poll_rc == 0)
	{
		(void) close(pipe_fds[0]);
		wait_worker_exit(pid);
		return PGRAC_FENCED_PROVIDER_WORKER_TIMEOUT;
	}
	if (poll_rc < 0)
	{
		(void) close(pipe_fds[0]);
		wait_worker_exit(pid);
		return PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE;
	}
	do
	{
		got = read(pipe_fds[0], message, sizeof(*message));
	} while (got < 0 && errno == EINTR);
	(void) close(pipe_fds[0]);
	do
	{
		waited = waitpid(pid, &status, 0);
	} while (waited < 0 && errno == EINTR);
	if (got != sizeof(*message) || waited != pid || !WIFEXITED(status) ||
		WEXITSTATUS(status) != 0 || message->magic != PGRAC_FENCED_WORKER_MAGIC ||
		message->call_type != call_type ||
		message->provider_result > PGRAC_FENCED_PROVIDER_IO_ERROR)
	{
		memset(message, 0, sizeof(*message));
		message->provider_result = PGRAC_FENCED_PROVIDER_UNKNOWN;
		return PGRAC_FENCED_PROVIDER_WORKER_CRASHED;
	}
	return PGRAC_FENCED_PROVIDER_WORKER_OK;
}

PgracFencedProviderWorkerResult
pgrac_fenced_provider_worker_actuate(
	const PgracFencedProviderOpsV1 *ops, bool allow_test_only, bool turn_on,
	const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns,
	PgracFencedProviderResult *result, int32 *native_status)
{
	PgracFencedWorkerMessage message;
	PgracFencedProviderWorkerResult worker_result;

	if (result == NULL || native_status == NULL)
		return PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE;
	*result = PGRAC_FENCED_PROVIDER_UNAVAILABLE;
	*native_status = 0;
	worker_result = run_worker(ops, allow_test_only,
		PGRAC_FENCED_WORKER_ACTUATE, turn_on, target, deadline_mono_ns,
		&message);
	if (worker_result == PGRAC_FENCED_PROVIDER_WORKER_OK)
	{
		*result = (PgracFencedProviderResult) message.provider_result;
		*native_status = message.native_status;
	}
	else if (worker_result != PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE)
		*result = PGRAC_FENCED_PROVIDER_UNKNOWN;
	return worker_result;
}

PgracFencedProviderWorkerResult
pgrac_fenced_provider_worker_readback(
	const PgracFencedProviderOpsV1 *ops, bool allow_test_only,
	const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns,
	PgracFencedProviderResult *result, PgracFencedReadbackV1 *readback)
{
	PgracFencedWorkerMessage message;
	PgracFencedProviderWorkerResult worker_result;

	if (result == NULL || readback == NULL)
		return PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE;
	*result = PGRAC_FENCED_PROVIDER_UNAVAILABLE;
	memset(readback, 0, sizeof(*readback));
	worker_result = run_worker(ops, allow_test_only,
		PGRAC_FENCED_WORKER_READBACK, false, target, deadline_mono_ns,
		&message);
	if (worker_result == PGRAC_FENCED_PROVIDER_WORKER_OK)
	{
		*result = (PgracFencedProviderResult) message.provider_result;
		*readback = message.readback;
	}
	else if (worker_result != PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE)
		*result = PGRAC_FENCED_PROVIDER_UNKNOWN;
	return worker_result;
}
