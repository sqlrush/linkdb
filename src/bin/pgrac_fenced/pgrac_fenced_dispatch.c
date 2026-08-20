/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_dispatch.c
 *    Bounded live-admission owner for pgrac-fenced.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <unistd.h>

#include "pgrac_fenced_dispatch.h"
#include "pgrac_fenced_session.h"

static void
slot_close(PgracFencedDispatchSlotV1 *slot)
{
	if (slot->fd >= 0)
		(void) close(slot->fd);
	memset(slot, 0, sizeof(*slot));
	slot->fd = -1;
}

bool
pgrac_fenced_dispatch_init(
	PgracFencedDispatcherV1 *dispatcher,
	PgracFencedOperationContextV1 *context)
{
	uint32 i;

	if (dispatcher == NULL || context == NULL || !context->available)
		return false;
	memset(dispatcher, 0, sizeof(*dispatcher));
	dispatcher->context = context;
	for (i = 0; i < PGRAC_FENCED_MAX_CLIENTS; i++)
		dispatcher->clients[i].fd = -1;
	return true;
}

bool
pgrac_fenced_dispatch_accept_fd(
	PgracFencedDispatcherV1 *dispatcher,
	int client_fd,
	uint64 transport_deadline_mono_ns)
{
	PgracExternalFenceProtocolResponseV1 response;
	PgracFencedSessionDisposition disposition;
	bool capacity;
	uint32 i;

	if (dispatcher == NULL || dispatcher->context == NULL || client_fd < 0)
	{
		if (client_fd >= 0)
			(void) close(client_fd);
		return false;
	}
	capacity = pgrac_fenced_capacity_available(dispatcher->client_count,
		dispatcher->client_count);
	disposition = pgrac_fenced_session_exchange(dispatcher->context,
		client_fd, capacity, transport_deadline_mono_ns, &response);
	if (disposition == PGRAC_FENCED_SESSION_ERROR)
	{
		(void) close(client_fd);
		return false;
	}
	if (disposition == PGRAC_FENCED_SESSION_CLOSED)
	{
		(void) close(client_fd);
		return true;
	}
	for (i = 0; i < PGRAC_FENCED_MAX_CLIENTS; i++)
	{
		if (!dispatcher->clients[i].retained)
		{
			dispatcher->clients[i].fd = client_fd;
			dispatcher->clients[i].retained = true;
			dispatcher->clients[i].response = response;
			dispatcher->client_count++;
			return true;
		}
	}
	(void) pgrac_fenced_operation_invalidate(dispatcher->context, &response,
		16);
	(void) close(client_fd);
	return false;
}

bool
pgrac_fenced_dispatch_shutdown(
	PgracFencedDispatcherV1 *dispatcher,
	uint32 deny_reason)
{
	bool ok = true;
	uint32 i;

	if (dispatcher == NULL || dispatcher->context == NULL)
		return false;
	for (i = 0; i < PGRAC_FENCED_MAX_CLIENTS; i++)
	{
		if (!dispatcher->clients[i].retained)
			continue;
		if (!pgrac_fenced_operation_invalidate(dispatcher->context,
				&dispatcher->clients[i].response, deny_reason))
			ok = false;
		slot_close(&dispatcher->clients[i]);
	}
	dispatcher->client_count = 0;
	return ok;
}

bool
pgrac_fenced_dispatch_reap(
	PgracFencedDispatcherV1 *dispatcher,
	uint64 now_mono_ns)
{
	uint32 reason;
	uint32 i;

	if (dispatcher == NULL || dispatcher->context == NULL)
		return false;
	for (i = 0; i < PGRAC_FENCED_MAX_CLIENTS; i++)
	{
		if (!dispatcher->clients[i].retained ||
			!pgrac_fenced_session_retention_event(dispatcher->clients[i].fd,
				&dispatcher->clients[i].response, now_mono_ns, &reason))
			continue;
		if (!pgrac_fenced_operation_invalidate(dispatcher->context,
				&dispatcher->clients[i].response, reason))
		{
			(void) pgrac_fenced_dispatch_shutdown(dispatcher, 16);
			return false;
		}
		slot_close(&dispatcher->clients[i]);
		dispatcher->client_count--;
	}
	return true;
}
