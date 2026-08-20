/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_coordinator.c
 *    Socket, target scheduler and async-operation owner.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "pgrac_fenced_coordinator.h"
#include "pgrac_fenced_session.h"

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

static int
rejoin_target_slot(const PgracFencedCoordinatorV1 *coordinator,
			   const uint8 target_uuid[16])
{
	uint32 i;

	for (i = 0; i < PGRAC_FENCED_MAX_OPERATIONS; i++)
	{
		if (coordinator->rejoin_target_used[i] &&
			memcmp(coordinator->rejoin_targets[i], target_uuid,
				sizeof(coordinator->rejoin_targets[i])) == 0)
			return (int) i;
	}
	return -1;
}

static bool
rejoin_target_reserved(const PgracFencedCoordinatorV1 *coordinator,
				   const uint8 target_uuid[16])
{
	return coordinator != NULL && target_uuid != NULL &&
		rejoin_target_slot(coordinator, target_uuid) >= 0;
}

static void
client_clear(PgracFencedCoordinatorV1 *coordinator, uint32 slot)
{
	PgracFencedCoordinatorClientV1 *client = &coordinator->clients[slot];

	if (client->fd >= 0)
		(void) close(client->fd);
	if (client->state != PGRAC_FENCED_COORDINATOR_CLIENT_UNUSED)
		coordinator->client_count--;
	memset(client, 0, sizeof(*client));
	client->fd = -1;
}

static int
client_allocate(PgracFencedCoordinatorV1 *coordinator)
{
	uint32 i;

	if (coordinator->client_count >= PGRAC_FENCED_MAX_CLIENTS)
		return -1;
	for (i = 0; i < PGRAC_FENCED_MAX_CLIENTS; i++)
	{
		if (coordinator->clients[i].state ==
			PGRAC_FENCED_COORDINATOR_CLIENT_UNUSED)
			return (int) i;
	}
	return -1;
}

static void
capacity_response(PgracFencedCoordinatorV1 *coordinator,
				  const PgracExternalFenceProtocolRequestV1 *request,
				  PgracExternalFenceProtocolResponseV1 *response)
{
	memset(response, 0, sizeof(*response));
	response->verdict = 4;
	memcpy(response->request_nonce, request->request_nonce,
		sizeof(response->request_nonce));
	memcpy(response->daemon_boot_id, coordinator->context->daemon_boot_id,
		sizeof(response->daemon_boot_id));
	response->provider_id = coordinator->context->provider->provider_id;
	response->provider_abi_version = PGRAC_FENCED_PROVIDER_ABI_V1;
	response->provider_result = PGRAC_FENCED_PROVIDER_UNAVAILABLE;
	response->deny_reason = 10;
}

static bool coordinator_start_ticket(
	PgracFencedCoordinatorV1 *coordinator,
	const PgracFencedScheduleTicketV1 *ticket);

static bool
coordinator_release_ticket(PgracFencedCoordinatorV1 *coordinator,
				   const PgracFencedScheduleTicketV1 *ticket)
{
	PgracFencedScheduleTicketV1 started;

	if (!pgrac_fenced_schedule_release(&coordinator->schedule, ticket,
			&started))
		return false;
	return started.serial == 0 || coordinator_start_ticket(coordinator,
		&started);
}

/*
 * A rejoin reservation must not start the next scalar operation for the same
 * target.  Release an empty active ticket, but deliberately leave any FIFO
 * successor active without a worker; the reservation pass will cancel it.
 */
static bool
coordinator_release_empty_reserved_ticket(
	PgracFencedCoordinatorV1 *coordinator,
	const PgracFencedScheduleTicketV1 *ticket)
{
	PgracFencedScheduleSnapshotV1 snapshot;
	PgracFencedScheduleTicketV1 started;

	if (!pgrac_fenced_schedule_snapshot(&coordinator->schedule, ticket,
			&snapshot))
		return true;
	if (!snapshot.active || snapshot.client_count != 0)
		return true;
	if (ticket->slot >= PGRAC_FENCED_MAX_OPERATIONS ||
		coordinator->operations[ticket->slot].worker_active)
		return true;
	return pgrac_fenced_schedule_release(&coordinator->schedule, ticket,
		&started);
}

static bool
coordinator_start_ticket(PgracFencedCoordinatorV1 *coordinator,
				 const PgracFencedScheduleTicketV1 *ticket)
{
	PgracFencedScheduledClientV1 scheduled[PGRAC_FENCED_MAX_CLIENTS];
	PgracFencedCoordinatorOperationV1 *operation;
	PgracFencedCoordinatorClientV1 *owner;
	uint32 count;
	int owner_id;

	if (ticket == NULL || ticket->serial == 0 ||
		ticket->slot >= PGRAC_FENCED_MAX_OPERATIONS ||
		!pgrac_fenced_schedule_copy_clients(&coordinator->schedule, ticket,
			scheduled, lengthof(scheduled), &count) || count == 0)
		return false;
	owner_id = scheduled[0].client_id;
	if (owner_id < 0 || owner_id >= PGRAC_FENCED_MAX_CLIENTS)
		return false;
	owner = &coordinator->clients[owner_id];
	operation = &coordinator->operations[ticket->slot];
	if (owner->state != PGRAC_FENCED_COORDINATOR_CLIENT_WAITING ||
		operation->worker_active)
		return false;
	memset(operation, 0, sizeof(*operation));
	operation->worker.fd = -1;
	operation->ticket = *ticket;
	operation->owner_client_id = owner_id;
	if (!pgrac_fenced_async_start_preaccepted(coordinator->context,
			&owner->request, &owner->prepared, owner->deadline_mono_ns,
			&operation->worker))
		return false;
	operation->worker_active = true;
	return true;
}

bool
pgrac_fenced_coordinator_init(PgracFencedCoordinatorV1 *coordinator,
					  PgracFencedOperationContextV1 *context)
{
	uint32 i;

	if (coordinator == NULL || context == NULL || !context->available)
		return false;
	memset(coordinator, 0, sizeof(*coordinator));
	coordinator->context = context;
	if (!pgrac_fenced_schedule_init(&coordinator->schedule))
		return false;
	for (i = 0; i < PGRAC_FENCED_MAX_CLIENTS; i++)
		coordinator->clients[i].fd = -1;
	for (i = 0; i < PGRAC_FENCED_MAX_OPERATIONS; i++)
		coordinator->operations[i].worker.fd = -1;
	return true;
}

static bool
coordinator_submit_request(PgracFencedCoordinatorV1 *coordinator,
					   uint32 slot,
					   uint64 operation_deadline)
{
	PgracExternalFenceProtocolResponseV1 response;
	PgracFencedPreparedAcquireV1 prepared;
	PgracFencedOperationAcceptResult accepted;
	PgracFencedScheduleResult scheduled;
	PgracFencedScheduleTicketV1 ticket;
	PgracFencedCoordinatorClientV1 *client = &coordinator->clients[slot];

	if (client->state != PGRAC_FENCED_COORDINATOR_CLIENT_INGRESS)
		return false;
	if (pgrac_fenced_schedule_operation_count(
			&coordinator->schedule) >= PGRAC_FENCED_MAX_OPERATIONS)
	{
		capacity_response(coordinator, &client->request, &response);
		(void) pgrac_fenced_session_send_response(client->fd, &response,
			operation_deadline);
		client_clear(coordinator, slot);
		return true;
	}
	accepted = pgrac_fenced_operation_accept(coordinator->context,
		&client->request,
		operation_deadline, &prepared, &response);
	if (accepted == PGRAC_FENCED_OPERATION_ERROR)
	{
		client_clear(coordinator, slot);
		return false;
	}
	if (accepted == PGRAC_FENCED_OPERATION_COMPLETE)
	{
		(void) pgrac_fenced_session_send_response(client->fd, &response,
			operation_deadline);
		client_clear(coordinator, slot);
		return coordinator->context->available;
	}
	if (rejoin_target_reserved(coordinator, prepared.target.target_uuid))
	{
		if (!pgrac_fenced_operation_cancel_preaccepted(coordinator->context,
				&client->request, &prepared, 19, &response))
			return false;
		(void) pgrac_fenced_session_send_response(client->fd, &response,
			operation_deadline);
		client_clear(coordinator, slot);
		return coordinator->context->available;
	}
	scheduled = pgrac_fenced_schedule_submit(&coordinator->schedule, slot,
		prepared.target.target_uuid, prepared.binding_digest,
		operation_deadline, &ticket);
	if (scheduled == PGRAC_FENCED_SCHEDULE_DENY)
	{
		capacity_response(coordinator, &client->request, &response);
		(void) pgrac_fenced_session_send_response(client->fd, &response,
			operation_deadline);
		client_clear(coordinator, slot);
		return true;
	}
	client->state = PGRAC_FENCED_COORDINATOR_CLIENT_WAITING;
	client->deadline_mono_ns = operation_deadline;
	client->ticket = ticket;
	client->prepared = prepared;
	if (scheduled == PGRAC_FENCED_SCHEDULE_START &&
		!coordinator_start_ticket(coordinator, &ticket))
		return false;
	return true;
}

bool
pgrac_fenced_coordinator_accept_fd(PgracFencedCoordinatorV1 *coordinator,
					   int client_fd,
					   uint64 transport_deadline_mono_ns)
{
	PgracFencedCoordinatorClientV1 *client;
	PgracFencedSessionReceiveProgress ingress;
	uint64 operation_deadline;
	int slot;

	if (coordinator == NULL || coordinator->context == NULL ||
		coordinator->quiescing || client_fd < 0 ||
		transport_deadline_mono_ns == 0)
	{
		if (client_fd >= 0)
			(void) close(client_fd);
		return false;
	}
	slot = client_allocate(coordinator);
	if (slot < 0 || !pgrac_fenced_session_prepare_client(
			coordinator->context, client_fd))
	{
		(void) close(client_fd);
		return slot < 0;
	}
	client = &coordinator->clients[slot];
	memset(client, 0, sizeof(*client));
	client->fd = client_fd;
	client->state = PGRAC_FENCED_COORDINATOR_CLIENT_INGRESS;
	client->deadline_mono_ns = transport_deadline_mono_ns;
	coordinator->client_count++;
	ingress = pgrac_fenced_session_receive_progress(coordinator->context,
		client->fd, client->deadline_mono_ns, client->request_frame,
		&client->request_frame_used, &client->request, &operation_deadline);
	if (ingress == PGRAC_FENCED_SESSION_RECEIVE_PENDING)
		return true;
	if (ingress == PGRAC_FENCED_SESSION_RECEIVE_ERROR)
	{
		client_clear(coordinator, (uint32) slot);
		return false;
	}
	return coordinator_submit_request(coordinator, (uint32) slot,
		operation_deadline);
}

static bool
coordinator_send_operation_result(PgracFencedCoordinatorV1 *coordinator,
					  PgracFencedCoordinatorOperationV1 *operation,
					  const PgracExternalFenceProtocolResponseV1 *source)
{
	PgracFencedScheduledClientV1 scheduled[PGRAC_FENCED_MAX_CLIENTS];
	PgracExternalFenceProtocolResponseV1 response;
	PgracFencedCoordinatorClientV1 *client;
	uint32 count;
	uint32 i;
	int client_id;
	bool positive = source->verdict == 1;
	bool sent;

	if (!pgrac_fenced_schedule_copy_clients(&coordinator->schedule,
			&operation->ticket, scheduled, lengthof(scheduled), &count))
		return false;
	if (positive && operation->owner_client_id >= 0 &&
		operation->owner_client_id < PGRAC_FENCED_MAX_CLIENTS &&
		rejoin_target_reserved(coordinator,
			coordinator->clients[operation->owner_client_id].prepared.target.
			target_uuid))
	{
		for (i = 0; i < count; i++)
		{
			PgracFencedScheduleTicketV1 ignored;

			client_id = scheduled[i].client_id;
			if (client_id < 0 || client_id >= PGRAC_FENCED_MAX_CLIENTS)
				return false;
			client = &coordinator->clients[client_id];
			if (client->state != PGRAC_FENCED_COORDINATOR_CLIENT_WAITING)
				continue;
			if (!pgrac_fenced_operation_cancel_preaccepted(
					coordinator->context, &client->request, &client->prepared,
					19, &response))
				return false;
			(void) pgrac_fenced_session_send_response(client->fd, &response,
				client->deadline_mono_ns);
			if (!pgrac_fenced_schedule_cancel_client(&coordinator->schedule,
					client_id, &ignored))
				return false;
			client_clear(coordinator, (uint32) client_id);
		}
		return coordinator_release_empty_reserved_ticket(coordinator,
			&operation->ticket);
	}
	if (positive && !pgrac_fenced_schedule_set_state(&coordinator->schedule,
			&operation->ticket, PGRAC_FENCED_STATE_PROVEN_DURABLE))
		return false;
	for (i = 0; i < count; i++)
	{
		client_id = scheduled[i].client_id;
		if (client_id < 0 || client_id >= PGRAC_FENCED_MAX_CLIENTS)
			return false;
		client = &coordinator->clients[client_id];
		if (client->state != PGRAC_FENCED_COORDINATOR_CLIENT_WAITING)
			continue;
		if (positive && client_id != operation->owner_client_id)
		{
			if (!pgrac_fenced_operation_serve_joiner(coordinator->context,
					&client->request, &client->prepared, source,
					client->deadline_mono_ns, &response))
				return false;
		}
		else
		{
			response = *source;
			memcpy(response.request_nonce, client->request.request_nonce,
				sizeof(response.request_nonce));
		}
		sent = pgrac_fenced_session_send_response(client->fd, &response,
			client->deadline_mono_ns);
		if (positive && sent)
		{
			client->state = PGRAC_FENCED_COORDINATOR_CLIENT_RETAINED;
			client->response = response;
		}
		else
		{
			PgracFencedScheduleTicketV1 ignored;

			if (positive && !pgrac_fenced_operation_invalidate(
					coordinator->context, &response, 16))
				return false;
			if (positive && !pgrac_fenced_schedule_cancel_client(
					&coordinator->schedule, client_id, &ignored))
				return false;
			client_clear(coordinator, (uint32) client_id);
		}
	}
	if (coordinator->quiescing)
		return true;
	if (!positive)
		return coordinator_release_ticket(coordinator, &operation->ticket);
	if (!pgrac_fenced_schedule_copy_clients(&coordinator->schedule,
			&operation->ticket, scheduled, lengthof(scheduled), &count))
		return false;
	return count > 0 || coordinator_release_ticket(coordinator,
		&operation->ticket);
}

static bool
coordinator_service_worker(PgracFencedCoordinatorV1 *coordinator,
				   uint32 slot)
{
	PgracFencedCoordinatorOperationV1 *operation =
		&coordinator->operations[slot];
	PgracFencedCoordinatorOperationV1 completed;
	PgracExternalFenceProtocolResponseV1 response;
	PgracFencedAsyncEvent event;
	struct pollfd descriptor;

	if (!operation->worker_active)
		return true;
	descriptor.fd = pgrac_fenced_async_fd(&operation->worker);
	descriptor.events = POLLIN | POLLHUP | POLLERR;
	descriptor.revents = 0;
	if (poll(&descriptor, 1, 0) <= 0)
		return true;
	if (!pgrac_fenced_async_service(coordinator->context,
			&operation->worker, &event, &response))
		return false;
	if (event == PGRAC_FENCED_ASYNC_JOURNAL)
	{
		if (coordinator->quiescing)
			return true;
		if (operation->worker.last_record_kind ==
			PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED)
			return pgrac_fenced_schedule_set_state(&coordinator->schedule,
				&operation->ticket, PGRAC_FENCED_STATE_ACTUATING);
		if (operation->worker.last_record_kind ==
			PGRAC_FENCED_JOURNAL_KIND_ACTUATION_RESULT)
			return pgrac_fenced_schedule_set_state(&coordinator->schedule,
				&operation->ticket, PGRAC_FENCED_STATE_VERIFYING);
		return true;
	}
	if (event != PGRAC_FENCED_ASYNC_COMPLETE)
		return true;
	completed = *operation;
	memset(operation, 0, sizeof(*operation));
	operation->worker.fd = -1;
	completed.worker_active = false;
	return coordinator_send_operation_result(coordinator, &completed,
		&response);
}

static bool
waiting_client_event(int fd)
{
	struct pollfd descriptor;
	int rc;

	descriptor.fd = fd;
	descriptor.events = POLLIN | POLLHUP | POLLERR;
	descriptor.revents = 0;
	do
	{
		rc = poll(&descriptor, 1, 0);
	} while (rc < 0 && errno == EINTR);
	return rc != 0;
}

static bool
coordinator_service_clients(PgracFencedCoordinatorV1 *coordinator,
					uint64 now_mono_ns)
{
	PgracFencedCoordinatorClientV1 *client;
	PgracFencedScheduleTicketV1 started;
	PgracFencedScheduleSnapshotV1 snapshot;
	PgracExternalFenceProtocolResponseV1 response;
	PgracFencedSessionReceiveProgress ingress;
	uint64 operation_deadline;
	uint32 reason;
	uint32 i;

	for (i = 0; i < PGRAC_FENCED_MAX_CLIENTS; i++)
	{
		client = &coordinator->clients[i];
		if (client->state == PGRAC_FENCED_COORDINATOR_CLIENT_UNUSED)
			continue;
		if (client->state == PGRAC_FENCED_COORDINATOR_CLIENT_INGRESS)
		{
			ingress = pgrac_fenced_session_receive_progress(
				coordinator->context, client->fd, client->deadline_mono_ns,
				client->request_frame, &client->request_frame_used,
				&client->request, &operation_deadline);
			if (ingress == PGRAC_FENCED_SESSION_RECEIVE_PENDING)
				continue;
			if (ingress == PGRAC_FENCED_SESSION_RECEIVE_ERROR)
			{
				client_clear(coordinator, i);
				continue;
			}
			if (!coordinator_submit_request(coordinator, i,
					operation_deadline))
				return false;
			continue;
		}
		if (client->state == PGRAC_FENCED_COORDINATOR_CLIENT_RETAINED)
		{
			if (!pgrac_fenced_session_retention_event(client->fd,
					&client->response, now_mono_ns, &reason))
				continue;
			if (!pgrac_fenced_operation_invalidate(coordinator->context,
					&client->response, reason))
				return false;
		}
		else
		{
			if (now_mono_ns >= client->deadline_mono_ns)
			{
				if (!pgrac_fenced_operation_cancel_preaccepted(
						coordinator->context, &client->request,
						&client->prepared, 11, &response))
					return false;
				(void) pgrac_fenced_session_send_response(client->fd,
					&response, now_mono_ns + UINT64_C(100000000));
			}
			else if (waiting_client_event(client->fd))
			{
				if (!pgrac_fenced_operation_cancel_preaccepted(
						coordinator->context, &client->request,
						&client->prepared, 16, &response))
					return false;
			}
			else
				continue;
		}
		if (!pgrac_fenced_schedule_cancel_client(&coordinator->schedule,
				(int) i, &started))
			return false;
		if (started.serial != 0 &&
			!coordinator_start_ticket(coordinator, &started))
			return false;
		if (pgrac_fenced_schedule_snapshot(&coordinator->schedule,
				&client->ticket, &snapshot) && snapshot.active &&
			snapshot.client_count == 0 &&
			!coordinator->operations[client->ticket.slot].worker_active &&
			!coordinator_release_ticket(coordinator, &client->ticket))
			return false;
		client_clear(coordinator, i);
	}
	return true;
}

bool
pgrac_fenced_coordinator_service(PgracFencedCoordinatorV1 *coordinator,
					 uint64 now_mono_ns)
{
	uint32 i;

	if (coordinator == NULL || coordinator->context == NULL ||
		!coordinator->context->available || now_mono_ns == 0)
		return false;
	for (i = 0; i < PGRAC_FENCED_MAX_OPERATIONS; i++)
	{
		if (!coordinator_service_worker(coordinator, i))
			return false;
	}
	if (coordinator->quiescing)
		return true;
	return coordinator_service_clients(coordinator, now_mono_ns);
}

static bool
client_has_active_worker(const PgracFencedCoordinatorV1 *coordinator,
				 const PgracFencedCoordinatorClientV1 *client)
{
	const PgracFencedCoordinatorOperationV1 *operation;

	if (client->ticket.serial == 0 ||
		client->ticket.slot >= PGRAC_FENCED_MAX_OPERATIONS)
		return false;
	operation = &coordinator->operations[client->ticket.slot];
	return operation->worker_active &&
		operation->ticket.slot == client->ticket.slot &&
		operation->ticket.serial == client->ticket.serial;
}

bool
pgrac_fenced_coordinator_quiesce(PgracFencedCoordinatorV1 *coordinator,
					 uint32 deny_reason)
{
	PgracFencedCoordinatorClientV1 *client;
	PgracExternalFenceProtocolResponseV1 response;
	uint32 i;
	bool ok = true;

	if (coordinator == NULL || coordinator->context == NULL)
		return false;
	coordinator->quiescing = true;
	for (i = 0; i < PGRAC_FENCED_MAX_CLIENTS; i++)
	{
		client = &coordinator->clients[i];
		if (client->state == PGRAC_FENCED_COORDINATOR_CLIENT_UNUSED ||
			(client->state == PGRAC_FENCED_COORDINATOR_CLIENT_WAITING &&
			 client_has_active_worker(coordinator, client)))
			continue;
		if (client->state == PGRAC_FENCED_COORDINATOR_CLIENT_RETAINED)
		{
			if (!pgrac_fenced_operation_invalidate(coordinator->context,
					&client->response, deny_reason))
				ok = false;
		}
		else if (client->state == PGRAC_FENCED_COORDINATOR_CLIENT_WAITING)
		{
			if (!pgrac_fenced_operation_cancel_preaccepted(
					coordinator->context, &client->request, &client->prepared,
					deny_reason, &response))
				ok = false;
			else
				(void) pgrac_fenced_session_send_response(client->fd,
					&response, client->deadline_mono_ns);
		}
		client_clear(coordinator, i);
	}
	return ok;
}

bool
pgrac_fenced_coordinator_shutdown(PgracFencedCoordinatorV1 *coordinator,
					  uint32 deny_reason)
{
	PgracFencedCoordinatorClientV1 *client;
	PgracExternalFenceProtocolResponseV1 response;
	uint32 i;
	bool ok = true;

	if (coordinator == NULL || coordinator->context == NULL)
		return false;
	for (i = 0; i < PGRAC_FENCED_MAX_OPERATIONS; i++)
	{
		if (coordinator->operations[i].worker_active)
			return false;
	}
	for (i = 0; i < PGRAC_FENCED_MAX_CLIENTS; i++)
	{
		client = &coordinator->clients[i];
		if (client->state == PGRAC_FENCED_COORDINATOR_CLIENT_UNUSED)
			continue;
		if (client->state == PGRAC_FENCED_COORDINATOR_CLIENT_RETAINED)
		{
			if (!pgrac_fenced_operation_invalidate(coordinator->context,
					&client->response, deny_reason))
				ok = false;
		}
		else if (client->state == PGRAC_FENCED_COORDINATOR_CLIENT_WAITING)
		{
			if (!pgrac_fenced_operation_cancel_preaccepted(
					coordinator->context, &client->request, &client->prepared,
					deny_reason, &response))
				ok = false;
			else
				(void) pgrac_fenced_session_send_response(client->fd,
					&response, client->deadline_mono_ns);
		}
		client_clear(coordinator, i);
	}
	(void) pgrac_fenced_schedule_init(&coordinator->schedule);
	return ok;
}

uint32
pgrac_fenced_coordinator_active_worker_count(
	const PgracFencedCoordinatorV1 *coordinator)
{
	uint32 count = 0;
	uint32 i;

	if (coordinator == NULL)
		return 0;
	for (i = 0; i < PGRAC_FENCED_MAX_OPERATIONS; i++)
	{
		if (coordinator->operations[i].worker_active)
			count++;
	}
	return count;
}

PgracFencedCoordinatorRejoinTargetResult
pgrac_fenced_coordinator_rejoin_acquire_target(
	PgracFencedCoordinatorV1 *coordinator, const uint8 target_uuid[16])
{
	PgracFencedCoordinatorClientV1 *client;
	bool active = false;
	uint32 i;
	int slot;

	if (coordinator == NULL || coordinator->context == NULL ||
		target_uuid == NULL || !bytes_nonzero(target_uuid, 16))
		return PGRAC_FENCED_REJOIN_TARGET_ERROR;
	slot = rejoin_target_slot(coordinator, target_uuid);
	if (slot < 0)
	{
		if (coordinator->rejoin_target_count >= PGRAC_FENCED_MAX_OPERATIONS)
			return PGRAC_FENCED_REJOIN_TARGET_ERROR;
		for (i = 0; i < PGRAC_FENCED_MAX_OPERATIONS; i++)
		{
			if (!coordinator->rejoin_target_used[i])
			{
				slot = (int) i;
				break;
			}
		}
		if (slot < 0)
			return PGRAC_FENCED_REJOIN_TARGET_ERROR;
		coordinator->rejoin_target_used[slot] = true;
		memcpy(coordinator->rejoin_targets[slot], target_uuid,
			   sizeof(coordinator->rejoin_targets[slot]));
		coordinator->rejoin_target_count++;
	}
	for (i = 0; i < PGRAC_FENCED_MAX_CLIENTS; i++)
	{
		client = &coordinator->clients[i];
		if (client->state == PGRAC_FENCED_COORDINATOR_CLIENT_WAITING &&
			memcmp(client->prepared.target.target_uuid, target_uuid,
				sizeof(client->prepared.target.target_uuid)) == 0 &&
			client_has_active_worker(coordinator, client))
			active = true;
	}
	return active ? PGRAC_FENCED_REJOIN_TARGET_WAITING :
		PGRAC_FENCED_REJOIN_TARGET_READY;
}

PgracFencedCoordinatorRejoinTargetResult
pgrac_fenced_coordinator_rejoin_invalidate_target(
	PgracFencedCoordinatorV1 *coordinator, const uint8 target_uuid[16],
	uint32 deny_reason)
{
	PgracFencedCoordinatorClientV1 *client;
	PgracExternalFenceProtocolResponseV1 response;
	PgracFencedScheduleTicketV1 ignored;
	bool active = false;
	bool changed;
	uint32 pass;
	uint32 i;

	if (coordinator == NULL || coordinator->context == NULL ||
		target_uuid == NULL || !bytes_nonzero(target_uuid, 16) ||
		deny_reason == 0 || deny_reason > 31 ||
		rejoin_target_slot(coordinator, target_uuid) < 0)
		return PGRAC_FENCED_REJOIN_TARGET_ERROR;
	for (pass = 0; pass < PGRAC_FENCED_MAX_CLIENTS; pass++)
	{
		changed = false;
		active = false;
		for (i = 0; i < PGRAC_FENCED_MAX_CLIENTS; i++)
		{
			PgracFencedScheduleTicketV1 ticket;

			client = &coordinator->clients[i];
			if ((client->state != PGRAC_FENCED_COORDINATOR_CLIENT_WAITING &&
				 client->state != PGRAC_FENCED_COORDINATOR_CLIENT_RETAINED) ||
				memcmp(client->prepared.target.target_uuid, target_uuid,
					sizeof(client->prepared.target.target_uuid)) != 0)
				continue;
			if (client->state == PGRAC_FENCED_COORDINATOR_CLIENT_WAITING &&
				client_has_active_worker(coordinator, client))
			{
				active = true;
				continue;
			}
			if (client->state == PGRAC_FENCED_COORDINATOR_CLIENT_RETAINED)
			{
				if (!pgrac_fenced_operation_invalidate(coordinator->context,
						&client->response, deny_reason))
					return PGRAC_FENCED_REJOIN_TARGET_ERROR;
			}
			else
			{
				if (!pgrac_fenced_operation_cancel_preaccepted(
						coordinator->context, &client->request,
						&client->prepared, deny_reason, &response))
					return PGRAC_FENCED_REJOIN_TARGET_ERROR;
				(void) pgrac_fenced_session_send_response(client->fd,
					&response, client->deadline_mono_ns);
			}
			if (!pgrac_fenced_schedule_cancel_client(
					&coordinator->schedule, (int) i, &ignored))
				return PGRAC_FENCED_REJOIN_TARGET_ERROR;
			ticket = client->ticket;
			client_clear(coordinator, i);
			if (!coordinator_release_empty_reserved_ticket(coordinator,
					&ticket))
				return PGRAC_FENCED_REJOIN_TARGET_ERROR;
			changed = true;
		}
		if (!changed)
			break;
	}
	return active ? PGRAC_FENCED_REJOIN_TARGET_WAITING :
		PGRAC_FENCED_REJOIN_TARGET_READY;
}

bool
pgrac_fenced_coordinator_rejoin_release_target(
	PgracFencedCoordinatorV1 *coordinator, const uint8 target_uuid[16])
{
	int slot;

	if (coordinator == NULL || target_uuid == NULL)
		return false;
	slot = rejoin_target_slot(coordinator, target_uuid);
	if (slot < 0)
		return false;
	memset(coordinator->rejoin_targets[slot], 0,
		   sizeof(coordinator->rejoin_targets[slot]));
	coordinator->rejoin_target_used[slot] = false;
	coordinator->rejoin_target_count--;
	return true;
}
