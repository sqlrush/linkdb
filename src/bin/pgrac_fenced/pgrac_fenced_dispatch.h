/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_dispatch.h
 *    Bounded live-admission owner for pgrac-fenced.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_DISPATCH_H
#define PGRAC_FENCED_DISPATCH_H

#include "c.h"

#include "pgrac_fenced_core.h"
#include "pgrac_fenced_operation.h"

typedef struct PgracFencedDispatchSlotV1
{
	int fd;
	bool retained;
	PgracExternalFenceProtocolResponseV1 response;
} PgracFencedDispatchSlotV1;

typedef struct PgracFencedDispatcherV1
{
	PgracFencedOperationContextV1 *context;
	uint32 client_count;
	PgracFencedDispatchSlotV1 clients[PGRAC_FENCED_MAX_CLIENTS];
} PgracFencedDispatcherV1;

extern bool pgrac_fenced_dispatch_init(
	PgracFencedDispatcherV1 *dispatcher,
	PgracFencedOperationContextV1 *context);
extern bool pgrac_fenced_dispatch_accept_fd(
	PgracFencedDispatcherV1 *dispatcher,
	int client_fd,
	uint64 transport_deadline_mono_ns);
extern bool pgrac_fenced_dispatch_reap(
	PgracFencedDispatcherV1 *dispatcher,
	uint64 now_mono_ns);
extern bool pgrac_fenced_dispatch_shutdown(
	PgracFencedDispatcherV1 *dispatcher,
	uint32 deny_reason);

#endif /* PGRAC_FENCED_DISPATCH_H */
