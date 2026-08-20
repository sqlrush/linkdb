/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_session.h
 *    One-frame authenticated DB session boundary for pgrac-fenced.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_SESSION_H
#define PGRAC_FENCED_SESSION_H

#include "c.h"

#include "common/pgrac_external_fence_protocol.h"
#include "pgrac_fenced_operation.h"

typedef enum PgracFencedSessionDisposition
{
	PGRAC_FENCED_SESSION_ERROR = 0,
	PGRAC_FENCED_SESSION_CLOSED = 1,
	PGRAC_FENCED_SESSION_RETAINED = 2
} PgracFencedSessionDisposition;

typedef enum PgracFencedSessionReceiveProgress
{
	PGRAC_FENCED_SESSION_RECEIVE_ERROR = 0,
	PGRAC_FENCED_SESSION_RECEIVE_PENDING = 1,
	PGRAC_FENCED_SESSION_RECEIVE_COMPLETE = 2
} PgracFencedSessionReceiveProgress;

extern bool pgrac_fenced_session_prepare_client(
	PgracFencedOperationContextV1 *context,
	int client_fd);
extern PgracFencedSessionReceiveProgress
pgrac_fenced_session_receive_progress(
	PgracFencedOperationContextV1 *context,
	int client_fd,
	uint64 transport_deadline_mono_ns,
	uint8 request_frame[PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES],
	size_t *request_frame_used,
	PgracExternalFenceProtocolRequestV1 *request,
	uint64 *operation_deadline_mono_ns);

extern bool pgrac_fenced_session_receive_request(
	PgracFencedOperationContextV1 *context,
	int client_fd,
	uint64 transport_deadline_mono_ns,
	PgracExternalFenceProtocolRequestV1 *request,
	uint64 *operation_deadline_mono_ns);
extern bool pgrac_fenced_session_send_response(
	int client_fd,
	const PgracExternalFenceProtocolResponseV1 *response,
	uint64 deadline_mono_ns);

extern PgracFencedSessionDisposition pgrac_fenced_session_exchange(
	PgracFencedOperationContextV1 *context,
	int client_fd,
	bool capacity_available,
	uint64 transport_deadline_mono_ns,
	PgracExternalFenceProtocolResponseV1 *response);
extern bool pgrac_fenced_session_retention_event(
	int client_fd,
	const PgracExternalFenceProtocolResponseV1 *response,
	uint64 now_mono_ns,
	uint32 *deny_reason);

#endif /* PGRAC_FENCED_SESSION_H */
