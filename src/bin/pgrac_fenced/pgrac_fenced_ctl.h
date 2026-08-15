/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_ctl.h
 *	  Root-only, provider-neutral pgrac-fenced administration contract.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_CTL_H
#define PGRAC_FENCED_CTL_H

#include "c.h"
#include "common/pgrac_external_fence_protocol.h"

#include <sys/stat.h>

#include "pgrac_fenced_journal.h"

#define PGRAC_FENCED_CTL_EXIT_OK 0
#define PGRAC_FENCED_CTL_EXIT_USAGE 2
#define PGRAC_FENCED_CTL_EXIT_UNAVAILABLE 77

typedef struct PgracFencedCtlPrepareRejoinV1
{
	int32 node_id;
	uint64 old_incarnation;
	uint64 candidate_incarnation;
	uint32 timeout_ms;
} PgracFencedCtlPrepareRejoinV1;

typedef struct PgracFencedCtlJournalSummaryV1
{
	uint64 first_seq;
	uint64 last_seq;
	uint32 record_count;
	uint8 tail_digest[PGRAC_FENCED_JOURNAL_DIGEST_BYTES];
} PgracFencedCtlJournalSummaryV1;

extern bool pgrac_fenced_ctl_prepare_rejoin_valid(
	const PgracFencedCtlPrepareRejoinV1 *request);
extern bool pgrac_fenced_ctl_prepare_rejoin_frame(
	const PgracFencedCtlPrepareRejoinV1 *request,
	const uint8 transport_nonce[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES],
	uint8 frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES]);
extern bool pgrac_fenced_ctl_prepare_rejoin_response(
	const PgracFencedCtlPrepareRejoinV1 *request,
	const uint8 transport_nonce[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES],
	const uint8 frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES],
	PgracExternalFenceProtocolRejoinFrameV1 *response);
extern bool pgrac_fenced_ctl_journal_stat_secure(const struct stat *st);
extern bool pgrac_fenced_ctl_journal_scan(
	const uint8 *bytes, size_t len,
	PgracFencedCtlJournalSummaryV1 *summary);

#endif /* PGRAC_FENCED_CTL_H */
