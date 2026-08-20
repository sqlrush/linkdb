/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_runtime.h
 *    Secure production bootstrap helpers for pgrac-fenced.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_RUNTIME_H
#define PGRAC_FENCED_RUNTIME_H

#include "c.h"

#include <sys/stat.h>

#include "pgrac_fenced_journal.h"

extern bool pgrac_fenced_runtime_dir_stat_secure(
	const struct stat *st, uint64 allowed_gid);
extern bool pgrac_fenced_journal_dir_stat_secure(const struct stat *st);
extern bool pgrac_fenced_journal_file_stat_secure(const struct stat *st);
extern bool pgrac_fenced_journal_filesystem_local(int fd);
extern bool pgrac_fenced_journal_sealed_name_parse(
	const char *name,
	uint64 *first_seq,
	uint64 *last_seq,
	uint8 digest[PGRAC_FENCED_JOURNAL_DIGEST_BYTES]);
extern bool pgrac_fenced_journal_load_active_fd(
	int fd,
	PgracFencedJournalScanState *state,
	PgracFencedJournalRecordV1 *last_record,
	bool *have_last_record);
extern bool pgrac_fenced_journal_load_active_reconcile_fd(
	int fd,
	PgracFencedJournalScanState *state,
	PgracFencedJournalRecordV1 *last_record,
	bool *have_last_record,
	PgracFencedJournalReconcileState *reconcile);
extern bool pgrac_fenced_journal_load_sealed_fd(
	int fd,
	PgracFencedJournalScanState *state,
	PgracFencedJournalRecordV1 *last_record,
	bool *have_last_record);
extern bool pgrac_fenced_journal_load_sealed_reconcile_fd(
	int fd,
	PgracFencedJournalScanState *state,
	PgracFencedJournalRecordV1 *last_record,
	bool *have_last_record,
	PgracFencedJournalReconcileState *reconcile);

#endif /* PGRAC_FENCED_RUNTIME_H */
