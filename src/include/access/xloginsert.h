/*
 * xloginsert.h
 *
 * Functions for generating WAL records
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	src/include/access/xloginsert.h
 *
 * PGRAC MODIFICATIONS (Stage 8 JIT Task3)
 *	Modified by: SqlRush <sqlrush@gmail.com>
 *	What changed: declared the bounded page-version edge encoder owned by
 *	xloginsert.c.
 */
#ifndef XLOGINSERT_H
#define XLOGINSERT_H

#include "access/rmgr.h"
#include "access/xlogdefs.h"
#include "access/xlogrecord.h"
#include "storage/block.h"
#include "storage/buf.h"
#include "storage/relfilelocator.h"
#include "utils/relcache.h"

/*
 * The minimum size of the WAL construction working area. If you need to
 * register more than XLR_NORMAL_MAX_BLOCK_ID block references or have more
 * than XLR_NORMAL_RDATAS data chunks in a single WAL record, you must call
 * XLogEnsureRecordSpace() first to allocate more working memory.
 */
#define XLR_NORMAL_MAX_BLOCK_ID		4
#define XLR_NORMAL_RDATAS			20

/* flags for XLogRegisterBuffer */
#define REGBUF_FORCE_IMAGE	0x01	/* force a full-page image */
#define REGBUF_NO_IMAGE		0x02	/* don't take a full-page image */
#define REGBUF_WILL_INIT	(0x04 | 0x02)	/* page will be re-initialized at
											 * replay (implies NO_IMAGE) */
#define REGBUF_STANDARD		0x08	/* page follows "standard" page layout,
									 * (data between pd_lower and pd_upper
									 * will be skipped) */
#define REGBUF_KEEP_DATA	0x10	/* include data even if a full-page image
									 * is taken */

/* prototypes for public functions in xloginsert.c: */
extern void XLogBeginInsert(void);
extern void XLogSetRecordFlags(uint8 flags);
extern XLogRecPtr XLogInsert(RmgrId rmid, uint8 info);
extern void XLogEnsureRecordSpace(int max_block_id, int ndatas);
extern void XLogRegisterData(char *data, uint32 len);
extern void XLogRegisterBuffer(uint8 block_id, Buffer buffer, uint8 flags);
extern void XLogRegisterBlock(uint8 block_id, RelFileLocator *rlocator,
							  ForkNumber forknum, BlockNumber blknum, char *page,
							  uint8 flags);
extern void XLogRegisterBufData(uint8 block_id, char *data, uint32 len);
extern void XLogResetInsertion(void);
extern bool XLogCheckBufferNeedsBackup(Buffer buffer);

/* PGRAC Stage 8 JIT Task3: strong production encoder capability. */
#define XLOG_PAGE_VERSION_EDGE_ENCODER_V1 1
extern bool XLogEncodePageVersionEdgeV1(uint8 *output,
										Size output_capacity,
										uint64 result_token,
										const RfPageVersionEdgeEntryV1 *entries,
										uint8 entry_count,
										Size *output_size);

extern XLogRecPtr log_newpage(RelFileLocator *rlocator, ForkNumber forknum,
							  BlockNumber blkno, char *page, bool page_std);
extern void log_newpages(RelFileLocator *rlocator, ForkNumber forknum, int num_pages,
						 BlockNumber *blknos, char **pages, bool page_std);
extern XLogRecPtr log_newpage_buffer(Buffer buffer, bool page_std);
extern void log_newpage_range(Relation rel, ForkNumber forknum,
							  BlockNumber startblk, BlockNumber endblk, bool page_std);
extern XLogRecPtr XLogSaveBufferForHint(Buffer buffer, bool buffer_std);

extern void InitXLogInsert(void);

#endif							/* XLOGINSERT_H */
