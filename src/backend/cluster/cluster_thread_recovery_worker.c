/*-------------------------------------------------------------------------
 *
 * cluster_thread_recovery_worker.c
 *	  pgrac online thread-recovery EXECUTOR (spec-4.11 D1, increment 3b-4b
 *	  Part 2).
 *
 *	  LMON owns one process-local dynamic-background-worker handle per in-scope
 *	  dead thread and relaunches only after exact matching STOPPED evidence.  This
 *	  file owns that A2 scheduler lifecycle and the executor entry point:
 *
 *	    thread_recovery_worker_run -- the executor core.  It reads the
 *	      per-thread replay slot (the launch marked it REPLAYING and stamped the
 *	      launch episode), enforces the L235 episode-staleness guard against the
 *	      live GRD recovery episode (a superseded worker aborts and publishes
 *	      nothing -- keep frozen).  RF-ROOT P3 deliberately stops at the strong
 *	      fail-closed seam until P4 supplies typed NeedSet/AdmissionSet evidence;
 *	      no replay or authority publication is reachable before then.
 *	      The slot is OBSERVABILITY + episode coordination ONLY -- the
 *	      authoritative unfreeze/serve gate reads the node-local merged.authority,
 *	      NOT this slot (spec-4.11 §2.4 Q4).
 *
 *	    cluster_thread_recovery_worker_main -- the dynamic-bgworker entry point
 *	      (BGWORKER_SHMEM_ACCESS only, like the spec-4.4 recovery worker: no
 *	      database connection).  A thin wrapper: validate the full bgw_extra duty
 *	      against the live slot, register authority cleanup, unblock signals, run,
 *	      and log.
 *
 *	  Exit discipline: the existing GRD cleanup callback releases this process's
 *	  holder state.  It never writes scheduler slots; LMON owns STOPPED reaping
 *	  and the sole matched REPLAYING->IDLE reset.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_thread_recovery_worker.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.11-thread-recovery.md (FROZEN v0.3)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#include "cluster/cluster_conf.h"			   /* node_count / has_peers / CLUSTER_MAX_NODES */
#include "cluster/cluster_grd.h"			   /* live recovery episode epoch (L235)        */
#include "cluster/cluster_guc.h"			   /* cluster_online_thread_recovery (scope)    */
#include "cluster/cluster_ir.h"				   /* spec-5.7 D8 — IR(X) recovery-owner gate    */
#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_thread_recovery.h"   /* slot helpers + replay_one + gates          */
#include "cluster/storage/cluster_shared_fs.h" /* shared backend (scope)                    */

/*
 * thread_recovery_worker_run -- online-recover ONE dead thread in the calling
 *	process.  Returns the replay verdict.  NOT_APPLICABLE also covers
 *	"the slot is not REPLAYING / the launch epoch is stale": the worker aborts,
 *	publishes nothing, and leaves the slot for the live episode (keep frozen).
 */
static ClusterThreadRecResult
thread_recovery_worker_run(
	const ClusterThreadRecLaunchEligibility *eligibility)
{
	ClusterThreadRecReplayState state;
	uint64 launch_epoch;
	uint16 dead_tid;
	bool slot_read;

	if (eligibility == NULL)
		return CLUSTER_THREADREC_DEFERRED;
	dead_tid = eligibility->origin_thread;

	/* The launch must have marked the slot REPLAYING; anything else means this
	 * spawn raced a reset or a newer launch -> moot (do not touch the slot). */
	slot_read = cluster_thread_recovery_replay_read(dead_tid, &state,
												&launch_epoch);
	if (!cluster_thread_recovery_worker_start_valid(
			eligibility, dead_tid, slot_read, state, launch_epoch))
		return CLUSTER_THREADREC_DEFERRED;

	/* L235 BEFORE: a stale launch epoch means the reconfig episode advanced past
	 * this worker.  Abort -- never run replay_one (which would publish authority)
	 * into a different episode.  Leave the slot for the new episode's launch. */
	if (cluster_thread_recovery_replay_epoch_aborts(launch_epoch,
													cluster_grd_redeclare_episode_epoch()))
		return CLUSTER_THREADREC_NOT_APPLICABLE;

	/* RF-ROOT P3 precedes the P4 NeedSet/AdmissionSet producer in the binding
	 * implementation order.  The old {dead_node,episode_epoch} IR gate is
	 * removed; until P4 can rebuild and fresh-revalidate the full duty evidence,
	 * this strong seam is deliberately nonaffirmative: no GES, replay or publish. */
	return CLUSTER_THREADREC_DEFERRED;
}

/*
 * cluster_thread_recovery_worker_main -- dynamic-bgworker entry point
 *	(InternalBGWorkers); main_arg carries the dead thread id.
 */
void
cluster_thread_recovery_worker_main(Datum main_arg)
{
	int32 dead_tid = DatumGetInt32(main_arg);
	ClusterThreadRecLaunchEligibility eligibility;
	ClusterThreadRecReplayState state = CLUSTER_THREADREC_REPLAY_IDLE;
	ClusterThreadRecReplayState terminal_state;
	ClusterThreadReplayMatchResult match_result;
	uint64 launch_epoch = 0;
	ClusterThreadRecResult res;
	bool slot_read;

	/* Returning from a bgworker entry point is a clean exit(0); cppcheck does
	 * not model proc_exit as noreturn, so guards use return (mirrors the
	 * spec-4.4 recovery worker). */
	if (dead_tid < XLP_THREAD_ID_FIRST_REAL || dead_tid > CLUSTER_WAL_THREAD_MAX)
		return;

	/* bgw_extra is a non-authoritative carrier.  Revalidate its full duty and
	 * exact attempt identity against the live slot before any authority acquire. */
	if (MyBgworkerEntry == NULL)
		return;
	memcpy(&eligibility, MyBgworkerEntry->bgw_extra, sizeof(eligibility));
	slot_read = cluster_thread_recovery_replay_read(
		(uint16)dead_tid, &state, &launch_epoch);
	if (!cluster_thread_recovery_worker_start_valid(
			&eligibility, (uint16)dead_tid, slot_read, state, launch_epoch))
		return;

	/* Cleanup authority on every controlled exit; scheduler state is reaped only
	 * by the LMON owner after exact BGWH_STOPPED evidence. */
	before_shmem_exit(cluster_grd_cleanup_on_backend_exit_callback, 0);

	/* The bgworker framework starts the entry point with signals blocked;
	 * unblock before any I/O so a SIGTERM during a stuck shared-storage read or
	 * a shutdown is delivered (the default bgworker_die handler FATALs, landing
	 * in the BLOCKED exit path above). */
	BackgroundWorkerUnblockSignals();

	res = thread_recovery_worker_run(&eligibility);
	if (cluster_thread_recovery_worker_terminal_state(res, &terminal_state)) {
		match_result = cluster_thread_recovery_replay_transition_if_match(
			(uint16)dead_tid, eligibility.attempt_stamp,
			CLUSTER_THREADREC_REPLAY_REPLAYING, terminal_state);
		if (match_result != CLUSTER_THREADREC_MATCH_CHANGED)
			ereport(PANIC,
					(errmsg("online thread-recovery terminal slot transition failed"),
					 errdetail("thread=%u stamp=" UINT64_FORMAT
							   " result=%d match=%d",
							   (unsigned)dead_tid, eligibility.attempt_stamp,
							   (int)res, (int)match_result)));
	}

	ereport(LOG, (errmsg("online thread recovery: dead thread %d -> %s", dead_tid,
						 res == CLUSTER_THREADREC_DONE
							 ? "done"
							 : (res == CLUSTER_THREADREC_BLOCKED ? "blocked (kept frozen)"
								: (res == CLUSTER_THREADREC_DEFERRED ? "deferred"
															  : "not applicable"))),
				  res == CLUSTER_THREADREC_BLOCKED
					  ? errhint("The dead thread's resources stay frozen; check the shared WAL "
								"storage and cluster.thread_recovery_on_unrecoverable.")
					  : 0));
}

typedef struct ClusterThreadRecOwnedWorker {
	BackgroundWorkerHandle *handle;
	uint64 attempt_stamp;
	bool terminate_sent;
} ClusterThreadRecOwnedWorker;

static ClusterThreadRecOwnedWorker
	thread_recovery_owned[CLUSTER_WAL_THREAD_MAX + 1];

StaticAssertDecl(sizeof(ClusterThreadRecLaunchEligibility) <= BGW_EXTRALEN,
				 "thread recovery eligibility must fit bgw_extra");

static bool
thread_recovery_eligibility_valid(
	const ClusterThreadRecLaunchEligibility *eligibility)
{
	return eligibility != NULL
		&& cluster_thread_recovery_worker_start_valid(
			eligibility, eligibility->origin_thread, true,
			CLUSTER_THREADREC_REPLAY_REPLAYING,
			eligibility->attempt_stamp);
}

static bool
register_one_worker(const ClusterThreadRecLaunchEligibility *eligibility,
					BackgroundWorkerHandle **handle_out)
{
	BackgroundWorker bgw;
	MemoryContext old_context;
	bool registered;
	uint16 dead_tid;

	if (handle_out == NULL || !thread_recovery_eligibility_valid(eligibility))
		return false;
	*handle_out = NULL;
	dead_tid = eligibility->origin_thread;

	memset(&bgw, 0, sizeof(bgw));
	/* SHMEM_ACCESS only -- no database connection (replay_one works by raw
	 * relfilelocator over the shared smgr + per-origin SLRU). */
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;
	bgw.bgw_start_time = BgWorkerStart_PostmasterStart;
	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	strlcpy(bgw.bgw_library_name, "postgres", sizeof(bgw.bgw_library_name));
	strlcpy(bgw.bgw_function_name, "cluster_thread_recovery_worker_main",
			sizeof(bgw.bgw_function_name));
	snprintf(bgw.bgw_name, sizeof(bgw.bgw_name), "pgrac thread recovery %u", (unsigned)dead_tid);
	strlcpy(bgw.bgw_type, "cluster thread recovery", sizeof(bgw.bgw_type));
	bgw.bgw_main_arg = Int32GetDatum((int32)dead_tid);
	memcpy(bgw.bgw_extra, eligibility, sizeof(*eligibility));
	bgw.bgw_notify_pid = 0;

	old_context = MemoryContextSwitchTo(TopMemoryContext);
	registered = RegisterDynamicBackgroundWorker(&bgw, handle_out);
	MemoryContextSwitchTo(old_context);
	return registered;
}

static void
thread_recovery_reap_one(uint16 dead_tid, bool *retained_out)
{
	ClusterThreadRecOwnedWorker *owned = &thread_recovery_owned[dead_tid];
	ClusterThreadRecReplayState state = CLUSTER_THREADREC_REPLAY_IDLE;
	ClusterThreadRecReapDecision decision;
	BgwHandleStatus status;
	uint64 slot_stamp = 0;
	pid_t pid = InvalidPid;
	bool slot_read = false;

	if (owned->handle == NULL)
		return;
	status = GetBackgroundWorkerPid(owned->handle, &pid);
	if (status == BGWH_STOPPED)
		slot_read = cluster_thread_recovery_replay_read(dead_tid, &state,
															 &slot_stamp);
	decision = cluster_thread_recovery_reap_decide(
		status, slot_read, owned->attempt_stamp, state, slot_stamp);
	if (decision == CLUSTER_THREADREC_REAP_RETAIN) {
		if (retained_out != NULL)
			*retained_out = true;
		return;
	}
	if (decision == CLUSTER_THREADREC_REAP_INVALID)
		ereport(FATAL,
				(errmsg("invalid online thread-recovery worker/slot relation"),
				 errdetail("thread=%u owned_stamp=" UINT64_FORMAT
						   " status=%d slot_read=%s slot_state=%d slot_stamp=" UINT64_FORMAT,
						   (unsigned)dead_tid, owned->attempt_stamp, (int)status,
						   slot_read ? "true" : "false", (int)state,
						   slot_stamp)));
	if (decision == CLUSTER_THREADREC_REAP_RESET_IDLE) {
		ClusterThreadReplayMatchResult match_result
			= cluster_thread_recovery_replay_transition_if_match(
				dead_tid, owned->attempt_stamp,
				CLUSTER_THREADREC_REPLAY_REPLAYING,
				CLUSTER_THREADREC_REPLAY_IDLE);

		if (match_result != CLUSTER_THREADREC_MATCH_CHANGED)
			ereport(FATAL,
					(errmsg("online thread-recovery STOPPED reap could not reset slot"),
					 errdetail("thread=%u stamp=" UINT64_FORMAT " match=%d",
							   (unsigned)dead_tid, owned->attempt_stamp,
							   (int)match_result)));
	}
	pfree(owned->handle);
	memset(owned, 0, sizeof(*owned));
}

static void
thread_recovery_launch_one(
	const ClusterThreadRecLaunchEligibility *eligibility)
{
	ClusterThreadRecOwnedWorker *owned;
	ClusterThreadRecReplayState state;
	ClusterThreadReplayMatchResult match_result;
	uint64 slot_stamp;
	uint16 dead_tid = eligibility->origin_thread;

	owned = &thread_recovery_owned[dead_tid];
	if (owned->handle != NULL)
		return;
	if (!cluster_thread_recovery_replay_read(dead_tid, &state, &slot_stamp)
		|| state != CLUSTER_THREADREC_REPLAY_IDLE)
		ereport(FATAL,
				(errmsg("invalid online thread-recovery launch slot"),
				 errdetail("thread=%u state=%d stamp=" UINT64_FORMAT,
						   (unsigned)dead_tid, (int)state, slot_stamp)));
	if (!cluster_thread_recovery_replay_mark_replaying(
			dead_tid, eligibility->attempt_stamp))
		ereport(FATAL,
				(errmsg("could not stamp online thread-recovery slot for dead thread %u",
						(unsigned)dead_tid)));
	if (register_one_worker(eligibility, &owned->handle)) {
		owned->attempt_stamp = eligibility->attempt_stamp;
		owned->terminate_sent = false;
		return;
	}

	match_result = cluster_thread_recovery_replay_transition_if_match(
		dead_tid, eligibility->attempt_stamp,
		CLUSTER_THREADREC_REPLAY_REPLAYING,
		CLUSTER_THREADREC_REPLAY_IDLE);
	if (match_result != CLUSTER_THREADREC_MATCH_CHANGED)
		ereport(FATAL,
				(errmsg("could not reset failed online thread-recovery launch"),
				 errdetail("thread=%u stamp=" UINT64_FORMAT " match=%d",
						   (unsigned)dead_tid, eligibility->attempt_stamp,
						   (int)match_result)));
	ereport(WARNING,
			(errmsg("could not register online thread-recovery worker for dead thread %u",
					(unsigned)dead_tid),
			 errhint("Background worker slots are exhausted (max_worker_processes); the "
					 "dead thread stays frozen until recovery can run.")));
}

void
cluster_thread_recovery_lmon_tick(void)
{
	ClusterThreadRecScope scope;
	bool shared_fs;
	int survivors;
	uint16 dead_tid;

	for (dead_tid = XLP_THREAD_ID_FIRST_REAL;
		 dead_tid <= CLUSTER_WAL_THREAD_MAX; dead_tid++)
		thread_recovery_reap_one(dead_tid, NULL);

	shared_fs = (cluster_shared_storage_backend
				 == CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS);
	survivors = cluster_conf_node_count() - 1;
	scope = cluster_thread_recovery_decide_scope(
		cluster_online_thread_recovery, cluster_conf_has_peers(), shared_fs,
		survivors);
	if (scope != CLUSTER_THREADREC_SCOPE_APPLICABLE)
		return;

	for (dead_tid = XLP_THREAD_ID_FIRST_REAL;
		 dead_tid <= CLUSTER_WAL_THREAD_MAX; dead_tid++) {
		ClusterThreadRecLaunchEligibility eligibility;
		ClusterThreadRecOwnedWorker *owned
			= &thread_recovery_owned[dead_tid];

		memset(&eligibility, 0, sizeof(eligibility));
		if (!cluster_reconfig_thread_recovery_eligibility_consume(
				dead_tid, &eligibility))
			continue;
		if (!thread_recovery_eligibility_valid(&eligibility)
			|| eligibility.origin_thread != dead_tid)
			ereport(FATAL,
					(errmsg("invalid online thread-recovery launch eligibility"),
					 errdetail("requested_thread=%u carrier_thread=%u stamp=" UINT64_FORMAT,
							   (unsigned)dead_tid,
							   (unsigned)eligibility.origin_thread,
							   eligibility.attempt_stamp)));
		if (owned->handle != NULL) {
			if (owned->attempt_stamp != eligibility.attempt_stamp
				&& !owned->terminate_sent) {
				TerminateBackgroundWorker(owned->handle);
				owned->terminate_sent = true;
			}
			continue;
		}
		thread_recovery_launch_one(&eligibility);
	}
}

void
cluster_thread_recovery_lmon_shutdown(void)
{
	TimestampTz deadline
		= TimestampTzPlusMilliseconds(GetCurrentTimestamp(), 5000);
	uint16 dead_tid;

	for (dead_tid = XLP_THREAD_ID_FIRST_REAL;
		 dead_tid <= CLUSTER_WAL_THREAD_MAX; dead_tid++) {
		ClusterThreadRecOwnedWorker *owned
			= &thread_recovery_owned[dead_tid];

		if (owned->handle != NULL && !owned->terminate_sent) {
			TerminateBackgroundWorker(owned->handle);
			owned->terminate_sent = true;
		}
	}
	for (;;) {
		TimestampTz now;
		bool retained = false;
		long wait_ms;
		int rc;

		for (dead_tid = XLP_THREAD_ID_FIRST_REAL;
			 dead_tid <= CLUSTER_WAL_THREAD_MAX; dead_tid++)
			thread_recovery_reap_one(dead_tid, &retained);
		if (!retained)
			return;
		now = GetCurrentTimestamp();
		if (now >= deadline)
			ereport(FATAL,
					(errmsg("timed out reaping online thread-recovery workers during LMON shutdown")));
		wait_ms = (long)((deadline - now + INT64CONST(999)) / INT64CONST(1000));
		if (wait_ms > 50)
			wait_ms = 50;
		if (wait_ms < 1)
			wait_ms = 1;
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   wait_ms, WAIT_EVENT_CLUSTER_BGPROC_LMON_MAIN_LOOP);
		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);
	}
}

#else /* !USE_PGRAC_CLUSTER */

/* Disable-cluster build: this file compiles to nothing. */

#endif /* USE_PGRAC_CLUSTER */
