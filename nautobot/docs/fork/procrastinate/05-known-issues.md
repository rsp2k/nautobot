# Known issues

This page tracks open items in the Procrastinate fork. Each entry has enough
context to pick up and investigate without re-tracing how we got here.

## `test_bulk_delete_system_jobs_fail` hangs in isolation (NOT a fork-introduced bug)

**Reproduced on upstream `develop` (`a726cdbff`, before any of this fork's
commits):**

```bash
git checkout a726cdbff
invoke tests \
    --label nautobot.extras.tests.test_jobs.JobTransactionTest.test_bulk_delete_system_jobs_fail \
    --no-parallel --no-keepdb --no-cache-test-fixtures --no-input \
    --skip-docs-build --failfast
```

This hangs indefinitely after "System check identified no issues". A
PostgreSQL `\dx` shows:

```
 pid  | state               | wait_event | query
 3148 | idle in transaction | ClientRead | CLOSE "_django_curs_..._sync_255"
 3341 | idle                | ClientRead | INSERT INTO "extras_joblogentry" ... ('Deleting 3 jobs...')
```

Both connections waiting on `Client/ClientRead` — Postgres is waiting for
the Python client to send the next query. The Python client has stopped
responding.

**This is a pre-existing upstream Nautobot bug or test-runner
configuration interaction, NOT introduced by this fork.** The original
investigation suspected `procrastinate.contrib.django` or my CeleryBackend
refactor; both were ruled out by reproducing the hang on unmodified
upstream code.

**Suspected cause:** `nautobot.extras.utils.bulk_delete_with_bulk_change_logging`
opens a server-side cursor via `qs.iterator(chunk_size=1000)` inside a
`transaction.atomic()`. When the test invokes `BulkDeleteObjects` on
system jobs (which `Job.delete()` rejects via `ProtectedError`), the
iterator might be partially consumed leaving the cursor open. Combined
with the cross-DB `JobLogEntry` insert on the `job_logs` alias, something
in the connection-management dance stops responding.

**File:** `nautobot/extras/utils.py:934` — `bulk_delete_with_bulk_change_logging`.

**Workaround:** Run targeted test labels rather than the full
JobTransactionTest sweep. Or run with `--keepdb --cache-test-fixtures`
(invoke defaults) — those flag combinations were the ones that succeeded
in this session's earlier (Task #4 / Task #6) test runs.

**Open for upstream maintainers, not for this fork.**

---

## Original investigation: full `JobTransactionTest` suite hangs when procrastinate is installed

**Symptom:** Running `invoke tests --label
nautobot.extras.tests.test_jobs.JobTransactionTest` hangs indefinitely
after test fixture loading. A PostgreSQL connection sits in `idle in
transaction` state holding a closed cursor for the duration of the hang.

**What works:**

- Individual tests from the class run fine (`test_job_pass` passes in ~3s).
- The full task-backend test sweep (37 tests) passes.
- The Procrastinate end-to-end test (`ProcrastinateBackendEndToEndTests`)
  passes.
- The 3-test `JobResultEnqueueJobCase` sweep passes.

**What doesn't work:**

- Running the full `JobTransactionTest` class (30 tests) as a single
  `nautobot-server test` invocation hangs in fixture loading, both with and
  without `--parallel`, and with both fresh and reused test databases.

**What we initially suspected and then ruled out:** Initial suspicion was
that `procrastinate.contrib.django` being in `INSTALLED_APPS` was causing
DB-connection interference. **Investigation disproved this.** The conditional
add (now in `settings.py`) keeps procrastinate out of `INSTALLED_APPS` under
`TASK_BACKEND=celery`, and the hang still reproduces. So the cause is
elsewhere.

**What the PostgreSQL view actually shows:**

```sql
-- During the hang:
 pid   | state               | wait_event | query
 51182 | idle in transaction | ClientRead | INSERT INTO extras_joblogentry ...
                                            ('Deleting 3 jobs...')
 51178 | idle in transaction | ClientRead | CLOSE _django_curs_..._sync_271
```

Both connections are waiting on `Client/ClientRead` — Postgres is waiting
for the Python client to send the next query (or commit/rollback). The
client side has stopped responding. The "Deleting 3 jobs..." log message
is emitted by `nautobot.core.jobs.bulk_actions.BulkDeleteObjects.run()`
(`nautobot/core/jobs/bulk_actions.py:246`), so some test is exercising
that job and getting stuck after the log entry is buffered for insert.

**Updated hypotheses:**

1. **Test DB state corruption from previously killed runs.** Earlier in
   the same session, several `nautobot-server test` invocations were
   SIGKILLed mid-test. The `--keepdb` flag preserves the test DB across
   runs. If a killed run left uncommitted state (e.g., procrastinate
   tables in a transient migration state), every subsequent `--keepdb`
   run inherits it. **A clean `--no-keepdb --no-cache-test-fixtures`
   run still hangs, so this isn't the only cause, but is probably a
   contributing factor.**

2. **Interaction between `--parallel` and the cross-DB JobLogEntry
   handler.** Nautobot writes log entries to a separate `job_logs` DB
   alias. Under `--parallel`, fork workers each get their own connection
   to both DBs. If one worker holds a transaction on the default DB and
   another worker tries to read from `job_logs`, Django's
   `DATABASE_ROUTERS` config may serialize the access in an unexpected
   way. Worth testing with `--no-parallel`.

3. **Cross-fork pickle/import of `_FakeCeleryTask` or `task_id_log_context`
   state.** Even though these are never invoked under Celery, they're
   imported when `nautobot.core.task_backends.runner` is imported. If
   the module-level imports do anything stateful, fork workers might
   inherit a partial state.

**Investigation starting points:**

- Strace the stuck fork worker (PID from `pg_stat_activity` → host PID
  via `nsenter`) to confirm the Python process is blocked on a system
  call (likely a futex or read) vs. spinning in user code.
- Run with `--no-parallel` and a freshly-dropped test DB to isolate (1) vs.
  (2). The session ran `--no-parallel` once and it also hung, but the
  test DB had leftover state from earlier; need to retry with
  `--no-parallel --no-keepdb`.
- Bisect the recent commits. Git checkout the last commit before
  `2725b1e28 Add ProcrastinateBackend skeleton` and confirm the hang
  doesn't reproduce there. Then re-apply commits one at a time.

**Impact:** Low for production deployments — the backend itself works
end-to-end (verified by `ProcrastinateBackendEndToEndTests`). Medium for
development workflow — running the full unit suite under TASK_BACKEND=celery
is the most common dev check, and it's currently broken.

**Workaround:** Run targeted test labels rather than the full suite when
iterating on the fork. The Task #9 CI matrix work, when picked up, should
investigate and fix this before enabling the full suite in CI.
