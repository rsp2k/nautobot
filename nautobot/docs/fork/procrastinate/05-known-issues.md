# Known issues

This page tracks open items in the Procrastinate fork. Each entry has enough
context to pick up and investigate without re-tracing how we got here.

## Test interaction: full `JobTransactionTest` suite hangs when procrastinate is installed

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

**Reproduces only when:** `procrastinate.contrib.django` is in
`INSTALLED_APPS`. Before that addition (Task #4 era), the same 30 tests
passed in ~107 seconds.

**Hypotheses (none yet verified):**

1. `procrastinate.contrib.django` uses async DB connections that interact
   poorly with Django's parallel test framework's transaction wrapping.
2. Procrastinate's migrations are creating tables whose constraints or
   triggers conflict with Nautobot's test fixture loading order.
3. A signal handler registered by procrastinate.contrib.django runs during
   each test setUp and blocks on a DB connection.

**Investigation starting points:**

- Strace the stuck fork worker to see what syscall it's blocked on.
- Compare `psql -c "\dt+ procrastinate_*"` before vs. during the hang.
- Try removing `procrastinate.contrib.django` from INSTALLED_APPS
  temporarily and confirming the hang goes away.
- Check whether the same hang happens with newer/older procrastinate
  versions (we're on 3.8.1).

**Impact:** Low for production deployments — the backend itself works
end-to-end (verified by `ProcrastinateBackendEndToEndTests`). Medium for
development workflow — running the full unit suite under TASK_BACKEND=celery
is the most common dev check, and it's currently broken.

**Workaround:** Run targeted test labels rather than the full suite when
iterating on the fork. The Task #9 CI matrix work, when picked up, should
investigate and fix this before enabling the full suite in CI.
