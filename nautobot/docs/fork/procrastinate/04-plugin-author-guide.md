# Plugin author guide: the TaskBackend abstraction

> Audience: developers writing Nautobot apps/plugins that dispatch Jobs.
>
> **TL;DR:** if your plugin just calls `JobResult.enqueue_job(...)` you have
> nothing to change. The dispatch path is now backend-agnostic. The rest of
> this document covers the rare cases where you need to know which backend
> is active or want to do something custom.

## What did and didn't change

### Unchanged

- The `Job` class API. Subclass it, define `run()`, register with
  `register_jobs(...)`. Same as before.
- `JobResult.enqueue_job(...)` — same signature, same return shape. This is
  the canonical way to dispatch a job and continues to work under any backend.
- `JobResult` / `JobLogEntry` / `ScheduledJob` model fields. Status values,
  log levels, scheduled-job schedule formats — all preserved.
- Lifecycle hooks: `before_start`, `__call__`/`run`, `on_success`,
  `on_failure`, `on_retry`, `after_return`. Order and signatures are
  identical across backends.
- `self.request.id` inside a Job still resolves to the JobResult's task id.
  Under Celery, `self.request` is the real Celery `Task.request`; under
  Procrastinate it's a `JobRequest` dataclass with `.id` and `.properties`
  attributes. **If your code only reads `.id` and `.properties[...]` you
  won't notice the difference.**
- `JobResult.celery_kwargs` (a DB field) is still populated with the legacy
  `nautobot_job_*` keys regardless of backend, so any UI/reporting code that
  reads it continues to work.

### Changed (and why)

- A new package `nautobot.core.task_backends` houses the `TaskBackend` ABC
  and its implementations (`CeleryBackend`, `ProcrastinateBackend`). The
  active backend is resolved via `get_task_backend()`.
- `JobResult.enqueue_job()`'s implementation now delegates dispatch to the
  active backend. The shape of the legacy `celery_kwargs` dict it builds
  is preserved.
- A new setting `TASK_BACKEND` (env: `NAUTOBOT_TASK_BACKEND`) selects which
  backend is active. Defaults to `"celery"`.

## When does this matter to you?

You **only** need to think about this if:

1. You're writing a plugin that wraps or replaces `JobResult.enqueue_job`.
2. You're writing a plugin that introspects `self.request` for attributes
   other than `.id` and `.properties` (e.g., Celery-specific
   `.delivery_info`, `.callbacks`, `.is_eager`).
3. You're shipping a custom task backend (uncommon).

For every other case, your code is already portable.

## API surface for backend-aware code

If you genuinely need to know which backend is active:

```python
from nautobot.core.task_backends import get_task_backend

backend = get_task_backend()
if backend.name == "celery":
    ...
elif backend.name == "procrastinate":
    ...
```

`get_task_backend()` returns a cached `TaskBackend` instance. Tests that
switch backends mid-run should call `get_task_backend.cache_clear()`.

### The TaskBackend interface

```python
class TaskBackend(abc.ABC):
    name: str  # "celery" | "procrastinate" | ...

    def enqueue(self, *, job_result_id, job_class_path, args, kwargs, options) -> DispatchResult:
        ...

    def enqueue_sync(self, *, job_result_id, job_class_path, args, kwargs, options) -> DispatchResult:
        ...

    def get_active_workers(self) -> int:
        ...

    def get_periodic_runner(self) -> PeriodicRunner | None:
        ...
```

You normally don't call these directly — `JobResult.enqueue_job` does. But
they're public for advanced cases.

### EnqueueOptions

Dispatch options are passed as an `EnqueueOptions` dataclass:

```python
from nautobot.core.task_backends import EnqueueOptions

options = EnqueueOptions(
    queue="default",
    soft_time_limit=300,
    time_limit=600,
    profile=False,
    console_log=False,
    ignore_singleton_lock=False,
    user_id=user.id,
    job_model_id=job.id,
    schedule_id=schedule.id if schedule else None,
    branch_name=None,                # for nautobot_version_control
    extra={"some_celery_arg": ...},  # escape hatch
)
```

Backends consume the fields they understand. `extra` is a passthrough dict —
CeleryBackend folds it into `apply_async()` kwargs; ProcrastinateBackend
ignores it.

## Writing a custom backend

If you need a different broker (e.g., RabbitMQ via something other than
Celery, or a custom in-memory queue for tests), subclass `TaskBackend`:

```python
# my_plugin/backends.py
from nautobot.core.task_backends import TaskBackend, DispatchResult

class MyBackend(TaskBackend):
    name = "my_backend"

    def enqueue(self, *, job_result_id, job_class_path, args, kwargs, options):
        # Your dispatch logic here.
        # Required guarantees:
        #   - The job's BaseJob lifecycle hooks fire in order
        #   - JobResult.status transitions: PENDING -> STARTED -> SUCCESS/FAILURE
        #   - JobLogEntry rows are created (use the helpers in
        #     nautobot.core.task_backends.runner)
        ...
        return DispatchResult(task_id=job_result_id, backend=self.name)

    def enqueue_sync(self, *, job_result_id, ...):
        ...

    def get_active_workers(self) -> int:
        return -1  # unknown is fine
```

Activate it with:

```bash
NAUTOBOT_TASK_BACKEND=my_plugin.backends.MyBackend
```

Any dotted path that resolves to a `TaskBackend` subclass is accepted —
you're not limited to the two built-in names.

### Helpers for custom backends

The runner module exposes building blocks:

```python
from nautobot.core.task_backends.runner import (
    JobRequest,                       # synthesize a Job.request shape
    make_job_request,                 # = JobRequest with properties dict
    open_branch_context,              # BranchContext from EnqueueOptions
    ensure_job_log_handler_attached,  # idempotent NautobotDatabaseHandler attach
    task_id_log_context,              # ctx mgr: make NautobotDatabaseHandler
                                      #   work for non-Celery backends
)
```

Use `task_id_log_context(task_id)` around any place a job's BaseJob lifecycle
hooks run. Without it, `logger.info(...)` calls inside the Job silently fail
to write `JobLogEntry` rows.

## Reading `Job.request`

Under Celery your `Job.request` is a `celery.app.task.Context`. Under
Procrastinate (and future backends using the runner helpers) it's a
`JobRequest` dataclass:

```python
@dataclass
class JobRequest:
    id: str
    properties: dict[str, Any]
```

**If your plugin reads anything beyond `.id` and `.properties`, document
that as a Celery-specific feature and gate it.** Example:

```python
class MyJob(Job):
    def run(self):
        task_id = self.request.id  # always works
        user_id = self.request.properties.get("nautobot_job_user_id")  # always works

        if get_task_backend().name == "celery":
            # Celery-specific introspection
            from celery.utils import gen_unique_id
            ...
```

## Testing your plugin under both backends

Nautobot's test config sets:

```python
CELERY_TASK_ALWAYS_EAGER = True
PROCRASTINATE_ALWAYS_EAGER = True
```

Both are the "run inline, don't defer to a worker" flag for their respective
backends. To test your plugin under Procrastinate:

```python
from django.test import TransactionTestCase, override_settings
from nautobot.core.task_backends import get_task_backend


@override_settings(TASK_BACKEND="procrastinate", PROCRASTINATE_ALWAYS_EAGER=True)
class MyJobTests(TransactionTestCase):
    databases = ("default", "job_logs")  # JobLogEntry uses 'job_logs' alias

    def setUp(self):
        super().setUp()
        get_task_backend.cache_clear()  # backend resolution is cached

    def tearDown(self):
        super().tearDown()
        get_task_backend.cache_clear()
```

See `nautobot.core.tests.test_task_backends.ProcrastinateBackendEndToEndTests`
for a full example.

## Migration checklist for existing plugins

- [ ] Your plugin imports `JobResult.enqueue_job` somewhere → no change needed.
- [ ] You read `self.request.id` in your Job class → still works.
- [ ] You read `self.request.<other-attr>` → check it's on `JobRequest` or
      gate behind `get_task_backend().name == "celery"`.
- [ ] You patch `nautobot.extras.jobs.run_job` in tests → still works under
      Celery, but won't fire under Procrastinate (different code path).
      Consider parameterizing those tests across backends.
- [ ] You add Celery signal handlers → these only fire under Celery. If
      you need backend-agnostic startup hooks, use a Django `AppConfig.ready()`.

## Further reading

- [01-celery-surface-inventory.md](./01-celery-surface-inventory.md) — every
  place Nautobot touches Celery and why it's there.
- [02-task-backend-design.md](./02-task-backend-design.md) — the abstraction
  design and open questions.
- [03-running-procrastinate.md](./03-running-procrastinate.md) — operator
  guide for enabling and running the Procrastinate backend.
