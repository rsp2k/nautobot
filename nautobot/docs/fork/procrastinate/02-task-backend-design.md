# TaskBackend Abstraction — Design

> **Status:** Draft for review.
> **Predecessor:** [01-celery-surface-inventory.md](./01-celery-surface-inventory.md)
> **Next:** Implementation in `nautobot.core.task_backends` (new package)

## Goals

1. Let operators choose between Celery (current) and Procrastinate (new) per deployment, via a single setting.
2. Job authors write the **same code** regardless of backend. The Job class and its lifecycle hooks don't change.
3. Existing Nautobot tests pass under Celery with the abstraction in place — proof of behavior-preservation before adding Procrastinate.
4. Keep upstream sync cheap. Concentrate fork-specific code in a small number of new files; touch existing code minimally.

## Non-goals (v1)

- Migrating data between backends at runtime. Pick one per deployment.
- Per-job backend selection. The backend is a deployment-wide choice.
- Workflow composition (`chain`, `group`, `chord`). Nautobot doesn't use it — confirmed by audit.
- Replacing the existing JobResult/JobLogEntry models. We bridge to them; we don't redesign them.

## Design summary

Introduce a small `TaskBackend` interface in `nautobot.core.task_backends`. Existing Celery wiring stays in `nautobot.core.celery` but is wrapped as `CeleryBackend`. A second module `nautobot.core.task_backends.procrastinate` adds `ProcrastinateBackend`. A single setting (`NAUTOBOT_TASK_BACKEND`) selects which backend the runtime uses.

`JobResult.enqueue_job()` — the single chokepoint identified in the audit — is the only place in `nautobot/extras/` that calls into the backend. Everything else (logging handlers, scheduler, prometheus, control commands) stays in each backend's own module. This is the **minimal interface** option chosen during design review: backend-specific concerns aren't forced into a shared shape they don't fit.

```
┌─────────────────────────────────────────────────────────────────┐
│                  Existing Nautobot code                         │
│   (Job class, JobResult, JobLogEntry, ScheduledJob, views,      │
│    API, management commands — all unchanged in v1)              │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            │  calls
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│   JobResult.enqueue_job()      <- only call site that changes   │
│   - resolves Job model                                          │
│   - builds dispatch kwargs                                      │
│   - calls get_task_backend().enqueue(...)                       │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              nautobot.core.task_backends.TaskBackend (ABC)              │
│                                                                 │
│   enqueue(job_result_id, job_class_path, args, kwargs, **opts)  │
│   enqueue_sync(...)            # for CELERY_TASK_ALWAYS_EAGER   │
│   get_active_workers() -> int                                   │
│   get_periodic_runner() -> PeriodicRunner | None                │
└───────────┬─────────────────────────────────────┬───────────────┘
            │                                     │
            ▼                                     ▼
   ┌────────────────────┐                ┌──────────────────────────┐
   │   CeleryBackend    │                │  ProcrastinateBackend    │
   │  (wraps existing   │                │  (new code, opt-in via   │
   │   celery glue)     │                │   pyproject extra)       │
   └────────────────────┘                └──────────────────────────┘
```

## Settings

A single, env-overridable Django setting:

```python
# nautobot/core/settings.py (new)
TASK_BACKEND = os.getenv("NAUTOBOT_TASK_BACKEND", "celery")
# Allowed values: "celery", "procrastinate"
```

Old settings remain. `CELERY_*` settings only matter when `TASK_BACKEND == "celery"`. We add `PROCRASTINATE_*` settings only when needed.

## Module layout

```
nautobot/
  core/
    task_backends/             # NOTE: not `tasks/` because nautobot.core.tasks
                               # already exists (release-fetching module)
      __init__.py              # get_task_backend() factory + caching
      base.py                  # TaskBackend ABC + PeriodicRunner ABC
      celery_backend.py        # CeleryBackend (thin wrapper)
      procrastinate_backend.py # ProcrastinateBackend (new)
      serializers.py           # Shared encoder/decoder (extracted from
                               #   nautobot.core.celery.encoders for reuse)
    celery/                    # Unchanged for v1; CeleryBackend delegates here
      __init__.py
      task.py
      schedulers.py
      ...
```

Why a separate `nautobot.core.task_backends` package instead of putting `TaskBackend` inside `nautobot.core.celery`? Naming. Calling the abstract layer "celery.TaskBackend" is misleading once Procrastinate is real. Future maintainers should be able to find the abstraction by name.

## The interface

```python
# nautobot/core/task_backends/base.py
from __future__ import annotations
import abc
from dataclasses import dataclass
from typing import Any, Iterable
from uuid import UUID


@dataclass(frozen=True)
class DispatchResult:
    """Returned by TaskBackend.enqueue() to the caller."""
    task_id: UUID            # JobResult.task_id (also Celery task_id under CeleryBackend)
    backend: str             # "celery" | "procrastinate" (for logging/debugging only)


@dataclass(frozen=True)
class EnqueueOptions:
    """All optional dispatch knobs. Backends ignore what they don't support."""
    queue: str | None = None
    soft_time_limit: float | None = None
    time_limit: float | None = None
    profile: bool = False
    console_log: bool = False
    ignore_singleton_lock: bool = False
    user_id: UUID | None = None
    job_model_id: UUID | None = None
    scheduled_job_id: UUID | None = None
    branch_name: str | None = None     # for nautobot_version_control plugin


class TaskBackend(abc.ABC):
    """Backend-agnostic interface for enqueueing and inspecting Nautobot Jobs.

    Implementations live in nautobot.core.task_backends.celery_backend and
    nautobot.core.task_backends.procrastinate_backend.
    """

    name: str  # "celery" | "procrastinate"

    @abc.abstractmethod
    def enqueue(
        self,
        *,
        job_result_id: UUID,
        job_class_path: str,
        args: Iterable[Any],
        kwargs: dict[str, Any],
        options: EnqueueOptions,
    ) -> DispatchResult:
        """Dispatch a job for asynchronous execution.

        Backends MUST:
          - Run the job inside Django's normal request context (DB session, etc.)
          - Call the job's BaseJob lifecycle hooks (before_start/__call__/run/
            on_success/on_failure/after_return) in the worker process
          - Update JobResult.status as the task transitions
          - Capture log records into JobLogEntry via the existing
            NautobotDatabaseHandler (or an equivalent)
        """

    @abc.abstractmethod
    def enqueue_sync(
        self,
        *,
        job_result_id: UUID,
        job_class_path: str,
        args: Iterable[Any],
        kwargs: dict[str, Any],
        options: EnqueueOptions,
    ) -> DispatchResult:
        """Run a job synchronously in the calling process.

        Used by CELERY_TASK_ALWAYS_EAGER and by JobResult.enqueue_job(synchronous=True).
        Implementations should run the same lifecycle as enqueue() but inline.
        """

    @abc.abstractmethod
    def get_active_workers(self) -> int:
        """Return number of active workers for the StatusView health check.
        Return -1 if the backend cannot determine this (will be surfaced as 'unknown')."""

    def get_periodic_runner(self) -> "PeriodicRunner | None":
        """Return the periodic-task runner for this backend, or None if scheduling
        is handled out-of-band (e.g., celery beat as a separate process).

        v1: CeleryBackend returns None (existing celery beat scheduler stays as-is).
            ProcrastinateBackend returns a NautobotProcrastinatePeriodicRunner.
        """
        return None
```

```python
# nautobot/core/task_backends/base.py (continued)
class PeriodicRunner(abc.ABC):
    """Runs ScheduledJob rows on schedule. Distinct from TaskBackend because:
       - Celery beat runs as a separate OS process (delegated to it entirely)
       - Procrastinate has no built-in DB-row-driven scheduler, we write our own
    """

    @abc.abstractmethod
    def tick(self) -> int:
        """Examine ScheduledJob rows whose next-run time has elapsed and enqueue them.
        Returns the number of jobs enqueued this tick."""
```

## Backend selection

```python
# nautobot/core/task_backends/__init__.py
from functools import lru_cache
from django.conf import settings
from django.utils.module_loading import import_string

from .base import TaskBackend, EnqueueOptions, DispatchResult, PeriodicRunner

_BUILTIN_BACKENDS = {
    "celery":       "nautobot.core.task_backends.celery_backend.CeleryBackend",
    "procrastinate":"nautobot.core.task_backends.procrastinate_backend.ProcrastinateBackend",
}


@lru_cache(maxsize=1)
def get_task_backend() -> TaskBackend:
    name = getattr(settings, "TASK_BACKEND", "celery")
    dotted_path = _BUILTIN_BACKENDS.get(name, name)  # custom backends OK
    backend_cls = import_string(dotted_path)
    return backend_cls()


__all__ = ["TaskBackend", "EnqueueOptions", "DispatchResult", "PeriodicRunner",
           "get_task_backend"]
```

Cached singleton via `lru_cache`. Test settings can reset it with `get_task_backend.cache_clear()`.

## How `JobResult.enqueue_job()` changes

Today (simplified):

```python
# nautobot/extras/models/jobs.py (today)
@classmethod
def enqueue_job(cls, job_model, user, ...):
    job_result = cls.objects.create(...)
    celery_kwargs = cls._build_celery_kwargs(...)
    if synchronous:
        run_job.apply(args=[...], kwargs=celery_kwargs, task_id=str(job_result.id))
    else:
        transaction.on_commit(
            lambda: run_job.apply_async(args=[...], kwargs=celery_kwargs,
                                        task_id=str(job_result.id), **routing)
        )
    return job_result
```

After:

```python
# nautobot/extras/models/jobs.py (after)
from nautobot.core.task_backends import get_task_backend, EnqueueOptions

@classmethod
def enqueue_job(cls, job_model, user, ...):
    job_result = cls.objects.create(...)
    options = EnqueueOptions(
        queue=...,
        user_id=user.id,
        job_model_id=job_model.id,
        scheduled_job_id=...,
        soft_time_limit=...,
        time_limit=...,
        profile=...,
        console_log=...,
        branch_name=...,
        ignore_singleton_lock=...,
    )
    backend = get_task_backend()
    fn = backend.enqueue_sync if synchronous else backend.enqueue
    transaction.on_commit(
        lambda: fn(
            job_result_id=job_result.id,
            job_class_path=job_model.class_path,
            args=task_args,
            kwargs=task_kwargs,
            options=options,
        )
    )
    return job_result
```

Diff is small. The Celery-specific `celery_kwargs` keys (`nautobot_job_user_id`, `nautobot_job_branch_name`, etc.) move into `CeleryBackend.enqueue()` where they belong.

## Implementation plan per backend

### CeleryBackend (Task #4)

A thin wrapper. The body of `CeleryBackend.enqueue()` is essentially what `enqueue_job()` does today, lifted out of `JobResult` and into the backend module.

```python
# nautobot/core/task_backends/celery_backend.py
class CeleryBackend(TaskBackend):
    name = "celery"

    def enqueue(self, *, job_result_id, job_class_path, args, kwargs, options):
        from nautobot.core.celery import app
        from nautobot.extras.jobs import run_job, run_console_log_job_and_return_job_result
        task = run_console_log_job_and_return_job_result if options.console_log else run_job
        celery_kwargs = self._build_celery_kwargs(options)
        task.apply_async(
            args=list(args),
            kwargs={**kwargs, **celery_kwargs},
            task_id=str(job_result_id),
            queue=options.queue,
            soft_time_limit=options.soft_time_limit,
            time_limit=options.time_limit,
        )
        return DispatchResult(task_id=job_result_id, backend=self.name)

    def enqueue_sync(self, ...):  # mirror, using task.apply()
        ...

    def get_active_workers(self) -> int:
        from nautobot.core.celery import app
        active = app.control.inspect().active() or {}
        return len(active)

    def get_periodic_runner(self):
        return None  # celery beat runs out-of-band
```

The Celery `run_job` task, `NautobotTask` base class, signal handlers, scheduler, custom serializer — **all stay where they are in `nautobot.core.celery`**. CeleryBackend just calls into them.

This makes Task #4 a refactor in the strict sense: no behavior change. Existing tests prove it.

### ProcrastinateBackend (Task #5)

The new code. Sketch:

```python
# nautobot/core/task_backends/procrastinate_backend.py
class ProcrastinateBackend(TaskBackend):
    name = "procrastinate"

    def __init__(self):
        from procrastinate.contrib.django import app as procrastinate_app
        self.app = procrastinate_app
        self._task = self._register_run_job_task()

    def _register_run_job_task(self):
        @self.app.task(name="nautobot.run_job", pass_context=True)
        def _run_job(context, *, job_result_id, job_class_path, args, kwargs, options_dict):
            # Mirrors the body of nautobot.extras.jobs.run_job(),
            # but without the Celery Task bound self.
            return _execute_job_lifecycle(
                job_result_id=job_result_id,
                job_class_path=job_class_path,
                args=args,
                kwargs=kwargs,
                options=EnqueueOptions(**options_dict),
                context=context,
            )
        return _run_job

    def enqueue(self, *, job_result_id, job_class_path, args, kwargs, options):
        self._task.defer(
            queue=options.queue or "default",
            task_kwargs=dict(
                job_result_id=str(job_result_id),
                job_class_path=job_class_path,
                args=list(args),
                kwargs=kwargs,
                options_dict=asdict(options),
            ),
        )
        return DispatchResult(task_id=job_result_id, backend=self.name)
```

The shared lifecycle helper `_execute_job_lifecycle()` lives in `nautobot.core.task_backends.runner` and is called by both `run_job` (Celery wrapper) and `_run_job` (Procrastinate wrapper). That's where we *deduplicate* the job execution body so both backends behave identically.

### Serializer shim

Extracted from `nautobot.core.celery.encoders` into `nautobot.core.task_backends.serializers`:

```python
# nautobot/core/task_backends/serializers.py
from nautobot.core.celery.encoders import NautobotKombuJSONEncoder, nautobot_kombu_json_loads_hook
import json

def dumps(obj) -> str:
    return json.dumps(obj, cls=NautobotKombuJSONEncoder)

def loads(s: str):
    return json.loads(s, object_hook=nautobot_kombu_json_loads_hook)
```

Procrastinate's `defer()` accepts any JSON-serializable payload. We pre-serialize Django model arguments through `dumps()` at the call site and reverse with `loads()` inside the worker. CeleryBackend keeps Kombu registration as-is.

(Future: PR upstream to move `NautobotKombuJSONEncoder` from `nautobot.core.celery.encoders` into `nautobot.core.task_backends.serializers` and re-export from old path. Upstream-friendly.)

### Periodic runner (Task #7)

`NautobotProcrastinatePeriodicRunner.tick()`:

1. Query `ScheduledJob` rows where `enabled=True` and next-run time has passed.
2. For each, compute whether it should fire (mirror `NautobotScheduleEntry.is_due()` logic).
3. Call `procrastinate_backend.enqueue(...)` with the scheduled-job context.
4. Update `total_run_count`, `last_run_at`.
5. Touch `CELERY_BEAT_HEARTBEAT_FILE` (rename to `NAUTOBOT_SCHEDULER_HEARTBEAT_FILE`).

Run via a new management command:

```bash
nautobot-server scheduler
```

Implemented as an asyncio loop with a 5-second tick (configurable). One scheduler per deployment.

## Settings touched

```python
# Existing (kept):
CELERY_*  # only meaningful when TASK_BACKEND == "celery"

# New:
TASK_BACKEND = "celery"   # default keeps current behavior

# New (only used by ProcrastinateBackend):
PROCRASTINATE_DATABASE_URL = ...  # defaults to Django DATABASES["default"]
PROCRASTINATE_WORKER_CONCURRENCY = 4
PROCRASTINATE_SCHEDULER_TICK_SECONDS = 5
```

## Test strategy

1. **Phase 1 (Task #4):** Land `TaskBackend` + `CeleryBackend`. Run full Nautobot test suite — must pass with zero changes to test code. This proves the refactor is behavior-preserving.
2. **Phase 2 (Task #5–#6):** Implement `ProcrastinateBackend`. Add backend-parametrized tests for the small set of dispatch-behavior tests. CI matrix runs both.
3. **Phase 3 (Task #7):** Periodic runner tests using freezegun for clock control.

## Open questions

1. **Worker entry point under Procrastinate.** Procrastinate workers are `procrastinate worker`. Do we wrap with `nautobot-server worker` to give a unified UX? *Lean: yes — symmetry with `nautobot-server celery worker`.*
2. **Heartbeat / liveness files.** Rename `CELERY_WORKER_HEARTBEAT_FILE` → `NAUTOBOT_WORKER_HEARTBEAT_FILE` (backend-agnostic) and keep `CELERY_*` as deprecated alias? *Lean: yes, with deprecation warning.*
3. **`apps.jobs.register_jobs`.** Currently triggered by Celery's `import_modules` signal. Move into the Django `AppConfig.ready()` so both backends get it for free? *Lean: yes — Django AppConfig is the right place anyway.*
4. **Prometheus metrics.** Each backend module owns its prometheus startup. *Confirmed by "minimal interface" design choice.*

---

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Procrastinate's Django integration imports differ across versions | Pin `procrastinate>=2.10,<3` initially; bump in a separate PR. |
| Branch context propagation breaks under Procrastinate | Each backend module is responsible for installing its own context middleware (`NautobotTask.apply_async` equivalent inside Procrastinate's `pass_context=True`). |
| Singleton lock relies on Redis cache | Redis stays installed (Nautobot uses it for Django cache anyway). The singleton lock logic stays in `BaseJob.before_start()` and is backend-agnostic. |
| Upstream renames `enqueue_job()` keyword args | This is the one fork-modified method; rebase conflicts are localized to it. Worst case we re-port. |
| Tests that introspect Celery state break under Procrastinate | Mark them `@pytest.mark.celery_only`. CI matrix skips them under Procrastinate. |
