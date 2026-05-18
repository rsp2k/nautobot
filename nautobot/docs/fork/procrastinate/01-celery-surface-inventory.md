# Nautobot Celery Integration Surface Inventory

> **Fork context:** This document tracks every place Nautobot touches Celery, as the foundation for introducing a `TaskBackend` abstraction that supports both Celery (current) and Procrastinate (new) as alternative execution backends.
>
> Based on Nautobot 3.1.3a0 (commit `a726cdbff`, branch `develop`).

---

## Overview

This document maps every touchpoint where Celery is integrated into the Nautobot codebase. The goal is to identify all integration points that must be abstracted or reimplemented when introducing an alternative task backend (e.g., Procrastinate).

---

## 1. Celery App Definition & Configuration

### 1.1 Core Celery Application

| **Location** | **Details** |
|---|---|
| `nautobot/core/celery/__init__.py:32-36` | **Celery App Instantiation**: Custom `NautobotCelery` class extends `Celery`, sets custom task class to `NautobotTask`. App instance `app` is created with name `"nautobot"`. Configuration loaded from Django settings with `CELERY_` prefix. |
| `nautobot/core/celery/__init__.py:42` | **Config Loading**: `app.config_from_object("django.conf:settings", namespace="CELERY")` |
| `nautobot/core/celery/__init__.py:45` | **Task Auto-discovery**: `app.autodiscover_tasks()` |

### 1.2 Django Settings (Configuration)

**File**: `nautobot/core/settings.py` (lines 1042–1126)

Key settings (full list in original audit):

- `CELERY_BROKER_URL` — Redis (from `NAUTOBOT_CELERY_BROKER_URL`)
- `CELERY_RESULT_BACKEND = "nautobot.core.celery.backends.NautobotDatabaseBackend"`
- `CELERY_ACCEPT_CONTENT = ["nautobot_json"]` — Custom Kombu serializer
- `CELERY_TASK_SERIALIZER = "nautobot_json"`
- `CELERY_BEAT_SCHEDULER = "nautobot.core.celery.schedulers:NautobotDatabaseScheduler"`
- `CELERY_TASK_TRACK_STARTED = True`
- `CELERY_TASK_DEFAULT_QUEUE = "default"`
- `CELERY_TASK_SOFT_TIME_LIMIT = 300` / `CELERY_TASK_TIME_LIMIT = 600`
- Heartbeat files: `CELERY_BEAT_HEARTBEAT_FILE`, `CELERY_WORKER_HEARTBEAT_FILE`, `CELERY_WORKER_READINESS_FILE`
- `CELERY_HEALTH_PROBES_AS_FILES = False` (env-configurable)
- `CELERY_WORKER_PROMETHEUS_PORTS = []`

### 1.3 Custom Serialization (Kombu JSON)

| **Location** | **Details** |
|---|---|
| `nautobot/core/celery/encoders.py` | **`NautobotKombuJSONEncoder`**: Handles Django model instances, sets, TagsManager, exceptions, ZoneInfo. Models serialized with `id` + `__nautobot_type__` for lazy deserialization. Supports branching context. |
| `nautobot/core/celery/__init__.py:213-247` | **Kombu registration**: `nautobot_kombu_json_loads_hook()` deserializes via `nautobot_deserialize()` classmethod. Custom serializer registered as `"nautobot_json"`. |

---

## 2. Task Decoration & Registration

### 2.1 Shared Task Decoration

| **Location** | **Details** |
|---|---|
| `nautobot/core/celery/__init__.py:259` | **`nautobot_task` alias**: Re-exports Celery's `shared_task` decorator. Canonical decorator for Nautobot tasks. |
| `nautobot/extras/jobs.py:34` | `from nautobot.core.celery import import_jobs, nautobot_task` |
| `nautobot/extras/tasks.py:10` | System tasks decorated with `@nautobot_task(bind=True)` |

### 2.2 Job Runner Tasks (Core Entry Points)

**File**: `nautobot/extras/jobs.py`

| **Line** | **Task** | **Details** |
|---|---|---|
| `1349` | `run_job()` | `@nautobot_task(bind=True)`. Primary task runner. Calls `before_start()`, `__call__()`/`run()`, `on_success()`/`on_failure()`, `after_return()`. |
| `1448` | `run_console_log_job_and_return_job_result()` | `@nautobot_task(bind=True)`. Variant for console log capture. Delegates to `JobConsoleLogExecutor`. |

### 2.3 Job Registration

| **Location** | **Details** |
|---|---|
| `nautobot/core/celery/__init__.py:265-271` | `register_jobs()`: Registers Job instances to `registry["jobs"]` by class path. |
| `nautobot/core/celery/__init__.py:48-70` | `@signals.import_modules.connect` → `import_jobs()`: Loads system jobs, JOBS_ROOT, plugin jobs, Git-provided jobs. |
| `nautobot/apps/jobs.py:60` | `"register_jobs"` exported from core app. |

### 2.4 Custom Task Base Class

| **Location** | **Details** |
|---|---|
| `nautobot/core/celery/task.py:15-135` | **`NautobotTask`**: Extends `celery.Task`. Overrides `before_start()`, `after_return()`, `apply_async()`, `apply()`. Manages `BranchContext`, supports version control plugin. Handles `CELERY_TASK_ALWAYS_EAGER` mode. |

---

## 3. Nautobot Job Framework

### 3.1 Job Model

**File**: `nautobot/extras/models/jobs.py:97-320`

Significant fields: `module_name`, `job_class_name`, `grouping`, `name`, `description`, `installed`, `enabled`, `is_job_hook_receiver`, `is_job_button_receiver`, `has_sensitive_variables`, `is_singleton`, `console_log_default`, `hidden`, `dryrun_default`, `read_only`, `soft_time_limit`, `time_limit`, `supports_dryrun`, `job_queues` (M2M → `JobQueue`), `default_job_queue` (FK → `JobQueue`), `*_override` flags.

Properties: `job_class` (fetches class from `nautobot.extras.jobs.get_job()`), `class_path` (`"{module_name}.{job_class_name}"`).

### 3.2 JobResult Model

**File**: `nautobot/extras/models/jobs.py:400-1100`

Relationships: FK `Job`, FK `ScheduledJob` (nullable), FK `User`, OneToMany `JobLogEntry`.

Fields: `name`, `task_name`, `task_id` (UUID matching Celery task ID), `status` (`JobResultStatusChoices`, mapped from Celery states), `date_queued`, `date_started`, `date_done`, `result` (JSON), `traceback`, `celery_kwargs`, `task_args`, `task_kwargs`, `user_id`, `job_model_id`, `scheduled_job_id`.

**Key methods**:
- `enqueue_job()` (classmethod, lines 928–1103): **Central entry point**. Creates JobResult, builds celery_kwargs, dispatches via `run_job.apply_async()` or `run_job.apply()`. Handles Kubernetes queue logic, console logging, singleton locks, profiling.
- `_build_celery_kwargs()`: Constructs kwargs (`user_id`, `job_model_id`, `scheduled_job_id`, queue, branch, profile, etc.).
- `_sync_eager_result_to_job_result()`: Syncs synchronous task result to JobResult.
- `log()`: Creates `JobLogEntry` records.

### 3.3 JobLogEntry Model

Fields: `job_result` (FK), `log_level` (LogLevelChoices), `grouping` (main, initialization, post_run, …), `message`, `log_object` (nullable), `absolute_url` (nullable), `created`.

**Capture**: `NautobotDatabaseHandler` (in `nautobot/core/celery/log.py`) routes log records to `JobResult.log()`. Attached to celery task logger.

### 3.4 ScheduledJob Model

Fields: `name`, `job_model` (FK), `job_queue` (FK nullable), `user` (FK nullable), `schedule` (django-celery-beat JSONField), `enabled`, `total_run_count`, `last_run_at`, `args`, `kwargs`, `celery_kwargs`, `state` (`ACTIVE`/`ERRORED`/`DISABLED`), `start_time`, `stop_time`, `time_zone`.

Signals: `pre_save`, `pre_delete`, `post_save` connected to `ScheduledJobs.changed` / `.update_changed` for scheduler sync.

### 3.5 Job Enqueueing (Primary Entry Point)

`nautobot/extras/models/jobs.py:928-1103` — `JobResult.enqueue_job()`. **This is THE chokepoint** for all execution dispatch. Wraps job dispatch in `transaction.on_commit()`. Routes to `run_job.apply_async()` (async) or `run_job.apply()` (sync) or `run_kubernetes_job_and_return_job_result()` (K8s).

### 3.6 Job Class Lifecycle (User-Defined)

`nautobot/extras/jobs.py:123-900` — `BaseJob` / `Job`.

Hooks (in execution order):
1. `before_start()` (202-214)
2. `__call__()` (154-191) — deserializes kwargs, calls `run()`
3. `run()` — user-defined
4. `on_success()` (222-235)
5. `on_failure()` (253-268)
6. `on_retry()` (237-251)
7. `after_return()` (270-284)

---

## 4. Scheduling & Periodic Tasks

### 4.1 django-celery-beat Integration

- `pyproject.toml:37` — `django-celery-beat==2.8.1` (exact pin, due to API overrides)
- `nautobot/core/settings.py:1126` — `CELERY_BEAT_SCHEDULER = "nautobot.core.celery.schedulers:NautobotDatabaseScheduler"`
- `nautobot/core/admin.py:4-5,28` — Imports `django_celery_beat.admin`, removes Beat from admin menu

### 4.2 NautobotDatabaseScheduler

**File**: `nautobot/core/celery/schedulers.py:196-292`

- `NautobotScheduleEntry` (94-194): Extends `ModelEntry`. Sets task to `run_job` or `run_console_log_job_and_return_job_result`. Builds `options` with `nautobot_job_user_id`, `nautobot_job_job_model_id`, `nautobot_job_scheduled_job_id`. Overrides `_disable()` to mark schedule `ERRORED`. Handles missing user via `_record_missing_user_failure()`.
- `NautobotDatabaseScheduler` (196-292): Extends `DatabaseScheduler`. `Entry = NautobotScheduleEntry`, `Model = ScheduledJob`, `Changes = ScheduledJobs`. Override `apply_async()` to sync `total_run_count` and detect Kubernetes queue. Override `enabled_models_qs()`, `tick()` (touches heartbeat file).

---

## 5. Worker Entry Points

### 5.1 Management Command

`nautobot/core/management/commands/celery.py` — Thin wrapper around `celery.bin.celery.celery_main()`. Enables `nautobot-server celery worker`, `nautobot-server celery beat`.

### 5.2 Docker Compose Services

`development/docker-compose.yml`:

- `celery_worker`: `watchmedo auto-restart ... -- nautobot-server celery worker -l INFO --events`. Port 8081 for metrics. Health: `nautobot-server celery inspect ping`.
- `celery_beat`: `watchmedo auto-restart ... -- nautobot-server celery beat -l INFO`. Health: file mtime of `/tmp/nautobot_celery_beat_heartbeat`.

---

## 6. Signals & Lifecycle Hooks

### 6.1 Celery Signals Connected

| Location | Signal | Handler | Purpose |
|---|---|---|---|
| `nautobot/core/celery/__init__.py:48-70` | `import_modules` | `import_jobs()` | Load all Job classes at worker start |
| `nautobot/core/celery/__init__.py:161-165` | `after_setup_logger` | `setup_nautobot_global_logging()` | Add SUCCESS/FAILURE log levels |
| `nautobot/core/celery/__init__.py:168-172` | `after_setup_task_logger` | `setup_nautobot_task_logging()` | Add SUCCESS/FAILURE log levels |
| `nautobot/core/celery/__init__.py:175-182` | `celeryd_after_setup` | `setup_nautobot_job_logging()` | Add `NautobotDatabaseHandler` to celery.task / celery.redirected |
| `nautobot/core/celery/__init__.py:185-210` | `worker_ready` | `setup_prometheus()` | Start Prometheus HTTP server |
| `nautobot/core/celery/__init__.py:274-279` | `worker_ready` | `worker_ready()` | Touch worker readiness file (K8s) |
| `nautobot/core/celery/__init__.py:282-287` | `worker_shutdown` | `worker_shutdown()` | Remove readiness file |

### 6.2 Django Model Signals

`nautobot/extras/models/jobs.py:1835-1837` — `pre_delete`, `pre_save`, `post_save` on `ScheduledJob` → `ScheduledJobs.changed()` / `update_changed()` (scheduler sync).

### 6.3 Worker Boot Step

`nautobot/core/celery/__init__.py:290-316` — `LivenessProbe` (custom `bootsteps.StartStopStep`). Updates `CELERY_WORKER_HEARTBEAT_FILE` every 1s.

---

## 7. Result Backend

### 7.1 Custom Result Backend

`nautobot/core/celery/backends.py:8-95` — `NautobotDatabaseBackend` extends `django_celery_results.backends.DatabaseBackend`. Uses `JobResult` as TaskModel. Overrides `_get_extended_properties()` to extract custom kwargs (`nautobot_job_user_id`, `nautobot_job_branch_name`, `nautobot_job_job_model_id`, `nautobot_job_scheduled_job_id`). Maps task name back to original Job class path. Sanitizes tracebacks.

### 7.2 Result Hydration Flow

1. `JobResult.enqueue_job()` creates record with `status=PENDING`.
2. `NautobotTask.before_start()` sets `status=STARTED`.
3. Celery result backend updates `status`/`result`/`traceback` via `_get_extended_properties()`.

---

## 8. Canvas / Workflow Composition

**Finding**: Search for `chain(`, `group(`, `chord(`, `signature(`, `s(`, `.apply_async`, `.delay` reveals **no use of Celery canvas primitives** in the main codebase. Tasks are atomic; `apply_async()` is only called from `JobResult.enqueue_job()` and the scheduler. **This is huge for our scope** — we don't have to port workflow composition.

---

## 9. Tests

### 9.1 Test Settings

`nautobot/core/tests/nautobot_config.py:45-47`:
- `CELERY_TASK_ALWAYS_EAGER = True`
- `CELERY_TASK_STORE_EAGER_RESULT = True`
- `CELERY_BROKER_URL = "memory://"`

### 9.2 Test Utilities

- `nautobot/core/testing/__init__.py:143-151` — `CelerySubprocessTestCase` for E2E tests with actual worker subprocess.
- `nautobot/core/tests/runner.py:106` — Skips init if `CELERY_TASK_ALWAYS_EAGER`.
- `nautobot/core/testing/views.py:1518-1688` — Job view tests mock `JobResult.enqueue_job()`.
- `nautobot/core/tests/test_settings_schema.py:114-124` — Schema validation expects `CELERY_*` settings.

---

## 10. Docker & Development

- `development/docker-compose.yml` — Services: `nautobot`, `celery_worker`, `celery_beat`, `redis`.
- `development/docker-compose.final.yml` — Production image variants.
- `development/dev.env` — Local env (Celery URLs, etc.).
- `development/nautobot_config.py:92` — `CELERY_WORKER_PROMETHEUS_PORTS = [8080]`.

---

## 11. Dependencies (pyproject.toml)

| Package | Version | Purpose |
|---|---|---|
| `celery` | `>=5.6.3,<5.7` | Core |
| `django-celery-beat` | `==2.8.1` (exact pin) | Periodic tasks |
| `django-celery-results` | `>=2.6.0,<2.7` | Result backend |
| `django-structlog[celery]` | `>=10.0.0,<10.1` | Structured logging |
| `prometheus-client` | `>=0.24.1,<0.25` | Metrics |

Kombu pulled in transitively (used by custom serializer).

---

## 12. Imports Across Codebase

Major importers of Celery / Kombu (full list in original audit):

- `nautobot/core/celery/__init__.py` — `bootsteps`, `Celery`, `shared_task`, `signals`; `kombu.serialization.register`
- `nautobot/core/celery/task.py` — `states`, `Task`, `Retry`, `EagerResult`
- `nautobot/core/celery/log.py` — `current_task`
- `nautobot/core/celery/schedulers.py` — `current_app`, `AsyncResult`
- `nautobot/core/celery/control.py` — `control_command`
- `nautobot/extras/jobs.py` — `Ignore`, `Reject`, `get_task_logger`
- `nautobot/extras/choices.py` — `states`
- `nautobot/extras/models/jobs.py` — `states`, `NotRegistered`
- `nautobot/core/__init__.py` — `from nautobot.core.celery import app as celery_app`

---

## 13. Worker Control Commands

`nautobot/core/celery/control.py`:
- `refresh_git_repository` (11-28) — Refresh Git repo head on all active workers.
- `discard_git_repository` (31-41) — Unload/delete Git repo from workers.

---

## 14. API Health Check

`nautobot/core/api/views.py:552-573` — `StatusView` queries `celery_app.control.inspect().active()` for worker count. Sets `"celery-workers-running"` in response. Retry logic for Redis connection failures.

---

## Risk Areas

### **High Risk**

1. **Job Lifecycle Integration** — `NautobotTask` base class deeply woven; backend must support `before_start`, `after_return`, bound task context.
2. **Custom Serialization** — `nautobot_json` handles model lazy deserialization + branch context. Procrastinate uses plain JSON; we need an adapter.
3. **Signals & Job Registry** — 7 Celery signals initialize logging, prometheus, job imports. Must trigger equivalents.
4. **django-celery-beat Replacement** — `NautobotDatabaseScheduler` has complex business logic (missing user handling, ERRORED state, K8s detection, heartbeat). Procrastinate has no direct equivalent.
5. **Result Backend Hydration** — JobResult tightly coupled to Celery task lifecycle.

### **Medium Risk**

6. **Singleton Lock Management** — Redis cache lock in `BaseJob.before_start()`.
7. **Console Log Streaming** — `run_console_log_job_and_return_job_result()` separate task path with `JobConsoleLogExecutor`.
8. **Version Control Branch Context** — `nautobot_job_branch_name` propagated through `NautobotTask.apply_async()`.
9. **Worker Health Probes** — Heartbeat files for K8s.
10. **Prometheus Metrics** — Worker-startup HTTP server.

### **Low Risk**

11. **Database Logging Handler** — `NautobotDatabaseHandler` only needs `current_task` equivalent.
12. **Worker Control Commands** — Could be re-architected.
13. **Status Choices Mapping** — `JobResultStatusChoices` maps to Celery states; can add Procrastinate equivalents.
14. **Docker Compose** — Easy entrypoint swaps.

---

## Summary

- **~50 files** import or reference Celery
- **7 custom signal handlers**
- **2 custom Celery subclasses** (`NautobotCelery`, `NautobotTask`)
- **Custom: serializer, result backend, scheduler, log handler, worker boot step**
- **No canvas usage** — chains/groups/chords absent. Major scope win.
- **`JobResult.enqueue_job()` is the single chokepoint** for execution dispatch. One method to refactor.

The work is feasible. The chokepoint structure plus the absence of canvas usage means a clean `TaskBackend` interface can capture everything.
