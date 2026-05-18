# Running Nautobot with the Procrastinate task backend

> Status: dev/preview. The Procrastinate backend is functional for ad-hoc job
> dispatch and JobResult/JobLogEntry capture. Periodic tasks (the
> `django-celery-beat` replacement) still ship in follow-up work.

## Why choose Procrastinate?

- **One fewer broker to operate.** Procrastinate uses PostgreSQL's
  `LISTEN/NOTIFY` as the broker. Nautobot already requires PostgreSQL, so no
  new infrastructure is added. Redis stays installed for caching/sessions but
  no longer carries the task queue.
- **Tasks live in the same database as your data.** Job state is auditable,
  backup-able, and queryable with the rest of your Nautobot DB.
- **Smaller dependency surface.** No Celery, no Kombu, no amqp.

You should stick with Celery if you depend on Celery-specific features
(`chain`, `group`, `chord`, complex routing, `inspect()` worker introspection,
the existing operational tooling around it). The audit at
`docs/fork/procrastinate/01-celery-surface-inventory.md` lists what's
Celery-only today.

## Enabling Procrastinate

Two changes:

1. **Install the optional extra** (also pulls procrastinate's deps):

   ```bash
   pip install 'nautobot[procrastinate]'
   ```

   The extra installs `procrastinate` itself plus `psycopg[binary]`.
   Note this is the **psycopg 3.x** client library (with bundled libpq),
   not `psycopg2`. Nautobot's other ORM operations continue to use
   `psycopg2-binary`; the two libraries coexist without conflict. If you
   build your own runtime image and don't want the bundled libpq, install
   `libpq5` on the system and depend on `psycopg` instead of
   `psycopg[binary]`.

   When the `procrastinate` package is importable, Nautobot automatically
   adds `procrastinate.contrib.django` to `INSTALLED_APPS` and exposes the
   `nautobot-server procrastinate` management command. Sites that don't
   install the extra are unaffected.

2. **Set the backend env var:**

   ```bash
   export NAUTOBOT_TASK_BACKEND=procrastinate
   ```

   Default is `celery`. Any value other than `celery` or `procrastinate` is
   treated as a dotted import path to a custom `TaskBackend` subclass.

After enabling, run migrations to create Procrastinate's tables
(`procrastinate_jobs`, `procrastinate_events`, etc.):

```bash
nautobot-server migrate
```

## Running workers

Replace the Celery worker process with the Procrastinate one:

```bash
# Was:
nautobot-server celery worker -l INFO

# Becomes:
nautobot-server procrastinate worker --verbosity 1
```

Worker concurrency, queues, and graceful shutdown follow Procrastinate's
conventions. See the
[Procrastinate Django docs](https://procrastinate.readthedocs.io/en/stable/howto/django.html)
for the full CLI.

You can leave `celery_worker` / `celery_beat` running alongside if you want a
clean switchover path; jobs are dispatched to one backend only based on
`NAUTOBOT_TASK_BACKEND`, so the other worker just sits idle.

## Development environment (Docker compose)

The dev compose file ships a `procrastinate_worker` service behind a Docker
profile so it's opt-in. To bring up the stack with Procrastinate instead of
Celery:

```bash
# Start the usual services + the procrastinate worker:
COMPOSE_PROFILES=procrastinate NAUTOBOT_TASK_BACKEND=procrastinate invoke start

# Or directly:
COMPOSE_PROFILES=procrastinate docker compose up
```

Set `NAUTOBOT_TASK_BACKEND=procrastinate` in `development/dev.env` if you want
the toggle to persist across `invoke` commands.

## What's not yet bridged (heads-up)

The Procrastinate backend honors:

- `JobResult` status transitions (PENDING → STARTED → SUCCESS/FAILURE)
- `JobLogEntry` capture via `NautobotDatabaseHandler`
- `BaseJob` lifecycle hooks (`before_start`, `__call__`, `on_success`,
  `on_failure`, `after_return`)
- Branch context for the `nautobot_version_control` plugin

These are **not yet implemented** under Procrastinate (tracked as
follow-ups):

- `soft_time_limit` / `time_limit` enforcement (SIGALRM trick lives in
  CeleryBackend only)
- Singleton-lock pre-flight check (`is_singleton=True` jobs may run
  concurrently)
- Periodic / scheduled jobs (django-celery-beat replacement, separate task)
- Prometheus task metrics
- `nautobot.jobs.job.started` / `.completed` event publication

If your deployment relies on any of the above, stay on Celery for now.

## Verifying the switch

A quick way to confirm Procrastinate is actually dispatching:

```bash
# Inside the nautobot container (or any environment with the app loaded):
nautobot-server shell -c "
from nautobot.core.task_backends import get_task_backend
print(get_task_backend().name)
"
# Expected: procrastinate
```

Then trigger any job from the UI/API and watch the `procrastinate_worker`
service logs.

## Switching back

Set `NAUTOBOT_TASK_BACKEND=celery` (or unset the variable; `celery` is the
default), restart, and resume normal celery operation. Procrastinate's tables
will remain in the database; they're empty and harmless. You can drop them
manually if you wish:

```sql
DROP TABLE IF EXISTS procrastinate_events CASCADE;
DROP TABLE IF EXISTS procrastinate_jobs CASCADE;
-- and procrastinate_periodic_defers if present
```
