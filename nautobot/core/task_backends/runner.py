"""Backend-agnostic helpers for running Nautobot Jobs.

The pieces here are shared by ``CeleryBackend`` and ``ProcrastinateBackend``:

- ``JobRequest`` is the minimal request shape that user-defined Jobs read via
  ``self.request.id``. Under Celery the real ``Task.request`` is used; under
  Procrastinate (no equivalent native object) we synthesize one of these.
- ``open_branch_context`` mirrors what ``NautobotTask.before_start`` does for
  Celery: it constructs a ``BranchContext`` based on the user and branch name
  in ``EnqueueOptions``.
- ``ensure_job_log_handler_attached`` makes sure the ``NautobotDatabaseHandler``
  is attached to the ``celery.task`` logger so ``JobLogEntry`` records are
  written. Under Celery this happens via the ``celeryd_after_setup`` signal;
  Procrastinate has no equivalent, so the backend calls this explicitly.

This module imports nothing from procrastinate or celery at the top level so
that it stays inexpensive to import.
"""
from __future__ import annotations

import contextlib
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Iterator
from uuid import UUID

if TYPE_CHECKING:
    from nautobot.core.branching import BranchContext

    from .base import EnqueueOptions

logger = logging.getLogger(__name__)


@dataclass
class JobRequest:
    """Minimal Job.request shape consumed by user-defined Jobs.

    Under Celery, ``Job.request`` is the real ``celery.app.task.Context``
    object. Under Procrastinate (and any future backend) we set ``Job.request``
    to an instance of this dataclass.

    Only ``.id`` is actually read by Nautobot internals today
    (``BaseJob.job_result`` uses it to look up the JobResult). ``.properties``
    is provided so NautobotTask-shaped code that reads
    ``request.properties["nautobot_job_*"]`` continues to work transparently.
    """

    id: str  # task_id (string repr of JobResult.id)
    properties: dict[str, Any] = field(default_factory=dict)


def open_branch_context(options: "EnqueueOptions") -> "BranchContext":
    """Construct (but do not enter) a BranchContext for these options.

    Mirrors ``NautobotTask.before_start`` for non-Celery backends. The caller
    enters the context manager around the lifecycle so the branch is active
    during the job's run and torn down on exit.

    Returns an unentered context manager. Use::

        with open_branch_context(options):
            # job runs here
    """
    # Local imports keep this module cheap to load.
    from django.contrib.auth import get_user_model

    from nautobot.core.branching import BranchContext
    from nautobot.extras.models.jobs import JOB_LOGS

    User = get_user_model()
    user = None
    if options.user_id is not None:
        try:
            user = User.objects.get(id=options.user_id)
        except User.DoesNotExist:
            logger.warning(
                "User %s referenced by job options does not exist; "
                "branch context will run without an authenticated user.",
                options.user_id,
            )
    return BranchContext(
        branch_name=options.branch_name,
        user=user,
        using=["default", JOB_LOGS],
    )


class _TaskIdFilter(logging.Filter):
    """Inject ``task_id`` onto log records that don't already have it.

    Under Celery, ``celery.utils.log.TaskFormatter`` attaches ``task_id`` to
    log records automatically because Celery's task wrapper installs the
    context. Under Procrastinate there's no equivalent, so we inject it via
    this filter only while a job is running.
    """

    def __init__(self, task_id: str) -> None:
        super().__init__()
        self.task_id = task_id

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        if not hasattr(record, "task_id"):
            record.task_id = self.task_id
        return True


class _FakeCeleryRequest:
    """Minimal stand-in for celery.app.task.Context, exposing ``.id``."""

    def __init__(self, task_id: str) -> None:
        self.id = task_id


class _FakeCeleryTask:
    """Minimal stand-in for a celery Task instance.

    Push one of these onto ``celery._state._task_stack`` so
    ``celery.current_task`` resolves non-None during Procrastinate-driven
    execution. The existing ``NautobotDatabaseHandler.emit()`` checks
    ``current_task`` as a guard against running outside of a job context;
    making it non-None lets the handler run under Procrastinate without any
    changes to upstream code.
    """

    def __init__(self, task_id: str) -> None:
        self.request = _FakeCeleryRequest(task_id)
        self.name = "nautobot.run_job"


@contextlib.contextmanager
def task_id_log_context(task_id: str) -> "Iterator[None]":
    """Make NautobotDatabaseHandler treat the current thread as inside a job.

    Two coordinated effects:

      1. Pushes a minimal fake task onto celery's ``_task_stack`` so
         ``celery.current_task`` resolves non-None. ``NautobotDatabaseHandler``
         uses ``current_task`` as its "am I in a job?" gate; without this push
         every log record from Procrastinate-driven jobs would be silently
         dropped.

      2. Attaches a logging filter to the ``NautobotDatabaseHandler`` that
         injects ``task_id`` onto records that don't already carry it. Under
         Celery, ``celery.utils.log.TaskFormatter`` attaches this attribute
         automatically; under Procrastinate it has to come from somewhere.
         Filters on the handler (not the logger) apply to all records reaching
         the handler regardless of which child logger emitted them.

    Exits cleanly even on exception so we don't pollute celery's state across
    job boundaries.
    """
    from celery._state import _task_stack
    from celery.utils.log import get_logger

    from nautobot.core.celery.log import NautobotDatabaseHandler

    fake_task = _FakeCeleryTask(task_id)
    _task_stack.push(fake_task)

    task_logger = get_logger("celery.task")
    db_handlers = [h for h in task_logger.handlers if isinstance(h, NautobotDatabaseHandler)]
    flt = _TaskIdFilter(task_id)
    for h in db_handlers:
        h.addFilter(flt)

    try:
        yield
    finally:
        for h in db_handlers:
            h.removeFilter(flt)
        _task_stack.pop()


def ensure_job_log_handler_attached() -> None:
    """Make sure NautobotDatabaseHandler is attached to the celery.task logger.

    Under Celery, this happens automatically via the ``celeryd_after_setup``
    signal. Under any non-Celery backend, that signal never fires, so this
    function attaches the handler explicitly.

    Safe to call multiple times — ``setup_nautobot_job_logging`` uses
    ``add_nautobot_log_handler`` which is idempotent (it checks whether an
    instance of ``NautobotDatabaseHandler`` is already on the logger).
    """
    from nautobot.core.celery import app, setup_nautobot_job_logging

    # The signal handler signature is (sender, instance, conf, **kwargs).
    # We pass conf=app.conf; sender/instance aren't used by the body.
    setup_nautobot_job_logging(None, None, app.conf)


def build_celery_shaped_properties(options: "EnqueueOptions") -> dict[str, Any]:
    """Translate EnqueueOptions into the ``nautobot_job_*`` keys that
    NautobotTask code reads from ``self.request.properties``.

    Used by ProcrastinateBackend when synthesizing a JobRequest so that any
    user-defined Job that introspects request.properties (e.g., custom
    middleware) sees the same key shape under both backends.
    """
    return {
        "nautobot_job_user_id": str(options.user_id) if options.user_id else None,
        "nautobot_job_job_model_id": str(options.job_model_id) if options.job_model_id else None,
        "nautobot_job_schedule_id": str(options.schedule_id) if options.schedule_id else None,
        "nautobot_job_branch_name": options.branch_name,
        "nautobot_job_ignore_singleton_lock": options.ignore_singleton_lock,
        "nautobot_job_console_log": options.console_log,
        "nautobot_job_profile": options.profile,
    }


def make_job_request(task_id: UUID, options: "EnqueueOptions") -> JobRequest:
    """Construct a JobRequest from a task id and options."""
    return JobRequest(
        id=str(task_id),
        properties=build_celery_shaped_properties(options),
    )
