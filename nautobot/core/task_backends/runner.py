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

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
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
