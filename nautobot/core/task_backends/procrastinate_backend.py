"""Procrastinate implementation of the TaskBackend interface.

Procrastinate (https://procrastinate.readthedocs.io/) uses PostgreSQL
LISTEN/NOTIFY as its broker; Nautobot already requires PostgreSQL, so no
additional infrastructure is needed beyond enabling this backend.

This module is import-safe even when ``procrastinate`` is not installed: the
actual procrastinate imports happen inside methods. This lets sites that stick
with Celery keep procrastinate out of their dependency tree.

Enable with::

    NAUTOBOT_TASK_BACKEND=procrastinate

The site must also have ``procrastinate.contrib.django`` in
``INSTALLED_APPS`` (so Procrastinate's own job tables get migrated) and the
``procrastinate`` extra installed (``pip install nautobot[procrastinate]``).

**This file is a v1 skeleton.** The dispatch path is functional, but full
JobResult / JobLogEntry bridging and lifecycle hook integration arrive in
follow-up commits (see docs/fork/procrastinate/ for the staged plan).
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Iterable
from uuid import UUID

from django.utils import timezone

from .base import DispatchResult, EnqueueOptions, TaskBackend

if TYPE_CHECKING:
    from procrastinate import App as ProcrastinateApp
    from procrastinate.tasks import Task as ProcrastinateTask

logger = logging.getLogger(__name__)

# Stable Procrastinate task name. Changing this in a deployed environment
# would orphan in-flight jobs whose payload references the old name.
PROCRASTINATE_JOB_TASK_NAME = "nautobot.run_job"


class ProcrastinateBackend(TaskBackend):
    """Procrastinate-backed task dispatch.

    Maintains a single registered task ``nautobot.run_job`` that re-resolves
    the Job class at execution time and runs it with the supplied args/kwargs.

    The Job lifecycle (before_start / __call__ / on_success / on_failure /
    after_return) is invoked the same way CeleryBackend does it. See
    ``_execute_job`` for the in-worker entry point.
    """

    name = "procrastinate"

    def __init__(self) -> None:
        self._app: "ProcrastinateApp | None" = None
        self._task: "ProcrastinateTask | None" = None

    # --- procrastinate app / task registration (lazy) ---

    def _get_app(self) -> "ProcrastinateApp":
        if self._app is None:
            try:
                from procrastinate.contrib.django import app as procrastinate_app
            except ImportError as exc:
                raise ImportError(
                    "ProcrastinateBackend requires the 'procrastinate' extra. "
                    "Install with: pip install 'nautobot[procrastinate]' and add "
                    "'procrastinate.contrib.django' to INSTALLED_APPS."
                ) from exc
            self._app = procrastinate_app
            self._task = self._register_task(procrastinate_app)
        return self._app

    def _get_task(self) -> "ProcrastinateTask":
        # Side effect: triggers _register_task on first call.
        self._get_app()
        assert self._task is not None  # set by _get_app
        return self._task

    @staticmethod
    def _register_task(app: "ProcrastinateApp") -> "ProcrastinateTask":
        """Register the universal Nautobot job runner with Procrastinate.

        Called exactly once per process. Re-registration is idempotent because
        Procrastinate's task registry is keyed by name.
        """

        @app.task(name=PROCRASTINATE_JOB_TASK_NAME, pass_context=True)
        def _run_job_via_procrastinate(
            context,  # procrastinate.tasks.JobContext
            *,
            job_result_id: str,
            job_class_path: str,
            args: list,
            kwargs: dict,
            options_dict: dict,
        ) -> Any:
            return ProcrastinateBackend._execute_job(
                job_result_id=UUID(job_result_id),
                job_class_path=job_class_path,
                args=args,
                kwargs=kwargs,
                options=_options_from_jsonable_dict(options_dict),
                procrastinate_context=context,
            )

        return _run_job_via_procrastinate

    # --- public TaskBackend interface ---

    def enqueue(
        self,
        *,
        job_result_id: UUID,
        job_class_path: str,
        args: Iterable[Any],
        kwargs: dict[str, Any],
        options: EnqueueOptions,
    ) -> DispatchResult:
        task = self._get_task()
        task.defer(
            queue=options.queue or "default",
            job_result_id=str(job_result_id),
            job_class_path=job_class_path,
            args=list(args),
            kwargs=kwargs,
            options_dict=_options_to_jsonable_dict(options),
        )
        return DispatchResult(task_id=job_result_id, backend=self.name)

    def enqueue_sync(
        self,
        *,
        job_result_id: UUID,
        job_class_path: str,
        args: Iterable[Any],
        kwargs: dict[str, Any],
        options: EnqueueOptions,
    ) -> DispatchResult:
        # Bypass the deferral, run the lifecycle inline. This is the
        # CELERY_TASK_ALWAYS_EAGER analogue.
        self._get_task()  # ensure task registry is warm
        ProcrastinateBackend._execute_job(
            job_result_id=job_result_id,
            job_class_path=job_class_path,
            args=list(args),
            kwargs=kwargs,
            options=options,
            procrastinate_context=None,
        )
        return DispatchResult(task_id=job_result_id, backend=self.name)

    def get_active_workers(self) -> int:
        # Procrastinate does not expose a synchronous "active workers" query.
        # The release-readiness story here is to read a heartbeat table that
        # the worker writes on each tick. Punted to a follow-up; -1 means
        # "unknown" to the StatusView.
        return -1

    # --- in-worker execution ---

    @staticmethod
    def _execute_job(
        *,
        job_result_id: UUID,
        job_class_path: str,
        args: list,
        kwargs: dict,
        options: EnqueueOptions,
        procrastinate_context: Any | None,
    ) -> Any:
        """Run a job under Procrastinate.

        v1 skeleton: marks JobResult as STARTED, attempts to run the job,
        marks SUCCESS or FAILURE. Does not yet:
          - install NautobotDatabaseHandler for log capture (Task #6)
          - propagate branch context (Task #6)
          - honor soft_time_limit / time_limit (Task #6)

        Implementations of those concerns live in CeleryBackend today; the
        bridging work in Task #6 extracts the shared parts into a helper that
        both backends call.
        """
        # Lazy imports to keep module import cheap and avoid Django setup
        # ordering issues with model imports.
        from nautobot.extras.choices import JobResultStatusChoices
        from nautobot.extras.jobs import get_job
        from nautobot.extras.models.jobs import JobResult

        job_result = JobResult.objects.get(id=job_result_id)
        job_result.date_started = timezone.now()
        job_result.status = JobResultStatusChoices.STATUS_STARTED
        job_result.save()

        try:
            job_class = get_job(job_class_path)
            if job_class is None:
                raise LookupError(f"Job class not found: {job_class_path}")
            job_instance = job_class()
            job_instance.job_result = job_result
            # Mirror Celery's lifecycle order. Lacking a bound-task self,
            # before_start / after_return get None placeholders for now.
            job_instance.before_start(task_id=str(job_result_id), args=args, kwargs=kwargs)
            try:
                result = job_instance(*args, **kwargs)
                job_instance.on_success(retval=result, task_id=str(job_result_id), args=args, kwargs=kwargs)
                job_result.status = JobResultStatusChoices.STATUS_SUCCESS
                job_result.result = result
                return result
            except Exception as exc:
                job_instance.on_failure(
                    exc=exc,
                    task_id=str(job_result_id),
                    args=args,
                    kwargs=kwargs,
                    einfo=None,
                )
                job_result.status = JobResultStatusChoices.STATUS_FAILURE
                job_result.result = {"exc_type": type(exc).__name__, "exc_message": str(exc)}
                raise
            finally:
                job_instance.after_return(
                    status=job_result.status,
                    retval=job_result.result,
                    task_id=str(job_result_id),
                    args=args,
                    kwargs=kwargs,
                    einfo=None,
                )
        finally:
            job_result.date_done = timezone.now()
            job_result.save()


# --- helpers ---


def _options_to_jsonable_dict(options: EnqueueOptions) -> dict[str, Any]:
    """Convert EnqueueOptions to a JSON-serializable dict for Procrastinate's payload.

    Procrastinate serializes the task payload as JSON when it lands in
    postgres. UUIDs become strings; everything else is already JSON-safe.
    """
    return {
        "queue": options.queue,
        "soft_time_limit": options.soft_time_limit,
        "time_limit": options.time_limit,
        "profile": options.profile,
        "console_log": options.console_log,
        "ignore_singleton_lock": options.ignore_singleton_lock,
        "user_id": str(options.user_id) if options.user_id else None,
        "job_model_id": str(options.job_model_id) if options.job_model_id else None,
        "schedule_id": str(options.schedule_id) if options.schedule_id else None,
        "branch_name": options.branch_name,
        "extra": dict(options.extra),
    }


def _options_from_jsonable_dict(data: dict[str, Any]) -> EnqueueOptions:
    """Reverse of ``_options_to_jsonable_dict``: restore typed UUIDs."""
    return EnqueueOptions(
        queue=data.get("queue"),
        soft_time_limit=data.get("soft_time_limit"),
        time_limit=data.get("time_limit"),
        profile=data.get("profile", False),
        console_log=data.get("console_log", False),
        ignore_singleton_lock=data.get("ignore_singleton_lock", False),
        user_id=UUID(data["user_id"]) if data.get("user_id") else None,
        job_model_id=UUID(data["job_model_id"]) if data.get("job_model_id") else None,
        schedule_id=UUID(data["schedule_id"]) if data.get("schedule_id") else None,
        branch_name=data.get("branch_name"),
        extra=dict(data.get("extra") or {}),
    )
