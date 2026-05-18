"""Procrastinate-friendly periodic task runner.

Reads ``ScheduledJob`` rows from the database and enqueues due jobs via the
configured ``TaskBackend``. Replaces ``django-celery-beat`` for sites running
``NAUTOBOT_TASK_BACKEND=procrastinate``.

This is a v1 implementation. Known gaps vs. ``NautobotDatabaseScheduler``:

- Does not record a "missing user" failure ``JobResult`` when a
  ``ScheduledJob`` references a deleted user (the Celery scheduler does this
  via ``NautobotScheduleEntry._record_missing_user_failure``). For v1 we
  log and skip.
- Does not transition ``ScheduledJob.state`` to ``ERRORED`` on validation
  failures. Errors are logged; the entry stays enabled.
- No heartbeat-file integration for Kubernetes liveness probes.
- No clocked-schedule integration (one-shot timestamps).

These can land as follow-ups as the Procrastinate backend matures.
"""
from __future__ import annotations

import logging
from datetime import timedelta

from django.utils import timezone

from .base import PeriodicRunner

logger = logging.getLogger(__name__)


class NautobotProcrastinatePeriodicRunner(PeriodicRunner):
    """Tick-based periodic runner.

    Call ``tick()`` on a schedule (the ``nautobot-server scheduler`` management
    command does this in an infinite loop). Each call looks at every enabled
    ``ScheduledJob`` and enqueues the ones whose next-run time has elapsed.
    """

    def tick(self) -> int:
        # Lazy imports keep this module cheap to load and dodge Django setup
        # ordering issues.
        from nautobot.extras.choices import ScheduledJobStateChoices
        from nautobot.extras.models.jobs import JobResult, ScheduledJob

        now = timezone.now()
        enqueued = 0

        qs = ScheduledJob.objects.filter(
            enabled=True,
            state=ScheduledJobStateChoices.ACTIVE,
        ).select_related("job_model", "user", "job_queue")

        for sched in qs:
            if not self._should_fire(sched, now):
                continue

            try:
                self._enqueue(sched, now)
                enqueued += 1
            except Exception:  # pragma: no cover - defensive
                logger.exception("Scheduler failed to enqueue ScheduledJob %s", sched.pk)
                continue

            # Bookkeeping mirrors what NautobotDatabaseScheduler.apply_async does.
            ScheduledJob.objects.filter(pk=sched.pk).update(
                last_run_at=now,
                total_run_count=(sched.total_run_count or 0) + 1,
            )

        return enqueued

    @staticmethod
    def _should_fire(sched, now) -> bool:
        """Return True if this ScheduledJob is due to fire at ``now``.

        Uses django-celery-beat's ``schedule.is_due`` machinery so cron and
        interval semantics stay identical to the Celery scheduler — operators
        switching backends don't see schedules drift.
        """
        # Don't fire schedules before their declared start window.
        if sched.start_time and sched.start_time > now:
            return False
        if sched.stop_time and sched.stop_time < now:
            return False

        # Anchor: last_run_at or start_time or now-tick. Mirrors the choice
        # ModelEntry makes in django-celery-beat.
        last_run = sched.last_run_at or sched.start_time or (now - timedelta(seconds=1))

        try:
            schedule = sched.schedule  # cron/interval/clocked from django_celery_beat
        except Exception:  # pragma: no cover - bad row
            logger.exception("ScheduledJob %s has an unparseable schedule; skipping", sched.pk)
            return False

        if schedule is None:
            return False

        is_due, _next_call_secs = schedule.is_due(last_run)
        return bool(is_due)

    @staticmethod
    def _enqueue(sched, now) -> None:
        """Hand the scheduled job off to JobResult.enqueue_job."""
        from nautobot.extras.models.jobs import JobResult

        if sched.job_model is None or not sched.job_model.enabled:
            logger.warning(
                "ScheduledJob %s references a missing or disabled Job %s; skipping.",
                sched.pk,
                sched.job_model_id,
            )
            return

        if sched.user is None:
            # v1: log and skip. v2 should record a missing-user failure JobResult
            # to match NautobotScheduleEntry._record_missing_user_failure behavior.
            logger.warning(
                "ScheduledJob %s has no user (the user was likely deleted); skipping.",
                sched.pk,
            )
            return

        JobResult.enqueue_job(
            job_model=sched.job_model,
            user=sched.user,
            schedule=sched,
            job_queue=sched.job_queue,
            **(sched.kwargs or {}),
        )
