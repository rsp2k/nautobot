"""Backend-agnostic Nautobot periodic-task scheduler.

The Celery analogue is ``nautobot-server celery beat``. Use this command
when ``NAUTOBOT_TASK_BACKEND=procrastinate`` (or any future backend that
needs a DB-row-driven scheduler).

The scheduler is a thin loop: each tick, the active backend's
``PeriodicRunner.tick()`` is called. The runner queries ``ScheduledJob``
rows and enqueues due jobs via ``JobResult.enqueue_job``.

Usage::

    nautobot-server nautobot_scheduler
    nautobot-server nautobot_scheduler --tick-seconds 10

Behavior under Celery: the active backend's ``get_periodic_runner()``
returns ``None`` (celery beat is its own process). In that case this
command exits early with a helpful message rather than spinning silently.
"""
from __future__ import annotations

import logging
import signal
import time

from django.core.management.base import BaseCommand

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Run the Nautobot periodic-task scheduler (Procrastinate / non-Celery backends)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--tick-seconds",
            type=float,
            default=5.0,
            help="Seconds between scheduler ticks (default: 5.0).",
        )

    def handle(self, *args, tick_seconds: float, **options) -> None:
        from nautobot.core.task_backends import get_task_backend

        backend = get_task_backend()
        runner = backend.get_periodic_runner()

        if runner is None:
            self.stderr.write(
                self.style.NOTICE(
                    f"Backend '{backend.name}' does not provide a periodic runner. "
                    f"If you are running Celery, use 'nautobot-server celery beat' instead. "
                    f"Exiting."
                )
            )
            return

        self.stdout.write(
            self.style.SUCCESS(
                f"Nautobot scheduler started (backend={backend.name}, tick={tick_seconds}s)"
            )
        )

        stopping = {"flag": False}

        def _on_signal(signum, _frame):
            self.stdout.write(self.style.NOTICE(f"Received signal {signum}; shutting down."))
            stopping["flag"] = True

        signal.signal(signal.SIGINT, _on_signal)
        signal.signal(signal.SIGTERM, _on_signal)

        while not stopping["flag"]:
            try:
                count = runner.tick()
            except Exception:  # pragma: no cover - tick should self-recover
                logger.exception("Scheduler tick raised; continuing.")
                count = 0

            if count:
                self.stdout.write(f"Enqueued {count} scheduled job(s) this tick.")

            # Sleep in short slices so SIGTERM is honored within a tick.
            slept = 0.0
            slice_secs = min(0.5, tick_seconds)
            while slept < tick_seconds and not stopping["flag"]:
                time.sleep(slice_secs)
                slept += slice_secs

        self.stdout.write(self.style.SUCCESS("Nautobot scheduler stopped."))
