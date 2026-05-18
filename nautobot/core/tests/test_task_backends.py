"""Tests for the TaskBackend abstraction in nautobot.core.task_backends.

These tests cover the backend-agnostic interface introduced to enable
Procrastinate as an alternative to Celery. They do not require a running
broker or database; they validate the dataclasses, the ABC contract, the
factory's settings-driven dispatch, and the CeleryBackend legacy kwargs
shape.
"""
from __future__ import annotations

from dataclasses import FrozenInstanceError, is_dataclass
from unittest import mock
from uuid import UUID, uuid4

from django.test import SimpleTestCase, TransactionTestCase, override_settings

from nautobot.core.task_backends import (
    DispatchResult,
    EnqueueOptions,
    PeriodicRunner,
    TaskBackend,
    get_task_backend,
)
from nautobot.core.task_backends.celery_backend import (
    CeleryBackend,
    _build_celery_kwargs_dict,
)
from nautobot.core.task_backends.procrastinate_backend import (
    PROCRASTINATE_JOB_TASK_NAME,
    ProcrastinateBackend,
    _options_from_jsonable_dict,
    _options_to_jsonable_dict,
)
from nautobot.core.task_backends.procrastinate_periodic import (
    NautobotProcrastinatePeriodicRunner,
)


class DispatchResultTests(SimpleTestCase):
    def test_is_dataclass(self):
        self.assertTrue(is_dataclass(DispatchResult))

    def test_is_frozen(self):
        result = DispatchResult(task_id=uuid4(), backend="celery")
        with self.assertRaises(FrozenInstanceError):
            result.backend = "procrastinate"  # type: ignore[misc]

    def test_fields(self):
        tid = uuid4()
        result = DispatchResult(task_id=tid, backend="celery")
        self.assertEqual(result.task_id, tid)
        self.assertEqual(result.backend, "celery")


class EnqueueOptionsTests(SimpleTestCase):
    def test_defaults(self):
        opts = EnqueueOptions()
        self.assertIsNone(opts.queue)
        self.assertIsNone(opts.soft_time_limit)
        self.assertIsNone(opts.time_limit)
        self.assertFalse(opts.profile)
        self.assertFalse(opts.console_log)
        self.assertFalse(opts.ignore_singleton_lock)
        self.assertIsNone(opts.user_id)
        self.assertIsNone(opts.job_model_id)
        self.assertIsNone(opts.schedule_id)
        self.assertIsNone(opts.branch_name)
        self.assertEqual(opts.extra, {})

    def test_extra_field_is_independent_per_instance(self):
        # Guards against the classic dataclass-mutable-default footgun.
        a = EnqueueOptions()
        b = EnqueueOptions()
        a.extra["only_in_a"] = True
        self.assertNotIn("only_in_a", b.extra)


class TaskBackendABCTests(SimpleTestCase):
    def test_cannot_instantiate_abstract_class(self):
        with self.assertRaises(TypeError):
            TaskBackend()  # type: ignore[abstract]

    def test_concrete_subclass_must_implement_all_abstract_methods(self):
        # Subclass missing enqueue_sync should still be unconstructable.
        class Incomplete(TaskBackend):
            name = "incomplete"

            def enqueue(self, **kwargs):
                pass

            def get_active_workers(self):
                return 0

        with self.assertRaises(TypeError):
            Incomplete()  # type: ignore[abstract]

    def test_complete_subclass_works(self):
        class Complete(TaskBackend):
            name = "complete"

            def enqueue(self, **kwargs):
                return DispatchResult(task_id=uuid4(), backend=self.name)

            def enqueue_sync(self, **kwargs):
                return DispatchResult(task_id=uuid4(), backend=self.name)

            def get_active_workers(self):
                return 1

        backend = Complete()
        self.assertEqual(backend.name, "complete")
        self.assertIsNone(backend.get_periodic_runner())  # default impl

    def test_periodic_runner_is_abstract(self):
        with self.assertRaises(TypeError):
            PeriodicRunner()  # type: ignore[abstract]


class GetTaskBackendFactoryTests(SimpleTestCase):
    def setUp(self):
        # Each test should start with a fresh cache.
        get_task_backend.cache_clear()

    def tearDown(self):
        get_task_backend.cache_clear()

    @override_settings(TASK_BACKEND="celery")
    def test_celery_is_default(self):
        backend = get_task_backend()
        self.assertIsInstance(backend, CeleryBackend)
        self.assertEqual(backend.name, "celery")

    def test_result_is_cached(self):
        get_task_backend.cache_clear()
        first = get_task_backend()
        second = get_task_backend()
        self.assertIs(first, second)

    def test_custom_dotted_path_resolves(self):
        # Use a stub backend defined in this test module via a real dotted path.
        with override_settings(TASK_BACKEND="nautobot.core.task_backends.celery_backend.CeleryBackend"):
            get_task_backend.cache_clear()
            backend = get_task_backend()
            self.assertIsInstance(backend, CeleryBackend)


class CeleryBackendLegacyKwargsTests(SimpleTestCase):
    """The legacy ``nautobot_job_*`` keys must match the exact shape produced by
    the old ``JobResult._build_celery_kwargs`` method, because NautobotTask /
    NautobotDatabaseScheduler / JobResult.celery_kwargs all consume them.
    """

    def test_minimal_options(self):
        user_id = uuid4()
        job_model_id = uuid4()
        options = EnqueueOptions(
            queue="default",
            user_id=user_id,
            job_model_id=job_model_id,
        )
        result = _build_celery_kwargs_dict(options)
        self.assertEqual(result["nautobot_job_user_id"], str(user_id))
        self.assertEqual(result["nautobot_job_job_model_id"], str(job_model_id))
        self.assertEqual(result["queue"], "default")
        self.assertFalse(result["nautobot_job_profile"])
        self.assertFalse(result["nautobot_job_console_log"])
        self.assertFalse(result["nautobot_job_ignore_singleton_lock"])
        self.assertNotIn("nautobot_job_schedule_id", result)
        self.assertNotIn("soft_time_limit", result)
        self.assertNotIn("time_limit", result)

    def test_time_limits_omitted_when_none(self):
        options = EnqueueOptions(soft_time_limit=None, time_limit=None)
        result = _build_celery_kwargs_dict(options)
        self.assertNotIn("soft_time_limit", result)
        self.assertNotIn("time_limit", result)

    def test_time_limits_included_when_set(self):
        options = EnqueueOptions(soft_time_limit=10.0, time_limit=20.0)
        result = _build_celery_kwargs_dict(options)
        self.assertEqual(result["soft_time_limit"], 10.0)
        self.assertEqual(result["time_limit"], 20.0)

    def test_schedule_id_is_passed_as_raw_uuid(self):
        # Matches the original behavior; NautobotDatabaseScheduler also sets
        # this key as a raw UUID, not a string.
        schedule_id = uuid4()
        options = EnqueueOptions(schedule_id=schedule_id)
        result = _build_celery_kwargs_dict(options)
        self.assertEqual(result["nautobot_job_schedule_id"], schedule_id)
        self.assertIsInstance(result["nautobot_job_schedule_id"], UUID)

    def test_extra_overrides_defaults(self):
        # The original code allowed caller-supplied celery_kwargs to override
        # keys like 'queue'. Preserve that behavior via EnqueueOptions.extra.
        options = EnqueueOptions(queue="default", extra={"queue": "override"})
        result = _build_celery_kwargs_dict(options)
        self.assertEqual(result["queue"], "override")

    def test_extra_can_add_unknown_keys(self):
        options = EnqueueOptions(extra={"custom_celery_arg": "foo"})
        result = _build_celery_kwargs_dict(options)
        self.assertEqual(result["custom_celery_arg"], "foo")


class CeleryBackendDispatchTests(SimpleTestCase):
    """Verify the backend calls into Celery's apply_async with the right shape.

    Heavier end-to-end tests live in nautobot.extras.tests.test_jobs.
    """

    def test_enqueue_calls_run_job_apply_async(self):
        backend = CeleryBackend()
        job_result_id = uuid4()
        options = EnqueueOptions(queue="default", user_id=uuid4(), job_model_id=uuid4())

        with mock.patch("nautobot.extras.jobs.run_job") as mock_run_job:
            result = backend.enqueue(
                job_result_id=job_result_id,
                job_class_path="dummy.module.DummyJob",
                args=[],
                kwargs={"foo": "bar"},
                options=options,
            )

        mock_run_job.apply_async.assert_called_once()
        call_kwargs = mock_run_job.apply_async.call_args.kwargs
        self.assertEqual(call_kwargs["task_id"], str(job_result_id))
        self.assertEqual(call_kwargs["kwargs"], {"foo": "bar"})
        self.assertEqual(call_kwargs["args"], ["dummy.module.DummyJob"])
        self.assertEqual(call_kwargs["queue"], "default")
        self.assertEqual(result.task_id, job_result_id)
        self.assertEqual(result.backend, "celery")

    def test_enqueue_console_log_routes_to_console_log_task(self):
        backend = CeleryBackend()
        options = EnqueueOptions(console_log=True, user_id=uuid4(), job_model_id=uuid4())

        with mock.patch(
            "nautobot.extras.jobs.run_console_log_job_and_return_job_result"
        ) as mock_console_task, mock.patch("nautobot.extras.jobs.run_job") as mock_run_job:
            backend.enqueue(
                job_result_id=uuid4(),
                job_class_path="dummy.module.DummyJob",
                args=[],
                kwargs={},
                options=options,
            )

        mock_console_task.apply_async.assert_called_once()
        mock_run_job.apply_async.assert_not_called()


class ProcrastinateBackendImportSafetyTests(SimpleTestCase):
    """ProcrastinateBackend must be import-safe even when procrastinate
    isn't installed, so sites that stick with Celery don't carry the dep.
    """

    def test_module_imports_without_procrastinate(self):
        # If this test module loaded, the procrastinate_backend module
        # also loaded successfully — and procrastinate may or may not be
        # installed. Either way, the import succeeded.
        self.assertIsNotNone(ProcrastinateBackend)
        self.assertEqual(PROCRASTINATE_JOB_TASK_NAME, "nautobot.run_job")

    def test_can_instantiate_without_procrastinate(self):
        # Instantiation must not import procrastinate. The lazy load happens
        # on first call to a method that actually needs it.
        backend = ProcrastinateBackend()
        self.assertEqual(backend.name, "procrastinate")
        self.assertIsNone(backend._app)
        self.assertIsNone(backend._task)

    def test_get_active_workers_returns_unknown(self):
        # Even without procrastinate installed, this must not raise.
        backend = ProcrastinateBackend()
        self.assertEqual(backend.get_active_workers(), -1)

    def test_missing_procrastinate_gives_clear_error(self):
        backend = ProcrastinateBackend()
        # Simulate procrastinate not being installed.
        with mock.patch.dict(
            "sys.modules", {"procrastinate.contrib.django": None}
        ):
            with self.assertRaises(ImportError) as cm:
                backend._get_app()
        self.assertIn("procrastinate", str(cm.exception).lower())
        self.assertIn("nautobot[procrastinate]", str(cm.exception))


class ProcrastinateOptionsSerializationTests(SimpleTestCase):
    """The options payload travels through Procrastinate's JSON storage in
    PostgreSQL. UUIDs must survive a round-trip through ``dict[str, Any]``
    without losing their type.
    """

    def test_round_trip_with_all_fields(self):
        user_id = uuid4()
        job_model_id = uuid4()
        schedule_id = uuid4()
        original = EnqueueOptions(
            queue="cleanup",
            soft_time_limit=30.0,
            time_limit=60.0,
            profile=True,
            console_log=True,
            ignore_singleton_lock=True,
            user_id=user_id,
            job_model_id=job_model_id,
            schedule_id=schedule_id,
            branch_name="feature-x",
            extra={"custom": "value"},
        )
        as_dict = _options_to_jsonable_dict(original)
        restored = _options_from_jsonable_dict(as_dict)
        self.assertEqual(restored, original)
        # And specifically: types are preserved, not stringified UUIDs.
        self.assertIsInstance(restored.user_id, UUID)
        self.assertIsInstance(restored.job_model_id, UUID)
        self.assertIsInstance(restored.schedule_id, UUID)

    def test_jsonable_dict_is_actually_json_serializable(self):
        # Procrastinate stores the payload as JSON; if any field can't be
        # serialized this test catches it before runtime.
        import json

        options = EnqueueOptions(
            queue="q",
            user_id=uuid4(),
            job_model_id=uuid4(),
            schedule_id=uuid4(),
            branch_name="b",
            extra={"k": "v"},
        )
        payload = _options_to_jsonable_dict(options)
        # Must not raise.
        encoded = json.dumps(payload)
        decoded = json.loads(encoded)
        self.assertEqual(decoded["queue"], "q")
        self.assertIsInstance(decoded["user_id"], str)

    def test_round_trip_with_minimal_fields(self):
        original = EnqueueOptions()
        restored = _options_from_jsonable_dict(_options_to_jsonable_dict(original))
        self.assertEqual(restored, original)


class GetTaskBackendProcrastinateTests(SimpleTestCase):
    """The factory recognizes 'procrastinate' as a built-in name and resolves
    to ProcrastinateBackend.
    """

    def setUp(self):
        get_task_backend.cache_clear()

    def tearDown(self):
        get_task_backend.cache_clear()

    @override_settings(TASK_BACKEND="procrastinate")
    def test_procrastinate_string_resolves_to_backend(self):
        backend = get_task_backend()
        self.assertIsInstance(backend, ProcrastinateBackend)
        self.assertEqual(backend.name, "procrastinate")


class JobRequestTests(SimpleTestCase):
    """JobRequest is the duck-typed request object set on Job.request when
    running under non-Celery backends. Existing user code reads .id and
    .properties — both must work the same as under Celery.
    """

    def test_default_properties_is_empty_dict(self):
        from nautobot.core.task_backends.runner import JobRequest

        req = JobRequest(id="abc")
        self.assertEqual(req.id, "abc")
        self.assertEqual(req.properties, {})

    def test_properties_isolation_between_instances(self):
        # Mutable-default footgun guard.
        from nautobot.core.task_backends.runner import JobRequest

        a = JobRequest(id="a")
        b = JobRequest(id="b")
        a.properties["k"] = 1
        self.assertNotIn("k", b.properties)


class BuildCeleryShapedPropertiesTests(SimpleTestCase):
    """The ``nautobot_job_*`` keys NautobotTask reads from request.properties
    must be present even under Procrastinate, so any user middleware that
    introspects them continues to work.
    """

    def test_all_fields_translated(self):
        from nautobot.core.task_backends.runner import build_celery_shaped_properties

        user_id = uuid4()
        job_model_id = uuid4()
        schedule_id = uuid4()
        opts = EnqueueOptions(
            user_id=user_id,
            job_model_id=job_model_id,
            schedule_id=schedule_id,
            branch_name="b1",
            ignore_singleton_lock=True,
            console_log=True,
            profile=True,
        )
        props = build_celery_shaped_properties(opts)
        self.assertEqual(props["nautobot_job_user_id"], str(user_id))
        self.assertEqual(props["nautobot_job_job_model_id"], str(job_model_id))
        self.assertEqual(props["nautobot_job_schedule_id"], str(schedule_id))
        self.assertEqual(props["nautobot_job_branch_name"], "b1")
        self.assertTrue(props["nautobot_job_ignore_singleton_lock"])
        self.assertTrue(props["nautobot_job_console_log"])
        self.assertTrue(props["nautobot_job_profile"])

    def test_none_uuids_serialize_to_none(self):
        from nautobot.core.task_backends.runner import build_celery_shaped_properties

        opts = EnqueueOptions()
        props = build_celery_shaped_properties(opts)
        self.assertIsNone(props["nautobot_job_user_id"])
        self.assertIsNone(props["nautobot_job_job_model_id"])
        self.assertIsNone(props["nautobot_job_schedule_id"])


class MakeJobRequestTests(SimpleTestCase):
    def test_constructs_with_string_id_and_translated_properties(self):
        from nautobot.core.task_backends.runner import make_job_request

        task_id = uuid4()
        opts = EnqueueOptions(user_id=uuid4(), branch_name="dev")
        req = make_job_request(task_id, opts)
        self.assertEqual(req.id, str(task_id))
        self.assertIsInstance(req.id, str)
        self.assertEqual(req.properties["nautobot_job_branch_name"], "dev")


class ProcrastinateAlwaysEagerDispatchTests(SimpleTestCase):
    """Verify that PROCRASTINATE_ALWAYS_EAGER routes enqueue() to enqueue_sync().

    This is the Celery-ALWAYS_EAGER analogue. Without it, tests that trigger
    jobs under TASK_BACKEND=procrastinate would hang waiting for a worker.
    """

    def test_eager_mode_redirects_to_enqueue_sync(self):
        backend = ProcrastinateBackend()
        job_result_id = uuid4()
        options = EnqueueOptions(user_id=uuid4(), job_model_id=uuid4())

        with mock.patch.object(backend, "enqueue_sync") as mock_sync, override_settings(
            PROCRASTINATE_ALWAYS_EAGER=True
        ):
            backend.enqueue(
                job_result_id=job_result_id,
                job_class_path="x.y.Z",
                args=[],
                kwargs={},
                options=options,
            )
        mock_sync.assert_called_once()
        # Verify it didn't also try to load the procrastinate app for defer().
        self.assertIsNone(backend._app)

    def test_default_mode_defers_to_procrastinate(self):
        backend = ProcrastinateBackend()
        # The test settings enable PROCRASTINATE_ALWAYS_EAGER (so suite-wide
        # tests don't hang on a missing worker), but here we need to exercise
        # the non-eager path: explicitly turn it off and mock the task to
        # avoid actually hitting PostgreSQL.
        with override_settings(PROCRASTINATE_ALWAYS_EAGER=False), mock.patch.object(
            ProcrastinateBackend, "_get_task"
        ) as mock_get_task:
            mock_task = mock.MagicMock()
            mock_get_task.return_value = mock_task
            backend.enqueue(
                job_result_id=uuid4(),
                job_class_path="x.y.Z",
                args=[],
                kwargs={},
                options=EnqueueOptions(user_id=uuid4(), job_model_id=uuid4()),
            )
        mock_task.defer.assert_called_once()


class EnsureJobLogHandlerAttachedTests(SimpleTestCase):
    """The handler attachment must be idempotent so repeated calls (e.g. one
    per Procrastinate job) don't pile up duplicates on the same logger.
    """

    def test_idempotent_attach(self):
        from celery.utils.log import get_logger

        from nautobot.core.celery.log import NautobotDatabaseHandler
        from nautobot.core.task_backends.runner import ensure_job_log_handler_attached

        ensure_job_log_handler_attached()
        ensure_job_log_handler_attached()
        ensure_job_log_handler_attached()

        task_logger = get_logger("celery.task")
        nautobot_handlers = [
            h for h in task_logger.handlers if isinstance(h, NautobotDatabaseHandler)
        ]
        self.assertEqual(
            len(nautobot_handlers),
            1,
            f"Expected exactly 1 NautobotDatabaseHandler after 3 calls; got {len(nautobot_handlers)}",
        )


@override_settings(TASK_BACKEND="procrastinate", PROCRASTINATE_ALWAYS_EAGER=True)
class ProcrastinateBackendEndToEndTests(TransactionTestCase):
    # JobLogEntry writes use the separate 'job_logs' database alias.
    databases = ("default", "job_logs")

    """End-to-end dispatch test: a real Nautobot Job, executed through
    JobResult.enqueue_job under TASK_BACKEND=procrastinate, with
    PROCRASTINATE_ALWAYS_EAGER short-circuiting to inline execution.

    Verifies the full bridging:
      - get_task_backend() resolves to ProcrastinateBackend
      - enqueue() short-circuits to enqueue_sync() under eager mode
      - The full lifecycle runs (before_start -> run -> on_success ->
        after_return) — `TestPassJob` raises if any of those see wrong values
      - JobResult ends up in STATUS_SUCCESS with date_done set
      - JobLogEntry rows are captured

    Mirrors the spirit of JobResultEnqueueJobCase but on the Procrastinate
    path.
    """

    def setUp(self):
        super().setUp()
        # Backend resolution is cached. Settings are overridden at class level
        # but the cache may carry a Celery instance from earlier tests.
        get_task_backend.cache_clear()
        # Import inside setUp to avoid model imports at module load time.
        from django.contrib.auth import get_user_model

        from nautobot.extras.models.jobs import Job

        User = get_user_model()
        self.user, _ = User.objects.get_or_create(username="procrastinate-e2e-test")
        self.job_model = Job.objects.get_for_class_path("pass_job.TestPassJob")
        self.job_model.enabled = True
        self.job_model.save()

    def tearDown(self):
        super().tearDown()
        get_task_backend.cache_clear()

    def test_pass_job_runs_to_success(self):
        from nautobot.extras.choices import JobResultStatusChoices
        from nautobot.extras.models.jobs import JobResult

        # Sanity: the active backend is Procrastinate.
        self.assertIsInstance(get_task_backend(), ProcrastinateBackend)
        self.assertEqual(get_task_backend().name, "procrastinate")

        job_result = JobResult.enqueue_job(
            job_model=self.job_model,
            user=self.user,
            synchronous=False,  # eager mode redirects this to enqueue_sync()
        )
        job_result.refresh_from_db()

        self.assertEqual(
            job_result.status,
            JobResultStatusChoices.STATUS_SUCCESS,
            f"Expected SUCCESS, got {job_result.status}. Traceback: {job_result.traceback}",
        )
        self.assertIsNotNone(job_result.date_started)
        self.assertIsNotNone(job_result.date_done)
        self.assertEqual(job_result.result, True)

        # JobLogEntry capture: the lifecycle hooks in TestPassJob log
        # info messages. Verify at least one made it through the handler.
        log_messages = list(job_result.job_log_entries.values_list("message", flat=True))
        self.assertTrue(
            any("Success" in m or "called as expected" in m for m in log_messages),
            f"Expected at least one lifecycle log entry, got: {log_messages}",
        )


class ProcrastinatePeriodicRunnerTests(SimpleTestCase):
    """Unit-level checks on the periodic runner's tick logic without touching
    the ScheduledJob model (those tests live in the integration test class).
    """

    def test_runner_returned_by_backend(self):
        """ProcrastinateBackend.get_periodic_runner() returns an instance."""
        backend = ProcrastinateBackend()
        runner = backend.get_periodic_runner()
        self.assertIsInstance(runner, NautobotProcrastinatePeriodicRunner)

    def test_celery_backend_has_no_runner(self):
        """CeleryBackend uses celery beat as a separate process."""
        backend = CeleryBackend()
        self.assertIsNone(backend.get_periodic_runner())

    def test_should_fire_respects_start_time_window(self):
        """A schedule with a future start_time is not yet due."""
        import datetime

        # Sentinel schedule object — only the attributes accessed by
        # _should_fire need to exist.
        now = datetime.datetime(2026, 5, 17, 22, 0, tzinfo=datetime.timezone.utc)

        class _Sched:
            start_time = datetime.datetime(2026, 6, 1, tzinfo=datetime.timezone.utc)
            stop_time = None
            last_run_at = None
            schedule = None  # not reached due to start_time guard
            pk = 1

        self.assertFalse(
            NautobotProcrastinatePeriodicRunner._should_fire(_Sched(), now)
        )

    def test_should_fire_respects_stop_time_window(self):
        """A schedule past its stop_time does not fire."""
        import datetime

        now = datetime.datetime(2026, 5, 17, 22, 0, tzinfo=datetime.timezone.utc)

        class _Sched:
            start_time = None
            stop_time = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
            last_run_at = None
            schedule = None
            pk = 2

        self.assertFalse(
            NautobotProcrastinatePeriodicRunner._should_fire(_Sched(), now)
        )
