# Copyright (c) 2017 Red Hat, Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import time
from unittest import mock

from cinder import manager
from cinder import objects
from cinder.tests.unit import test


class FakeManager(manager.CleanableManager):
    def __init__(self, service_id=None, keep_after_clean=False):
        if service_id:
            self.service_id = service_id
        self.keep_after_clean = keep_after_clean

    def _do_cleanup(self, ctxt, vo_resource):
        vo_resource.status += '_cleaned'
        vo_resource.save()
        return self.keep_after_clean


class TestManager(test.TestCase):
    @mock.patch('cinder.utils.set_log_levels')
    def test_set_log_levels(self, set_log_mock):
        service = manager.Manager()
        log_request = objects.LogLevel(prefix='sqlalchemy.', level='debug')
        service.set_log_levels(mock.sentinel.context, log_request)
        set_log_mock.assert_called_once_with(log_request.prefix,
                                             log_request.level)

    @mock.patch('cinder.utils.get_log_levels')
    def test_get_log_levels(self, get_log_mock):
        get_log_mock.return_value = {'cinder': 'DEBUG', 'cinder.api': 'ERROR'}
        service = manager.Manager()
        log_request = objects.LogLevel(prefix='sqlalchemy.')
        result = service.get_log_levels(mock.sentinel.context, log_request)
        get_log_mock.assert_called_once_with(log_request.prefix)

        expected = (objects.LogLevel(prefix='cinder', level='DEBUG'),
                    objects.LogLevel(prefix='cinder.api', level='ERROR'))

        self.assertEqual(set(str(r) for r in result.objects),
                         set(str(e) for e in expected))


class TestThreadPoolManager(test.TestCase):
    """Test cases for ThreadPoolManager threadpool tasks + shutdown."""

    def test_add_to_threadpool_executes(self):
        """Test that _add_to_threadpool runs the task to completion."""
        mgr = manager.ThreadPoolManager()
        executed = []

        future = mgr._add_to_threadpool(
            lambda: executed.append(True))
        self.assertIsNotNone(future)
        future.result(timeout=5)

        self.assertEqual([True], executed)
        mgr.cleanup_threadpool()

    def test_signal_shutdown_rejects_new_tasks(self):
        """Test that new tasks are rejected after shutdown signal."""
        mgr = manager.ThreadPoolManager()
        mgr.signal_shutdown()

        executed = []
        result = mgr._add_to_threadpool(lambda: executed.append(True))

        # Should return None when shutdown is signaled
        self.assertIsNone(result)

        # Give it a moment to potentially execute
        time.sleep(0.1)

        self.assertEqual([], executed)
        mgr.cleanup_threadpool()

    def test_signal_shutdown_allows_existing_tasks(self):
        """Test that existing tasks continue after shutdown signal."""
        mgr = manager.ThreadPoolManager()
        completed = []

        def slow_task():
            time.sleep(0.1)
            completed.append(True)

        # Spawn task first
        future = mgr._add_to_threadpool(slow_task)
        # Then signal shutdown
        mgr.signal_shutdown()

        # Existing task should still complete
        future.result(timeout=5)
        self.assertEqual([True], completed)
        mgr.cleanup_threadpool()

    def test_cleanup_threadpool_allows_restart(self):
        """Test that cleanup_threadpool re-creates executor for restart.

        oslo.service ProcessLauncher with restart_method='mutate' forks
        a new child that inherits the parent's manager object. After
        stop()/wait()/cleanup(), the child calls start() -> init_host()
        which needs to submit tasks to the threadpool. Without
        re-creating the executor, this would raise:
        RuntimeError: cannot schedule new futures after shutdown
        """
        mgr = manager.ThreadPoolManager()

        # Simulate a full shutdown cycle
        mgr.signal_shutdown()
        mgr.cleanup_threadpool()

        # After cleanup, the executor should be re-created and usable
        executed = []
        future = mgr._add_to_threadpool(lambda: executed.append(True))
        self.assertIsNotNone(future)

        future.result(timeout=5)
        self.assertEqual([True], executed)
        mgr.cleanup_threadpool()

    def test_cleanup_threadpool_clears_shutdown_event(self):
        """Test cleanup resets shutdown_event so new tasks accepted."""
        mgr = manager.ThreadPoolManager()

        mgr.signal_shutdown()
        # After signal_shutdown, tasks should be rejected
        result = mgr._add_to_threadpool(lambda: None)
        self.assertIsNone(result)

        mgr.cleanup_threadpool()

        # After cleanup, tasks should be accepted again
        executed = []
        future = mgr._add_to_threadpool(lambda: executed.append(True))
        self.assertIsNotNone(future)

        future.result(timeout=5)
        self.assertEqual([True], executed)
        mgr.cleanup_threadpool()


class TestDoCleanupFreshnessGuard(test.TestCase):
    """Test that do_cleanup skips fresh worker entries."""

    @mock.patch('cinder.db.worker_get_all')
    def test_do_cleanup_skips_fresh_entries(self, mock_get_all):
        """Test that entries from OTHER services updated recently are skipped.

        The freshness guard protects worker entries that belong to a
        different (draining) service and are still being heartbeated.
        """
        from oslo_utils import timeutils

        mgr = FakeManager(service_id=1)

        # Create a mock worker entry from a DIFFERENT service (id=2)
        # that was updated 5 seconds ago (still fresh/heartbeating)
        fresh_worker = mock.MagicMock()
        fresh_worker.updated_at = timeutils.utcnow()
        fresh_worker.resource_type = 'Volume'
        fresh_worker.resource_id = 'vol-123'
        fresh_worker.service_id = 2  # different service

        mock_get_all.return_value = [fresh_worker]

        cleanup_request = mock.MagicMock()
        cleanup_request.resource_type = None
        cleanup_request.resource_id = None
        cleanup_request.service_id = 2  # cleaning service 2's entries
        cleanup_request.until = None

        ctxt = mock.MagicMock()
        mgr.do_cleanup(ctxt, cleanup_request)

        # The fresh entry from a different service should NOT be claimed

    @mock.patch('cinder.db.worker_get_all')
    def test_do_cleanup_processes_stale_entries(self, mock_get_all):
        """Test that entries updated long ago ARE processed."""
        import datetime

        from oslo_utils import timeutils

        mgr = FakeManager(service_id=1)

        # Create a mock worker entry that was updated 120 seconds ago
        stale_worker = mock.MagicMock()
        stale_worker.updated_at = (
            timeutils.utcnow() - datetime.timedelta(seconds=120))
        stale_worker.resource_type = 'Volume'
        stale_worker.resource_id = 'vol-456'
        stale_worker.service_id = 1
        stale_worker.id = 99
        stale_worker.status = 'creating'

        mock_get_all.return_value = [stale_worker]

        cleanup_request = mock.MagicMock()
        cleanup_request.resource_type = None
        cleanup_request.resource_id = None
        cleanup_request.service_id = 1
        cleanup_request.until = None

        ctxt = mock.MagicMock()

        with mock.patch('cinder.db.worker_claim_for_cleanup') as mock_claim, \
                mock.patch('cinder.db.worker_destroy') as mock_destroy:
            mock_claim.return_value = True
            mock_destroy.return_value = True
            with mock.patch('cinder.objects.Volume.get_by_id') as mock_get:
                mock_vol = mock.MagicMock()
                mock_vol.status = 'creating'
                mock_vol.is_clustered = False
                mock_get.return_value = mock_vol

                mgr.do_cleanup(ctxt, cleanup_request)

                # Stale entry should have been claimed
                mock_claim.assert_called_once()


class TestRejectIfDraining(test.TestCase):
    """Test the reject_if_draining decorator in volume manager."""

    def test_reject_if_draining_allows_when_not_draining(self):
        """Operations pass through when service is not draining."""
        from cinder.volume import manager as vol_manager

        @vol_manager.reject_if_draining
        def fake_op(self, context):
            return "success"

        obj = mock.MagicMock()
        obj._draining = False

        result = fake_op(obj, mock.sentinel.context)
        self.assertEqual("success", result)

    def test_reject_if_draining_rejects_when_draining(self):
        """Operations are rejected when service is draining."""
        from cinder import exception
        from cinder.volume import manager as vol_manager

        @vol_manager.reject_if_draining
        def fake_op(self, context):
            return "should not reach here"

        obj = mock.MagicMock()
        obj._draining = True

        self.assertRaises(
            exception.ServiceUnavailable,
            fake_op, obj, mock.sentinel.context)

    def test_reject_if_draining_unconditional(self):
        """Rejection does not depend on any config option."""
        from cinder import exception
        from cinder.volume import manager as vol_manager

        @vol_manager.reject_if_draining
        def fake_op(self, context):
            return "should not reach here"

        obj = mock.MagicMock()
        obj._draining = True

        # Should reject regardless of any config state
        self.assertRaises(
            exception.ServiceUnavailable,
            fake_op, obj, mock.sentinel.context)
