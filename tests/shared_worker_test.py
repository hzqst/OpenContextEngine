"""Leases expire after client crashes, without interrupting active requests."""
from pathlib import Path
import sys
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src/retrieval'))
from shared_worker import SharedWorker, LEASE_SECONDS, EXIT_GRACE_SECONDS


class SharedWorkerTests(unittest.TestCase):
    def setUp(self):
        self.now = 0
        self.worker = SharedWorker({'state':'.','shared':{'protocol':1,'fingerprint':'test'}},clock=lambda:self.now)

    def lease(self, key='one', release=False, **changes):
        return self.worker.lease({'instance':self.worker.instance,'protocol':1,'fingerprint':'test',
                                  'lease':key,'release':release,**changes})[0]

    def test_client_release_does_not_interrupt_other_client(self):
        self.assertEqual(200,self.lease())
        self.assertEqual(200,self.lease('two'))
        self.lease(release=True)
        self.now += EXIT_GRACE_SECONDS+1
        self.assertFalse(self.worker.expire())
        self.lease('two',release=True)
        self.now += EXIT_GRACE_SECONDS
        self.assertTrue(self.worker.expire())
        self.assertEqual(503,self.lease())

    def test_crashed_client_expires_but_active_request_finishes(self):
        self.lease()
        self.assertTrue(self.worker.enter())
        self.now = LEASE_SECONDS+1
        self.assertFalse(self.worker.expire())
        self.now += EXIT_GRACE_SECONDS
        self.assertFalse(self.worker.expire())
        self.worker.leave()
        self.assertTrue(self.worker.expire())

    def test_wrong_instance_and_incompatible_configuration_cannot_lease(self):
        self.assertEqual(410,self.lease(instance='stale'))
        self.assertEqual(409,self.lease(fingerprint='different'))
        self.assertEqual(409,self.lease(protocol=2))
        self.assertEqual({},self.worker.leases)

    def test_never_connected_worker_exits_and_heartbeat_renews(self):
        self.now = LEASE_SECONDS
        self.assertTrue(self.worker.expire())
        self.setUp()
        self.lease()
        self.now = LEASE_SECONDS-1
        self.lease()
        self.now = LEASE_SECONDS+1
        self.assertFalse(self.worker.expire())
