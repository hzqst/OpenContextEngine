"""A numeric loopback worker must start without reverse DNS."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

SOURCE = Path(__file__).resolve().parents[1]/'scripts/retrieval-server.py'
SPEC = importlib.util.spec_from_file_location('retrieval_worker',SOURCE)
WORKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(WORKER)


class LoopbackServerTests(unittest.TestCase):
    def test_bind_uses_the_numeric_address_and_bound_port_without_dns(self):
        server = WORKER.LoopbackHTTPServer.__new__(WORKER.LoopbackHTTPServer)
        server.server_address = ('127.0.0.1',0)
        server.socket = Mock()
        server.socket.getsockname.return_value = ('127.0.0.1',23456)
        with patch('socket.getfqdn',side_effect=AssertionError('Reverse DNS must not run')):
            server.server_bind()
        server.socket.bind.assert_called_once_with(('127.0.0.1',0))
        self.assertEqual(server.server_name,'127.0.0.1')
        self.assertEqual(server.server_port,23456)


class StartupFailureTests(unittest.TestCase):
    def test_an_unusable_configuration_names_the_reason_before_exiting(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)/'repo'
            root.mkdir()
            config = {'root':str(root),'state':str(Path(directory)/'index'),'serviceKey':'test-only',
                      'excludeSuffixes':['md'],'pollSeconds':1,'debounceSeconds':.3}
            failed = subprocess.run([sys.executable,str(SOURCE)],input=json.dumps(config)+'\n',
                                    capture_output=True,encoding='utf-8')
            self.assertEqual(failed.returncode,1)
            # The launcher reports an exit code alone, so the worker names the reason itself.
            self.assertEqual(json.loads(failed.stdout.strip())['error'],
                             'ValueError: Invalid OCE_EXCLUDE_SUFFIXES entry: md')
            self.assertIn('ValueError',failed.stderr)
