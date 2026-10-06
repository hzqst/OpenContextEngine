"""Discovery and expiring client leases for one locked repository worker."""
import json
import os
from pathlib import Path
import secrets
import threading
import time

LEASE_SECONDS = 30
EXIT_GRACE_SECONDS = 2


class SharedWorker:
    def __init__(self, config, *, clock=time.monotonic):
        self.config = config
        self.clock = clock
        self.instance = secrets.token_hex(24)
        self.lock = threading.Lock()
        self.leases = {}
        self.active = 0
        self.closing = False
        self.empty_since = clock()
        self.grace = LEASE_SECONDS  # Allow the first client to discover the endpoint.
        self.path = Path(config['state'])/'worker.json'

    def publish(self, port):
        record = {'protocol':self.config['shared']['protocol'], 'instance':self.instance,
                  'fingerprint':self.config['shared']['fingerprint'], 'pid':os.getpid(),
                  'baseUrl':f'http://127.0.0.1:{port}', 'apiKey':self.config['serviceKey']}
        temporary = self.path.with_name(f'worker-{self.instance}.tmp')
        try:
            with temporary.open('x', encoding='utf-8') as output:
                os.chmod(temporary, 0o600)
                json.dump(record, output)
            os.replace(temporary, self.path)
        finally:
            temporary.unlink(missing_ok=True)

    def lease(self, body):
        with self.lock:
            if self.closing:
                return 503, {'error':'Worker is stopping'}
            if body.get('instance') != self.instance:
                return 410, {'error':'Worker instance changed'}
            if (body.get('protocol') != self.config['shared']['protocol'] or
                    body.get('fingerprint') != self.config['shared']['fingerprint']):
                return 409, {'error':'Incompatible worker configuration'}
            key = body.get('lease')
            if not isinstance(key, str) or not 1 <= len(key) <= 128 or type(body.get('release')) is not bool:
                return 422, {'error':'Invalid lease'}
            if body['release']:
                self.leases.pop(key, None)
                if not self.leases:
                    self.empty_since = self.clock()
                    self.grace = EXIT_GRACE_SECONDS
            else:
                self.leases[key] = self.clock() + LEASE_SECONDS
                self.empty_since = None
            return 200, {'instance':self.instance}

    def enter(self):
        with self.lock:
            if self.closing:
                return False
            self.active += 1
            return True

    def leave(self):
        with self.lock:
            self.active -= 1

    def expire(self):
        with self.lock:
            now = self.clock()
            self.leases = {key:deadline for key,deadline in self.leases.items() if deadline > now}
            if not self.leases and self.empty_since is None:
                self.empty_since = now
                self.grace = EXIT_GRACE_SECONDS
            if not self.leases and not self.active and now-self.empty_since >= self.grace:
                self.closing = True
            return self.closing

    def monitor(self, server):
        while not self.expire():
            time.sleep(.25)
        server.shutdown()

    def remove(self):
        # Called before releasing the writer lock, so a successor cannot be removed.
        try:
            if json.loads(self.path.read_text(encoding='utf-8')).get('instance') == self.instance:
                self.path.unlink(missing_ok=True)
        except (OSError, ValueError):
            pass
