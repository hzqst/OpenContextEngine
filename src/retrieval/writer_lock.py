"""Nonblocking process lock, released when its file handle is closed."""
from datetime import datetime, timezone
import json
import os
import sys

if sys.platform == 'win32':
    import msvcrt

    def lock(file):
        # Every writer locks the same byte, including when the file is empty.
        file.seek(0)
        msvcrt.locking(file.fileno(), msvcrt.LK_NBLCK, 1)
else:
    import fcntl

    def lock(file):
        fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)


class WriterBusy(ValueError):
    pass


def owner_path(path):
    return path.with_name(path.name + '.owner')


def record_owner(path):
    """Advisory holder identity for contention messages, valid only while the lock is held."""
    try:
        owner_path(path).write_text(json.dumps({'pid':os.getpid(),
            'acquiredAt':datetime.now(timezone.utc).isoformat(timespec='seconds')}) + '\n',
            encoding='utf-8')
    except OSError:
        pass  # Diagnostics must never block indexing.


def holder(path):
    """Describe the recorded holder, or nothing when the record is missing or damaged."""
    try:
        owner = json.loads(owner_path(path).read_text(encoding='utf-8'))
        return f" (pid {owner['pid']}, holding since {owner['acquiredAt']})"
    except (OSError, ValueError, KeyError, TypeError):
        return ''


def acquire_writer_lock(path):
    file = path.open('a+b')
    try:
        lock(file)
    except OSError:
        file.close()
        raise WriterBusy(f'This index directory already has a running writer{holder(path)}: {path}') from None
    record_owner(path)
    return file
