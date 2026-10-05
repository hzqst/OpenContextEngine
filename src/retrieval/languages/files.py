"""Shared file eligibility for discovery and explicitly supplied snapshots."""
import hashlib
import os
from pathlib import Path, PurePosixPath
import re
import subprocess

MAX_BYTES = 1024 * 1024
MAX_SUFFIXES = 64
SKIP_DIRS = frozenset({'.git', '.hg', '.svn', 'node_modules', 'vendor', '.venv', 'venv',
                       '__pycache__', 'dist', 'build', 'target', '.next', '.cache',
                       '.pilot-state', 'runs'})
SKIP_SUFFIXES = ('.min.js', '.min.css', '.map', '.pyc', '.pyo', '.so', '.dylib', '.dll',
                 '.exe', '.o', '.a', '.class', '.jar', '.wasm', '.zip', '.gz', '.tar',
                 '.png', '.jpg', '.jpeg', '.gif', '.webp', '.ico', '.pdf', '.woff', '.woff2',
                 '.mp3', '.mp4', '.sqlite', '.db', '.pem', '.key')
LOCKFILES = frozenset({'package-lock.json', 'pnpm-lock.yaml', 'yarn.lock', 'poetry.lock',
                       'uv.lock', 'Cargo.lock', 'go.sum'})
GENERATED = re.compile(r'(?im)^\s*(?://|#|/\*|\*)\s*(?:code generated\b[^\n]*do not edit|'
                       r'(?:this file (?:is|was) |@)?(?:auto[- ]?)?generated\b[^\n]*do not edit)')


def normalize_suffixes(value):
    """Validate configured exclusion suffixes. Each entry names a file ending, such as '.md'."""
    if value is None:
        return ()
    if isinstance(value, str):
        value = value.split(',')
    if not isinstance(value, (list, tuple)):
        raise ValueError('OCE_EXCLUDE_SUFFIXES must be a comma-separated list of suffixes')
    if len(value) > MAX_SUFFIXES:
        raise ValueError(f'OCE_EXCLUDE_SUFFIXES accepts at most {MAX_SUFFIXES} suffixes')
    suffixes = []
    for entry in value:
        if not isinstance(entry, str):
            raise ValueError('OCE_EXCLUDE_SUFFIXES entries must be strings')
        suffix = entry.strip().lower()
        if not suffix:
            continue
        # A leading dot is required so a value like 'md' cannot match unrelated endings.
        if (len(suffix) < 2 or not suffix.startswith('.') or suffix == '..'
                or '/' in suffix or '\\' in suffix
                or any(character in '*?' or character.isspace() for character in suffix)):
            raise ValueError('Invalid OCE_EXCLUDE_SUFFIXES entry: ' + entry)
        suffixes.append(suffix)
    return tuple(dict.fromkeys(suffixes))


def path_exclusion(name, exclude_suffixes=()):
    path = PurePosixPath(name)
    if set(path.parts[:-1]) & SKIP_DIRS:
        return 'dependency-or-build-directory'
    if exclude_suffixes and name.lower().endswith(exclude_suffixes):
        return 'configured-exclusion'
    if path.name in LOCKFILES or name.lower().endswith(SKIP_SUFFIXES):
        return 'generated-or-non-source-name'
    if path.name == '.env' or (path.name.startswith('.env.') and path.name not in {'.env.example', '.env.sample'}):
        return 'local-environment'
    return None


def read_text(path):
    """Return decoded source or an explicit skip reason; never replace bad bytes."""
    if path.stat().st_size > MAX_BYTES:
        return None, None, 'file-too-large'
    raw = path.read_bytes()
    if len(raw) > MAX_BYTES:
        return None, None, 'file-too-large'
    if any(byte < 32 and byte not in (9, 10, 12, 13) for byte in raw):
        return raw, None, 'binary-content'
    try:
        text = raw.decode('utf-8-sig')
    except UnicodeDecodeError:
        return raw, None, 'non-utf8'
    if GENERATED.search(text[:2048]):
        return raw, None, 'generated-header'
    return raw, text, None


def discover_snapshot(root, exclude_suffixes=()):
    """Use Git's ignore rules when available, otherwise a bounded directory walk."""
    root = Path(root).resolve()
    if not root.is_dir():
        raise ValueError('Source root must be a directory')
    exclude_suffixes = normalize_suffixes(exclude_suffixes)
    git = subprocess.run(['git', '-C', str(root), 'rev-parse', '--is-inside-work-tree'],
                         capture_output=True, text=True)
    excluded, files = [], []
    if git.returncode == 0 and git.stdout.strip() == 'true':
        result = subprocess.run(['git', '-C', str(root), 'ls-files', '-c', '-o',
                                 '--exclude-standard', '-z'], capture_output=True, check=True)
        names = sorted(set(os.fsdecode(name) for name in result.stdout.split(b'\0') if name))
        discovery = 'git-tracked-and-unignored'
    else:
        names = []
        for directory, dirs, entries in os.walk(root, followlinks=False):
            for name in sorted(dirs):
                path = Path(directory)/name
                if name in SKIP_DIRS or path.is_symlink():
                    excluded.append({'path': path.relative_to(root).as_posix()+'/',
                                     'reason': 'symlink' if path.is_symlink() else 'dependency-or-build-directory'})
            dirs[:] = [name for name in dirs if name not in SKIP_DIRS and not (Path(directory)/name).is_symlink()]
            names.extend((Path(directory)/name).relative_to(root).as_posix() for name in entries)
        names.sort()
        discovery = 'directory-walk'
    for name in names:
        path = root/name
        reason = path_exclusion(name, exclude_suffixes)
        if path.is_symlink() or not path.resolve().is_relative_to(root):
            reason = 'symlink'
        elif not path.is_file():
            reason = 'not-a-regular-file'
        if not reason:
            raw, _, reason = read_text(path)
        if reason:
            excluded.append({'path': name, 'reason': reason})
        else:
            files.append({'path': name, 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()})
    return {'schemaVersion': 1, 'scope': 'eligible UTF-8 text; structural adapters where available',
            'discovery': discovery, 'files': files, 'excluded': sorted(excluded, key=lambda item: item['path'])}
