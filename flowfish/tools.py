import collections
import os
from pathlib import Path
import re
import shutil
from typing import List, Optional, Union


from flowfish import flow, TYPE_CHECKING
from flowfish.logger import logger
from flowfish.utils import humansize

if TYPE_CHECKING:
    from flowfish.flow import Flow


def _find_files(data_dir: Path, flows, find_all: bool = False):
    slug_dirs = set()
    base_dirs = set()

    for f in flows:
        for scope in f:
            base_dirs.add(scope._base_dir)
            for node in scope:
                # skip locked nodes
                if not node._locked:
                    slug_dirs.add(node._work_dir)

    if find_all:
        base_dirs.update(list([p for p in data_dir.iterdir() if p.is_dir() and not p.name.startswith('.')]))

    # only search files within base_dirs
    data_files = collections.defaultdict(set)
    data_sizes, data_counts = dict(), dict()

    file_pattern = re.compile(r'^\w+\.[a-z0-9]+\.(data|data\.tmp|data\.mdb|data\.mdb\.tmp|json)$', re.ASCII)
    lock_pattern = re.compile(r'^\w+\.[a-z0-9]+\.lock$', re.ASCII)
    sync_pattern = re.compile(r'^\w+\.[a-z0-9]+\.sync$', re.ASCII)
    slug_pattern = re.compile(r'^\w+\.[a-z0-9]+$', re.ASCII)

    for base_dir in base_dirs:
        if base_dir.is_dir():
            for f in base_dir.iterdir():
                # find files
                if f.is_file() and file_pattern.match(f.name):
                    slug_dir = base_dir / f.stem
                    if slug_dir not in slug_dirs:
                        data_files[slug_dir].add(f)
                # find dirs
                elif f.is_dir() and slug_pattern.match(f.name):
                    if f not in slug_dirs:
                        data_files[f].add(f)

            lock_dir = base_dir / '.lock'
            if lock_dir.is_dir():
                for f in lock_dir.iterdir():
                    if f.is_file() and lock_pattern.match(f.name):
                        slug_dir = base_dir / f.stem
                        if slug_dir not in slug_dirs:
                            data_files[slug_dir].add(f)

            sync_dir = base_dir / '.sync'
            if sync_dir.is_dir():
                for f in sync_dir.iterdir():
                    if f.is_file() and sync_pattern.match(f.name):
                        slug_dir = base_dir / f.stem
                        if slug_dir not in slug_dirs:
                            data_files[slug_dir].add(f)

    # some stats
    for slug_dir, files in data_files.items():
        count, size = 0, 0
        for f in files:
            count += 1
            size += f.lstat().st_size
            if f.is_dir():
                files = list(f.glob('**/*'))
                count += len(files)
                size += sum(f.lstat().st_size for f in files)
        data_counts[slug_dir] = count
        data_sizes[slug_dir] = size

    return data_files, data_counts, data_sizes


def flow_prune(data_dir: Path,
               sync_dir: Optional[Path],
               conf_files: List[Union[str, Path]],
               find_all: bool = False,
               confirmed: bool = False):
    if not conf_files:
        conf_files = []
        # add flow confs from current working directory
        conf_files += list(Path.cwd().glob('*.json'))
        # add flow confs from data_dir
        conf_files += list(Path(data_dir).glob('*.json'))

    # create flows from confs
    flows: List['Flow'] = []
    for conf_file in conf_files:
        try:
            flows.append(flow(conf_file, data_dir=data_dir, sync_dir=sync_dir))
        except Exception as e:
            logger.warning(f'flow failed: {conf_file} ({e!r})')

    # find data files for flows
    data_files, data_counts, data_sizes = _find_files(data_dir, flows, find_all)

    # only list if not confirmed ("dry mode")
    if not confirmed:
        for slug_dir, files in sorted(data_files.items()):
            print(f'+ {slug_dir.relative_to(data_dir)} ({humansize(data_sizes[slug_dir])})')
        print(f'{humansize(sum(data_sizes.values()))} in '
              f'{sum(data_counts.values()):,} unused file(s) in "{data_dir}" prunable')
    else:
        for slug_dir, files in sorted(data_files.items()):
            print(f'- {slug_dir.relative_to(data_dir)} ({humansize(data_sizes[slug_dir])})')
            for f in files:
                if f.is_file():
                    os.remove(f)
                elif f.is_dir():
                    shutil.rmtree(f, ignore_errors=True)
        print(f'{humansize(sum(data_sizes.values()))} in '
              f'{sum(data_counts.values()):,} unused file(s) in "{data_dir}" pruned')
