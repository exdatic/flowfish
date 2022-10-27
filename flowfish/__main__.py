import argparse
import configparser
import logging
import os
from pathlib import Path
import sys
import time
from typing import Iterator, List, Tuple

from flowfish import flow, TYPE_CHECKING
from flowfish.logger import logger
from flowfish.tools import flow_prune


if TYPE_CHECKING:
    from flowfish.flow import Flow
    from flowfish.scope import Scope
    from flowfish.node import Node


__FISH__ = "><\u001b[31;1m(\u001b[33;1m(\u001b[32;1m(\u001b[34;1m(\u001b[35;1m'\u001b[0m>"

# configure loguru
logger.remove()
logger.add(sys.stderr, level='INFO', format='<green>[{time:YYYY-MM-DD HH:mm:ss}]</green> <level>{message}</level>')


def flow_push(data_dir, sync_dir, conf_files, names, push_all: bool = False):
    if sync_dir is None or not sync_dir.is_dir():
        raise Exception('sync_dir must exist')

    flows: List['Flow'] = [flow(f, data_dir=data_dir, sync_dir=sync_dir) for f in conf_files]
    for flow_ in flows:
        for name in names:
            if '.' in name:
                scope_name, node_name = name.split('.', 1)
                if hasattr(flow_, scope_name):
                    scope: 'Scope' = getattr(flow_, scope_name)
                    if hasattr(scope, node_name):
                        node: 'Node' = getattr(scope, node_name)
                        node.push(copy_all=push_all)


def flow_pull(data_dir, sync_dir, conf_files, names):
    if sync_dir is None or not sync_dir.is_dir():
        raise Exception('sync_dir must exist')

    flows: List['Flow'] = [flow(f, data_dir=data_dir, sync_dir=sync_dir) for f in conf_files]
    for flow_ in flows:
        for name in names:
            if '.' in name:
                scope_name, node_name = name.split('.', 1)
                if hasattr(flow_, scope_name):
                    scope: 'Scope' = getattr(flow_, scope_name)
                    if hasattr(scope, node_name):
                        node: 'Node' = getattr(scope, node_name)
                        node.pull()


def flow_run(data_dir, sync_dir, conf_files, names, agent=None):
    logger.info(f"{__FISH__} Running {', '.join(conf_files)}")
    flows: List['Flow'] = [flow(f, data_dir=data_dir, sync_dir=sync_dir) for f in conf_files]

    for conf_file, flow_ in zip(conf_files, flows):
        for name in names:
            if '.' in name:
                scope_name, node_name = name.split('.', 1)
                if hasattr(flow_, scope_name):
                    scope: 'Scope' = getattr(flow_, scope_name)
                    if hasattr(scope, node_name):
                        node: 'Node' = getattr(scope, node_name)
                        try:
                            if agent:
                                node(_agent=agent)
                            else:
                                node()
                        except Exception:
                            logger.error(f'{conf_file}#{node.scope}.{node.name} failed')


def find_job(job, data_dir, sync_dir) -> Iterator[Tuple[Path, 'Node']]:
    if sync_dir is None or not sync_dir.is_dir():
        raise Exception('sync_dir must exist')

    for base_dir in sync_dir.iterdir():
        if base_dir.is_dir():
            jobs_dir = base_dir / '.jobs'
            if jobs_dir.is_dir():
                for job_file in jobs_dir.glob(f'*.{job}.json'):
                    if job_file.is_file():
                        logger.info(f'job: {job_file.relative_to(sync_dir)} found')
                        slug = '.'.join(job_file.name.split('.')[:2])
                        flow_ = flow(job_file, data_dir=data_dir, sync_dir=sync_dir)
                        for scope in flow_:
                            for node in scope:
                                if node._slug == slug:
                                    yield job_file, node


def flow_agent(agent, data_dir, sync_dir):
    if sync_dir is None or not sync_dir.is_dir():
        raise Exception('sync_dir must exist')

    logger.info(f"{__FISH__} Running...")
    while True:
        for job_file, node in find_job(agent, data_dir, sync_dir):
            node.pull()
            node()
            node.push()
            # mark job as done
            job_file.rename(job_file.with_suffix('.done'))
        time.sleep(1)


def main():
    # disable stack dump on ctrl+c
    logging.getLogger("concurrent.futures").addFilter(lambda record: False)

    class MainParser(argparse.ArgumentParser):

        def error(self, message):
            self.print_help()
            sys.stderr.write('\nerror: %s\n' % message)
            sys.exit(2)

    # read settings from flowfish.flowconfig
    settings = configparser.ConfigParser()
    settings.read([os.path.expanduser(ini_file) for ini_file in (
        '.flow/config', '~/.flowconfig', '~/.config/flow/config')])

    # flow ... [-d data_dir] [-s sync_dir]
    folder_parser = argparse.ArgumentParser(add_help=False)
    folder_parser.add_argument(
        '-d', '--data-dir', dest='data_dir',
        default=settings.get('flow', 'data_dir', fallback='.'),
        help='the data folder (default: %(default)s)'
    )
    folder_parser.add_argument(
        '-s', '--sync-dir', dest='sync_dir',
        default=settings.get('flow', 'sync_dir', fallback=None),
        help='the sync folder (default: %(default)s)'
    )

    # flow
    flow_parser = MainParser("flow")
    flow_subparsers = flow_parser.add_subparsers(
        title='command', dest='command'
    )

    # flow run
    run_parser = flow_subparsers.add_parser(
        'run', parents=[folder_parser], help='run flow'  # type: ignore
    )
    run_parser.add_argument(
        '-c', '--conf', nargs='+', required=True, help='flow config file'
    )
    run_parser.add_argument(
        '-n', '--name', nargs='+', required=True, help='the step name (e.g. foo.bar)'
    )
    run_parser.add_argument(
        '-a', '--agent', help='push step to agent'
    )

    # flow agent
    agent_parser = flow_subparsers.add_parser(
        'agent', parents=[folder_parser], help='start agent'  # type: ignore
    )
    agent_parser.add_argument(
        '-a', '--agent', required=True, help='the agent name'
    )

    # flow push
    push_parser = flow_subparsers.add_parser(
        'push', parents=[folder_parser], help='push data to sync_dir'  # type: ignore
    )
    push_parser.add_argument(
        '-c', '--conf', nargs='+', required=True, help='flow config file'
    )
    push_parser.add_argument(
        '-n', '--name', nargs='+', required=True, help='the step name (e.g. foo.bar)'
    )
    push_parser.add_argument(
        '--push-all', dest='push_all', action='store_true', help='include work_dir'
    )

    # flow pull
    pull_parser = flow_subparsers.add_parser(
        'pull', parents=[folder_parser], help='pull data from sync_dir'  # type: ignore
    )
    pull_parser.add_argument(
        '-c', '--conf', nargs='+', required=True, help='flow config file'
    )
    pull_parser.add_argument(
        '-n', '--name', nargs='+', required=True, help='the step name (e.g. foo.bar)'
    )

    # flow prune
    prune_parser = flow_subparsers.add_parser(
        'prune', parents=[folder_parser], help='prune files in data_dir'  # type: ignore
    )
    prune_parser.add_argument(
        '-c', '--conf', nargs='+', required=False, help='flow config file'
    )
    prune_parser.add_argument(
        '-a', '--all', action='store_true', help='prune all files'
    )
    prune_parser.add_argument(
        '-y', '--yes', action='store_true', help='confirm removal'
    )

    if len(sys.argv) == 1:
        flow_parser.print_help()
    else:
        args = flow_parser.parse_args()

        # ensure Path object
        data_dir = Path(args.data_dir).expanduser() if args.data_dir is not None else Path('.')
        sync_dir = Path(args.sync_dir).expanduser() if args.sync_dir is not None else None

        try:
            if args.command == 'run':
                flow_run(data_dir, sync_dir, args.conf, args.name, agent=args.agent)
            elif args.command == 'agent':
                flow_agent(args.agent, data_dir, sync_dir)
            elif args.command == 'push':
                flow_push(data_dir, sync_dir, args.conf, args.name, push_all=args.push_all)
            elif args.command == 'pull':
                flow_pull(data_dir, sync_dir, args.conf, args.name)
            elif args.command == 'prune':
                flow_prune(data_dir, sync_dir, args.conf, find_all=args.all, confirmed=args.yes)
        except KeyboardInterrupt:
            # fail silently
            pass


if __name__ == "__main__":

    main()
