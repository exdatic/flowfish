from asyncio.events import AbstractEventLoop
import configparser
import os
from pathlib import Path
from typing import Dict, List, Optional, Union, TYPE_CHECKING

from flowfish.builtins import get, map_simpleeval, run
from flowfish.error import FlowError
from flowfish.flux import Flux
from flowfish.func import Func
from flowfish.locks import KeyedLocks

if TYPE_CHECKING:
    from flowfish.flow import Flow

__all__ = ['flow', 'FlowError']


def flow(conf: Union[Path, str, Dict, List[Union[Path, str, Dict]]], props: Optional[Dict] = None,
         data_dir: Optional[Union[Path, str]] = None,
         sync_dir: Optional[Union[Path, str]] = None,
         funcs: Optional[Dict] = None,
         share: bool = False,
         loop: Optional[AbstractEventLoop] = None,
         ) -> 'Flow':
    """load flow from file or dict

    Parameters
    ----------
    conf : Union[Path, str, Dict]
        a JSON configuration file or dictionary
    props : Dict, optional
        properties to override configuration parameters
    data_dir : Union[Path, str], optional
        the location where all the data is stored
    sync_dir : Union[Path, str], optional
        the location where all the data is synced
    share: bool
        share internal caches between flows

    Returns
    -------
    Flow
        the flow to go
    """

    # read settings file
    settings = configparser.ConfigParser()
    settings.read([os.path.expanduser(ini_file) for ini_file in (
        '.flowconfig', '.flow/config', '~/.flowconfig', '~/.config/flow/config')])

    # read data_dir/sync_dir from settings file
    if data_dir is None:
        data_dir = os.environ.get('FLOW_DATA_DIR')
        if data_dir is None:
            data_dir = settings.get('flow', 'data_dir', fallback='.')

    if sync_dir is None:
        sync_dir = os.environ.get('FLOW_SYNC_DIR')
        if sync_dir is None:
            sync_dir = settings.get('flow', 'sync_dir', fallback=None)

    # ensure Path object
    if data_dir is not None:
        data_dir = Path(data_dir).expanduser()

    if sync_dir is not None:
        sync_dir = Path(sync_dir).expanduser()

    assert data_dir is not None and isinstance(data_dir, Path), "data_dir missing"
    assert sync_dir is None or isinstance(sync_dir, Path), "sync_dir missing"

    funcs = dict((k, Func(k, v)) for k, v in funcs.items()) if funcs else dict()
    funcs.update(get=Func('get', get),
                 map=Func('map', map_simpleeval),
                 run=Func('run', run))

    cache = dict()
    locks = KeyedLocks()

    flux = Flux(data_dir, sync_dir, funcs, cache, locks, share, loop)

    if isinstance(conf, (Path, str)):
        return flux.load_flow(conf, props=props)
    else:
        if type(conf) is list:
            conf = _merge_confs(flux, conf, props)
        assert isinstance(conf, Dict)
        return flux.make_flow(conf, props=props)


def _merge_confs(flux: Flux, confs: List[Union[Path, str, Dict]], props: Optional[Dict]):
    new_conf = dict()
    scopes = dict()
    for conf in confs:
        if isinstance(conf, (Path, str)):
            file = conf
            _, conf = flux.load_conf(conf, props)
            for scope_name in conf:
                if scope_name.startswith('#') or scope_name.startswith('_'):
                    continue
                if '@' in scope_name:
                    scope_name, _ = scope_name.split('@', 1)
                scope_link = f'{scope_name}@{file}#{scope_name}'
                scopes[scope_name] = scope_link
                new_conf[scope_link] = dict()
        elif type(conf) is dict:
            for scope_name, scope_conf in conf.items():
                if '@' in scope_name:
                    _, base_name = scope_name.split('@', 1)
                    if base_name in scopes:
                        # add base scope
                        base_scope = scopes[base_name]
                        new_conf[base_scope] = dict()
                        new_conf[scope_name] = scope_conf
                    else:
                        new_conf[scope_name] = scope_conf
                elif scope_name in scopes:
                    base_scope = scopes[scope_name]
                    new_conf[base_scope] = scope_conf
                else:
                    new_conf[scope_name] = scope_conf
        else:
            raise FlowError(f'conf must be dict and not {type(conf)}')
    return new_conf
