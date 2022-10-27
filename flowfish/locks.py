from threading import RLock, Lock
from typing import Any, MutableMapping
from weakref import WeakValueDictionary


class KeyedLocks:

    _keys: MutableMapping[Any, RLock]
    _lock: Lock

    def __init__(self):
        self._keys = WeakValueDictionary()
        self._lock = Lock()

    def __getitem__(self, key: Any) -> RLock:
        return self.get(key)

    def get(self, key: Any) -> RLock:
        with self._lock:
            if key in self._keys:
                lock = self._keys[key]
            else:
                lock = RLock()
                self._keys[key] = lock
            return lock
