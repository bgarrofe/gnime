from abc import ABC, abstractmethod

import diskcache

from .exceptions import InvalidCacheTypeException, InvalidKeyTypeException, InvalidValueTypeException


class Cache(ABC):
    """
    Abstract base class for all cache implementations. ALl subclasses must
    implement a read, write, and delete method.
    """

    @abstractmethod
    def read(self, *args, **kwargs):
        ...

    @abstractmethod
    def write(self, *args, **kwargs):
        ...

    @abstractmethod
    def delete(self, *args, **kwargs):
        ...


class DiskCache(Cache):
    """
    Implementation for a disk cache to be used by users who wish to inherit
    this functionality. We use the diskcache Python package from pypi to
    implement this caching feature.
    """

    def __init__(self, disk_cache: diskcache.Cache):
        if not isinstance(disk_cache, diskcache.Cache):
            raise InvalidCacheTypeException(
                'Please ensure disk_cache is of type diskcache.Cache')

        self.disk_cache = disk_cache

    def read(self, k: str) -> bytes:
        """Read a value from the disk cache given the associated string key."""
        if not isinstance(k, str):
            raise InvalidKeyTypeException('Please ensure key is a string')

        # return self.disk_cache[k]
        return self.disk_cache.get(k)

    def write(self, k: str, v: bytes) -> None:
        """Write a value to the disk cache given a key-value pair."""
        if not isinstance(k, str):
            raise InvalidKeyTypeException('Please ensure key is a string')

        if not isinstance(v, (str, bytes)):
            raise InvalidValueTypeException(
                'Please ensure value is of type string or bytes')

        self.disk_cache[k] = v

    def delete(self, k: str) -> None:
        """Delete a value from disk cache given the associated string key."""
        if not isinstance(k, str):
            raise InvalidKeyTypeException('Please ensure key is a string')

        self.disk_cache.delete(k)
