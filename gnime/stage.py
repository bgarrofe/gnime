from abc import ABC, abstractmethod
import diskcache

from .serialization import PickleSerializer
from .cache import DiskCache
from .core import Node


class Stage(ABC):
    """
    Base abstract class from which all stages must inherit. All subclasses must
    implement at least `name` and `run` methods.

    preceding_stages: List of preceding stages for the stage
    name: Name of the stage
    """

    def __init__(self):
        self.preceding_stages = list()

    def after(self, pipeline_stage):
        """Method to add stages as dependencies for the current stage."""
        self.preceding_stages.append(pipeline_stage)
        return self

    @property
    @abstractmethod
    def name(self) -> str:
        ...

    @abstractmethod
    def run(self, *args, **kwargs):
        ...


class StageExecutor(PickleSerializer, DiskCache):
    """
    Context manager for the execution of a stage, or group of stages, of a
    pipeline.

    The setup phase (__enter__) persists relevant metadata such as the stages
    currently in progress to a Redis server.

    The teardown phase (__exit__) deletes relevant metadata from the Redis
    server.

    redis_instance: A connection to Redis.
    stages: The stages that are currently in progress.
    """

    def __init__(self, disk_cache: diskcache.Cache, stages):
        DiskCache.__init__(self, disk_cache)

        self.pipeline = self.read('pipeline')
        self.stages = stages

    def __enter__(self):
        self.write('in_progress', self.serialize(self.stages))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        completed = self.deserialize(self.read('in_progress'))
        current_done = self.read('done')
        if current_done is None:
            current_done = []
        else:
            current_done = self.deserialize(current_done)
        current_done += completed
        self.write('done', self.serialize(current_done))
        self.delete('in_progress')

    @staticmethod
    def execute(fn: callable, *args, **kwargs) -> None:
        """Execute the stage/group of stages."""
        fn(*args, **kwargs)


class DiskCacheStage(Stage, PickleSerializer, DiskCache):
    """
    Stage type to use if a disk-based cache is required. The disk cache can be
    used to pass data between stages, or cache values to be used elsewhere
    downstream. It's completely up to the implementer/user, as this interface
    to the disk cache is generic, and enables the reading/writing of generic
    Python objects from/to the disk cache through pickle-based serialization.
    """

    def __init__(self, cache: diskcache.Cache):
        DiskCache.__init__(self, disk_cache=cache)
        Stage.__init__(self)

    def read(self, k: str) -> object:
        return self.deserialize(DiskCache.read(self, k))

    def write(self, k: str, v: object) -> None:
        DiskCache.write(self, k, self.serialize(v))

    @property
    def name(self) -> str:
        """
        The name is the final subclass (i.e. the name class defined by the user
        when subclassing this class.
        """
        return self.__class__.__name__


class NodeStage(Node, DiskCacheStage):

    def __init__(self, cache: diskcache.Cache, config: dict = None):
        DiskCacheStage.__init__(self, cache=cache)
        self.config = config

    def pre_execute(self):
        inputs = [
            self.read(port.name)
            for port in getattr(self, 'input_ports', [])
        ]
        return inputs

    def execute(self):
        inputs = self.pre_execute()
        outputs = self.run(*inputs)
        self.post_execute(outputs)

    def post_execute(self, outputs):
        if not isinstance(outputs, tuple):
            outputs = (outputs,)
        for idx, port in enumerate(getattr(self, 'output_ports', [])):
            self.write(port.name, outputs[idx])
