from abc import ABC, abstractmethod
import cloudpickle
import pickle

from .exceptions import InvalidTypeForDeserializationException


class Serializer(ABC):
    @abstractmethod
    def serialize(self, *args, **kwargs):
        ...

    @abstractmethod
    def deserialize(self, *args, **kwargs):
        ...


class PickleSerializer(Serializer):  # Cannot serialize decorated functions
    def serialize(self, obj: object) -> bytes:
        return pickle.dumps(obj)

    def deserialize(self, serialized_obj: bytes) -> object:
        if not isinstance(serialized_obj, bytes):
            raise InvalidTypeForDeserializationException(
                'Please ensure the serialized object is of type bytes.')
        return pickle.loads(serialized_obj)


class CloudPickleSerializer(Serializer):
    def serialize(self, obj: object) -> bytes:
        return cloudpickle.dumps(obj)

    def deserialize(self, serialized_obj: bytes) -> object:
        if not isinstance(serialized_obj, bytes):
            raise InvalidTypeForDeserializationException(
                'Please ensure the serialized object is of type bytes.')
        return cloudpickle.loads(serialized_obj)
