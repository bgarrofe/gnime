

class InvalidCacheTypeException(Exception):
    pass


class InvalidKeyTypeException(Exception):
    pass


class InvalidValueTypeException(Exception):
    pass


class InvalidStageTypeException(Exception):
    pass


class DAGVerificationException(Exception):
    pass


class SerializationException(Exception):
    pass


class InvalidTypeForDeserializationException(SerializationException):
    pass
