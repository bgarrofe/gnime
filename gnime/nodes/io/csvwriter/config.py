
from .context import CSVWriterContext


class CSVWriterConfig:

    def __init__(self, context):
        self._settings = self._validate_context(context)

    def _validate_context(self, context):
        return CSVWriterContext(**context)

    @property
    def settings(self) -> CSVWriterContext:
        return self._settings
