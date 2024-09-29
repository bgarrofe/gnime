
from .context import CSVReaderContext


class CSVReaderConfig:

    def __init__(self, context):
        self._settings = self._validate_context(context)

    def _validate_context(self, context):
        return CSVReaderContext(**context)

    @property
    def settings(self) -> CSVReaderContext:
        return self._settings
