import pandas as pd

from gnime.core import Node
from .config import CSVWriterConfig


class CSVWriterNode(Node):

    def configure(self, context):
        self.config: CSVWriterConfig = CSVWriterConfig(context)

    def execute(self, input):
        input.to_csv(
            self.config.settings.file_path,
            sep=self.config.settings.delimiter,
            header=self.config.settings.header,
            index=self.config.settings.index
        )
        return None
