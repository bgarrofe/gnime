import pandas as pd

from gnime.core import Node
from .config import CSVReaderConfig


class CSVReaderNode(Node):

    def configure(self, context):
        self.config: CSVReaderConfig = CSVReaderConfig(context)

    def execute(self, input):

        return pd.read_csv(
            self.config.settings.file_path,
            delimiter=self.config.settings.delimiter,
            header=self.config.settings.header,
            names=self.config.settings.columns
        )
