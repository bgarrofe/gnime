import pandas as pd

from gnime.nodes.node import output_table
from .config import CSVReaderConfig
from gnime.core import Node


@output_table(name="Output Data", description="The data read from the CSV file.")
class CSVReaderNode(Node):

    def configure(self, context):
        self.config: CSVReaderConfig = CSVReaderConfig(context)

    def execute(self):

        return pd.read_csv(
            self.config.settings.file_path,
            delimiter=self.config.settings.delimiter,
            header=self.config.settings.header,
            names=self.config.settings.columns
        )
