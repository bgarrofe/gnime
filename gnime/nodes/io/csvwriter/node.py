import pandas as pd

from gnime.nodes.node import input_table
from .config import CSVWriterConfig
from gnime.core import Node


@input_table(name="Input Data", description="The data to be written to a CSV file.")
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
