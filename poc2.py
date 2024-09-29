from abc import ABC, abstractmethod
from pipefunc import Pipeline, PipeFunc
from pydantic import BaseModel
import pandas as pd
import sys


class Node(ABC):

    @abstractmethod
    def configure(self, context):
        pass

    @abstractmethod
    def execute(self):
        pass


class CSVReaderContext(BaseModel):

    file_path: str
    delimiter: str = ','
    header: int = 0
    columns: list = None


class CSVWriterContext(BaseModel):

    file_path: str
    delimiter: str = ','
    header: bool = True
    index: bool = False


class CSVReaderConfig:

    def __init__(self, context):
        self._settings = self._validate_context(context)

    def _validate_context(self, context):
        return CSVReaderContext(**context)

    @property
    def settings(self) -> CSVReaderContext:
        return self._settings


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

    @property
    def output_name(self):
        return 'output'


class CSVWriterConfig:

    def __init__(self, context):
        self._settings = self._validate_context(context)

    def _validate_context(self, context):
        return CSVWriterContext(**context)

    @property
    def settings(self) -> CSVWriterContext:
        return self._settings


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

    @property
    def output_name(self):
        return 'output'


class Runner:

    _nodes = []

    def __init__(self):
        self.pipeline = None

    def add(self, node: Node, context):
        try:
            node.configure(context)
        except AttributeError:
            print('failed to configure node')
            sys.exit(1)

        self._nodes.append(node)

    def run(self, *args, **kwargs):
        self.pipeline = Pipeline(
            [
                PipeFunc(
                    node.execute,
                    output_name=node.output_name
                ) for node in self._nodes
            ],
        )
        return self.pipeline(*args, **kwargs)


if __name__ == '__main__':
    runner = Runner()
    runner.add(CSVReaderNode(), {'file_path': 'data.csv'})
    runner.add(CSVWriterNode(), {'file_path': 'data2.csv'})
    print(runner.run('output2', input1=None))
