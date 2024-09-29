from abc import ABC, abstractmethod
from pipefunc import Pipeline, PipeFunc


class Node(ABC):

    @abstractmethod
    def execute(self):
        pass


class ProductNode(Node):

    def execute(self, a, b):
        return a * b

    @property
    def output_name(self):
        return 'c'


class SumNode(Node):

    def execute(self, c, b, e):
        return c + b + e

    @property
    def output_name(self):
        return 'd'


class Runner:

    _nodes = []

    def __init__(self):
        self.pipeline = None

    def add(self, node):
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
    runner.add(ProductNode())
    runner.add(SumNode())
    print(runner.run('d', a=2, b=3, e=9))
