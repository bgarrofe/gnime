from pipefunc import Pipeline, PipeFunc
import sys

from .core import Node


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

    def _renames_for_node(self, node: Node):

        if node._node_id == 2:
            return {'input': 'node_1_output'}
        else:
            return {}

    def _create_pipe_function(self, node: Node):

        return PipeFunc(
            node.execute,
            output_name=node.output_name,
            renames=self._renames_for_node(node)
        )

    def _create_pipe_functions(self):
        return [
            self._create_pipe_function(node) for node in self._nodes
        ]

    def run(self, *args, **kwargs):

        functions = self._create_pipe_functions()

        self.pipeline = Pipeline(functions)
        return self.pipeline(*args, **kwargs)
