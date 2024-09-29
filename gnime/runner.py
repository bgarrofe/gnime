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

    def _generate_pipe_function(self, node: Node, renames: dict = None):

        return PipeFunc(
            node.execute,
            output_name=node.output_name,
            renames=renames
        )

    def _renames_for_node(self, node: Node):

        if node._node_id == 2:
            return {'input': 'node_1-output'}
        else:
            return {}

    def run(self, *args, **kwargs):
        self.pipeline = Pipeline(
            [
                self._generate_pipe_function(node, renames=self._renames_for_node(node)) for node in self._nodes
            ],
        )
        return self.pipeline(*args, **kwargs)
