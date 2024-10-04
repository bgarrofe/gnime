from typing import List
import sys

from .core import Node, NodeConnection


class Workflow:

    _nodes: List[Node] = []

    _connections = List[NodeConnection] = []

    def add(self, node: Node, context):
        try:
            node.configure(context)
        except AttributeError:
            print('failed to configure node')
            sys.exit(1)

        self._nodes.append(node)
