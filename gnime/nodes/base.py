from abc import ABC, abstractmethod


class Node(ABC):

    def __init__(self, node_id: int):
        self._node_id = node_id
        self._node_name = self._generate_node_name()
        self._output_name = self._generate_output_name()

    def _generate_node_name(self):
        return f'node_{str(self._node_id)}'

    def _generate_output_name(self):
        return f'{self._node_name}-output'

    @abstractmethod
    def configure(self, context):
        pass

    @abstractmethod
    def execute(self):
        pass

    @property
    def output_name(self):
        return self._output_name
