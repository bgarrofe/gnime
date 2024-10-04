from abc import ABC, abstractmethod
from typing import List, Optional
from inspect import signature
from enum import Enum

from gnime.nodes.port import Port, PortType


def _get_attr_from_instance(self, attr) -> List[Port]:
    if hasattr(self, attr) and (ps := getattr(self, attr)) is not None:
        return ps

    return None


def _get_ports(self, port_slot) -> List[Port]:
    ps = _get_attr_from_instance(self, port_slot)

    if ps is None:
        return []
    else:
        return ps


def _add_port(node_factory, port_slot: str, port: Port):
    """
    Add a port to a node factory object.

    Parameters
    ----------
    node_factory : object
        The node factory object to add the port to.
    port_slot : str
        The name of the attribute in the node factory object to add the port to.
    port : Union[Port, PortGroup]
        The port to be added to the node factory.

    Returns
    -------
    object
        The updated node factory object.

    Raises
    ------
    ValueError
        If the attribute specified by `port_slot` already exists in the node factory object and is not decorated.
    """
    if not hasattr(node_factory, port_slot) or getattr(node_factory, port_slot) is None:
        setattr(node_factory, port_slot, [])

    port_list = getattr(node_factory, port_slot)

    port_list.insert(0, port)

    return node_factory


def input_table(name: str, description: str = None, optional: Optional[bool] = False):
    """
    Use this decorator to define an input port of type "Table" of a node.

    Parameters
    ----------
        name : str
            The name of the input port.
        description : str
            A description of the input port.
        optional: bool
            Whether the port is optional i.e. can be added by the user
    """
    return lambda node_factory: _add_port(
        node_factory,
        "input_ports",
        Port(PortType.TABLE, name, description, optional=optional),
    )


def output_table(name: str, description: str = None):
    """
    Use this decorator to define an output port of type "Table" of a node.

    Parameters
    ----------
    name : str
        The name of the port.
    description : str
        Description of what the port is used for.
    """
    return lambda node_factory: _add_port(
        node_factory, "output_ports", Port(PortType.TABLE, name, description)
    )


class NodeType(Enum):
    """
    Defines the different node types that are available for Python based nodes.
    """

    SOURCE = "Source"
    """A node producing data."""
    SINK = "Sink"
    """A node consuming data."""
    LEARNER = "Learner"
    """A node learning a model that is typically consumed by a PREDICTOR."""
    PREDICTOR = "Predictor"
    """A node that predicts something typically using a model provided by a LEARNER."""
    MANIPULATOR = "Manipulator"
    """A node that manipulates data."""
    VISUALIZER = "Visualizer"
    """A node that visualizes data."""
    OTHER = "Other"
    """A node that doesn't fit one of the other node types."""


class Connection:
    pass


class Node(ABC):

    input_ports: List[Port]
    output_ports: List[Port]
    connections: List[Connection]

    def __init__(self, name: str, config: dict = None):
        self.name = name
        self.input_ports = _get_ports(self, "input_ports")
        self.output_ports = _get_ports(self, "output_ports")
        if config:
            self.configure(config)

    # @abstractmethod
    # def configure(self, context):
    #     pass

    @abstractmethod
    def execute(self):
        pass

    def get_inputs(self):
        return list(signature(self.execute).parameters)

    def get_outputs(self):
        return [f'{self.name}_output_{i}' for i in range(len(self.output_ports))]

    @property
    def input_name(self):
        if len(self.input_ports) == 0:
            return None
        elif len(self.input_ports) == 1:
            return list(signature(self.execute).parameters)[0]
        return tuple(signature(self.execute).parameters)

    @property
    def output_name(self):
        if len(self.output_ports) == 0:
            return f'{self.name}_output_0'  # return dummy port
        elif len(self.output_ports) == 1:
            return f'{self.name}_output_0'
        else:
            return tuple([f'{self.name}_output_{i}' for i in range(len(self.output_ports))])
