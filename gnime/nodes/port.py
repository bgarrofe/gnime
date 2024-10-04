from dataclasses import dataclass
from typing import Optional


@dataclass
class PortType:
    """
    Represents a type of port.
    """

    id: str
    name: str


PortType.TABLE = "PortType.TABLE"


@dataclass
class Port:
    """
    Represents a port on a node.
    """

    type: PortType
    name: str
    description: Optional[str] = None
    id: Optional[str] = (
        None  # can be used by BINARY and CONNECTION ports to only allow linking ports with matching IDs
    )
    optional: Optional[bool] = False
