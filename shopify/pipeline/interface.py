from typing import Callable, Any
from dataclasses import dataclass, field

@dataclass
class Resource:
    endpoint: str
    data_key: str
    fields: list[str]
    params: dict[str, Any] = field(default_factory=dict)

@dataclass
class Pipeline:
    table: str
    resource: Resource
    transform: Callable[[list[dict[str, Any]]], list[dict[str, Any]]]
    schema: list[dict[str, Any]]
    id_key: str = "id"
    cursor_key: str = "updated_at"
