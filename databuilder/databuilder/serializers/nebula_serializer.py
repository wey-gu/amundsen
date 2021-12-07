# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

from typing import (
    Any, Dict, Optional,
)

from databuilder.models.graph_node import GraphNode
from databuilder.models.graph_relationship import GraphRelationship
from databuilder.models.graph_serializable import (
    NODE_KEY, NODE_LABEL, RELATION_END_KEY, RELATION_END_LABEL,
    RELATION_REVERSE_TYPE, RELATION_START_KEY,
    RELATION_START_LABEL, RELATION_TYPE,
)


def serialize_node(node: Optional[GraphNode]) -> Dict[str, Any]:
    if node is None:
        return {}

    node_dict = {
        NODE_LABEL: node.label,
        NODE_KEY: node.key
    }
    for key, value in node.attributes.items():
        property_type = _get_property_type(value)
        formatted_key = f'{key}:{property_type}'
        node_dict[formatted_key] = value
    return node_dict


def serialize_relationship(
        relationship: Optional[GraphRelationship]) -> Dict[str, Any]:
    if relationship is None:
        return {}

    relationship_dict = {
        RELATION_START_KEY: relationship.start_key,
        RELATION_START_LABEL: relationship.start_label,
        RELATION_END_KEY: relationship.end_key,
        RELATION_END_LABEL: relationship.end_label,
        RELATION_TYPE: relationship.type,
        RELATION_REVERSE_TYPE: relationship.reverse_type,
    }
    for key, value in relationship.attributes.items():
        property_type = _get_property_type(value)
        formatted_key = f'{key}:{property_type}'
        relationship_dict[formatted_key] = value

    return relationship_dict


def _get_property_type(value: Any) -> str:
    if isinstance(value, str):
        return "string"
    elif isinstance(value, bool):
        return "bool"
    elif isinstance(value, int):
        return "int64"
    elif isinstance(value, float):
        return "float"

    return None
