import logging
import operator

from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Type

from baseplate.lib.experiments.targeting.base import Targeting


logger = logging.getLogger(__name__)


class TargetingNodeError(Exception):
    pass


class UnknownTargetingOperatorError(Exception):
    pass


class EqualNode(Targeting):
    """Used to determine whether an attribute equals a single value or a value in a list.

    :param input_node: dict with the field name and value or list
    of values to accept for targeting. This dict will contain two keys:
    "field", and one of "value" or "values". If "value" is provided,
    a single value is expected. If "values" is instead provided, then
    a list of values is expected.

    A full EqualNode in a targeting tree configuration looks like this::

        {
            EQ:{
                field: <field_name>
                value: <accepted_value>
            }
        }

    The expected input to this constructor from the above example would be::

        {
            field: <field_name>,
            value: <accepted_value>
        }

    """

    def __init__(self, input_node: Dict[str, Any]):
        if len(input_node) != 2:
            raise ValueError("EqualNode expects exactly two fields.")

        if "field" not in input_node:
            raise ValueError("EqualNode expects input key 'field'.")

        if "value" not in input_node and "values" not in input_node:
            raise ValueError("EqualNode expects input key 'value' or 'values'.")

        self._accepted_key = input_node["field"].lower()
        self._accepted_values = input_node.get("values") or [input_node["value"]]

    def evaluate(self, **kwargs: Any) -> bool:
        candidate_value = kwargs.get(self._accepted_key)
        try:
            if candidate_value in self._accepted_values:
                return True
        except TypeError as err:
            logger.warning(err)

        return False


class AllNode(Targeting):
    """All child nodes return True.

    :param input_node: a list of Targeting nodes

    """

    def __init__(self, input_node: List[Dict[str, Any]]):
        if not isinstance(input_node, list):
            raise TypeError("Input to AllNode expects a list.")

        self._children: List[Targeting] = []

        for node in input_node:
            self._children.append(create_targeting_tree(node))

    def evaluate(self, **kwargs: Any) -> bool:
        return all(node.evaluate(**kwargs) for node in self._children)


class AnyNode(Targeting):
    """At least one child node return True.

    :param input_node: a list of Targeting nodes

    """

    def __init__(self, input_node: List[Dict[str, Any]]):
        if not isinstance(input_node, list):
            raise TypeError("Input to AnyNode expects a list.")

        self._children: List[Targeting] = []

        for node in input_node:
            self._children.append(create_targeting_tree(node))

    def evaluate(self, **kwargs: Any) -> bool:
        return any(node.evaluate(**kwargs) for node in self._children)


class NotNode(Targeting):
    """Boolean 'not' operator.

    :param input_node: a Targeting node

    """

    def __init__(self, input_node: Dict[str, Any]):
        if len(input_node) != 1:
            raise ValueError("NotNode expects exactly one field.")

        if not isinstance(input_node, dict):
            raise TypeError("Input to NotNode expects a dictionary")

        self._child = create_targeting_tree(input_node)

    def evaluate(self, **kwargs: Any) -> bool:
        return not self._child.evaluate(**kwargs)


class OverrideNode(Targeting):
    """Always return True/False."""

    def __init__(self, input_node: bool):
        if input_node is True:
            self._return_value = True
        else:
            self._return_value = False

    def evaluate(self, **kwargs: Any) -> bool:
        return self._return_value


class ComparisonNode(Targeting):
    """Non-equality comparison operators (gt, ge, lt, le).

    Expects as input the input node as well as an operator (from the
    operator module). Operator must be one that expects two inputs (
    ie: gt, ge, lt, le, eq, ne)

    :param input_node: a Targeting node
    :param operator: an operator to apply to the input node
    """

    def __init__(self, input_node: Dict[str, Any], comparator: Callable[[Any, Any], bool]):
        if len(input_node) != 2:
            raise ValueError("ComparisonNode expects exactly two fields.")

        if "field" not in input_node:
            raise ValueError("ComparisonNode expects input key 'field'.")

        if "value" not in input_node:
            raise ValueError("ComparisonNode expects input key 'value'.")

        self._accepted_key = input_node["field"].lower()
        self._accepted_value = input_node["value"]
        self.comparator = comparator

    def evaluate(self, **kwargs: Any) -> bool:
        candidate_value = kwargs.get(self._accepted_key)

        # Python 3 throws a TypeError when comparing two Nones. Handle
        # this here by returning false for consistency between python 2 and 3
        if candidate_value is None and self._accepted_value is None:
            return False
        return self.comparator(candidate_value, self._accepted_value)


OPERATOR_NODE_TYPE_MAPPING: Dict[str, Type[Targeting]] = {
    "any": AnyNode,
    "all": AllNode,
    "eq": EqualNode,
    "not": NotNode,
    "override": OverrideNode,
    "gt": ComparisonNode,
    "ge": ComparisonNode,
    "lt": ComparisonNode,
    "le": ComparisonNode,
    "ne": ComparisonNode,
}


def create_targeting_tree(input_node: Dict[str, Any]) -> Targeting:
    """Create a tree-based targeting evaluator.

    Processes input json to create a tree against which a set of inputs
    can be evaluated to determine whether or not a user with the input
    input attributes should be targeted for an experiment.

    Each node is represented by a dict with one key which represents the
    operator. The value for the operator is either a dictionary (another
    node) or a list of dictionaries (each of which is another node).

    An example of a targeting tree config might look like:

    targeting_cfg = {
        'ALL':[
            {'ANY':[
                {'EQ': {'field': 'is_mod', 'value': True}},
                {'EQ': {'field': 'user_id', 'values':['t2_1','t2_2','t2_3','t2_4']}},
            ]},
            {'NOT': {
                'EQ': {'field': 'has_commented', 'value': True}}},
            {'EQ': {'field': 'is_logged_in', 'values': [True, False]}},
            {'NOT': {
                'EQ': {'field': 'subreddit_id', 'values': ['t5_1','t5_2']}}},
            {'ALL':[
                {'EQ':{'field':'votes_cast', 'values':[1,2,3,4,5]}},
                {'EQ':{'field':'votes_cast', 'value':5}}
            ]}
        ]
    }

    In the above example, we see that all of the following conditions must be
    met in order for a user to be targeted:

    1. a user must be a mod or have the user id between 1 and 4
    2. the must not have commented
    3. they can be logged in or logged out
    4. they must have cast 5 votes

    """
    if not isinstance(input_node, dict) or not len(input_node) == 1:
        raise TargetingNodeError("Call to create_targeting_tree expects a single input key.")

    operator_name, input_node_value = list(input_node.items())[0]

    operator_name = operator_name.lower()

    if operator_name in OPERATOR_NODE_TYPE_MAPPING:
        operator_node_type = OPERATOR_NODE_TYPE_MAPPING[operator_name]
        try:
            subnode: Targeting
            if issubclass(operator_node_type, ComparisonNode):
                subnode = operator_node_type(input_node_value, getattr(operator, operator_name))
            else:
                subnode = operator_node_type(input_node_value)  # type: ignore
            return subnode
        except (TypeError, ValueError) as e:
            raise TargetingNodeError(
                "Error while constructing targeting tree: {}".format(getattr(e, "message", None))
            )
    else:
        raise UnknownTargetingOperatorError(
            f"Unrecognized operator while constructing targeting tree: {operator_name}"
        )
