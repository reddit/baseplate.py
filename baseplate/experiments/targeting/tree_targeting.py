from .base import Targeting


class TargetingNodeError(Exception):
    pass


class UnknownTargetingOperatorError(Exception):
    pass


class EqualNode(Targeting):
    """Used to determine whether an attribute equals a single value or a value
    in a list
    """
    def __init__(self, input_node):
        """Build a node to evaluate equality (single value or list).

        :param dict input_node: dict with the field name and value or list
        of values to accept for targeting. This dict will contain two keys:
        "field", and one of "value" or "values". If "value" is provided,
        a single value is expected. If "values" is instead provided, then
        a list of values is expected.

        A full EqualNode in a targeting tree configuration looks like this:
        {
            EQ:{
                field: <field_name>
                value: <accepted_value>
            }
        }

        The expected input to this constructor from the above example would be:
        {
            field: <field_name>,
            value: <accepted_value>
        }
        """

        if len(input_node) != 2:
            raise ValueError("EqualNode expects exactly two fields.")

        if not 'field' in input_node:
            raise ValueError("EqualNode expects input key 'field'.")

        if not 'value' in input_node and not 'values' in input_node:
            raise ValueError("EqualNode expects input key 'value' or 'values'.")

        self._accepted_key = input_node.get('field').lower()
        self._accepted_values = input_node.get('values') or [input_node.get('value')]

    def evaluate(self, **kwargs):
        candidate_value = kwargs.get(self._accepted_key)
        if candidate_value in self._accepted_values:
            return True

        return False


class AllNode(Targeting):
    """All child nodes return True"""
    def __init__(self, input_node):
        """Build a node to evaluate multiple children and return True if they
        all evaluate to True (boolean 'and').

        :param list input_node: a list of Targeting nodes
        """
        if not isinstance(input_node, list):
            raise TypeError("Input to AllNode expects a list.")

        self._children = []

        for node in input_node:
            self._children.append(create_targeting_tree(node))

    def evaluate(self, **kwargs):
        return all(node.evaluate(**kwargs) for node in self._children)


class AnyNode(Targeting):
    """At least one child node return True"""

    def __init__(self, input_node):
        """Build a node to evaluate multiple children and return True if at
        least one of them evaluates to True (boolean 'or').

        :param list input_node: a list of Targeting nodes
        """
        if not isinstance(input_node, list):
            raise TypeError("Input to AnyNode expects a list.")

        self._children = []

        for node in input_node:
            self._children.append(create_targeting_tree(node))

    def evaluate(self, **kwargs):
        return any(node.evaluate(**kwargs) for node in self._children)


class NotNode(Targeting):
    """Boolean 'not' operator"""
    def __init__(self, input_node):
        """Build a node that evaluates to true if its sole child node evaluates
        to False (boolean 'not')

        :param dict input_node: a Targeting node
        """
        if len(input_node) != 1:
            raise ValueError("NotNode expects exactly one field.")

        if not isinstance(input_node, dict):
            raise TypeError("Input to NotNode expects a dictionary")

        self._child = create_targeting_tree(input_node)

    def evaluate(self, **kwargs):
        return not self._child.evaluate(**kwargs)


class OverrideNode(Targeting):
    """Always return True/False
    """
    def __init__(self, input_node):
        if input_node is True:
            self._return_value = True
        else:
            self._return_value = False

    def evaluate(self, **kwargs):
        return self._return_value


OPERATOR_NODE_TYPE_MAPPING = {
    "ANY": AnyNode,
    "ALL": AllNode,
    "EQ": EqualNode,
    "NOT": NotNode,
    "OVERRIDE": OverrideNode,
}


def create_targeting_tree(input_node):
    """Creates a tree-based targeting evaluator.

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

    operator, input_node_value = list(input_node.items())[0]

    if operator in OPERATOR_NODE_TYPE_MAPPING:
        try:
            subnode = OPERATOR_NODE_TYPE_MAPPING[operator](input_node_value)
            return subnode
        except (TypeError, ValueError) as e:
            raise TargetingNodeError("Error while constructing targeting "
                "tree: {}".format(getattr(e, 'message', None)))
    else:
        raise UnknownTargetingOperatorError("Unrecognized operator while constructing targeting "
            "tree: {}".format(operator))
