import unittest

from baseplate.lib.experiments.targeting.tree_targeting import create_targeting_tree
from baseplate.lib.experiments.targeting.tree_targeting import TargetingNodeError
from baseplate.lib.experiments.targeting.tree_targeting import UnknownTargetingOperatorError


def get_simple_config():
    targeting_cfg = {
        "ALL": [
            {
                "ANY": [
                    {"EQ": {"field": "is_mod", "value": True}},
                    {"EQ": {"field": "user_id", "values": ["t2_1", "t2_2", "t2_3", "t2_4"]}},
                ]
            },
            {"NOT": {"EQ": {"field": "is_pita", "value": True}}},
            {"EQ": {"field": "is_logged_in", "values": [True, False]}},
            {"NOT": {"EQ": {"field": "subreddit_id", "values": ["t5_1", "t5_2"]}}},
            {
                "ALL": [
                    {"EQ": {"field": "random_numeric", "values": [1, 2, 3, 4, 5]}},
                    {"EQ": {"field": "random_numeric", "value": 5}},
                ]
            },
        ]
    }

    return targeting_cfg


def get_input_set():
    inputs = {}
    inputs["bool_field"] = True
    inputs["str_field"] = "string_value"
    inputs["num_field"] = 5
    inputs["explicit_none_field"] = None

    return inputs


class TestTreeTargeting(unittest.TestCase):
    def test_nominal(self):
        targeting_tree = create_targeting_tree(get_simple_config())

        inputs = {}
        inputs["user_id"] = "t2_1"
        inputs["is_mod"] = False
        inputs["is_pita"] = False
        inputs["random_numeric"] = 5

        self.assertFalse(targeting_tree.evaluate(**inputs))

        inputs["is_logged_in"] = True
        self.assertTrue(targeting_tree.evaluate(**inputs))

    def test_create_tree_multiple_keys(self):
        config = get_simple_config()
        config["ANY"] = [
            {"EQ": {"field": "is_mod", "value": True}},
            {"EQ": {"field": "user_id", "values": ["t2_1", "t2_2", "t2_3", "t2_4"]}},
        ]

        with self.assertRaises(TargetingNodeError):
            create_targeting_tree(config)

    def test_create_tree_unknown_operator(self):
        config = get_simple_config()
        config["UNKNOWN"] = config.pop("ALL")

        with self.assertRaises(UnknownTargetingOperatorError):
            create_targeting_tree(config)


class TestEqualNode(unittest.TestCase):
    def test_equal_single_value_node_bool(self):

        inputs = get_input_set()

        # test bool field
        targeting_config_bool = {"EQ": {"field": "bool_field", "value": True}}
        targeting_tree_bool = create_targeting_tree(targeting_config_bool)
        self.assertTrue(targeting_tree_bool.evaluate(**inputs))

    def test_equal_single_value_node_num(self):

        inputs = get_input_set()

        # test numeric field
        targeting_config_num = {"EQ": {"field": "num_field", "value": 5}}
        targeting_tree_num = create_targeting_tree(targeting_config_num)
        self.assertTrue(targeting_tree_num.evaluate(**inputs))

    def test_equal_single_value_node_string(self):

        inputs = get_input_set()
        # test string field
        targeting_config_str = {"EQ": {"field": "str_field", "value": "string_value"}}
        targeting_tree_str = create_targeting_tree(targeting_config_str)
        self.assertTrue(targeting_tree_str.evaluate(**inputs))

    def test_equal_single_value_node_none(self):

        inputs = get_input_set()

        # test explicit none in inputs
        targeting_config_none = {"EQ": {"field": "explicit_none_field", "value": None}}
        targeting_tree_none = create_targeting_tree(targeting_config_none)
        self.assertTrue(targeting_tree_none.evaluate(**inputs))

        # test field missing in inputs (implicit none)
        targeting_config_none = {"EQ": {"field": "implicit_none_field", "value": None}}
        targeting_tree_none = create_targeting_tree(targeting_config_none)
        self.assertTrue(targeting_tree_none.evaluate(**inputs))

    def test_equal_list_value_node_bool(self):
        inputs = get_input_set()

        # test bool field
        targeting_config_bool = {"EQ": {"field": "bool_field", "values": [True, False]}}
        targeting_tree_bool = create_targeting_tree(targeting_config_bool)
        self.assertTrue(targeting_tree_bool.evaluate(**inputs))

    def test_equal_list_value_node_number(self):

        inputs = get_input_set()

        targeting_config_num = {"EQ": {"field": "num_field", "values": [5, 6, 7, 8, 9]}}
        targeting_tree_num = create_targeting_tree(targeting_config_num)
        self.assertTrue(targeting_tree_num.evaluate(**inputs))

    def test_equal_list_value_node_string(self):

        inputs = get_input_set()

        targeting_config_str = {
            "EQ": {
                "field": "str_field",
                "values": ["string_value", "string_value_2", "string_value_3"],
            }
        }
        targeting_tree_str = create_targeting_tree(targeting_config_str)
        self.assertTrue(targeting_tree_str.evaluate(**inputs))

    def test_equal_list_value_node_none(self):

        inputs = get_input_set()

        # test explicit none in inputs
        targeting_config_none = {"EQ": {"field": "explicit_none_field", "values": [None, True]}}
        targeting_tree_none = create_targeting_tree(targeting_config_none)
        self.assertTrue(targeting_tree_none.evaluate(**inputs))

        # test field missing in inputs (implicit none)
        targeting_config_none = {"EQ": {"field": "implicit_none_field", "values": [None]}}
        targeting_tree_none = create_targeting_tree(targeting_config_none)
        self.assertTrue(targeting_tree_none.evaluate(**inputs))

    def test_equal_node_bad_inputs(self):
        targeting_config_empty = {"EQ": {}}
        with self.assertRaises(TargetingNodeError):
            create_targeting_tree(targeting_config_empty)

        targeting_config_one_arg = {"EQ": {"field": "some_field"}}
        with self.assertRaises(TargetingNodeError):
            create_targeting_tree(targeting_config_one_arg)

        targeting_config_three_args = {
            "EQ": {"field": "some_field", "values": ["one", True], "value": "str_arg"}
        }
        with self.assertRaises(TargetingNodeError):
            create_targeting_tree(targeting_config_three_args)

        targeting_config_no_field = {"EQ": {"fields": "some_field", "value": "str_arg"}}
        with self.assertRaises(TargetingNodeError):
            create_targeting_tree(targeting_config_no_field)

        targeting_config_no_value = {"EQ": {"field": "some_field", "valu": "str_arg"}}
        with self.assertRaises(TargetingNodeError):
            create_targeting_tree(targeting_config_no_value)


class TestNotNode(unittest.TestCase):
    def test_not_node(self):

        inputs = {}
        inputs["str_field"] = "string_value"

        targeting_config = {"NOT": {"EQ": {"field": "str_field", "value": "string_value"}}}
        targeting_tree_str = create_targeting_tree(targeting_config)
        self.assertFalse(targeting_tree_str.evaluate(**inputs))

        inputs["str_field"] = "str_value"
        self.assertTrue(targeting_tree_str.evaluate(**inputs))

    def test_not_node_bad_inputs(self):
        targeting_config_empty = {"NOT": {}}
        with self.assertRaises(TargetingNodeError):
            create_targeting_tree(targeting_config_empty)

        targeting_config_multiple_args = {
            "NOT": {
                "EQ": {"field": "is_mod", "value": True},
                "ALL": {"field": "user_id", "values": ["t2_1", "t2_2", "t2_3", "t2_4"]},
            }
        }
        with self.assertRaises(TargetingNodeError):
            create_targeting_tree(targeting_config_multiple_args)


class TestOverrideNode(unittest.TestCase):
    def test_nominal(self):
        inputs = get_input_set()

        targeting_config_true = {"OVERRIDE": True}
        targeting_tree_true = create_targeting_tree(targeting_config_true)
        self.assertTrue(targeting_tree_true.evaluate(**inputs))

        targeting_config_false = {"OVERRIDE": False}
        targeting_tree_false = create_targeting_tree(targeting_config_false)
        self.assertFalse(targeting_tree_false.evaluate(**inputs))

    def test_bad_inputs(self):
        inputs = get_input_set()

        targeting_config_str = {"OVERRIDE": "string"}
        targeting_tree_str = create_targeting_tree(targeting_config_str)
        self.assertFalse(targeting_tree_str.evaluate(**inputs))

        targeting_config_struct = {"OVERRIDE": {"key": "value"}}
        targeting_tree_struct = create_targeting_tree(targeting_config_struct)
        self.assertFalse(targeting_tree_struct.evaluate(**inputs))


class TestAnyNode(unittest.TestCase):
    def test_any_node_one_match(self):
        inputs = get_input_set()

        targeting_config = {
            "ANY": [
                {"EQ": {"field": "num_field", "value": 5}},
                {"EQ": {"field": "str_field", "value": "str_value_1"}},
                {"EQ": {"field": "bool_field", "value": False}},
            ]
        }
        targeting_tree = create_targeting_tree(targeting_config)
        self.assertTrue(targeting_tree.evaluate(**inputs))

    def test_any_node_no_match(self):
        inputs = get_input_set()

        targeting_config = {
            "ANY": [
                {"EQ": {"field": "num_field", "value": 6}},
                {"EQ": {"field": "str_field", "value": "str_value_1"}},
                {"EQ": {"field": "bool_field", "value": False}},
            ]
        }
        targeting_tree = create_targeting_tree(targeting_config)
        self.assertFalse(targeting_tree.evaluate(**inputs))

    def test_any_node_empty_list(self):
        inputs = get_input_set()

        targeting_config_empty_list = {"ANY": []}
        targeting_tree_empty_list = create_targeting_tree(targeting_config_empty_list)

        self.assertFalse(targeting_tree_empty_list.evaluate(**inputs))

    def test_any_node_invalid_inputs(self):
        targeting_config_not_list = {"ANY": {"field": "fieldname", "value": "notalist"}}
        with self.assertRaises(TargetingNodeError):
            create_targeting_tree(targeting_config_not_list)


class TestAllNode(unittest.TestCase):
    def test_all_node_no_match(self):
        inputs = get_input_set()

        targeting_config = {
            "ALL": [
                {"EQ": {"field": "num_field", "value": 6}},
                {"EQ": {"field": "str_field", "value": "str_value_1"}},
                {"EQ": {"field": "bool_field", "value": False}},
            ]
        }

        targeting_tree = create_targeting_tree(targeting_config)
        self.assertFalse(targeting_tree.evaluate(**inputs))

    def test_all_node_some_match(self):
        inputs = get_input_set()

        targeting_config = {
            "ALL": [
                {"EQ": {"field": "num_field", "value": 5}},
                {"EQ": {"field": "str_field", "value": "str_value_1"}},
                {"EQ": {"field": "bool_field", "value": False}},
            ]
        }

        targeting_tree = create_targeting_tree(targeting_config)
        self.assertFalse(targeting_tree.evaluate(**inputs))

    def test_all_node_all_match(self):
        inputs = get_input_set()

        targeting_config = {
            "ALL": [
                {"EQ": {"field": "num_field", "value": 5}},
                {"EQ": {"field": "str_field", "value": "string_value"}},
                {"EQ": {"field": "bool_field", "value": True}},
            ]
        }

        targeting_tree = create_targeting_tree(targeting_config)
        self.assertTrue(targeting_tree.evaluate(**inputs))

    def test_any_node_empty_list(self):
        inputs = get_input_set()

        targeting_config_empty_list = {"ANY": []}
        targeting_tree_empty_list = create_targeting_tree(targeting_config_empty_list)

        self.assertFalse(targeting_tree_empty_list.evaluate(**inputs))

    def test_all_node_invalid_inputs(self):
        targeting_config_not_list = {"ALL": {"field": "fieldname", "value": "notalist"}}
        with self.assertRaises(TargetingNodeError):
            create_targeting_tree(targeting_config_not_list)


class TestComparisonNode(unittest.TestCase):
    def test_gt_node(self):

        inputs = get_input_set()

        targeting_config_eq = {"GT": {"field": "num_field", "value": 5}}
        targeting_tree_eq = create_targeting_tree(targeting_config_eq)
        self.assertFalse(targeting_tree_eq.evaluate(**inputs))

        targeting_config_lt = {"GT": {"field": "num_field", "value": 4}}
        targeting_tree_lt = create_targeting_tree(targeting_config_lt)
        self.assertTrue(targeting_tree_lt.evaluate(**inputs))

        targeting_config_gt = {"GT": {"field": "num_field", "value": 6}}
        targeting_tree_gt = create_targeting_tree(targeting_config_gt)
        self.assertFalse(targeting_tree_gt.evaluate(**inputs))

    def test_lt_node(self):

        inputs = get_input_set()

        targeting_config_eq = {"LT": {"field": "num_field", "value": 5}}
        targeting_tree_eq = create_targeting_tree(targeting_config_eq)
        self.assertFalse(targeting_tree_eq.evaluate(**inputs))

        targeting_config_lt = {"LT": {"field": "num_field", "value": 4}}
        targeting_tree_lt = create_targeting_tree(targeting_config_lt)
        self.assertFalse(targeting_tree_lt.evaluate(**inputs))

        targeting_config_gt = {"LT": {"field": "num_field", "value": 6}}
        targeting_tree_gt = create_targeting_tree(targeting_config_gt)
        self.assertTrue(targeting_tree_gt.evaluate(**inputs))

    def test_ge_node(self):

        inputs = get_input_set()

        targeting_config_eq = {"GE": {"field": "num_field", "value": 5}}
        targeting_tree_eq = create_targeting_tree(targeting_config_eq)
        self.assertTrue(targeting_tree_eq.evaluate(**inputs))

        targeting_config_lt = {"GE": {"field": "num_field", "value": 4}}
        targeting_tree_lt = create_targeting_tree(targeting_config_lt)
        self.assertTrue(targeting_tree_lt.evaluate(**inputs))

        targeting_config_gt = {"GE": {"field": "num_field", "value": 6}}
        targeting_tree_gt = create_targeting_tree(targeting_config_gt)
        self.assertFalse(targeting_tree_gt.evaluate(**inputs))

    def test_le_node(self):

        inputs = get_input_set()

        targeting_config_eq = {"LE": {"field": "num_field", "value": 5}}
        targeting_tree_eq = create_targeting_tree(targeting_config_eq)
        self.assertTrue(targeting_tree_eq.evaluate(**inputs))

        targeting_config_lt = {"LE": {"field": "num_field", "value": 4}}
        targeting_tree_lt = create_targeting_tree(targeting_config_lt)
        self.assertFalse(targeting_tree_lt.evaluate(**inputs))

        targeting_config_gt = {"LE": {"field": "num_field", "value": 6}}
        targeting_tree_gt = create_targeting_tree(targeting_config_gt)
        self.assertTrue(targeting_tree_gt.evaluate(**inputs))

    def test_ne_node(self):

        inputs = get_input_set()

        targeting_config_eq = {"NE": {"field": "num_field", "value": 5}}
        targeting_tree_eq = create_targeting_tree(targeting_config_eq)
        self.assertFalse(targeting_tree_eq.evaluate(**inputs))

        targeting_config_lt = {"NE": {"field": "num_field", "value": 4}}
        targeting_tree_lt = create_targeting_tree(targeting_config_lt)
        self.assertTrue(targeting_tree_lt.evaluate(**inputs))

        targeting_config_gt = {"NE": {"field": "num_field", "value": 6}}
        targeting_tree_gt = create_targeting_tree(targeting_config_gt)
        self.assertTrue(targeting_tree_gt.evaluate(**inputs))

    def test_comparison_le_none(self):

        inputs = get_input_set()

        # test explicit none in inputs
        targeting_config_none = {"LE": {"field": "explicit_none_field", "value": None}}
        targeting_tree_none = create_targeting_tree(targeting_config_none)
        self.assertFalse(targeting_tree_none.evaluate(**inputs))

        # test field missing in inputs (implicit none)
        targeting_config_none = {"LE": {"field": "implicit_none_field", "value": None}}
        targeting_tree_none = create_targeting_tree(targeting_config_none)
        self.assertFalse(targeting_tree_none.evaluate(**inputs))

    def test_comparison_ge_none(self):

        inputs = get_input_set()

        # test explicit none in inputs
        targeting_config_none = {"GE": {"field": "explicit_none_field", "value": None}}
        targeting_tree_none = create_targeting_tree(targeting_config_none)
        self.assertFalse(targeting_tree_none.evaluate(**inputs))

        # test field missing in inputs (implicit none)
        targeting_config_none = {"GE": {"field": "implicit_none_field", "value": None}}
        targeting_tree_none = create_targeting_tree(targeting_config_none)
        self.assertFalse(targeting_tree_none.evaluate(**inputs))

    def test_comparison_lt_none(self):

        inputs = get_input_set()

        # test explicit none in inputs
        targeting_config_none = {"LT": {"field": "explicit_none_field", "value": None}}
        targeting_tree_none = create_targeting_tree(targeting_config_none)
        self.assertFalse(targeting_tree_none.evaluate(**inputs))

        # test field missing in inputs (implicit none)
        targeting_config_none = {"LT": {"field": "implicit_none_field", "value": None}}
        targeting_tree_none = create_targeting_tree(targeting_config_none)
        self.assertFalse(targeting_tree_none.evaluate(**inputs))

    def test_comparison_gt_none(self):

        inputs = get_input_set()

        # test explicit none in inputs
        targeting_config_none = {"GT": {"field": "explicit_none_field", "value": None}}
        targeting_tree_none = create_targeting_tree(targeting_config_none)
        self.assertFalse(targeting_tree_none.evaluate(**inputs))

        # test field missing in inputs (implicit none)
        targeting_config_none = {"GT": {"field": "implicit_none_field", "value": None}}
        targeting_tree_none = create_targeting_tree(targeting_config_none)
        self.assertFalse(targeting_tree_none.evaluate(**inputs))

    def test_comparison_ne_none(self):

        inputs = get_input_set()

        # test explicit none in inputs
        targeting_config_none = {"NE": {"field": "explicit_none_field", "value": None}}
        targeting_tree_none = create_targeting_tree(targeting_config_none)
        self.assertFalse(targeting_tree_none.evaluate(**inputs))

        # test field missing in inputs (implicit none)
        targeting_config_none = {"NE": {"field": "implicit_none_field", "value": None}}
        targeting_tree_none = create_targeting_tree(targeting_config_none)
        self.assertFalse(targeting_tree_none.evaluate(**inputs))

    def test_comparison_node_bad_inputs(self):
        targeting_config_empty = {"LE": {}}
        with self.assertRaises(TargetingNodeError):
            create_targeting_tree(targeting_config_empty)

        targeting_config_one_arg = {"LE": {"field": "some_field"}}
        with self.assertRaises(TargetingNodeError):
            create_targeting_tree(targeting_config_one_arg)

        targeting_config_three_args = {
            "LE": {"field": "some_field", "values": ["one", True], "value": "str_arg"}
        }
        with self.assertRaises(TargetingNodeError):
            create_targeting_tree(targeting_config_three_args)

        targeting_config_no_field = {"LE": {"fields": "some_field", "value": "str_arg"}}
        with self.assertRaises(TargetingNodeError):
            create_targeting_tree(targeting_config_no_field)

        targeting_config_no_value = {"LE": {"field": "some_field", "valu": "str_arg"}}
        with self.assertRaises(TargetingNodeError):
            create_targeting_tree(targeting_config_no_value)
