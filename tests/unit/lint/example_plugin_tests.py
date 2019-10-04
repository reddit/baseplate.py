# Libraries needed for tests
import astroid
import pylint.testutils

from baseplate.lint import example_plugin


class TestNoReassignmentChecker(pylint.testutils.CheckerTestCase):
    CHECKER_CLASS = example_plugin.NoReassigmentChecker

    def test_finds_reassigned_variable(self):
        assign_node_a, assign_node_b = astroid.extract_node(
            """
        test = 1 #@
        test = 2 #@
            """
        )

        self.checker.visit_assign(assign_node_a)
        self.checker.visit_assign(assign_node_b)
        self.assertAddsMessages(
            pylint.testutils.Message(msg_id="reassigned-variable", node=assign_node_a)
        )

    def test_ignores_no_reassigned_variable(self):
        assign_node_a, assign_node_b = astroid.extract_node(
            """
        test1 = 1 #@
        test2 = 2 #@
            """
        )

        with self.assertNoMessages():
            self.checker.visit_assign(assign_node_a)
            self.checker.visit_assign(assign_node_b)

    def test_ignores_variable_outside_function(self):
        func_node, assign_node_a, assign_node_b = astroid.extract_node(
            """
        def test1(): #@
            test = 1 #@

        def test2():
            test = 2 #@
            """
        )

        with self.assertNoMessages():
            self.checker.visit_assign(assign_node_a)
            self.checker.leave_functiondef(func_node)
            self.checker.visit_assign(assign_node_b)