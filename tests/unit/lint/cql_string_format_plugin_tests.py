import astroid
import pylint.testutils

from baseplate.lint import cql_string_format_plugin


class TestNoCQLStringFormatChecker(pylint.testutils.CheckerTestCase):
    CHECKER_CLASS = cql_string_format_plugin.NoCQLStringFormatChecker

    def test_finds_variable_binop_string_format_query(self):
        func_node, assign_node_a, call_node_b = astroid.extract_node(
            """
        def test(cassandra_session): #@
            query = "select * from %s" % "users" #@
            cassandra_session.execute(query) #@
        """
        )

        self.checker.visit_assign(assign_node_a)
        self.checker.visit_call(call_node_b)
        self.assertAddsMessages(
            pylint.testutils.Message(msg_id="cql-string-format", node=call_node_b)
        )

    def test_finds_variable_call_string_format_query(self):
        func_node, assign_node_a, call_node_b = astroid.extract_node(
            """
        def test(cassandra_session): #@
            query = "select * from %s".format("users") #@
            cassandra_session.execute(query) #@
        """
        )

        self.checker.visit_assign(assign_node_a)
        self.checker.visit_call(call_node_b)
        self.assertAddsMessages(
            pylint.testutils.Message(msg_id="cql-string-format", node=call_node_b)
        )

    def test_finds_binop_string_format_query(self):
        func_node, call_node_a = astroid.extract_node(
            """
        def test(cassandra_session): #@
            cassandra_session.execute("select * from %s" % "users") #@
        """
        )

        self.checker.visit_call(call_node_a)
        self.assertAddsMessages(
            pylint.testutils.Message(msg_id="cql-string-format", node=call_node_a)
        )

    def test_finds_call_string_format_query(self):
        func_node, call_node_a = astroid.extract_node(
            """
        def test(cassandra_session): #@
            cassandra_session.execute("select * from %s".format("users")) #@
        """
        )

        self.checker.visit_call(call_node_a)
        self.assertAddsMessages(
            pylint.testutils.Message(msg_id="cql-string-format", node=call_node_a)
        )

    def test_ignores_variable_non_string_format_query(self):
        func_node, assign_node_a, call_node_b = astroid.extract_node(
            """
        def test(cassandra_session): #@
            query = "select * from users" #@
            cassandra_session.execute(query) #@
        """
        )

        with self.assertNoMessages():
            self.checker.visit_assign(assign_node_a)
            self.checker.visit_call(call_node_b)

    def test_ignores_non_string_format_query(self):
        func_node, call_node_a = astroid.extract_node(
            """
        def test(cassandra_session): #@
            cassandra_session.execute("select * from users") #@
        """
        )

        with self.assertNoMessages():
            self.checker.visit_call(call_node_a)

    def test_ignores_no_query(self):
        func_node, call_node_a = astroid.extract_node(
            """
        def test(kb): #@
            kb.execute() #@
        """
        )

        with self.assertNoMessages():
            self.checker.visit_call(call_node_a)

    def test_variable_reset(self):
        func_node, assign_node_a, assign_node_b, call_node_c = astroid.extract_node(
            """
        def test(cassandra_session): #@
            query = "select * from %s" % "users" #@
            cassandra_session.execute(query)

        def other_test(cassandra_session):
            query = "select * from users" #@
            cassandra_session.execute(query) #@
        """
        )

        with self.assertNoMessages():
            self.checker.visit_assign(assign_node_a)
            self.checker.leave_functiondef(func_node)
            self.checker.visit_assign(assign_node_b)
            self.checker.visit_call(call_node_c)
