from astroid import nodes
from pylint.checkers import BaseChecker
from pylint.interfaces import IAstroidChecker
from pylint.lint import PyLinter


class NoDbQueryStringFormatChecker(BaseChecker):
    __implements__ = IAstroidChecker

    name = "no-database-query-string-format"
    priority = -1
    msgs = {
        "W9000": (
            "Python string formatting found in database query. Database queries should use native parameter substitution.",
            "database-query-string-format",
            "This allows CQL/SQL injection.",
        )
    }

    def __init__(self, linter: PyLinter = None):
        super().__init__(linter)
        self.string_sub_queries: set = set()
        self.query_verbs = {"select", "update", "delete", "insertinto", "truncate"}

    def check_string_is_query(self, string: str) -> bool:
        query = string.split(" ")
        if query[0].lower() in self.query_verbs and ("{}" or "%s" in query):
            return True
        return False

    def visit_assign(self, node: nodes) -> None:
        """Check variables with queries using string formatting."""

        variable_is_query = False
        if (
            isinstance(node.value, nodes.BinOp)
            and node.value.op == "%"
            and isinstance(node.value.left, nodes.Const)
            and self.check_string_is_query(node.value.left.value)
        ):
            variable_is_query = True
        elif (
            isinstance(node.value, nodes.Call)
            and isinstance(node.targets[0], nodes.Name)
            and isinstance(node.value.func, nodes.Attribute)
            and node.value.func.attrname == "format"
            and self.check_string_is_query(node.value.func.expr.value)
        ):
            variable_is_query = True

        if variable_is_query and node.targets[0].name not in self.string_sub_queries:
            self.string_sub_queries.add(node.targets[0].name)
        elif not variable_is_query and node.targets[0].name in self.string_sub_queries:
            self.string_sub_queries.remove(node.targets[0].name)

    def visit_call(self, node: nodes) -> None:
        """Check whether execute calls have queries using string formatting."""

        if isinstance(node.func, nodes.Attribute) and node.func.attrname == "execute" and node.args:
            if (
                isinstance(node.args[0], nodes.Name)
                and node.args[0].name in self.string_sub_queries
            ):
                self.add_message("database-query-string-format", node=node)
            elif (
                isinstance(node.args[0], nodes.BinOp)
                and node.args[0].op == "%"
                and isinstance(node.args[0].left, nodes.Const)
                and self.check_string_is_query(node.args[0].left.value)
            ):
                self.add_message("database-query-string-format", node=node)
            elif (
                isinstance(node.args[0], nodes.Call)
                and isinstance(node.args[0].func, nodes.Attribute)
                and node.args[0].func.attrname == "format"
                and self.check_string_is_query(node.args[0].func.expr.value)
            ):
                self.add_message("database-query-string-format", node=node)

    def leave_module(self, node: nodes) -> nodes:
        self.string_sub_queries = set()
        return node

    def leave_classdef(self, node: nodes) -> nodes:
        self.string_sub_queries = set()
        return node

    def leave_functiondef(self, node: nodes) -> nodes:
        self.string_sub_queries = set()
        return node
