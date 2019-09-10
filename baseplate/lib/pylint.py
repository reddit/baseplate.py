from pylint.checkers import BaseChecker
from pylint.interfaces import IAstroidChecker


class NoCQLInjectionChecker(BaseChecker):
    __implements__ = IAstroidChecker

    name = "no-cql-injection"
    priority = -1
    msgs = {
        "W0001": (
            "Python string substitution found in Cassandra database query",
            "cql-injection",
            "Database queries should be using Cassandra driver parameter substitution",
        )
    }

    def visit_call(self, node):
        if hasattr(node.func, "attrname") and node.func.attrname == "execute":
            if len(node.args) < 2:
                self.add_message(
                    "cql-injection",
                    node=node
                )


def register(linter):
    checker = NoCQLInjectionChecker(linter)
    linter.register_checker(checker)
