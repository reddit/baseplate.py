from pylint.checkers import BaseChecker
from pylint.interfaces import IAstroidChecker


class NoRelativeImportsChecker(BaseChecker):
    __implements__ = IAstroidChecker

    name = "no-relative-imports"
    priority = -1
    msgs = {
        "W5403": (
            "Explicit relative import %r should be %r",
            "explicit-relative-import",
            "All imports should be absolute",
        )
    }

    def __init__(self, linter=None):
        super().__init__(linter)
        self.module = None

    def visit_module(self, node):
        self.module = node

    def leave_module(self, node):
        self.module = None

    def visit_importfrom(self, node):
        if node.level is not None:
            self.add_message(
                "explicit-relative-import",
                node=node,
                args=(
                    "." * node.level + node.modname,
                    self.module.relative_to_absolute_name(node.modname, node.level),
                ),
            )


def register(linter):
    checker = NoRelativeImportsChecker(linter)
    linter.register_checker(checker)
