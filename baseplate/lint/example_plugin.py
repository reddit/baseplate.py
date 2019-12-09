# Pylint documentation for writing a checker: http://pylint.pycqa.org/en/latest/how_tos/custom_checkers.html
# This is an example of a Pylint AST checker and should not be registered to use
# In an AST (abstract syntax tree) checker, the code will be represented as nodes of a tree
# We will use the astroid library: https://astroid.readthedocs.io/en/latest/api/general.html to visit and leave nodes
# Libraries needed for an AST checker
from astroid import nodes
from pylint.checkers import BaseChecker
from pylint.interfaces import IAstroidChecker
from pylint.lint import PyLinter


# Basic example of a Pylint AST (astract syntax tree) checker
# Checks for variables that have been reassigned in a function. If it finds a reassigned variable, it will throw an error
class NoReassignmentChecker(BaseChecker):
    __implements__ = IAstroidChecker

    # Checker name
    name = "no-reassigned-variable"
    # Set priority to -1
    priority = -1
    # Message dictionary
    msgs = {
        # message-id, consists of a letter and numbers
        # Letter will be one of following letters (C=Convention, W=Warning, E=Error, F=Fatal, R=Refactoring)
        # Numbers need to be unique and in-between 9000-9999
        # Check https://baseplate.readthedocs.io/en/stable/linters/index.html#custom-checkers-list
        # for numbers that are already in use
        "W9001": (
            # displayed-message shown to user
            "Reassigned variable found.",
            # message-symbol used as alias for message-id
            "reassigned-variable",
            # message-help shown to user when calling pylint --help-msg
            "Ensure variables are not reassigned.",
        )
    }

    def __init__(self, linter: PyLinter = None):
        super().__init__(linter)
        self.variables: set = set()

    # The following two methods are called for us by pylint/astroid
    # The linter walks through the tree, visiting and leaving desired nodes
    # Methods should start with visit_ or leave_ followed by lowercase class name of nodes
    # List of available nodes: https://astroid.readthedocs.io/en/latest/api/astroid.nodes.html

    # Visit the Assign node: https://astroid.readthedocs.io/en/latest/api/astroid.nodes.html#astroid.nodes.Assign
    def visit_assign(self, node: nodes) -> None:
        for variable in node.targets:
            if variable.name not in self.variables:
                self.variables.add(variable.name)
            else:
                self.add_message("non-unique-variable", node=node)

    # Leave the FunctionDef node: https://astroid.readthedocs.io/en/latest/api/astroid.nodes.html#astroid.nodes.FunctionDef
    def leave_functiondef(self, node: nodes) -> nodes:
        self.variables = set()
        return node
