# This is an example pylint checker and should not be registered to use
# http://pylint.pycqa.org/en/latest/how_tos/custom_checkers.html

from astroid import nodes
from pylint.checkers import BaseChecker
from pylint.interfaces import IAstroidChecker
from pylint.lint import PyLinter


class UniqueVariableChecker(BaseChecker):
    __implements__ = IAstroidChecker

    name = "unique-variable"
    priority = -1
    msgs = {
        "W9001": (
            "Non-unique variable found.",
            "non-unique-variable",
            "Ensure only unique variables are used.",
        )
    }

    def __init__(self, linter: PyLinter = None):
        super().__init__(linter)
        self.variables = []
        
    def visit_assign(self, node: nodes):
        

    def leave_module(self, node: nodes) -> nodes:
        self.string_sub_queries = set()
        return node

    def leave_classdef(self, node: nodes) -> nodes:
        self.string_sub_queries = set()
        return node

    def leave_functiondef(self, node: nodes) -> nodes:
        self.string_sub_queries = set()
        return node
