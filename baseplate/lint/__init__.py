from cql_string_format_plugin import NoCQLStringFormatChecker
from pylint.lint import PyLinter


def register(linter: PyLinter) -> None:
    checker = NoCQLStringFormatChecker(linter)
    linter.register_checker(checker)
