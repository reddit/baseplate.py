from pylint.lint import PyLinter

from baseplate.lint.cql_string_format_plugin import NoCQLStringFormatChecker


def register(linter: PyLinter) -> None:
    checker = NoCQLStringFormatChecker(linter)
    linter.register_checker(checker)
