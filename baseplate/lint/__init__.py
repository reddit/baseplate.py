from baseplate.lint.db_query_string_format_plugin import NoDbQueryStringFormatChecker
from pylint.lint import PyLinter


def register(linter: PyLinter) -> None:
    checker = NoDbQueryStringFormatChecker(linter)
    linter.register_checker(checker)
