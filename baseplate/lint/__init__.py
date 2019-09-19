from cql_string_format_plugin import NoCQLStringFormatChecker


def register(linter):
    checker = NoCQLStringFormatChecker(linter)
    linter.register_checker(checker)
