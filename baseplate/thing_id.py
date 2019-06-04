"""Thing ID prefix and validation."""


class ThingPrefix:
    """Thing object prefixes."""

    COMMENT = "t1_"
    ACCOUNT = "t2_"
    LINK = "t3_"
    MESSAGE = "t4_"
    SUBREDDIT = "t5_"
    AWARD = "t6_"


def is_valid_id(id_to_check, prefix, required=True):
    """Check to see if the fullname is a valid format.

    Make sure that the id is a string, starts with a legal prefix,
    and has a valid base36 string after the prefix.
    """
    if not id_to_check and not required:
        return True

    if not isinstance(id_to_check, str):
        return False

    if isinstance(prefix, tuple):
        for option in prefix:
            if is_valid_id(id_to_check, option, required):
                return True
        return False

    if not id_to_check.startswith(prefix):
        return False

    try:
        # Check that the foo portion of t3_foo is a valid base36 string
        int(id_to_check[len(prefix) :], 36)
    except ValueError:
        return False

    return len(id_to_check) > len(prefix)
