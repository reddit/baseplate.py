from opentelemetry.semconv.trace import (
    DbSystemValues,
    NetTransportValues,
    SpanAttributes,
)
def _extract_conn_attributes(conn_kwargs):
    """Transform redis conn info into dict"""
    attributes = {
        SpanAttributes.DB_SYSTEM: DbSystemValues.REDIS.value,
    }
    db = conn_kwargs.get("db", 0)
    attributes[SpanAttributes.DB_REDIS_DATABASE_INDEX] = db
    try:
        attributes[SpanAttributes.NET_PEER_NAME] = conn_kwargs.get(
            "host", "localhost"
        )
        attributes[SpanAttributes.NET_PEER_PORT] = conn_kwargs.get(
            "port", 6379
        )
        attributes[
            SpanAttributes.NET_TRANSPORT
        ] = NetTransportValues.IP_TCP.value
    except KeyError:
        attributes[SpanAttributes.NET_PEER_NAME] = conn_kwargs.get("path", "")
        attributes[
            SpanAttributes.NET_TRANSPORT
        ] = NetTransportValues.OTHER.value

    return attributes

def _format_command_args(args):
    """Format and sanitize command arguments, and trim them as needed"""
    cmd_max_len = 1000
    value_too_long_mark = "..."

    # Sanitized query format: "COMMAND ? ?"
    args_length = len(args)
    if args_length > 0:
        out = [str(args[0])] + ["?"] * (args_length - 1)
        out_str = " ".join(out)

        if len(out_str) > cmd_max_len:
            out_str = (
                out_str[: cmd_max_len - len(value_too_long_mark)]
                + value_too_long_mark
            )
    else:
        out_str = ""

    return out_str