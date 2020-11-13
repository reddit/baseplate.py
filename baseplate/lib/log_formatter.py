from pythonjsonlogger import jsonlogger


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def process_log_record(self, log_record: dict) -> dict:
        log_record["level"] = log_record.pop("levelname", None)
        try:
            # see if there's an integer thread name, that's probably a traceID
            # from the logging observer.
            trace_id = int(log_record["threadName"])

            # unfortunately we have to convert back to string because our log
            # processor can't handle giant integers.
            log_record["traceID"] = str(trace_id)
        except (KeyError, ValueError):
            pass
        return super().process_log_record(log_record)
