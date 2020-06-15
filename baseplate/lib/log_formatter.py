from pythonjsonlogger import jsonlogger


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def process_log_record(self, log_record: dict) -> dict:
        log_record["level"] = log_record.pop("levelname", None)
        try:
            log_record["traceID"] = int(log_record.get("threadName", None))
        except ValueError:
            pass
        return super().process_log_record(log_record)
