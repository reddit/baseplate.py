from pythonjsonlogger import jsonlogger

class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def process_log_record(self, log_record):
        log_record['level'] = log_record.pop('levelname', None)
        return jsonlogger.JsonFormatter.process_log_record(self, log_record)
