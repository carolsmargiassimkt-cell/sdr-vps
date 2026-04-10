class PipelineLogger:

    def __init__(self, log_path, base_logger=None):
        self.log_path = log_path
        self.base_logger = base_logger

    def log(self, message):
        try:
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(message + "\n")
        except Exception:
            pass

        if self.base_logger:
            try:
                self.base_logger.info(message)
            except Exception:
                pass
