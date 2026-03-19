"""Logging configuration shared by backend and UI."""

from __future__ import annotations

import logging
import queue

from app.config import settings


class QueueLogHandler(logging.Handler):
    """Send formatted log messages to a queue consumed by the UI."""

    def __init__(self, log_queue: "queue.Queue[str]") -> None:
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record: logging.LogRecord) -> None:
        """Publish one formatted log record to the queue."""
        try:
            self.log_queue.put(self.format(record))
        except Exception:
            self.handleError(record)


def configure_logging(log_queue: "queue.Queue[str] | None" = None) -> logging.Logger:
    """Configure the application logger once."""
    logger = logging.getLogger("rpa_panel_cliente")
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    log_path = settings.LOGS_DIR / settings.LOG_FILE_NAME
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if log_queue is not None:
        queue_handler = QueueLogHandler(log_queue)
        queue_handler.setFormatter(formatter)
        logger.addHandler(queue_handler)

    logger.propagate = False
    return logger


def get_logger() -> logging.Logger:
    """Return the shared app logger."""
    return logging.getLogger("rpa_panel_cliente")
