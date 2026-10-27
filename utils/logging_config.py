"""Import the ready-to-use logger with: from utils.logging_config import logger."""

import logging
import sys
from pathlib import Path


# Keep logs in the project directory regardless of the command's working directory.
PROJECT_DIR = Path(__file__).resolve().parents[1]
LOG_DIR = PROJECT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "pipeline.log"
MONITORING_LOG_FILE = LOG_DIR / "monitoring.log"

logger = logging.getLogger("citibike.pipeline")
logger.setLevel(logging.DEBUG)
# These handlers own console/file output; avoid duplicate output from root handlers.
logger.propagate = False

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

if not logger.handlers:
    # Console: DEBUG and above.
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Main log file: INFO and above.
    file_handler = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Monitoring log file: ERROR and CRITICAL.
    monitoring_file_handler = logging.FileHandler(
        MONITORING_LOG_FILE, mode="a", encoding="utf-8"
    )
    monitoring_file_handler.setLevel(logging.ERROR)
    monitoring_file_handler.setFormatter(formatter)
    logger.addHandler(monitoring_file_handler)

# Convenience aliases, for example: logger.setLevel(logger.DEBUG).
logger.DEBUG = logging.DEBUG             # type: ignore[attr-defined]
logger.INFO = logging.INFO               # type: ignore[attr-defined]
logger.WARNING = logging.WARNING         # type: ignore[attr-defined]
logger.ERROR = logging.ERROR             # type: ignore[attr-defined]
logger.CRITICAL = logging.CRITICAL       # type: ignore[attr-defined]

__all__ = ["logger"]
