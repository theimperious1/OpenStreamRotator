import asyncio
import logging
import sys
import os
import io

# Force UTF-8 encoding for console output to handle Unicode characters
os.environ['PYTHONIOENCODING'] = 'utf-8'

log_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(log_dir, 'automation.log')

# Setup logging with explicit formatter on handlers
formatter = logging.Formatter(
    fmt='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    datefmt='%H:%M:%S'
)

file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.DEBUG)  # File gets all levels

console_handler = logging.StreamHandler(io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8'))
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.DEBUG)  # Console gets all levels

# Configure root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)  # Root logger captures everything
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

# Suppress DEBUG logs from libraries to reduce spam
logging.getLogger('obsws_python').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# Import this last to ensure logging is configured first
from controllers.automation_controller import AutomationController
if __name__ == "__main__":
    try:
        controller = AutomationController()
        asyncio.run(controller.run())
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Shutdown complete")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Force-exit if a background download thread is still blocking
        # yt-dlp's extract_info() cannot be interrupted from another thread,
        # so os._exit is the last resort to avoid hanging indefinitely
        os._exit(0)
