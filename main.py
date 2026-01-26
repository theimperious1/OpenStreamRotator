import asyncio
import logging
import sys
import os

# Force UTF-8 encoding for console output to handle Unicode characters
os.environ['PYTHONIOENCODING'] = 'utf-8'

log_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(log_dir, 'automation.log')

# Setup logging with explicit formatter on handlers
formatter = logging.Formatter(
    fmt='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    datefmt='%H:%M:%S'
)

file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

logging.basicConfig(level=logging.INFO, handlers=[file_handler, console_handler])
logger = logging.getLogger(__name__)

from controllers.automation_controller import AutomationController


if __name__ == "__main__":
    try:
        controller = AutomationController()
        asyncio.run(controller.run())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
