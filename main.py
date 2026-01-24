import asyncio
import logging
import sys
import os
from controllers.automation_controller import AutomationController

# Setup logging with absolute path
log_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(log_dir, 'automation.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    try:
        controller = AutomationController()
        asyncio.run(controller.run())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
