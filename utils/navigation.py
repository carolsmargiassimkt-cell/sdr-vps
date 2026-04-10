import time
import logging
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError

logger = logging.getLogger(__name__)

def safe_goto(page: Page, url: str, max_retries: int = 3) -> bool:
    """
    Safely navigates to a URL with automatic retries and error logging.
    """
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Navigating to {url} (Attempt {attempt}/{max_retries})")
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            return True
        except (PlaywrightTimeoutError, Exception) as e:
            logger.warning(f"Navigation failed: {e}")
            if attempt < max_retries:
                time.sleep(2 * attempt) # Exponential backoff
            else:
                logger.error(f"Failed to navigate to {url} after {max_retries} attempts.")
                return False
    return False
