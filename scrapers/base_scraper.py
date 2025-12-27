"""
Base scraper infrastructure with retry logic, rate limiting, and error handling.

This module provides the foundation for all scrapers with common functionality
like session management, retry logic, rate limiting, and structured error handling.
"""

import time
import logging
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, Callable, TypeVar
from dataclasses import dataclass
from datetime import datetime, timedelta
import curl_cffi
from bs4 import BeautifulSoup
from functools import wraps

# Type variable for generic return types
T = TypeVar('T')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


@dataclass
class ScraperConfig:
    """Configuration for scraper behavior."""
    base_url: str
    max_retries: int = 3
    retry_delay: float = 1.0  # Base delay in seconds
    timeout: int = 30
    rate_limit_delay: float = 0.5  # Delay between requests
    user_agent: str = "chrome"  # For curl_cffi impersonation
    enable_checkpointing: bool = True
    checkpoint_dir: str = "data/checkpoints"


@dataclass
class ScraperStats:
    """Statistics tracking for scraper operations."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_retries: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    def record_success(self) -> None:
        """Record a successful request."""
        self.total_requests += 1
        self.successful_requests += 1
    
    def record_failure(self) -> None:
        """Record a failed request."""
        self.total_requests += 1
        self.failed_requests += 1
    
    def record_retry(self) -> None:
        """Record a retry attempt."""
        self.total_retries += 1
    
    def get_success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_requests == 0:
            return 0.0
        return (self.successful_requests / self.total_requests) * 100
    
    def get_duration(self) -> Optional[timedelta]:
        """Get total duration of scraping operation."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None
    
    def __str__(self) -> str:
        """Generate summary statistics string."""
        duration = self.get_duration()
        duration_str = str(duration).split('.')[0] if duration else "N/A"
        return (
            f"📊 Scraper Statistics:\n"
            f"  Total Requests: {self.total_requests}\n"
            f"  ✅ Successful: {self.successful_requests}\n"
            f"  ❌ Failed: {self.failed_requests}\n"
            f"  🔄 Retries: {self.total_retries}\n"
            f"  📈 Success Rate: {self.get_success_rate():.2f}%\n"
            f"  ⏱️  Duration: {duration_str}"
        )


class ScraperException(Exception):
    """Base exception for scraper errors."""
    pass


class NetworkException(ScraperException):
    """Exception for network-related errors."""
    pass


class ParseException(ScraperException):
    """Exception for parsing-related errors."""
    pass


class RateLimiter:
    """Simple rate limiter to control request frequency."""
    
    def __init__(self, min_delay: float = 0.5):
        """
        Initialize rate limiter.
        
        Args:
            min_delay: Minimum delay in seconds between requests
        """
        self.min_delay = min_delay
        self.last_request_time: Optional[float] = None
    
    def wait(self) -> None:
        """Wait if necessary to respect rate limit."""
        if self.last_request_time is not None:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.min_delay:
                time.sleep(self.min_delay - elapsed)
        self.last_request_time = time.time()


class BaseScraper(ABC):
    """
    Abstract base class for all scrapers.
    
    Provides common functionality:
    - Session management with curl_cffi
    - Retry logic with exponential backoff
    - Rate limiting
    - Statistics tracking
    - Structured error handling
    """
    
    def __init__(self, config: ScraperConfig):
        """
        Initialize base scraper.
        
        Args:
            config: Scraper configuration object
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.session: Optional[curl_cffi.Session] = None
        self.stats = ScraperStats()
        self.rate_limiter = RateLimiter(config.rate_limit_delay)
        self._initialize_session()
    
    def _initialize_session(self) -> None:
        """Initialize curl_cffi session with browser impersonation."""
        try:
            self.session = curl_cffi.Session(impersonate=self.config.user_agent)
            self.logger.info(f"✅ Session initialized with {self.config.user_agent} impersonation")
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize session: {e}")
            raise ScraperException(f"Session initialization failed: {e}")
    
    def fetch_page(
        self, 
        url: str, 
        max_retries: Optional[int] = None,
        retry_delay: Optional[float] = None
    ) -> BeautifulSoup:
        """
        Fetch a page with retry logic and exponential backoff.
        
        Args:
            url: URL to fetch
            max_retries: Maximum retry attempts (overrides config)
            retry_delay: Base delay for retries (overrides config)
        
        Returns:
            BeautifulSoup object of the parsed HTML
        
        Raises:
            NetworkException: If all retry attempts fail
        """
        max_retries = max_retries or self.config.max_retries
        retry_delay = retry_delay or self.config.retry_delay
        
        self.logger.info(f"📡 Fetching: {url}")
        
        for attempt in range(max_retries):
            try:
                # Respect rate limiting
                self.rate_limiter.wait()
                
                # Make request
                response = self.session.get(url, timeout=self.config.timeout)
                response.raise_for_status()
                
                # Parse and return
                soup = BeautifulSoup(response.text, 'html.parser')
                self.stats.record_success()
                self.logger.info(f"✅ Successfully fetched: {url}")
                return soup
                
            except Exception as e:
                self.stats.record_retry()
                self.logger.warning(
                    f"⚠️  Attempt {attempt + 1}/{max_retries} failed for {url}: {e}"
                )
                
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                    self.logger.info(f"⏳ Retrying in {wait_time:.1f}s...")
                    time.sleep(wait_time)
                else:
                    self.stats.record_failure()
                    self.logger.error(f"❌ All {max_retries} attempts failed for {url}")
                    raise NetworkException(f"Failed to fetch {url} after {max_retries} attempts: {e}")
    
    def safe_extract(
        self,
        extractor_func: Callable[[], T],
        default: T,
        error_message: str = "Extraction failed"
    ) -> T:
        """
        Safely execute an extraction function with error handling.
        
        Args:
            extractor_func: Function to execute
            default: Default value to return on error
            error_message: Error message to log
        
        Returns:
            Extracted value or default on error
        """
        try:
            return extractor_func()
        except Exception as e:
            self.logger.warning(f"⚠️  {error_message}: {e}")
            return default
    
    def start_operation(self) -> None:
        """Mark the start of a scraping operation."""
        self.stats.start_time = datetime.utcnow()
        self.logger.info("🚀 Scraping operation started")
    
    def end_operation(self) -> None:
        """Mark the end of a scraping operation and log statistics."""
        self.stats.end_time = datetime.utcnow()
        self.logger.info("🏁 Scraping operation completed")
        self.logger.info(f"\n{self.stats}")
    
    @abstractmethod
    def scrape(self) -> Any:
        """
        Main scraping logic (must be implemented by subclasses).
        
        Returns:
            Scraped data in appropriate format
        """
        pass
    
    def __enter__(self):
        """Context manager entry."""
        self.start_operation()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.end_operation()
        if exc_type is not None:
            self.logger.error(f"❌ Operation failed with error: {exc_val}")
        return False  # Don't suppress exceptions


def with_error_handling(default_return=None):
    """
    Decorator for adding error handling to scraper methods.
    
    Args:
        default_return: Value to return if an error occurs
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(func.__module__)
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(f"❌ Error in {func.__name__}: {e}", exc_info=True)
                return default_return
        return wrapper
    return decorator

