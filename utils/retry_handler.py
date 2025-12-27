"""
Exponential backoff retry handler for API calls and HTTP requests.
"""
import time
import logging
from functools import wraps
from typing import Callable, Any, Tuple, Type

logger = logging.getLogger(__name__)


def exponential_retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,)
) -> Callable:
    """
    Decorator for exponential backoff retry logic.
    
    Args:
        max_attempts: Maximum number of retry attempts
        base_delay: Initial delay in seconds
        max_delay: Maximum delay between retries
        exponential_base: Base for exponential calculation
        exceptions: Tuple of exceptions to catch and retry
        
    Returns:
        Decorated function with retry logic
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            attempt = 0
            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    attempt += 1
                    if attempt >= max_attempts:
                        logger.error(f"Max retries ({max_attempts}) reached for {func.__name__}: {e}")
                        raise
                    
                    # Calculate delay with exponential backoff
                    delay = min(base_delay * (exponential_base ** (attempt - 1)), max_delay)
                    logger.warning(
                        f"Attempt {attempt}/{max_attempts} failed for {func.__name__}: {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    time.sleep(delay)
            
        return wrapper
    return decorator


def retry_with_backoff(
    func: Callable,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,)
) -> Any:
    """
    Non-decorator version for inline retry logic.
    
    Args:
        func: Function to retry
        max_attempts: Maximum number of retry attempts
        base_delay: Initial delay in seconds
        exceptions: Tuple of exceptions to catch and retry
        
    Returns:
        Result of the function call
    """
    attempt = 0
    while attempt < max_attempts:
        try:
            return func()
        except exceptions as e:
            attempt += 1
            if attempt >= max_attempts:
                logger.error(f"Max retries ({max_attempts}) reached: {e}")
                raise
            
            delay = base_delay * (2 ** (attempt - 1))
            logger.warning(f"Attempt {attempt}/{max_attempts} failed: {e}. Retrying in {delay:.1f}s...")
            time.sleep(delay)

