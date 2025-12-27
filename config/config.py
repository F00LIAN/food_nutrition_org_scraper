"""
Configuration management for the scraper.
"""

from dataclasses import dataclass, field
from typing import Optional
import os
from pathlib import Path


@dataclass
class ScraperSettings:
    """
    Global settings for the scraping pipeline.
    """
    # Network settings
    base_url: str = "https://fastfoodnutrition.org"
    max_retries: int = 3
    retry_delay: float = 1.0
    timeout: int = 30
    rate_limit_delay: float = 0.5
    user_agent: str = "chrome"
    
    # Output settings
    output_dir: str = "output"
    checkpoint_dir: str = "output/checkpoints"
    enable_checkpointing: bool = True
    
    # Scraping behavior
    resume_from_checkpoint: bool = True
    save_intermediate_results: bool = True
    
    # Filtering options
    specific_restaurants: Optional[list] = None  # None = all, or list of restaurant names
    max_restaurants: Optional[int] = None  # Limit for testing
    max_items_per_restaurant: Optional[int] = None  # Limit for testing
    
    # Logging
    log_level: str = "INFO"
    log_file: Optional[str] = "scraper.log"
    
    # Data transformation
    normalize_data: bool = True
    export_formats: list = field(default_factory=lambda: ["json"])  # Future: ["json", "csv", "sql"]
    
    def __post_init__(self):
        """Validate and setup configuration after initialization."""
        # Ensure output directories exist
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.checkpoint_dir).mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def from_env(cls) -> 'ScraperSettings':
        """
        Load configuration from environment variables.
        
        Environment variables:
        - SCRAPER_BASE_URL: Base URL for scraping
        - SCRAPER_MAX_RETRIES: Maximum retry attempts
        - SCRAPER_RATE_LIMIT: Delay between requests in seconds
        - SCRAPER_OUTPUT_DIR: Output directory path
        - SCRAPER_LOG_LEVEL: Logging level (DEBUG, INFO, WARNING, ERROR)
        - SCRAPER_MAX_RESTAURANTS: Maximum number of restaurants to scrape
        - SCRAPER_MAX_ITEMS_PER_RESTAURANT: Maximum items per restaurant
        
        Returns:
            ScraperSettings instance
        """
        # Handle optional integer values
        max_restaurants = os.getenv('SCRAPER_MAX_RESTAURANTS')
        max_items = os.getenv('SCRAPER_MAX_ITEMS_PER_RESTAURANT')
        
        return cls(
            base_url=os.getenv('SCRAPER_BASE_URL', cls.base_url),
            max_retries=int(os.getenv('SCRAPER_MAX_RETRIES', str(cls.max_retries))),
            rate_limit_delay=float(os.getenv('SCRAPER_RATE_LIMIT', str(cls.rate_limit_delay))),
            output_dir=os.getenv('SCRAPER_OUTPUT_DIR', cls.output_dir),
            log_level=os.getenv('SCRAPER_LOG_LEVEL', cls.log_level),
            max_restaurants=int(max_restaurants) if max_restaurants else None,
            max_items_per_restaurant=int(max_items) if max_items else None,
        )
    
    def to_scraper_config(self):
        """
        Convert to ScraperConfig for use with base scraper.
        
        Returns:
            ScraperConfig instance
        """
        from scrapers.base_scraper import ScraperConfig
        
        return ScraperConfig(
            base_url=self.base_url,
            max_retries=self.max_retries,
            retry_delay=self.retry_delay,
            timeout=self.timeout,
            rate_limit_delay=self.rate_limit_delay,
            user_agent=self.user_agent,
            enable_checkpointing=self.enable_checkpointing,
            checkpoint_dir=self.checkpoint_dir,
        )

# Default configuration instance
DEFAULT_CONFIG = ScraperSettings()

