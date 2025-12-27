"""
Main orchestrator for the Fast Food Nutrition Scraper.

This module coordinates the entire scraping pipeline:
1. Scrape restaurant listings
2. Scrape menu items for each restaurant
3. Scrape nutritional data for each menu item
4. Transform data into normalized database schema
5. Export to multiple formats
"""

import logging
import sys
from datetime import datetime
from typing import Optional

from config.config import ScraperSettings, DEFAULT_CONFIG
from scrapers.base_scraper import ScraperConfig
from scrapers.restaurant_scraper import RestaurantScraper
from scrapers.menu_item_scraper import MenuItemScraper
from scrapers.nutrition_scraper import NutritionScraper
from src.data_transformer import DataTransformer
from src.utils import DataPersistence, ProgressTracker


# Configure logging
def setup_logging(settings: ScraperSettings) -> None:
    """
    Configure logging for the application.
    
    Args:
        settings: Scraper settings containing log configuration
    """
    from datetime import datetime
    from pathlib import Path
    
    # Determine if this is example or production
    is_example = 'example' in settings.output_dir
    log_subdir = 'example' if is_example else 'production'
    
    log_dir = Path(f"logs/{log_subdir}")
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Clear old logs from this subdirectory
    for log_file in log_dir.glob("*.log"):
        log_file.unlink()
    
    # Create new log file with timestamp
    log_file = log_dir / f"scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, settings.log_level))
    console_handler.setFormatter(logging.Formatter(log_format))
    
    # File handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(log_format))
    
    handlers = [console_handler, file_handler]
    
    # Configure root logger
    logging.basicConfig(
        level=logging.DEBUG,
        format=log_format,
        handlers=handlers,
        force=True  # Override any existing configuration
    )
    
    # Set specific logger levels
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('selenium').setLevel(logging.WARNING)
    
    logging.info(f"Logging to: {log_file}")


class FastFoodNutritionScraper:
    """
    Main orchestrator for the complete scraping pipeline.
    
    Coordinates all scraping stages and data transformation.
    """
    
    def __init__(self, settings: Optional[ScraperSettings] = None):
        """
        Initialize the scraper pipeline.
        
        Args:
            settings: Configuration settings (uses defaults if not provided)
        """
        self.settings = settings or DEFAULT_CONFIG
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize components
        self.scraper_config = self.settings.to_scraper_config()
        self.persistence = DataPersistence(self.settings.output_dir)
        self.transformer = DataTransformer()
        
        # Scrapers (initialized on-demand)
        self.restaurant_scraper: Optional[RestaurantScraper] = None
        self.menu_scraper: Optional[MenuItemScraper] = None
        self.nutrition_scraper: Optional[NutritionScraper] = None
    
    def run_full_pipeline(self) -> None:
        """
        Execute the complete scraping and transformation pipeline.
        
        Stages:
        1. Scrape restaurants
        2. Scrape menu items
        3. Scrape nutrition data
        4. Transform to normalized schema
        5. Export results
        """
        start_time = datetime.now()
        self.logger.info("="*70)
        self.logger.info("🚀 Starting Fast Food Nutrition Scraper Pipeline")
        self.logger.info("="*70)
        
        try:
            # Stage 1: Scrape restaurants
            self.logger.info("\n📍 STAGE 1: Scraping Restaurant Listings")
            self.logger.info("-"*70)
            restaurants = self.scrape_restaurants()
            
            if not restaurants:
                self.logger.error("❌ No restaurants found. Aborting pipeline.")
                return
            
            self.persistence.save_json(restaurants, "01_restaurants.json")
            
            # Stage 2: Scrape menu items
            self.logger.info("\n🍽️  STAGE 2: Scraping Menu Items")
            self.logger.info("-"*70)
            menu_data = self.scrape_menu_items(restaurants)
            
            if not menu_data:
                self.logger.error("❌ No menu data found. Aborting pipeline.")
                return
            
            self.persistence.save_json(menu_data, "02_menu_items.json")
            
            # Stage 3: Scrape nutrition data
            self.logger.info("\n📊 STAGE 3: Scraping Nutritional Data")
            self.logger.info("-"*70)
            enriched_data = self.scrape_nutrition_data(menu_data)
            
            if not enriched_data:
                self.logger.error("❌ No enriched data found. Aborting pipeline.")
                return
            
            self.persistence.save_json(enriched_data, "03_enriched_data.json")
            
            # Stage 4: Transform data
            if self.settings.normalize_data:
                self.logger.info("\n🔄 STAGE 4: Transforming to Normalized Schema")
                self.logger.info("-"*70)
                self.transform_and_export(enriched_data)
            
            # Summary
            duration = datetime.now() - start_time
            self.logger.info("\n" + "="*70)
            self.logger.info("✅ PIPELINE COMPLETE")
            self.logger.info("="*70)
            self.logger.info(f"⏱️  Total Duration: {duration}")
            self.logger.info(f"📁 Output Directory: {self.settings.output_dir}")
            self.logger.info("="*70)
            
        except KeyboardInterrupt:
            self.logger.warning("\n⚠️  Pipeline interrupted by user")
            sys.exit(1)
        except Exception as e:
            self.logger.error(f"\n❌ Pipeline failed with error: {e}", exc_info=True)
            sys.exit(1)
    
    def scrape_restaurants(self) -> list:
        """
        Scrape restaurant listings.
        
        Returns:
            List of restaurant dictionaries
        """
        self.restaurant_scraper = RestaurantScraper(self.scraper_config)
        
        with self.restaurant_scraper as scraper:
            restaurants = scraper.scrape()
        
        # Apply filtering if configured
        if self.settings.specific_restaurants:
            original_count = len(restaurants)
            restaurants = [
                r for r in restaurants 
                if r['name'] in self.settings.specific_restaurants
            ]
            self.logger.info(
                f"🔍 Filtered to {len(restaurants)} of {original_count} restaurants"
            )
        
        if self.settings.max_restaurants:
            restaurants = restaurants[:self.settings.max_restaurants]
            self.logger.info(f"🔍 Limited to {len(restaurants)} restaurants")
        
        self.logger.info(f"✅ Found {len(restaurants)} restaurants")
        return restaurants
    
    def scrape_menu_items(self, restaurants: list) -> dict:
        """
        Scrape menu items for all restaurants.
        
        Args:
            restaurants: List of restaurant dictionaries
        
        Returns:
            Dictionary mapping restaurant names to menu data
        """
        self.menu_scraper = MenuItemScraper(self.scraper_config)
        
        with self.menu_scraper as scraper:
            menu_data = scraper.scrape(restaurants)
        
        # Apply item limit if configured
        if self.settings.max_items_per_restaurant:
            for restaurant_name, data in menu_data.items():
                items = data.get('items', [])
                if len(items) > self.settings.max_items_per_restaurant:
                    data['items'] = items[:self.settings.max_items_per_restaurant]
                    self.logger.info(
                        f"  🔍 Limited {restaurant_name} to "
                        f"{self.settings.max_items_per_restaurant} items"
                    )
        
        total_items = sum(len(data.get('items', [])) for data in menu_data.values())
        self.logger.info(f"✅ Found {total_items} total menu items")
        return menu_data
    
    def scrape_nutrition_data(self, menu_data: dict) -> dict:
        """
        Scrape nutritional data for all menu items.
        
        Args:
            menu_data: Dictionary of menu data from previous stage
        
        Returns:
            Enriched menu data with nutrition information
        """
        # Check for existing checkpoints if resume is enabled
        if self.settings.resume_from_checkpoint:
            checkpoints = self.persistence.load_checkpoints()
            if checkpoints:
                self.logger.info(
                    f"🔄 Found {len(checkpoints)} existing checkpoints. "
                    "These will be skipped."
                )
                # Merge checkpoints with menu_data
                for restaurant_name in checkpoints.keys():
                    if restaurant_name in menu_data:
                        del menu_data[restaurant_name]
        
        self.nutrition_scraper = NutritionScraper(self.scraper_config)
        
        with self.nutrition_scraper as scraper:
            enriched_data = scraper.scrape(menu_data)
        
        # Merge with checkpoints if they exist
        if self.settings.resume_from_checkpoint:
            enriched_data = {**checkpoints, **enriched_data}
        
        total_variations = sum(
            sum(
                len(item.get('nutritional_values', {}).get('serving_sizes', []))
                for item in data.get('items', [])
            )
            for data in enriched_data.values()
        )
        self.logger.info(f"✅ Scraped {total_variations} total serving size variations")
        return enriched_data
    
    def transform_and_export(self, enriched_data: dict) -> None:
        """
        Transform scraped data to normalized schema and export.
        
        Args:
            enriched_data: Complete scraped data with nutrition information
        """
        # Transform to normalized collections
        restaurants, menu_items, variations = self.transformer.transform(enriched_data)
        
        # Save collections
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.persistence.save_collections(restaurants, menu_items, variations, timestamp)
        
        self.logger.info("✅ Data transformation and export complete")


def main():
    """
    Main entry point for the scraper.
    """
    # Load configuration (can be customized or loaded from env)
    settings = ScraperSettings.from_env()
    
    # Setup logging
    setup_logging(settings)
    
    # Create and run scraper
    scraper = FastFoodNutritionScraper(settings)
    scraper.run_full_pipeline()


if __name__ == "__main__":
    main()

