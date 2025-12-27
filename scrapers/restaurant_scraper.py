"""
Restaurant scraper module.

Handles scraping of restaurant brand information from fastfoodnutrition.org.
"""

from typing import List
from urllib.parse import urljoin
from bs4 import BeautifulSoup

from scrapers.base_scraper import BaseScraper, ScraperConfig, with_error_handling
from config.models import ScrapedRestaurant


class RestaurantScraper(BaseScraper):
    """
    Scraper for extracting restaurant brand information.
    
    Scrapes the main restaurant listing page and extracts:
    - Restaurant names
    - Restaurant URLs
    - (Future) Additional brand metadata
    """
    
    def __init__(self, config: ScraperConfig):
        """
        Initialize restaurant scraper.
        
        Args:
            config: Scraper configuration
        """
        super().__init__(config)
        self.restaurants_url = f"{config.base_url}/fast-food-restaurants"
    
    @with_error_handling(default_return=[])
    def extract_restaurant_cards(self, document: BeautifulSoup) -> List[dict]:
        """
        Extract restaurant information from the page.
        
        Args:
            document: Parsed HTML document
        
        Returns:
            List of restaurant data dictionaries
        """
        container = document.select_one(".rest_item_list.category")
        if not container:
            self.logger.warning("❌ Restaurant container not found on page")
            return []
        
        results_by_url = {}
        cards = container.select(".filter_target")
        self.logger.info(f"🔍 Found {len(cards)} restaurant cards")
        
        for card in cards:
            # Extract URL
            anchor = card.find("a", href=True)
            if not anchor:
                continue
            
            absolute_url = urljoin(self.config.base_url, anchor["href"])
            
            # Extract name
            label = card.find("div", class_="logo_box_text")
            raw_name = None
            
            if label:
                # Prefer first text node only (excludes nested span " Nutrition")
                raw_name = label.find(string=True, recursive=False)
                if raw_name:
                    raw_name = raw_name.strip()
                else:
                    # Fallback: remove trailing " Nutrition" if present
                    text = label.get_text(" ", strip=True)
                    raw_name = text.replace(" Nutrition", "").strip() if text else None
            
            if raw_name and absolute_url:
                # De-duplicate by URL
                results_by_url[absolute_url] = {
                    "name": raw_name,
                    "url": absolute_url
                }
        
        self.logger.info(f"✅ Extracted {len(results_by_url)} unique restaurants")
        return list(results_by_url.values())
    
    def scrape(self) -> List[dict]:
        """
        Scrape all restaurants from the main listing page.
        
        Returns:
            List of restaurant data dictionaries
        """
        self.logger.info(f"🏪 Scraping restaurants from: {self.restaurants_url}")
        
        document = self.fetch_page(self.restaurants_url)
        restaurants = self.extract_restaurant_cards(document)
        
        self.logger.info(f"✅ Successfully scraped {len(restaurants)} restaurants")
        return restaurants

