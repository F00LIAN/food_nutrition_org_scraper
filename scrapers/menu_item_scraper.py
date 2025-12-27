"""
Menu item scraper module.

Handles scraping of menu items for each restaurant, including categories,
item names, and URLs.
"""

from typing import List, Tuple, Dict
from urllib.parse import urljoin
from bs4 import BeautifulSoup

from scrapers.base_scraper import BaseScraper, ScraperConfig, with_error_handling
from config.models import ScrapedMenuItem


class MenuItemScraper(BaseScraper):
    """
    Scraper for extracting menu items from restaurant pages.
    
    For each restaurant, scrapes:
    - Restaurant logo URL
    - Menu categories
    - Menu item names and URLs
    - (Future) Menu item images and descriptions
    """
    
    def __init__(self, config: ScraperConfig):
        """
        Initialize menu item scraper.
        
        Args:
            config: Scraper configuration
        """
        super().__init__(config)
    
    @with_error_handling(default_return="")
    def extract_logo_url(self, document: BeautifulSoup) -> str:
        """
        Extract restaurant logo URL from page.
        
        Args:
            document: Parsed HTML document
        
        Returns:
            Absolute URL to restaurant logo
        """
        logo_img = document.select_one("img.logo_float[src]")
        if logo_img and logo_img.get("src"):
            logo_url = urljoin(self.config.base_url, logo_img["src"])
            self.logger.debug(f"  📷 Found logo: {logo_url}")
            return logo_url
        
        self.logger.warning("  ⚠️  No logo found")
        return ""
    
    @with_error_handling(default_return=None)
    def extract_item_image(self, document: BeautifulSoup) -> str:
        """
        Extract menu item image URL from individual item page.
        
        Args:
            document: Parsed HTML document
        
        Returns:
            Absolute URL to item image, or None
        """
        # Try 1: Explicit item photo path (most reliable)
        item_image = document.select_one("img[src^='/item-photos/']")
        if item_image and item_image.get("src"):
            return urljoin(self.config.base_url, item_image["src"])
        
        # Try 2: Any img in main content area, but NOT logos
        for img in document.select("div.col-12 img, div.col-md-6 img"):
            src = img.get("src", "")
            # Exclude logos and icons
            if src and "/logos/" not in src and "/icons/" not in src:
                return urljoin(self.config.base_url, src)
        
        # No valid item image found
        self.logger.debug("    ⚠️  No item image found (logos excluded)")
        return None
    
    @with_error_handling(default_return=None)
    def extract_item_description(self, document: BeautifulSoup) -> str:
        """
        Extract menu item description from individual item page.
        
        PSEUDOCODE - Implement if descriptions exist on site:
        
        Args:
            document: Parsed HTML document
        
        Returns:
            Item description text, or None
        """
        # Try meta description
        meta_desc = document.select_one("meta[name='description']")
        if meta_desc and meta_desc.get("content"):
            desc = meta_desc["content"]
            # Filter out generic nutrition facts text
            if "nutrition" not in desc.lower()[:30]:
                return desc
        
        # Try description div/paragraph
        desc_elem = document.select_one(".item_description, .description, .item_info p")
        if desc_elem:
            return desc_elem.get_text(" ", strip=True)
        
        return None
    
    @with_error_handling(default_return=[])
    def extract_menu_items(self, document: BeautifulSoup) -> List[Dict[str, str]]:
        """
        Extract menu items organized by category.
        
        Args:
            document: Parsed HTML document
        
        Returns:
            List of menu item dictionaries with name, url, category
        """
        items: List[Dict[str, str]] = []
        category_blocks = document.select("div.category")
        
        self.logger.info(f"  📋 Found {len(category_blocks)} category blocks")
        
        for category_block in category_blocks:
            # Extract category name
            heading = category_block.select_one(
                "a.toggle_category.topround.nomobileround.toggle_div h2"
            )
            category_name = heading.get_text(strip=True) if heading else "Uncategorized"
            
            # Extract items within category
            item_anchors = category_block.select("ul.list.rest_item_list.ab1 a[href]")
            
            for anchor in item_anchors:
                # Get item name (prefer direct text node)
                raw_name = anchor.find(string=True, recursive=False)
                item_name = (
                    raw_name.strip() 
                    if raw_name 
                    else anchor.get_text(" ", strip=True)
                )
                
                item_url = urljoin(self.config.base_url, anchor["href"])
                
                items.append({
                    "name": item_name,
                    "url": item_url,
                    "category": category_name,
                })
            
            self.logger.debug(f"    🍽️  {category_name}: {len(item_anchors)} items")
        
        # De-duplicate by URL while preserving order
        seen = set()
        unique_items: List[Dict[str, str]] = []
        for item in items:
            if item["url"] not in seen:
                seen.add(item["url"])
                unique_items.append(item)
        
        removed = len(items) - len(unique_items)
        if removed > 0:
            self.logger.info(f"  🔄 Removed {removed} duplicate items")
        
        self.logger.info(f"  ✅ Extracted {len(unique_items)} unique menu items")
        return unique_items
    
    def scrape_restaurant_menu(
        self, 
        restaurant_name: str, 
        restaurant_url: str
    ) -> Tuple[str, List[Dict[str, str]]]:
        """
        Scrape menu items for a single restaurant.
        
        Args:
            restaurant_name: Name of the restaurant
            restaurant_url: URL to the restaurant's menu page
        
        Returns:
            Tuple of (logo_url, list of menu items)
        """
        self.logger.info(f"\n🏪 Processing: {restaurant_name}")
        self.logger.info(f"  🔗 URL: {restaurant_url}")
        
        document = self.fetch_page(restaurant_url)
        logo_url = self.extract_logo_url(document)
        menu_items = self.extract_menu_items(document)
        
        return logo_url, menu_items
    
    def scrape(self, restaurants: List[Dict[str, str]]) -> Dict[str, Dict]:
        """
        Scrape menu items for all restaurants.
        
        Args:
            restaurants: List of restaurant dictionaries with 'name' and 'url'
        
        Returns:
            Dictionary mapping restaurant names to their menu data
        """
        menu_index: Dict[str, Dict] = {}
        total = len(restaurants)
        
        self.logger.info(f"🍽️  Scraping menu items for {total} restaurants")
        
        for i, restaurant in enumerate(restaurants, 1):
            name = restaurant.get("name")
            url = restaurant.get("url")
            
            if not name or not url:
                self.logger.warning(f"⚠️  Skipping invalid restaurant entry: {restaurant}")
                continue
            
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"[{i}/{total}] {name}")
            self.logger.info(f"{'='*60}")
            
            try:
                logo_url, items = self.scrape_restaurant_menu(name, url)
                
                menu_index[name] = {
                    "url": url,
                    "restaurant_logo": logo_url,
                    "items": items,
                }
                
                self.logger.info(f"✅ Completed {name}: {len(items)} items")
                
            except Exception as e:
                self.logger.error(f"❌ Failed to scrape {name}: {e}")
                # Add placeholder to maintain structure
                menu_index[name] = {
                    "url": url,
                    "restaurant_logo": "",
                    "items": [],
                    "error": str(e)
                }
        
        self.logger.info(f"\n✅ Completed menu scraping for {len(menu_index)} restaurants")
        return menu_index

