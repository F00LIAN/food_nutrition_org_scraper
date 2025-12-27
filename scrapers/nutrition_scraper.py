"""
Nutrition data scraper module.

Handles scraping of detailed nutritional information, allergens, and serving
size variations for menu items.
"""

import time
from typing import Dict, List, Tuple, Optional
from urllib.parse import urljoin
from bs4 import BeautifulSoup

from scrapers.base_scraper import BaseScraper, ScraperConfig, with_error_handling
from config.models import (
    ScrapedNutrition, 
    ScrapedAllergens, 
    ScrapedServingSize,
    ScrapedMenuItem
)


class NutritionScraper(BaseScraper):
    """
    Scraper for extracting nutritional information from menu item pages.
    
    For each menu item, scrapes:
    - Nutrition table data (calories, macros, vitamins, minerals)
    - Allergen information
    - Multiple serving size variations (if available)
    - (Future) Item images and descriptions
    """
    
    def __init__(self, config: ScraperConfig):
        """
        Initialize nutrition scraper.
        
        Args:
            config: Scraper configuration
        """
        super().__init__(config)
    
    @staticmethod
    def _to_snake_case(text: str) -> str:
        """
        Convert text to snake_case for consistent field naming.
        
        Args:
            text: Text to convert
        
        Returns:
            Snake-cased string
        """
        cleaned = "".join(ch if ch.isalnum() else "_" for ch in text.strip().lower())
        while "__" in cleaned:
            cleaned = cleaned.replace("__", "_")
        return cleaned.strip("_")
    
    @with_error_handling(default_return={})
    def extract_nutrition_table(self, document: BeautifulSoup) -> Dict[str, str]:
        """
        Extract nutrition values from the nutrition facts table.
        
        Args:
            document: Parsed HTML document
        
        Returns:
            Dictionary of nutrition field names to values (as strings with units)
        """
        # Try multiple selectors to find the nutrition table
        table = (
            document.select_one("table.item_nutrition")
            or document.select_one("table#item_nutrition")
            or document.select_one("table.nutrition")
        )
        
        if not table:
            self.logger.warning("    ❌ No nutrition table found")
            return {}
        
        self.logger.debug("    ✅ Found nutrition table")
        nutrition: Dict[str, str] = {}
        row_count = 0
        
        for row in table.select("tr"):
            # Prefer header cell as key when present, else first td
            header_cell = row.find("th")
            data_cells = row.find_all("td")
            key_text = None
            value_text = None
            
            if header_cell and data_cells:
                key_text = header_cell.get_text(" ", strip=True)
                # For rows with header cells, take the first data cell (actual values)
                value_text = data_cells[0].get_text(" ", strip=True)
            elif len(data_cells) >= 2:
                key_text = data_cells[0].get_text(" ", strip=True)
                # For 3-column layout: nutrient name, actual value, percentage
                # We want the actual value (second column, index 1)
                if len(data_cells) >= 3:
                    value_text = data_cells[1].get_text(" ", strip=True)
                else:
                    # Fallback for 2-column layout
                    value_text = data_cells[1].get_text(" ", strip=True)
            
            # Skip rows that are headers or have empty/meaningless keys
            if key_text and not key_text.lower().startswith(
                ("amount per serving", "% daily value", "percent daily")
            ):
                # Clean up the value text - remove empty values or placeholder content
                if value_text and value_text.strip() and value_text.strip() not in ["", "?", "&nbsp;", "-"]:
                    nutrition[self._to_snake_case(key_text)] = value_text.strip()
                    row_count += 1
        
        self.logger.debug(f"    📊 Extracted {row_count} nutrition values")
        return nutrition
    
    @with_error_handling(default_return=ScrapedAllergens())
    def extract_allergens(self, document: BeautifulSoup) -> ScrapedAllergens:
        """
        Extract allergen information from the allergens section.
        
        Args:
            document: Parsed HTML document
        
        Returns:
            ScrapedAllergens model with categorized allergen data
        """
        allergen_section = document.select_one("#allergens")
        if not allergen_section:
            self.logger.debug("    ℹ️  No allergen section found")
            return ScrapedAllergens()
        
        self.logger.debug("    🚨 Found allergen section")
        allergens_data = ScrapedAllergens()
        
        # Find each category by looking for <strong> headers
        columns = allergen_section.select(".col-12")
        self.logger.debug(f"    📋 Found {len(columns)} allergen columns")
        
        for col in columns:
            header = col.find("strong")
            if not header:
                continue
            
            header_text = header.get_text(strip=True).lower()
            dots = col.select(".dot")
            allergen_list = [dot.get_text(strip=True) for dot in dots if dot.get_text(strip=True)]
            
            if allergen_list:
                self.logger.debug(f"    🏷️  Category '{header_text}': {len(allergen_list)} items")
            
            # Categorize based on header text
            if "contains" in header_text and "not" not in header_text and "may" not in header_text:
                allergens_data.contains = allergen_list
            elif "does not contain" in header_text:
                allergens_data.does_not_contain = allergen_list
            elif "unknown" in header_text or "aren't sure" in header_text:
                allergens_data.unknown = allergen_list
        
        # Extract allergy information text
        info_paragraphs = allergen_section.select("p")
        for p in info_paragraphs:
            text = p.get_text(" ", strip=True)
            if text and ("allergy" in text.lower() or "allergen" in text.lower()):
                allergens_data.allergy_information = text
                self.logger.debug("    📝 Found allergy information")
        
        return allergens_data
    
    @with_error_handling(default_return="")
    def extract_title(self, document: BeautifulSoup) -> str:
        """
        Extract page title (menu item name).
        
        Args:
            document: Parsed HTML document
        
        Returns:
            Page title string
        """
        # Prefer page <h1>, fallback to title tag
        h1 = document.find("h1")
        if h1:
            return h1.get_text(" ", strip=True)
        if document.title:
            return document.title.get_text(" ", strip=True)
        return ""
    
    @with_error_handling(default_return=[])
    def extract_dropdown_options(self, document: BeautifulSoup) -> List[Dict[str, str]]:
        """
        Extract serving size options from dropdown menu.
        
        Many menu items have multiple serving sizes (Small, Medium, Large, etc.)
        that are accessible via a dropdown menu on the page.
        
        Args:
            document: Parsed HTML document
        
        Returns:
            List of dictionaries with 'title' and 'url' for each option
        """
        dropdown_menu = document.select_one("div.dropdown-menu")
        if not dropdown_menu:
            self.logger.debug("    ℹ️  No dropdown menu found - single option item")
            return []
        
        self.logger.debug("    🔽 Found dropdown menu - multiple options")
        options: List[Dict[str, str]] = []
        
        for anchor in dropdown_menu.select("a[href]"):
            # Use the text content (e.g., "Cup", "Container", "Small", "Large")
            option_text = anchor.get_text(" ", strip=True)
            option_url = urljoin(self.config.base_url, anchor["href"])
            options.append({
                "title": option_text,
                "url": option_url,
            })
            self.logger.debug(f"    🔗 Option: {option_text} -> {option_url}")
        
        # Remove duplicates by URL while preserving order
        seen = set()
        unique: List[Dict[str, str]] = []
        for opt in options:
            if opt["url"] not in seen:
                seen.add(opt["url"])
                unique.append(opt)
        
        self.logger.debug(f"    ✅ Found {len(unique)} unique dropdown options")
        return unique
    
    @with_error_handling(default_return="")
    def extract_ingredients(self, document: BeautifulSoup) -> str:
        """
        Extract ingredients as a single text string from menu item page.
        
        Args:
            document: Parsed HTML document
        
        Returns:
            Ingredients as a single text string
        """
        # Try multiple ingredient section selectors
        selectors = [
            "#ingredients2"
        ]
        
        for selector in selectors:
            ingredients_section = document.select_one(selector)
            if ingredients_section:
                # Get all text from the section
                text = ingredients_section.get_text(" ", strip=True)
                if text:
                    self.logger.debug(f"    📝 Found ingredients: {text[:50]}...")
                    return text
        
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
        self.logger.debug("    ⚠️  No item image found")
        return None
    
    def extract_item_nutrition_data(
        self, 
        item_url: str, 
        size_label: str = "", 
        fallback_title: str = ""
    ) -> Tuple[ScrapedServingSize, ScrapedAllergens, str, str]:
        """
        Extract nutrition, allergen, ingredient, and image data for a single serving size.
        
        Args:
            item_url: URL to the menu item page
            size_label: Label for this serving size (e.g., "Small", "12 oz")
            fallback_title: Fallback name if title extraction fails
        
        Returns:
            Tuple of (ScrapedServingSize, ScrapedAllergens, ingredients_text, image_url)
        """
        doc = self.fetch_page(item_url)
        title = self.extract_title(doc) or fallback_title
        
        # Extract nutrition, allergens, ingredients, and image
        nutrition_dict = self.extract_nutrition_table(doc)
        allergens = self.extract_allergens(doc)
        ingredients = self.extract_ingredients(doc)
        image_url = self.extract_item_image(doc)
        
        # Create nutrition model
        nutrition = ScrapedNutrition(**nutrition_dict)
        
        # Create serving size model
        serving_size = ScrapedServingSize(
            size_label=size_label or title,
            nutrition=nutrition
        )
        
        return serving_size, allergens, ingredients, image_url
    
    @with_error_handling(default_return={})
    def process_menu_item(self, item: Dict[str, str]) -> Dict:
        """
        Process a single menu item, extracting all nutrition data.
        
        Handles both single-serving items and items with multiple serving sizes.
        
        Args:
            item: Menu item dictionary with 'name', 'url', 'category'
        
        Returns:
            Enhanced item dictionary with nutrition and allergen data
        """
        item_url = item.get("url", "")
        item_name = item.get("name", "")
        
        if not item_url:
            self.logger.warning(f"  ⚠️  Skipping item '{item_name}' - no URL")
            return item
        
        self.logger.info(f"  🍽️  Processing: {item_name}")
        doc = self.fetch_page(item_url)
        
        # Check if item has multiple serving size options
        options = self.extract_dropdown_options(doc)
        
        if options:
            # Multiple serving sizes
            self.logger.info(f"    📝 Processing {len(options)} serving sizes...")
            serving_sizes: List[Dict] = []
            aggregated_allergens = ScrapedAllergens()
            aggregated_ingredients = ""
            
            for i, opt in enumerate(options, 1):
                size_label = opt.get("title", "Unknown")
                self.logger.info(f"    🔄 Processing serving size {i}/{len(options)}: {size_label}")
                
                serving_size, allergens, ingredients, image_url = self.extract_item_nutrition_data(
                    opt["url"], 
                    size_label=size_label, 
                    fallback_title=item_name
                )
                
                # Add image_url to serving size dict
                serving_dict = serving_size.dict()
                serving_dict['image_url'] = image_url
                serving_sizes.append(serving_dict)
                
                # Merge allergen and ingredient data (take last non-empty)
                if allergens.contains or allergens.does_not_contain or allergens.unknown:
                    aggregated_allergens = allergens
                if ingredients:
                    aggregated_ingredients = ingredients
            
            item["nutritional_values"] = {"serving_sizes": serving_sizes}
            item["allergens"] = aggregated_allergens.dict()
            item["ingredients"] = aggregated_ingredients
            self.logger.info(f"    ✅ Completed {len(serving_sizes)} serving sizes")
        
        else:
            # Single serving size
            self.logger.info("    📝 Processing single serving size...")
            serving_size, allergens, ingredients, image_url = self.extract_item_nutrition_data(
                item_url, 
                size_label="1 serving", 
                fallback_title=item_name
            )
            
            # Add image_url to serving size dict
            serving_dict = serving_size.dict()
            serving_dict['image_url'] = image_url
            
            item["nutritional_values"] = {"serving_sizes": [serving_dict]}
            item["allergens"] = allergens.dict()
            item["ingredients"] = ingredients
            self.logger.info("    ✅ Completed single serving size")
        
        # Add brief pause between items to be respectful to server
        time.sleep(self.config.rate_limit_delay)
        
        return item
    
    def scrape(self, menu_data: Dict[str, Dict]) -> Dict[str, Dict]:
        """
        Scrape nutrition data for all menu items across all restaurants.
        
        Args:
            menu_data: Dictionary mapping restaurant names to menu data
        
        Returns:
            Enhanced menu data with nutrition and allergen information
        """
        total_restaurants = len(menu_data)
        self.logger.info(f"🍽️  Scraping nutrition for {total_restaurants} restaurants")
        
        enriched_data = {}
        
        for i, (restaurant_name, info) in enumerate(menu_data.items(), 1):
            self.logger.info(f"\n{'='*70}")
            self.logger.info(f"[{i}/{total_restaurants}] 🏪 {restaurant_name}")
            self.logger.info(f"{'='*70}")
            self.logger.info(f"📍 URL: {info.get('url', 'N/A')}")
            
            items = info.get("items", [])
            total_items = len(items)
            self.logger.info(f"🍽️  Found {total_items} menu items")
            
            processed_items: List[Dict] = []
            
            for j, item in enumerate(items, 1):
                self.logger.info(f"\n  📋 [{j}/{total_items}] Item: {item.get('name', 'Unknown')}")
                
                try:
                    processed_item = self.process_menu_item(dict(item))
                    processed_items.append(processed_item)
                except Exception as e:
                    self.logger.error(f"    ❌ Failed to process item '{item.get('name', 'Unknown')}': {e}")
                    # Add placeholder item to maintain structure
                    processed_items.append({
                        **dict(item),
                        "nutritional_values": {"serving_sizes": []},
                        "allergens": {},
                        "error": str(e)
                    })
            
            enriched_data[restaurant_name] = {
                "url": info.get("url", ""),
                "restaurant_logo": info.get("restaurant_logo", ""),
                "items": processed_items,
            }
            
            self.logger.info(f"✅ Completed {restaurant_name} - {len(processed_items)} items processed")
            
            # Brief pause between restaurants
            time.sleep(2)
        
        return enriched_data

