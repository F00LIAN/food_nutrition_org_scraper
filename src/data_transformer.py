"""
Data transformation module.

Transforms scraped raw data into the normalized target database schema with
proper data types, relationships, and validation.
"""

import re
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime

from config.models import (
    RestaurantBrand,
    MenuItem,
    MenuItemVariation,
    NutritionInfo,
    ServingInfo,
    RestaurantCollection,
    MenuItemCollection,
    MenuItemVariationCollection,
    ScrapedNutrition,
)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NutritionParser:
    """
    Parser for converting nutrition text values to structured numeric data.
    
    Handles various formats like:
    - "100g" -> 100.0 grams
    - "250mg" -> 250.0 milligrams  
    - "2.5oz" -> 2.5 ounces
    - "1 cup" -> raw text (can't parse reliably)
    """
    
    # Regex patterns for parsing
    NUMERIC_PATTERN = re.compile(r'([0-9]+\.?[0-9]*)')
    UNIT_PATTERN = re.compile(r'([a-zA-Z]+)')
    
    @classmethod
    def parse_numeric_value(cls, text: str) -> Optional[float]:
        """
        Extract numeric value from text.
        
        Args:
            text: Text containing a number (e.g., "100g", "12.5", "250 mg")
        
        Returns:
            Float value or None if parsing fails
        """
        if not text:
            return None
        
        # Remove common non-numeric characters
        cleaned = text.replace(',', '').replace('<', '').replace('>', '').strip()
        
        # Extract first numeric value
        match = cls.NUMERIC_PATTERN.search(cleaned)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                return None
        
        return None
    
    @classmethod
    def parse_unit(cls, text: str) -> Optional[str]:
        """
        Extract unit from text.
        
        Args:
            text: Text containing a unit (e.g., "100g", "250mg")
        
        Returns:
            Unit string (lowercase) or None
        """
        if not text:
            return None
        
        match = cls.UNIT_PATTERN.search(text)
        if match:
            return match.group(1).lower()
        
        return None
    
    @classmethod
    def parse_nutrition_field(cls, value: str) -> Optional[float]:
        """
        Parse a nutrition field value to a float.
        
        Args:
            value: Raw value string (e.g., "10g", "250mg", "100")
        
        Returns:
            Numeric value or None
        """
        return cls.parse_numeric_value(value)
    
    @classmethod
    def parse_serving_size(cls, text: str) -> ServingInfo:
        """
        Return serving size as raw text only (no parsing).
        
        Args:
            text: Serving size text (e.g., "420g", "14oz", "1 cup")
        
        Returns:
            ServingInfo object with raw text
        """
        return ServingInfo(serving_text=text)


class DataTransformer:
    """
    Transforms scraped data into normalized database schema.
    
    Converts the flat scraped structure into:
    - RestaurantBrand entities
    - MenuItem entities
    - MenuItemVariation entities
    - LocationMenuVariation entities (placeholder for future)
    """
    
    def __init__(self):
        """Initialize data transformer."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.parser = NutritionParser()
    
    def transform_nutrition(self, scraped_nutrition: Dict[str, str]) -> NutritionInfo:
        """
        Transform scraped nutrition dictionary to NutritionInfo model.
        
        Args:
            scraped_nutrition: Raw nutrition data from scraping
        
        Returns:
            Validated NutritionInfo object with numeric values
        """
        # Parse all nutrition fields
        nutrition_data = {}
        
        # Map scraped field names to model field names
        field_mapping = {
            'calories': 'calories',
            'protein': 'protein_g',
            'total_carbohydrates': 'carbs_g',
            'carbohydrates': 'carbs_g',
            'total_fat': 'fat_g',
            'fat': 'fat_g',
            'saturated_fat': 'saturated_fat_g',
            'trans_fat': 'trans_fat_g',
            'cholesterol': 'cholesterol_mg',
            'sodium': 'sodium_mg',
            'dietary_fiber': 'fiber_g',
            'fiber': 'fiber_g',
            'sugars': 'sugars_g',
            'total_sugars': 'sugars_g',
            'added_sugars': 'added_sugars_g',
            'vitamin_d': 'vitamin_d_mcg',
            'calcium': 'calcium_mg',
            'iron': 'iron_mg',
            'potassium': 'potassium_mg',
        }
        
        for scraped_key, value_str in scraped_nutrition.items():
            model_key = field_mapping.get(scraped_key)
            if model_key:
                parsed_value = self.parser.parse_nutrition_field(value_str)
                if parsed_value is not None:
                    nutrition_data[model_key] = parsed_value
        
        return NutritionInfo(**nutrition_data)
    
    def transform_restaurant(
        self, 
        restaurant_name: str, 
        restaurant_data: Dict
    ) -> RestaurantBrand:
        """
        Transform scraped restaurant data to RestaurantBrand entity.
        
        Args:
            restaurant_name: Name of the restaurant
            restaurant_data: Scraped restaurant data
        
        Returns:
            RestaurantBrand entity
        """
        restaurant = RestaurantBrand(
            brand_name=restaurant_name,
            brand_image_url=restaurant_data.get('restaurant_logo'),
            source_url=restaurant_data.get('url'),
            eatery_full_verification_status=False,  # Default to unverified
        )
        
        self.logger.debug(f"  ✅ Created RestaurantBrand: {restaurant_name} (ID: {restaurant.id[:8]}...)")
        return restaurant
    
    def transform_menu_item(
        self,
        restaurant_brand_id: str,
        item_data: Dict
    ) -> MenuItem:
        """
        Transform scraped menu item data to MenuItem entity.
        
        Args:
            restaurant_brand_id: ID of parent restaurant brand
            item_data: Scraped menu item data
        
        Returns:
            MenuItem entity
        """
        menu_item = MenuItem(
            restaurant_brand_id=restaurant_brand_id,
            name=item_data.get('name', 'Unknown Item'),
            description=item_data.get('description'),
            category=item_data.get('category', 'Uncategorized'),
            is_active=True,
            source_url=item_data.get('url'),
        )
        
        self.logger.debug(f"    ✅ Created MenuItem: {menu_item.name} (ID: {menu_item.id[:8]}...)")
        return menu_item
    
    def transform_menu_item_variation(
        self,
        menu_item_id: str,
        restaurant_brand_id: str,
        serving_data: Dict,
        allergens: Dict = None,
        ingredients: str = None,
        image_url: str = None
    ) -> MenuItemVariation:
        """
        Transform serving size data to MenuItemVariation entity.
        
        Args:
            menu_item_id: ID of parent menu item
            restaurant_brand_id: ID of restaurant brand (denormalized)
            serving_data: Scraped serving size data
            allergens: Allergen dict from parent menu item (denormalized)
            ingredients: Ingredients text from parent menu item
            image_url: Image URL for this variation
        
        Returns:
            MenuItemVariation entity
        """
        from config.models import AllergenInfo
        
        # Parse serving size
        size_label = serving_data.get('size_label', '1 serving')
        
        # Extract and parse nutrition data
        nutrition_dict = serving_data.get('nutrition', {})
        nutrition = self.transform_nutrition(nutrition_dict)
        
        # Parse serving size from the serving_size field in nutrition
        serving_size_text = nutrition_dict.get('serving_size', '')
        serving = self.parser.parse_serving_size(serving_size_text)
        
        # Convert allergen dict to AllergenInfo model
        allergen_info = AllergenInfo(**allergens) if allergens else AllergenInfo()
        
        # Get image URL from serving data if available
        variation_image = serving_data.get('image_url') or image_url
        
        variation = MenuItemVariation(
            menu_item_id=menu_item_id,
            restaurant_brand_id=restaurant_brand_id,
            label=size_label,
            serving=serving,
            nutrition=nutrition,
            allergens=allergen_info,
            ingredients=ingredients,
            image_url=variation_image,
            is_active=True,
        )
        
        self.logger.debug(f"      ✅ Created Variation: {size_label} (ID: {variation.id[:8]}...)")
        return variation
    
    def transform(self, scraped_data: Dict[str, Dict]) -> Tuple[
        RestaurantCollection,
        MenuItemCollection,
        MenuItemVariationCollection
    ]:
        """
        Transform complete scraped dataset into normalized collections.
        
        Args:
            scraped_data: Dictionary mapping restaurant names to their data
        
        Returns:
            Tuple of (restaurants, menu_items, variations) collections
        """
        self.logger.info("🔄 Starting data transformation...")
        
        restaurants = RestaurantCollection()
        menu_items = MenuItemCollection()
        variations = MenuItemVariationCollection()
        
        total_restaurants = len(scraped_data)
        
        for idx, (restaurant_name, restaurant_data) in enumerate(scraped_data.items(), 1):
            self.logger.info(f"\n[{idx}/{total_restaurants}] 🏪 Transforming: {restaurant_name}")
            
            # Transform restaurant
            restaurant = self.transform_restaurant(restaurant_name, restaurant_data)
            restaurants.add(restaurant)
            
            # Transform menu items and variations
            items_data = restaurant_data.get('items', [])
            self.logger.info(f"  📋 Processing {len(items_data)} menu items...")
            
            for item_data in items_data:
                # Transform menu item
                menu_item = self.transform_menu_item(restaurant.id, item_data)
                menu_items.add(menu_item)
                
                # Extract allergens dict from scraped data
                allergens_data = item_data.get('allergens', {})
                
                # Extract ingredients text
                ingredients_text = item_data.get('ingredients', '')
                if not isinstance(ingredients_text, str):
                    ingredients_text = None
                
                # Transform serving size variations
                serving_sizes = item_data.get('nutritional_values', {}).get('serving_sizes', [])
                
                for serving_data in serving_sizes:
                    variation = self.transform_menu_item_variation(
                        menu_item.id,
                        restaurant.id,
                        serving_data,
                        allergens=allergens_data if allergens_data else None,
                        ingredients=ingredients_text,
                        image_url=None  # Images come from serving_data
                    )
                    variations.add(variation)
            
            self.logger.info(
                f"  ✅ Completed: "
                f"{len(items_data)} items, "
                f"{sum(len(item.get('nutritional_values', {}).get('serving_sizes', [])) for item in items_data)} variations"
            )
        
        self.logger.info("\n" + "="*70)
        self.logger.info("📊 Transformation Summary:")
        self.logger.info(f"  🏪 Restaurants: {restaurants.total_count}")
        self.logger.info(f"  🍽️  Menu Items: {menu_items.total_count}")
        self.logger.info(f"  📏 Variations: {variations.total_count}")
        self.logger.info("="*70)
        
        return restaurants, menu_items, variations

