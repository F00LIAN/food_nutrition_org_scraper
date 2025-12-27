"""
Pydantic validators for post-processed data before MongoDB upload.
Ensures data integrity and prevents malformed data from reaching the database.
"""
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator, ValidationError, ConfigDict
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


# ==================== Restaurant Brand Models ====================

class RestaurantBrand(BaseModel):
    """Validate restaurant brand data."""
    model_config = ConfigDict(
        extra="allow",
        populate_by_name=True
    )
    
    id: str = Field(..., min_length=1, alias="_id")
    brand_name: str = Field(..., min_length=1)
    restaurant_category: Optional[str] = None
    restaurant_cultural_cuisine: Optional[str] = None
    source_url: Optional[str] = None
    creation_date: Optional[str] = None
    last_updated: Optional[str] = None
    
    @field_validator('id', 'brand_name')
    @classmethod
    def not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError('Field cannot be empty')
        return v


# ==================== Menu Item Models ====================

class MenuItem(BaseModel):
    """Validate menu item data."""
    model_config = ConfigDict(
        extra="allow",
        populate_by_name=True
    )
    
    id: str = Field(..., min_length=1, alias="_id")
    restaurant_brand_id: str = Field(..., min_length=1)
    name: str = Field(..., min_length=1)
    description: Optional[str] = None
    category: Optional[str] = None
    estimated_price: Optional[str] = None
    cuisine_types: Optional[List[str]] = None
    macronutrient_profile: Optional[List[str]] = None
    menu_item_image_url: Optional[str] = None
    is_active: bool = True
    source_url: Optional[str] = None
    
    @field_validator('id', 'restaurant_brand_id', 'name')
    @classmethod
    def not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError('Required field cannot be empty')
        return v
    
    @field_validator('description')
    @classmethod
    def description_reasonable_length(cls, v):
        """Ensure description is reasonable length if present."""
        if v and len(v) > 500:
            logger.warning(f"Description too long ({len(v)} chars), truncating")
            return v[:500]
        return v
    
    @field_validator('estimated_price')
    @classmethod
    def price_format(cls, v):
        """Validate price format if present."""
        if v:
            # Remove $ and whitespace
            cleaned = v.strip().replace('$', '').replace(',', '')
            try:
                float(cleaned)
                return v
            except ValueError:
                logger.warning(f"Invalid price format: {v}")
                return None
        return v


# ==================== Menu Item Variation Models ====================

class Nutrition(BaseModel):
    """Validate nutrition data."""
    model_config = ConfigDict(extra="allow")
    
    calories: Optional[float] = None
    protein_g: Optional[float] = None
    carbs_g: Optional[float] = None
    fat_g: Optional[float] = None
    sodium_mg: Optional[float] = None
    sugars_g: Optional[float] = None
    fiber_g: Optional[float] = None
    saturated_fat_g: Optional[float] = None
    cholesterol_mg: Optional[float] = None
    
    @field_validator('*', mode='before')
    @classmethod
    def ensure_non_negative(cls, v):
        """Ensure all nutrition values are non-negative."""
        if v is not None:
            try:
                num_val = float(v)
                if num_val < 0:
                    logger.warning(f"Negative nutrition value: {v}, setting to None")
                    return None
                return num_val
            except (ValueError, TypeError):
                logger.warning(f"Invalid nutrition value: {v}, setting to None")
                return None
        return v


class MenuItemVariation(BaseModel):
    """Validate menu item variation data."""
    model_config = ConfigDict(
        extra="allow",
        populate_by_name=True
    )
    
    id: str = Field(..., min_length=1, alias="_id")
    menu_item_id: str = Field(..., min_length=1)
    nutrition: Optional[Nutrition] = None
    golden_ratio: Optional[float] = None
    golden_ratio_category: Optional[str] = None
    
    @field_validator('id', 'menu_item_id')
    @classmethod
    def not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError('Required field cannot be empty')
        return v
    
    @field_validator('golden_ratio')
    @classmethod
    def validate_golden_ratio(cls, v):
        """Validate golden ratio is reasonable."""
        if v is not None:
            if v < 0 or v > 100:  # Reasonable bounds
                logger.warning(f"Golden ratio out of bounds: {v}, setting to None")
                return None
        return v
    
    @field_validator('golden_ratio_category')
    @classmethod
    def validate_category(cls, v):
        """Validate golden ratio category."""
        if v is not None:
            valid_categories = ['Excellent', 'Good', 'Poor']
            if v not in valid_categories:
                logger.warning(f"Invalid golden ratio category: {v}")
                return None
        return v


# ==================== Validation Functions ====================

def validate_brands(brands_data: List[Dict]) -> tuple[List[Dict], List[Dict]]:
    """
    Validate restaurant brands data.
    
    Args:
        brands_data: List of brand dictionaries
        
    Returns:
        Tuple of (valid_brands, invalid_brands)
    """
    valid = []
    invalid = []
    
    for brand in brands_data:
        try:
            validated = RestaurantBrand(**brand)
            # Use by_alias=True to export 'id' as '_id' for MongoDB
            valid.append(validated.model_dump(by_alias=True))
        except ValidationError as e:
            logger.error(f"Brand validation failed for {brand.get('brand_name', 'unknown')}: {e}")
            invalid.append({'data': brand, 'errors': str(e)})
    
    logger.info(f"Brands validation: {len(valid)} valid, {len(invalid)} invalid")
    return valid, invalid


def validate_menu_items(items_data: List[Dict]) -> tuple[List[Dict], List[Dict]]:
    """
    Validate menu items data.
    
    Args:
        items_data: List of menu item dictionaries
        
    Returns:
        Tuple of (valid_items, invalid_items)
    """
    valid = []
    invalid = []
    
    for item in items_data:
        try:
            validated = MenuItem(**item)
            # Use by_alias=True to export 'id' as '_id' for MongoDB
            valid.append(validated.model_dump(by_alias=True))
        except ValidationError as e:
            logger.error(f"Menu item validation failed for {item.get('name', 'unknown')}: {e}")
            invalid.append({'data': item, 'errors': str(e)})
    
    logger.info(f"Menu items validation: {len(valid)} valid, {len(invalid)} invalid")
    return valid, invalid


def validate_variations(variations_data: List[Dict]) -> tuple[List[Dict], List[Dict]]:
    """
    Validate menu item variations data.
    
    Args:
        variations_data: List of variation dictionaries
        
    Returns:
        Tuple of (valid_variations, invalid_variations)
    """
    valid = []
    invalid = []
    
    for variation in variations_data:
        try:
            validated = MenuItemVariation(**variation)
            # Use by_alias=True to export 'id' as '_id' for MongoDB
            valid.append(validated.model_dump(by_alias=True))
        except ValidationError as e:
            logger.error(f"Variation validation failed for {variation.get('_id', 'unknown')}: {e}")
            invalid.append({'data': variation, 'errors': str(e)})
    
    logger.info(f"Variations validation: {len(valid)} valid, {len(invalid)} invalid")
    return valid, invalid


def save_validation_report(invalid_data: Dict[str, List], output_dir: str):
    """
    Save validation error report to file.
    
    Args:
        invalid_data: Dictionary with invalid data by collection
        output_dir: Directory to save report
    """
    import json
    from pathlib import Path
    
    report_file = Path(output_dir) / f"validation_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(invalid_data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Validation error report saved to: {report_file}")
    return report_file

