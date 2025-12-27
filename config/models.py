"""
Pydantic models for fast food nutrition data.

This module defines the data models used throughout the scraping pipeline,
providing validation, serialization, and type safety.
"""

from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, HttpUrl, field_validator
from enum import Enum
import uuid
import hashlib


def generate_id(*keys: str) -> str:
    """Generate deterministic ID from composite key parts."""
    composite = "|".join(str(k) for k in keys if k)
    return hashlib.sha256(composite.encode()).hexdigest()[:32]


class CurrencyEnum(str, Enum):
    """Supported currency types."""
    USD = "USD"
    CAD = "CAD"
    EUR = "EUR"


class AllergenEnum(str, Enum):
    """Common allergens."""
    GLUTEN = "gluten"
    DAIRY = "dairy"
    EGGS = "eggs"
    SOY = "soy"
    TREE_NUTS = "tree_nuts"
    PEANUTS = "peanuts"
    FISH = "fish"
    SHELLFISH = "shellfish"
    WHEAT = "wheat"
    SESAME = "sesame"


class DietaryLabelEnum(str, Enum):
    """Dietary classification labels."""
    VEGETARIAN = "vegetarian"
    VEGAN = "vegan"
    GLUTEN_FREE = "gluten_free"
    DAIRY_FREE = "dairy_free"
    KETO = "keto"
    LOW_CARB = "low_carb"
    HIGH_PROTEIN = "high_protein"
    HALAL = "halal"
    KOSHER = "kosher"


# ============================================================================
# SCRAPED DATA MODELS (Raw from website)
# ============================================================================

class ScrapedNutrition(BaseModel):
    """Raw nutrition data as scraped from website."""
    serving_size: Optional[str] = None
    calories: Optional[str] = None
    total_fat: Optional[str] = None
    saturated_fat: Optional[str] = None
    trans_fat: Optional[str] = None
    cholesterol: Optional[str] = None
    sodium: Optional[str] = None
    total_carbohydrates: Optional[str] = None
    dietary_fiber: Optional[str] = None
    sugars: Optional[str] = None
    added_sugars: Optional[str] = None
    protein: Optional[str] = None
    vitamin_d: Optional[str] = None
    calcium: Optional[str] = None
    iron: Optional[str] = None
    potassium: Optional[str] = None
    
    class Config:
        extra = "allow"  # Allow additional fields from scraping


class ScrapedAllergens(BaseModel):
    """Raw allergen data as scraped from website."""
    contains: List[str] = Field(default_factory=list)
    does_not_contain: List[str] = Field(default_factory=list)
    unknown: List[str] = Field(default_factory=list)
    allergy_information: str = ""


class ScrapedServingSize(BaseModel):
    """A single serving size variation with nutrition data."""
    size_label: str  # e.g., "Small", "12 oz", "Container"
    nutrition: ScrapedNutrition


class ScrapedMenuItem(BaseModel):
    """Raw menu item data as scraped from website."""
    name: str
    url: HttpUrl
    category: str
    nutritional_values: Dict[str, List[ScrapedServingSize]]
    allergens: ScrapedAllergens
    
    # Fields to be added (currently pseudocode gaps)
    description: Optional[str] = None
    image_url: Optional[HttpUrl] = None


class ScrapedRestaurant(BaseModel):
    """Raw restaurant data as scraped from website."""
    name: str
    url: HttpUrl
    restaurant_logo: Optional[HttpUrl] = None
    items: List[ScrapedMenuItem] = Field(default_factory=list)


# ============================================================================
# TARGET DATABASE MODELS (Normalized structure)
# ============================================================================

class RestaurantBrand(BaseModel):
    """Restaurant brand entity for database."""
    id: str = Field(default="", alias="_id", serialization_alias="_id")
    brand_name: str
    brand_image_url: Optional[HttpUrl] = None
    source_url: Optional[HttpUrl] = None  # Added for traceability
    
    def __init__(self, **data):
        super().__init__(**data)
        if not self.id:
            self.id = generate_id(self.brand_name)
    restaurant_category: Optional[str] = None  # e.g., "Fast Food", "Fast Casual", "Casual Dining"
    restaurant_cultural_cuisine: Optional[str] = None  # e.g., "American", "Mexican", "Asian"
    category_source: Optional[str] = None  # "lookup" or "ai_generated" for tracking
    eatery_full_verification_status: bool = False
    creation_date: datetime = Field(default_factory=datetime.utcnow)
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            HttpUrl: lambda v: str(v)
        }


class MenuItem(BaseModel):
    """Base menu item entity for database."""
    id: str = Field(default="", alias="_id", serialization_alias="_id")
    restaurant_brand_id: str
    name: str
    description: Optional[str] = None
    category: str
    estimated_price: Optional[str] = None
    cuisine_types: Optional[List[str]] = None
    macronutrient_profile: Optional[List[str]] = None
    is_active: bool = True
    source_url: Optional[HttpUrl] = None  # Added for traceability
    
    def __init__(self, **data):
        super().__init__(**data)
        if not self.id:
            self.id = generate_id(self.restaurant_brand_id, self.name, str(self.source_url or ""))
    
    class Config:
        json_encoders = {
            HttpUrl: lambda v: str(v)
        }


class ServingInfo(BaseModel):
    """Serving size as raw text only."""
    serving_text: Optional[str] = None


class NutritionInfo(BaseModel):
    """Structured nutrition information (normalized to numbers)."""
    calories: Optional[float] = None
    protein_g: Optional[float] = None
    carbs_g: Optional[float] = None
    fat_g: Optional[float] = None
    saturated_fat_g: Optional[float] = None
    trans_fat_g: Optional[float] = None
    cholesterol_mg: Optional[float] = None
    sodium_mg: Optional[float] = None
    fiber_g: Optional[float] = None
    sugars_g: Optional[float] = None
    added_sugars_g: Optional[float] = None
    vitamin_d_mcg: Optional[float] = None
    calcium_mg: Optional[float] = None
    iron_mg: Optional[float] = None
    potassium_mg: Optional[float] = None


class AllergenInfo(BaseModel):
    """Allergen information structure."""
    contains: List[str] = Field(default_factory=list)
    does_not_contain: List[str] = Field(default_factory=list)
    unknown: List[str] = Field(default_factory=list)
    allergy_information: Optional[str] = None


class MenuItemVariation(BaseModel):
    """Menu item variation entity for database."""
    id: str = Field(default="", alias="_id", serialization_alias="_id")
    menu_item_id: str
    restaurant_brand_id: str  # Denormalized for query performance
    label: str  # "Small", "Large", "12 oz", etc.
    serving: Optional[ServingInfo] = None
    nutrition: NutritionInfo
    allergens: AllergenInfo = Field(default_factory=AllergenInfo)
    ingredients: Optional[str] = None  # Ingredients as text
    image_url: Optional[HttpUrl] = None  # Item image for this variation
    is_active: bool = True
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    def __init__(self, **data):
        super().__init__(**data)
        if not self.id:
            self.id = generate_id(self.menu_item_id, self.label)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            HttpUrl: lambda v: str(v)
        }


class PriceInfo(BaseModel):
    """Price information structure."""
    amount: float
    currency: CurrencyEnum = CurrencyEnum.USD


class LocationMenuVariation(BaseModel):
    """Location-specific menu variation data."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    location_id: str
    variation_id: str
    price: Optional[PriceInfo] = None
    is_available: bool = True
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


# ============================================================================
# INTERMEDIATE PROCESSING MODELS
# ============================================================================

class ParsedNutritionValue(BaseModel):
    """Parsed nutrition value with unit and numeric value."""
    value: Optional[float] = None
    unit: Optional[str] = None
    raw_text: str


class NutritionParseResult(BaseModel):
    """Result of parsing nutrition text."""
    success: bool
    parsed_value: Optional[ParsedNutritionValue] = None
    error_message: Optional[str] = None


class ServingSizeParseResult(BaseModel):
    """Result of parsing serving size text."""
    success: bool
    grams: Optional[float] = None
    ounces: Optional[float] = None
    milliliters: Optional[float] = None
    error_message: Optional[str] = None


# ============================================================================
# COLLECTION MODELS (For batch operations)
# ============================================================================

class RestaurantCollection(BaseModel):
    """Collection of restaurant brands."""
    restaurants: List[RestaurantBrand] = Field(default_factory=list)
    total_count: int = 0
    
    def add(self, restaurant: RestaurantBrand) -> None:
        """Add a restaurant to the collection."""
        self.restaurants.append(restaurant)
        self.total_count = len(self.restaurants)


class MenuItemCollection(BaseModel):
    """Collection of menu items."""
    items: List[MenuItem] = Field(default_factory=list)
    total_count: int = 0
    
    def add(self, item: MenuItem) -> None:
        """Add a menu item to the collection."""
        self.items.append(item)
        self.total_count = len(self.items)


class MenuItemVariationCollection(BaseModel):
    """Collection of menu item variations."""
    variations: List[MenuItemVariation] = Field(default_factory=list)
    total_count: int = 0
    
    def add(self, variation: MenuItemVariation) -> None:
        """Add a variation to the collection."""
        self.variations.append(variation)
        self.total_count = len(self.variations)

