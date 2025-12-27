# Fast Food Nutrition Scraper

A production-ready pipeline for scraping, enriching, and uploading restaurant nutritional data from fastfoodnutrition.org to MongoDB. Features modular architecture, AI enrichment, and automated data validation.

## 🚀 Quick Start

### Interactive Pipeline Manager

```bash
# Install dependencies
pip install -r requirements.txt

# Run interactive menu
python example_usage.py
```

Choose from three pipeline modes:
- **Single Restaurant** (~2 min) - Quick test with 1 restaurant
- **Five Restaurants** (~10 min) - Medium test with 5 restaurants
- **Full Pipeline** (hours) - Complete production dataset

Each mode supports:
1. **Scrape Only** - Fetch raw data from website
2. **Post-Process Only** - AI enrichment on existing data
3. **Upload Only** - Push to MongoDB
4. **Full Pipeline** - All three steps

### Environment Setup

```bash
# Copy example environment file
cp env.example .env

# Configure required variables
OPENAI_API_KEY=sk-...                           # Required for AI enrichment
MONGODB_CONNECTION_STRING=mongodb+srv://...     # Required for upload
TEST_DATABASE_NAME=restaurant_v2_test           # Optional
PRODUCTION_DATABASE_NAME=restaurant_v2          # Optional
```

## 🏗️ Architecture

```
┌─────────────────┐
│  example_usage  │  Interactive Pipeline Manager
│      .py        │  (Single/Five/Full Restaurant Modes)
└────────┬────────┘
         │
    ┌────┴─────────────────────────────┐
    │                                  │
┌───▼──────────┐          ┌────────────▼──────────┐
│   main.py    │          │  run_processors.py    │
│   SCRAPER    │          │  AI ENRICHMENT        │
└───┬──────────┘          └────────────┬──────────┘
    │                                  │
    ├── restaurant_scraper             ├── 01_brand_category_cuisine
    ├── menu_item_scraper              ├── 02_menu_item_enrichment
    └── nutrition_scraper              ├── 03_macronutrient_profile
                                       ├── 04_golden_ratio
                                       └── 05_menu_item_images
                │                                  │
                └────────────┬─────────────────────┘
                             │
                    ┌────────▼────────┐
                    │  mongodb_       │
                    │  uploader.py    │
                    │                 │
                    │  + validators   │
                    └─────────────────┘
```

## 📦 Three-Stage Pipeline

### Stage 1: Web Scraping (`main.py`)

Scrapes fastfoodnutrition.org with retry logic, rate limiting, and checkpointing.

**Scrapers:**
- `restaurant_scraper.py` - Brand listings, logos, URLs
- `menu_item_scraper.py` - Menu items, categories
- `nutrition_scraper.py` - Nutrition facts, allergens, serving sizes

**Output Files:**
```
output/{mode}/
├── restaurant_brands_TIMESTAMP.json
├── menu_items_TIMESTAMP.json
└── menu_item_variations_TIMESTAMP.json
```

**Run Standalone:**
```python
from main import FastFoodNutritionScraper
from config.config import ScraperSettings

settings = ScraperSettings(
    specific_restaurants=["McDonald's", "Taco Bell"],
    max_items_per_restaurant=50,
    output_dir="output/example/test",
    log_level="INFO"
)

scraper = FastFoodNutritionScraper(settings)
scraper.run_full_pipeline()
```

### Stage 2: AI Enrichment (`run_processors.py`)

Enriches scraped data using OpenAI API and proprietary algorithms.

**Processors:**

1. **Brand Category & Cuisine** - Classifies restaurants (Fast Food/Casual Dining + American/Mexican/Asian)
2. **Menu Item Enrichment** - Generates descriptions, estimates prices, identifies cuisine types
3. **Macronutrient Profile** - Tags items (High Protein, Low Carb, Keto Friendly, etc.)
4. **Golden Ratio** - Calculates optimal nutrition efficiency scores
5. **Menu Item Images** - Maps images from variations to parent items

**Output Files:**
```
output/{mode}/post_processed/
├── restaurant_brands_TIMESTAMP_enriched.json
├── menu_items_TIMESTAMP_enriched.json
└── menu_item_variations_TIMESTAMP_enriched.json
```

**Run Standalone:**
```bash
# Process latest scraped files
python run_processors.py -d output/example/test

# Skip AI processors (faster, no API costs)
python run_processors.py -d output/example/test --skip-ai
```

### Stage 3: MongoDB Upload (`mongodb_uploader.py`)

Uploads enriched data with Pydantic validation and upsert logic.

**Features:**
- Pre-upload validation against Pydantic models
- Upsert operations (replaces existing data monthly)
- Detailed validation error reports
- Atomic bulk operations

**Collections:**
- `restaurant_brands` - Restaurant metadata
- `menu_items` - Base menu items (no nutrition)
- `menu_item_variations` - Serving size variations with nutrition data

**Run Standalone:**
```bash
python run_processors.py -d output/example/test \
  --upload-mongodb \
  --mongodb-connection "mongodb+srv://..." \
  --mongodb-database restaurant_v2_test
```

**Programmatic:**
```python
from utils.mongodb_uploader import MongoDBUploader
from pathlib import Path

uploader = MongoDBUploader(
    connection_string="mongodb+srv://...",
    database_name="restaurant_v2_test"
)

results = uploader.upload_post_processed_directory(
    Path("output/example/test/post_processed")
)
uploader.close()
```

## 🎯 Data Models

### Restaurant Brand
```json
{
  "_id": "a1b2c3...",
  "brand_name": "McDonald's",
  "brand_image_url": "https://...",
  "restaurant_category": "Fast Food",
  "restaurant_cultural_cuisine": "American",
  "category_source": "lookup",
  "eatery_full_verification_status": false,
  "creation_date": "2024-01-01T00:00:00",
  "last_updated": "2024-01-01T00:00:00"
}
```

### Menu Item
```json
{
  "_id": "d4e5f6...",
  "restaurant_brand_id": "a1b2c3...",
  "name": "Big Mac",
  "description": "Two all-beef patties...",
  "category": "Burgers",
  "estimated_price": "$5.99",
  "cuisine_types": ["American", "Burgers"],
  "macronutrient_profile": ["High Protein", "Balanced"],
  "source_url": "https://...",
  "is_active": true
}
```

### Menu Item Variation
```json
{
  "_id": "g7h8i9...",
  "menu_item_id": "d4e5f6...",
  "restaurant_brand_id": "a1b2c3...",
  "label": "Large",
  "serving": {
    "serving_text": "420g (14.8oz)"
  },
  "nutrition": {
    "calories": 550,
    "protein_g": 25,
    "carbs_g": 45,
    "fat_g": 30,
    "sodium_mg": 1040
  },
  "allergens": {
    "contains": ["gluten", "dairy"],
    "does_not_contain": ["shellfish"],
    "unknown": []
  },
  "image_url": "https://...",
  "is_active": true
}
```

## 🔧 Configuration

### Scraper Settings (`config/config.py`)

```python
ScraperSettings(
    # Network
    base_url="https://fastfoodnutrition.org",
    max_retries=3,
    retry_delay=1.0,  # seconds
    timeout=30,
    rate_limit_delay=0.5,
    
    # Output
    output_dir="output",
    checkpoint_dir="output/checkpoints",
    enable_checkpointing=True,
    
    # Behavior
    resume_from_checkpoint=True,
    specific_restaurants=None,  # List or None for all
    max_restaurants=None,
    max_items_per_restaurant=None,
    
    # Logging
    log_level="INFO",
    log_file="scraper.log",
    
    # Transform
    normalize_data=True,
    export_formats=["json"]
)
```

### Environment Variables

```bash
# Scraper Configuration
SCRAPER_BASE_URL=https://fastfoodnutrition.org
SCRAPER_MAX_RETRIES=3
SCRAPER_RATE_LIMIT=0.5
SCRAPER_OUTPUT_DIR=output
SCRAPER_LOG_LEVEL=INFO
SCRAPER_MAX_RESTAURANTS=10
SCRAPER_MAX_ITEMS_PER_RESTAURANT=50

# API Keys
OPENAI_API_KEY=sk-...

# MongoDB
MONGODB_CONNECTION_STRING=mongodb+srv://...
TEST_DATABASE_NAME=restaurant_v2_test
PRODUCTION_DATABASE_NAME=restaurant_v2
```

## 🛠️ Key Features

### Implemented

✅ **Web Scraping**
- curl_cffi for CloudFlare bypass
- Exponential backoff retry logic
- Rate limiting and checkpointing
- Multi-serving size support
- Allergen extraction

✅ **AI Enrichment**
- Restaurant category/cuisine classification
- Menu item descriptions and price estimation
- Macronutrient profile tagging (8+ categories)
- Golden ratio scoring algorithm
- Image mapping across variations

✅ **Data Management**
- Pydantic models for validation
- Normalized relational schema
- MongoDB upsert operations
- Validation error reporting
- Progress tracking with tqdm

✅ **Infrastructure**
- Modular architecture (ABC pattern)
- Comprehensive logging
- Interactive pipeline manager
- Environment-based configuration
- Graceful error handling

## 📊 AI Enrichment Details

### Macronutrient Profile Tags

Generated using dual criteria (absolute grams + calorie percentages):

- **High Protein** - ≥25g AND ≥30% calories from protein
- **Low Carb** - ≤20g AND ≤30% calories from carbs
- **Low Fat** - ≤10g AND ≤25% calories from fat
- **High Fiber** - ≥5g
- **Low Sugar** - ≤5g AND ≤10% calories from sugar
- **High Sodium** - ≥700mg (warning tag)
- **Keto Friendly** - ≤20g carbs, ≥70% fat calories
- **Balanced** - No extreme macros (fallback)

### Golden Ratio Score

Proprietary algorithm scoring nutritional efficiency (0-100):

```python
golden_ratio = (
    protein_density * protein_weight +
    fiber_density * fiber_weight -
    sugar_penalty -
    sodium_penalty -
    sat_fat_penalty
) * calorie_factor
```

Higher scores indicate better nutrition per calorie.

## 📁 Project Structure

```
Fast_Food_Nutrition_Scraper/
├── config/
│   ├── config.py              # Configuration management
│   └── models.py              # Pydantic data models
├── scrapers/
│   ├── base_scraper.py        # Abstract base class
│   ├── restaurant_scraper.py
│   ├── menu_item_scraper.py
│   └── nutrition_scraper.py
├── processors/
│   ├── 01_ai_brand_category_cuisine_.py
│   ├── 02_ai_enrich_menu_items.py
│   ├── 03_macronutrient_profile.py
│   ├── 04_golden_ratio.py
│   └── 05_map_menu_item_images.py
├── src/
│   ├── data_transformer.py    # Schema transformation
│   └── utils.py               # Persistence utilities
├── utils/
│   ├── mongodb_uploader.py    # MongoDB operations
│   ├── validators.py          # Pydantic validation
│   └── retry_handler.py       # Retry logic
├── data/
│   └── restaurant_categories/ # Pre-mapped categories
├── main.py                    # Scraper orchestrator
├── run_processors.py          # Enrichment pipeline
├── example_usage.py           # Interactive manager
└── requirements.txt
```

## 📝 Dependencies

```txt
# Web Scraping
curl_cffi>=0.14.0
beautifulsoup4>=4.14.3

# Data Models
pydantic>=2.12.5

# AI Enrichment
openai>=2.14.0

# Database
pymongo>=4.15.5

# Utilities
python-dotenv>=1.2.1
tqdm>=4.67.1
```

## 🚨 Error Handling

### Validation Errors

Invalid documents are excluded from upload and logged:

```
output/{mode}/post_processed/validation_errors.json
```

### Retry Logic

Network failures trigger exponential backoff:
- Max retries: 3 (configurable)
- Base delay: 1.0s
- Formula: `delay * (2 ** attempt)`

### Checkpointing

Resume interrupted scraping:
```
output/{mode}/checkpoints/{restaurant_name}.json
```

## 📈 Performance

### Typical Runtimes

| Mode | Restaurants | Items | Scrape | Enrich | Total |
|------|-------------|-------|--------|--------|-------|
| Single | 1 | ~50 | 30s | 1m | 2m |
| Five | 5 | ~500 | 3m | 7m | 10m |
| Full | ~200 | ~10k | 2h | 4h | 6h+ |

### Optimization Tips

1. **Skip AI for testing**: `--skip-ai` flag
2. **Limit items**: `max_items_per_restaurant=50`
3. **Use checkpoints**: `resume_from_checkpoint=True`
4. **Parallel processing**: Future enhancement

## 🤝 Contributing

Priority areas:
1. Async/parallel scraping
2. Additional data sources (prices, locations)
3. Enhanced error recovery
4. Performance optimization
5. Unit tests

## 📄 License

MIT License

---

**Built with** ❤️ **for the Eatery Database Project**