"""
AI Menu Item Enrichment - Combined Description, Price, and Cuisine Types
Combines processors 02 and 03 for cost optimization.
"""
import os
import json
import time
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI, RateLimitError, APIError
from tqdm import tqdm

load_dotenv()

log_dir = Path('logging')
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'menu_enrichment.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class MenuItemEnricher:
    """Enriches menu items with description, price, and cuisine types in single API call."""
    
    def __init__(self):
        self.client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        self.model = "gpt-4o-mini"
    
    def enrich_menu_item(
        self, 
        item_name: str, 
        category: str, 
        restaurant_name: str,
        retries: int = 3
    ) -> Dict[str, Any]:
        """Generate description, price, and cuisine types in one call."""
        
        prompt = f"""For this restaurant menu item, provide:
1. A mouth-watering description (1-2 sentences, under 200 chars)
2. Estimated price in USD
3. Three cuisine type tags

Item: {item_name}
Category: {category}
Restaurant: {restaurant_name}

Guidelines:
- Description: Appetizing, concise, highlight key ingredients/flavors
- Price: Realistic for this type of restaurant and item
- Cuisine Types: Choose 3 from: American, Mexican, Italian, Asian, Mediterranean, International, etc.

Respond ONLY with valid JSON:
{{"description": "...", "estimated_price": "8.99", "cuisine_types": ["American", "Mexican", "International"]}}"""

        for attempt in range(retries):
            try:
                if attempt > 0:
                    delay = 2 ** (attempt - 1)
                    time.sleep(delay)
                
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "You are a restaurant menu expert. Respond only with valid JSON."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.5,
                    max_tokens=200
                )
                
                result_text = response.choices[0].message.content.strip()
                result = json.loads(result_text)
                
                # Validate required fields
                if all(k in result for k in ['description', 'estimated_price', 'cuisine_types']):
                    if isinstance(result['cuisine_types'], list) and len(result['cuisine_types']) == 3:
                        logger.info(f"Enriched '{item_name}': ${result['estimated_price']}")
                        return result
                    
            except RateLimitError:
                logger.warning(f"Rate limit hit, retry {attempt + 1}/{retries}")
                time.sleep(5 * (attempt + 1))
            except Exception as e:
                logger.error(f"Error enriching '{item_name}': {e}")
            
            if attempt < retries - 1:
                time.sleep(1)
        
        # Fallback
        return {
            "description": f"A delicious {category.lower()} item from {restaurant_name}.",
            "estimated_price": "9.99",
            "cuisine_types": ["American", "International", "Casual"]
        }
    
    def process_menu_items_file(
        self,
        input_file: str,
        output_file: str = None,
        force_regenerate: bool = False
    ) -> Dict[str, Any]:
        """Process menu items file."""
        
        with open(input_file, 'r', encoding='utf-8') as f:
            items = json.load(f)
        
        restaurant_name = items[0].get('restaurant_brand_id', 'Restaurant') if items else 'Restaurant'
        
        stats = {'total': len(items), 'processed': 0, 'skipped': 0, 'errors': 0}
        
        for item in tqdm(items, desc="Enriching menu items"):
            try:
                # Skip if already enriched and not forcing
                if not force_regenerate and all(item.get(k) for k in ['description', 'estimated_price', 'cuisine_types']):
                    stats['skipped'] += 1
                    continue
                
                enrichment = self.enrich_menu_item(
                    item.get('name', 'Unknown'),
                    item.get('category', 'Food'),
                    restaurant_name
                )
                
                item.update(enrichment)
                stats['processed'] += 1
                
            except Exception as e:
                logger.error(f"Failed to enrich {item.get('name')}: {e}")
                stats['errors'] += 1
        
        # Save output
        output_path = output_file or input_file.replace('.json', '_enriched.json')
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(items, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Processed: {stats['processed']}, Skipped: {stats['skipped']}, Errors: {stats['errors']}")
        return {'output_file': output_path, 'stats': stats}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Enrich menu items with AI')
    parser.add_argument('input_file', help='Input JSON file')
    parser.add_argument('-o', '--output', help='Output file', default=None)
    parser.add_argument('-f', '--force', help='Force regeneration', action='store_true')
    args = parser.parse_args()
    
    enricher = MenuItemEnricher()
    result = enricher.process_menu_items_file(args.input_file, args.output, args.force)
    print(f"\n✅ Complete! Output: {result['output_file']}")

