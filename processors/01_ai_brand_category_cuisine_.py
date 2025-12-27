""" 
AI Generator for Restaurant Category, Cultural Cuisine, and Menu Item Description

File Input: 
"""
import os
import json
import time
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI, RateLimitError, APIError
from tqdm import tqdm

# Load environment variables
load_dotenv()

# Configure logging
from pathlib import Path
log_dir = Path('logging')
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'brand_category_cuisine.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


### Map the restaurant name to the restaurant_category and restaurant_cultural_cuisine from the restaurant_categories.json file
### If not found, use the AI generator to generate the restaurant_category and restaurant_cultural_cuisine
class BrandCategoryCuisineGenerator:
    """
    Enriches restaurant brand data with category and cultural cuisine information.
    
    Process:
    1. Load lookup data from restaurant_categories.json
    2. For each brand, try to match by name
    3. If found: use pre-mapped values
    4. If not found: use OpenAI to generate values
    """
    
    def __init__(self, categories_file: str = "data/restaurant_categories/restaurant_categories.json"):
        """
        Initialize the generator.
        
        Args:
            categories_file: Path to the restaurant categories lookup JSON file
        """
        self.categories_file = Path(categories_file)
        self.lookup_data = self._load_lookup_data()
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = "gpt-4o-mini"  # Cost-effective model for classification
        
    def _load_lookup_data(self) -> Dict[str, Dict[str, str]]:
        """
        Load and index the restaurant categories lookup data.
        
        Returns:
            Dictionary mapping lowercase brand names to category/cuisine data
        """
        try:
            with open(self.categories_file, 'r', encoding='utf-8') as f:
                categories = json.load(f)
            
            # Create a lookup dictionary with lowercase names as keys
            lookup = {}
            for item in categories:
                name_key = item['name'].lower().strip()
                lookup[name_key] = {
                    'restaurant_category': item['restaurant_category'],
                    'restaurant_cultural_cuisine': item['restaurant_cultural_cuisine'],
                    'url': item.get('url', '')
                }
            
            logger.info(f"Loaded {len(lookup)} restaurant categories from lookup file")
            return lookup
            
        except FileNotFoundError:
            logger.warning(f"Categories file not found: {self.categories_file}")
            return {}
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing categories file: {e}")
            return {}
    
    def _lookup_category_cuisine(self, brand_name: str) -> Optional[Dict[str, str]]:
        """
        Look up category and cuisine from the pre-mapped data.
        
        Args:
            brand_name: The restaurant brand name
            
        Returns:
            Dictionary with category and cuisine, or None if not found
        """
        name_key = brand_name.lower().strip()
        return self.lookup_data.get(name_key)
    
    def _generate_with_ai(self, brand_name: str, retries: int = 3) -> Dict[str, str]:
        """
        Use OpenAI to generate category and cuisine classification.
        
        Args:
            brand_name: The restaurant brand name
            retries: Number of retry attempts on failure
            
        Returns:
            Dictionary with restaurant_category and restaurant_cultural_cuisine
        """
        prompt = f"""Given the restaurant brand name "{brand_name}", classify it into:

1. restaurant_category: Choose ONE from:
   - Fast Food (counter service, quick, standard menu)
   - Fast Casual (counter service, higher quality, made-to-order)
   - Casual Dining (table service, relaxed atmosphere)
   - Fine Dining (formal, upscale, premium)
   - Convenience Store (retail with food items)
   - Coffee Shop (primary focus on beverages)
   - Bakery (primary focus on baked goods)

2. restaurant_cultural_cuisine: The primary cultural/regional cuisine type. Examples:
   - American, Mexican, Italian, Chinese, Japanese, Korean, Thai, Vietnamese
   - Indian, Mediterranean, Greek, French, Middle Eastern
   - Caribbean, Latin American, etc.
   - Use "American" for general/mixed American food
   - Use "Fusion" only if explicitly blending multiple cuisines

Respond with ONLY a JSON object in this exact format:
{{"restaurant_category": "Fast Food", "restaurant_cultural_cuisine": "American"}}"""

        for attempt in range(retries):
            try:
                # Exponential backoff delay before retry
                if attempt > 0:
                    delay = 2 ** (attempt - 1)
                    logger.info(f"Waiting {delay}s before retry...")
                    time.sleep(delay)
                
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "You are a restaurant classification expert. Respond only with valid JSON."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.3,
                    max_tokens=100
                )
                
                result_text = response.choices[0].message.content.strip()
                
                # Parse JSON response
                result = json.loads(result_text)
                
                # Validate required fields
                if 'restaurant_category' in result and 'restaurant_cultural_cuisine' in result:
                    logger.info(f"AI generated for {brand_name}: {result}")
                    return result
                else:
                    logger.warning(f"AI response missing required fields for {brand_name}")
                    
            except RateLimitError:
                logger.warning(f"Rate limit hit, waiting before retry {attempt + 1}/{retries}")
                time.sleep(5 * (attempt + 1))  # Exponential backoff
                
            except json.JSONDecodeError:
                logger.error(f"Failed to parse AI response as JSON for {brand_name}: {result_text}")
                
            except Exception as e:
                logger.error(f"Error generating AI classification for {brand_name}: {e}")
                
            if attempt < retries - 1:
                time.sleep(1)
        
        # Fallback defaults if all retries fail
        logger.warning(f"Using fallback defaults for {brand_name}")
        return {
            'restaurant_category': 'Fast Food',
            'restaurant_cultural_cuisine': 'American'
        }
    
    def enrich_brand(self, brand_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich a single brand with category and cuisine information.
        
        Args:
            brand_data: Restaurant brand dictionary
            
        Returns:
            Enriched brand data with category and cuisine fields
        """
        brand_name = brand_data.get('brand_name', '')
        
        if not brand_name:
            logger.warning("Brand data missing brand_name field")
            return brand_data
        
        # Try lookup first
        lookup_result = self._lookup_category_cuisine(brand_name)
        
        if lookup_result:
            logger.info(f"Found {brand_name} in lookup data")
            brand_data['restaurant_category'] = lookup_result['restaurant_category']
            brand_data['restaurant_cultural_cuisine'] = lookup_result['restaurant_cultural_cuisine']
            brand_data['category_source'] = 'lookup'
        else:
            logger.info(f"{brand_name} not found in lookup, using AI generation")
            ai_result = self._generate_with_ai(brand_name)
            brand_data['restaurant_category'] = ai_result['restaurant_category']
            brand_data['restaurant_cultural_cuisine'] = ai_result['restaurant_cultural_cuisine']
            brand_data['category_source'] = 'ai_generated'
        
        # Update last_updated timestamp
        brand_data['last_updated'] = datetime.now().isoformat()
        
        return brand_data
    
    def process_brands_file(self, input_file: str, output_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Process a JSON file containing restaurant brands.
        
        Args:
            input_file: Path to input JSON file with restaurant brands
            output_file: Path to output file (default: overwrite input with _enriched suffix)
            
        Returns:
            Dictionary with processing statistics
        """
        input_path = Path(input_file)
        
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        # Load brands
        with open(input_path, 'r', encoding='utf-8') as f:
            brands = json.load(f)
        
        if not isinstance(brands, list):
            raise ValueError("Input file must contain a JSON array of brand objects")
        
        logger.info(f"Processing {len(brands)} restaurant brands from {input_file}")
        
        # Process each brand
        enriched_brands = []
        stats = {
            'total': len(brands),
            'lookup_matches': 0,
            'ai_generated': 0,
            'errors': 0
        }
        
        for brand in tqdm(brands, desc="Enriching brands"):
            try:
                enriched = self.enrich_brand(brand)
                enriched_brands.append(enriched)
                
                if enriched.get('category_source') == 'lookup':
                    stats['lookup_matches'] += 1
                elif enriched.get('category_source') == 'ai_generated':
                    stats['ai_generated'] += 1
                    
            except Exception as e:
                logger.error(f"Error processing brand {brand.get('brand_name', 'unknown')}: {e}")
                stats['errors'] += 1
                enriched_brands.append(brand)  # Keep original on error
        
        # Determine output file
        if output_file is None:
            # Insert _enriched before the timestamp or before .json
            name_parts = input_path.stem.split('_')
            if len(name_parts) > 1 and name_parts[-1].isdigit():
                # Has timestamp: restaurant_brands_20251223_145658
                base_name = '_'.join(name_parts[:-2]) + '_enriched_' + '_'.join(name_parts[-2:])
            else:
                base_name = input_path.stem + '_enriched'
            output_file = input_path.parent / f"{base_name}.json"
        
        output_path = Path(output_file)
        
        # Save enriched data
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(enriched_brands, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved enriched data to {output_path}")
        logger.info(f"Statistics: {stats}")
        
        return {
            'output_file': str(output_path),
            'stats': stats
        }


def main():
    """Main entry point for the script."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Enrich restaurant brands with category and cuisine classifications'
    )
    parser.add_argument(
        'input_file',
        help='Path to input JSON file containing restaurant brands'
    )
    parser.add_argument(
        '-o', '--output',
        help='Path to output file (default: input file with _enriched suffix)',
        default=None
    )
    parser.add_argument(
        '-c', '--categories',
        help='Path to restaurant categories lookup file',
        default='data/restaurant_categories/restaurant_categories.json'
    )
    
    args = parser.parse_args()
    
    try:
        generator = BrandCategoryCuisineGenerator(categories_file=args.categories)
        result = generator.process_brands_file(args.input_file, args.output)
        
        print("\n✅ Processing complete!")
        print(f"📁 Output file: {result['output_file']}")
        print(f"📊 Statistics:")
        print(f"   Total brands: {result['stats']['total']}")
        print(f"   Lookup matches: {result['stats']['lookup_matches']}")
        print(f"   AI generated: {result['stats']['ai_generated']}")
        print(f"   Errors: {result['stats']['errors']}")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"\n❌ Error: {e}")
        exit(1)


if __name__ == '__main__':
    main()