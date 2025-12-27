"""
Golden Ratio Calculator for Menu Items.

For the file: menu_items

Formula: Golden Ratio = (Protein ÷ Calories) × 10

Purpose: Measures nutritional efficiency - higher values indicate better protein-to-calorie ratio

Classification:
- Excellent (≥1.0): 40g protein, 400 calories → 1.0 ratio
- Good (0.5-1.0): 25g protein, 500 calories → 0.5 ratio 
- Poor (<0.5): 15g protein, 600 calories → 0.25 ratio

Target Ratio: 1.0 (represents 10:1 calorie-to-protein efficiency)
"""
import json
import re
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path
from tqdm import tqdm

# Configure logging
from pathlib import Path
log_dir = Path('logging')
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'golden_ratio.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GoldenRatioGenerator:
    """
    Generates golden ratio scores for menu items based on protein-to-calorie efficiency.
    
    Process:
    1. Load menu items from JSON file
    2. Extract protein and calorie values from nutritional data
    3. Calculate golden ratio: (Protein ÷ Calories) × 10
    4. Add classification category (Excellent, Good, Poor)
    """
    
    def __init__(self):
        """Initialize the golden ratio generator."""
        logger.info("Initialized GoldenRatioGenerator")
        logger.info("Formula: Golden Ratio = (Protein ÷ Calories) × 10")
    
    @staticmethod
    def _extract_numeric_value(value_str: Any) -> float:
        """Extract numeric value from nutrition string or number."""
        if isinstance(value_str, (int, float)):
            return float(value_str)
        
        if not value_str or value_str == '0' or value_str == '':
            return 0.0
        
        # Remove units and extract number
        numeric_match = re.search(r'(\d+\.?\d*)', str(value_str))
        if numeric_match:
            return float(numeric_match.group(1))
        return 0.0
    
    @staticmethod
    def _calculate_golden_ratio(protein: float, calories: float) -> float:
        """
        Calculate the golden ratio: (Protein ÷ Calories) × 10
        
        Args:
            protein: Protein content in grams
            calories: Calorie content
            
        Returns:
            Golden ratio value, rounded to 3 decimal places
        """
        if calories == 0:
            return 0.0
        return round((protein / calories) * 10, 3)
    
    @staticmethod
    def _classify_ratio(ratio: float) -> str:
        """
        Classify golden ratio into category.
        
        Args:
            ratio: Golden ratio value
            
        Returns:
            Classification: "Excellent", "Good", or "Poor"
        """
        if ratio >= 1.0:
            return "Excellent"
        elif ratio >= 0.5:
            return "Good"
        else:
            return "Poor"

    def enrich_menu_item(self, item: Dict[str, Any], force_regenerate: bool = False) -> Dict[str, Any]:
        """
        Enrich a single menu item with golden ratio.
        
        Args:
            item: Menu item dictionary
            force_regenerate: If True, regenerate even if ratio exists
            
        Returns:
            Enriched item with golden_ratio field
        """
        # Check if nutritional values exist
        if 'nutritional_values' not in item:
            logger.debug(f"Skipping '{item.get('name', 'Unknown')}' - no nutritional data")
            return item
        
        # Handle nested serving_sizes structure
        serving_sizes = item['nutritional_values'].get('serving_sizes', [])
        if serving_sizes:
            # Process each serving size
            for serving_size in serving_sizes:
                if 'nutrition' in serving_size:
                    # Skip if already has golden_ratio and not forcing
                    if serving_size.get('golden_ratio') is not None and not force_regenerate:
                        continue
                    
                    nutrition = serving_size['nutrition']
                    protein = self._extract_numeric_value(nutrition.get('protein', 0))
                    calories = self._extract_numeric_value(nutrition.get('calories', 0))
                    
                    # Calculate and add golden ratio
                    golden_ratio = self._calculate_golden_ratio(protein, calories)
                    serving_size['golden_ratio'] = golden_ratio
                    serving_size['golden_ratio_category'] = self._classify_ratio(golden_ratio)
        else:
            # Handle direct nutrition data (if flat structure)
            nutrition = item['nutritional_values'].get('nutrition', {})
            if nutrition:
                if item.get('golden_ratio') is None or force_regenerate:
                    protein = self._extract_numeric_value(nutrition.get('protein', 0))
                    calories = self._extract_numeric_value(nutrition.get('calories', 0))
                    
                    golden_ratio = self._calculate_golden_ratio(protein, calories)
                    item['golden_ratio'] = golden_ratio
                    item['golden_ratio_category'] = self._classify_ratio(golden_ratio)
        
        return item
    
    def process_menu_items_file(
        self,
        input_file: str,
        output_file: Optional[str] = None,
        force_regenerate: bool = False,
        generate_stats: bool = False,
        variations_file: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Process a JSON file containing menu items and add golden ratios.
        
        Args:
            input_file: Path to input JSON file with menu items
            output_file: Path to output file (default: input with _enriched suffix)
            force_regenerate: If True, regenerate all ratios even if they exist
            generate_stats: If True, generate summary statistics file
            
        Returns:
            Dictionary with processing statistics
        """
        input_path = Path(input_file)
        
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        # Load variations (or fallback to menu items)
        logger.info(f"Loading from {input_file}")
        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Check if this is variations file (has nutrition field at top level)
        is_variations = isinstance(data, list) and len(data) > 0 and 'nutrition' in data[0]
        
        if is_variations:
            # Variations format - nutrition data directly available
            logger.info(f"Processing {len(data)} variations")
            items_list = data
        else:
            # Menu items format - need nested structure
            is_nested_dict = isinstance(data, dict) and not isinstance(data, list)
            if is_nested_dict:
                total_restaurants = len(data)
                logger.info(f"Processing {total_restaurants} restaurants (nested format)")
                items_list = []
                for restaurant_data in data.values():
                    if 'items' in restaurant_data:
                        items_list.extend(restaurant_data['items'])
            else:
                if not isinstance(data, list):
                    raise ValueError("Input file must contain a JSON array or nested restaurant dict")
                items_list = data
                logger.info(f"Processing {len(items_list)} menu items (flat format)")
        
        # Stats tracking
        stats = {
            'total_items': len(items_list),
            'processed': 0,
            'skipped': 0,
            'no_nutrition_data': 0,
            'errors': 0,
            'ratio_stats': {
                'excellent': 0,  # >= 1.0
                'good': 0,       # 0.5 - 1.0
                'poor': 0        # < 0.5
            }
        }
        
        all_ratios = []
        
        # Process items
        logger.info("Processing items...")
        
        if is_variations:
            # Process variations - nutrition data at top level
            for variation in tqdm(items_list, desc="Processing variations"):
                try:
                    nutrition = variation.get('nutrition')
                    if not nutrition:
                        stats['no_nutrition_data'] += 1
                        continue
                    
                    # Skip if already has golden_ratio and not forcing
                    if variation.get('golden_ratio') is not None and not force_regenerate:
                        stats['skipped'] += 1
                        continue
                    
                    # Extract nutrition values
                    protein = self._extract_numeric_value(nutrition.get('protein_g', 0))
                    calories = self._extract_numeric_value(nutrition.get('calories', 0))
                    
                    # Calculate and add golden ratio
                    golden_ratio = self._calculate_golden_ratio(protein, calories)
                    variation['golden_ratio'] = golden_ratio
                    variation['golden_ratio_category'] = self._classify_ratio(golden_ratio)
                    
                    stats['processed'] += 1
                    all_ratios.append(golden_ratio)
                    
                    # Update ratio stats
                    category = variation['golden_ratio_category']
                    if category == 'Excellent':
                        stats['ratio_stats']['excellent'] += 1
                    elif category == 'Good':
                        stats['ratio_stats']['good'] += 1
                    else:
                        stats['ratio_stats']['poor'] += 1
                        
                except Exception as e:
                    logger.error(f"Error processing variation: {e}")
                    stats['errors'] += 1
                    
        elif is_nested_dict:
            # Process nested structure
            for restaurant_name, restaurant_data in tqdm(data.items(), desc="Processing restaurants"):
                if 'items' not in restaurant_data:
                    continue
                
                for item in restaurant_data['items']:
                    try:
                        if 'nutritional_values' not in item:
                            stats['no_nutrition_data'] += 1
                            continue
                        
                        enriched = self.enrich_menu_item(item, force_regenerate)
                        stats['processed'] += 1
                        
                        # Collect ratios for statistics
                        for serving_size in item.get('nutritional_values', {}).get('serving_sizes', []):
                            if 'golden_ratio' in serving_size:
                                ratio = serving_size['golden_ratio']
                                all_ratios.append(ratio)
                                category = serving_size.get('golden_ratio_category', 'Poor')
                                if category == 'Excellent':
                                    stats['ratio_stats']['excellent'] += 1
                                elif category == 'Good':
                                    stats['ratio_stats']['good'] += 1
                                else:
                                    stats['ratio_stats']['poor'] += 1
                        
                    except Exception as e:
                        logger.error(f"Error processing item {item.get('name', 'unknown')}: {e}")
                        stats['errors'] += 1
        else:
            # Process flat list
            for item in tqdm(items_list, desc="Processing items"):
                try:
                    if 'nutritional_values' not in item:
                        stats['no_nutrition_data'] += 1
                        continue
                    
                    enriched = self.enrich_menu_item(item, force_regenerate)
                    stats['processed'] += 1
                    
                    # Collect ratios for statistics
                    for serving_size in item.get('nutritional_values', {}).get('serving_sizes', []):
                        if 'golden_ratio' in serving_size:
                            ratio = serving_size['golden_ratio']
                            all_ratios.append(ratio)
                            category = serving_size.get('golden_ratio_category', 'Poor')
                            if category == 'Excellent':
                                stats['ratio_stats']['excellent'] += 1
                            elif category == 'Good':
                                stats['ratio_stats']['good'] += 1
                            else:
                                stats['ratio_stats']['poor'] += 1
                    
                except Exception as e:
                    logger.error(f"Error processing item {item.get('name', 'unknown')}: {e}")
                    stats['errors'] += 1
        
        # Add distribution statistics
        if all_ratios:
            stats['ratio_distribution'] = {
                'min': round(min(all_ratios), 3),
                'max': round(max(all_ratios), 3),
                'average': round(sum(all_ratios) / len(all_ratios), 3),
                'median': round(sorted(all_ratios)[len(all_ratios) // 2], 3)
            }
        
        # Determine output file
        if output_file is None:
            name_parts = input_path.stem.split('_')
            if len(name_parts) > 1 and name_parts[-1].isdigit():
                # Has timestamp
                base_name = '_'.join(name_parts[:-2]) + '_enriched_' + '_'.join(name_parts[-2:])
            else:
                base_name = input_path.stem + '_enriched'
            output_file = input_path.parent / f"{base_name}.json"
        
        output_path = Path(output_file)
        
        # Save enriched data
        logger.info(f"Saving enriched data to {output_path}")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Generate summary statistics file if requested
        if generate_stats and all_ratios:
            stats_file = output_path.parent / f"{output_path.stem}_stats.json"
            logger.info(f"Saving statistics to {stats_file}")
            with open(stats_file, 'w', encoding='utf-8') as f:
                json.dump(stats, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Statistics: {stats}")
        
        return {
            'output_file': str(output_path),
            'stats': stats
        }



def main():
    """Main entry point for the script."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Calculate golden ratio (protein-to-calorie efficiency) for menu items'
    )
    parser.add_argument(
        'input_file',
        help='Path to input JSON file containing menu items'
    )
    parser.add_argument(
        '-o', '--output',
        help='Path to output file (default: input file with _enriched suffix)',
        default=None
    )
    parser.add_argument(
        '-f', '--force',
        help='Force regeneration of all ratios, even if they exist',
        action='store_true'
    )
    parser.add_argument(
        '-s', '--stats',
        help='Generate summary statistics file',
        action='store_true'
    )
    
    args = parser.parse_args()
    
    try:
        logger.info("=" * 60)
        logger.info("🏆 Golden Ratio Calculator")
        logger.info("=" * 60)
        logger.info("Formula: Golden Ratio = (Protein ÷ Calories) × 10")
        logger.info("Categories: Excellent (≥1.0), Good (0.5-1.0), Poor (<0.5)")
        logger.info("")
        
        generator = GoldenRatioGenerator()
        result = generator.process_menu_items_file(
            args.input_file,
            args.output,
            force_regenerate=args.force,
            generate_stats=args.stats
        )
        
        print("\n✅ Processing complete!")
        print(f"📁 Output file: {result['output_file']}")
        print(f"📊 Statistics:")
        print(f"   Total items: {result['stats']['total_items']}")
        print(f"   Processed: {result['stats']['processed']}")
        print(f"   No nutrition data: {result['stats']['no_nutrition_data']}")
        print(f"   Errors: {result['stats']['errors']}")
        print(f"\n🏆 Golden Ratio Distribution:")
        print(f"   Excellent (≥1.0): {result['stats']['ratio_stats']['excellent']}")
        print(f"   Good (0.5-1.0): {result['stats']['ratio_stats']['good']}")
        print(f"   Poor (<0.5): {result['stats']['ratio_stats']['poor']}")
        
        if 'ratio_distribution' in result['stats']:
            dist = result['stats']['ratio_distribution']
            print(f"\n📈 Ratio Statistics:")
            print(f"   Average: {dist['average']}")
            print(f"   Median: {dist['median']}")
            print(f"   Range: {dist['min']} - {dist['max']}")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"\n❌ Error: {e}")
        exit(1)


if __name__ == '__main__':
    main()