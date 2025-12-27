"""
Macronutrient Profile Generator for Menu Items.

For the file: menu_items

Adds macronutrient profile tags to menu items using dual criteria approach.
Uses both absolute values (grams) and relative percentages (calories) for accurate classification.
Based on FDA guidelines and nutritional standards.

Output format: ["High Protein", "Low Carb", "Balanced", "Keto Friendly", etc.]
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
        logging.FileHandler(log_dir / 'macronutrient_profile.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Macronutrient calorie values
MACRO_CALORIES = {
    'protein': 4,  # calories per gram
    'carbs': 4,    # calories per gram
    'fat': 9       # calories per gram
}

# FDA Daily Values for additional nutrients
DAILY_VALUES = {
    'sodium': 2300,  # mg (FDA DV)
    'sugar': 50      # g (estimated added sugar DV)
}

class MacronutrientProfileGenerator:
    """
    Generates macronutrient profile tags for menu items using proprietary dual criteria approach.
    
    Process:
    1. Load menu items from JSON file
    2. Extract nutritional values from each item
    3. Calculate absolute values (grams) and relative percentages (calories)
    4. Apply FDA-based classification thresholds
    5. Generate profile tags: ["High Protein", "Low Carb", "Balanced", etc.]
    """
    
    # Macronutrient calorie values
    MACRO_CALORIES = {
        'protein': 4,  # calories per gram
        'carbs': 4,    # calories per gram
        'fat': 9       # calories per gram
    }
    
    # FDA Daily Values for additional nutrients
    DAILY_VALUES = {
        'sodium': 2300,  # mg (FDA DV)
        'sugar': 50      # g (estimated added sugar DV)
    }
    
    def __init__(self):
        """Initialize the macronutrient profile generator."""
        logger.info("Initialized MacronutrientProfileGenerator")
        logger.info(f"Macro calories: {self.MACRO_CALORIES}")
        logger.info(f"Daily values: {self.DAILY_VALUES}")
    
    @staticmethod
    def _extract_numeric_value(value_str: str) -> float:
        """Extract numeric value from nutrition string (e.g., '25g' -> 25.0)."""
        if not value_str or value_str == '0' or value_str == '':
            return 0.0
        
        # Remove units and extract number
        numeric_match = re.search(r'(\d+\.?\d*)', str(value_str))
        if numeric_match:
            return float(numeric_match.group(1))
        return 0.0
    
    def _calculate_macronutrient_percentages(
        self, 
        calories: float, 
        protein_g: float, 
        carbs_g: float, 
        fats_g: float
    ) -> tuple:
        """Calculate macronutrient percentages of total calories."""
        # Calculate macronutrient calories
        protein_cals = protein_g * self.MACRO_CALORIES['protein']
        carbs_cals = carbs_g * self.MACRO_CALORIES['carbs']
        fats_cals = fats_g * self.MACRO_CALORIES['fat']
        
        # Use the higher of reported calories or calculated calories
        total_cals = max(calories, protein_cals + carbs_cals + fats_cals, 1)
        
        # Calculate percentages
        protein_pct = (protein_cals / total_cals) * 100
        carbs_pct = (carbs_cals / total_cals) * 100
        fats_pct = (fats_cals / total_cals) * 100
        
        return protein_pct, carbs_pct, fats_pct, total_cals
    
    def _get_macronutrient_profile(self, nutrition_data: Dict[str, str]) -> List[str]:
        """
        Determine macronutrient profile using dual criteria (absolute + relative).
        
        Args:
            nutrition_data: Dictionary containing nutritional values
            
        Returns:
            List of macronutrient profile tags
        """
        # Extract nutritional values
        calories = self._extract_numeric_value(nutrition_data.get('calories', '0'))
        protein_g = self._extract_numeric_value(nutrition_data.get('protein', '0'))
        carbs_g = self._extract_numeric_value(nutrition_data.get('total_carbohydrates', '0'))
        fats_g = self._extract_numeric_value(nutrition_data.get('total_fat', '0'))
        sodium_mg = self._extract_numeric_value(nutrition_data.get('sodium', '0'))
        sugar_g = self._extract_numeric_value(nutrition_data.get('sugar', '0'))
        
        # Check if we have sufficient nutritional data
        if calories == 0 and (protein_g == 0 and carbs_g == 0 and fats_g == 0):
            return []
        
        # Calculate macronutrient percentages
        protein_pct, carbs_pct, fats_pct, total_cals = self._calculate_macronutrient_percentages(
            calories, protein_g, carbs_g, fats_g
        )
        
        macro_profile = []
        
        # Protein tagging (absolute and relative criteria)
        if protein_g >= 25 or (protein_pct >= 30 and protein_g >= 15):
            macro_profile.append("High Protein")
        elif protein_g < 10 and total_cals > 250:
            macro_profile.append("Low Protein")
        
        # Carb tagging (absolute and relative criteria)
        if carbs_g < 20 or (carbs_pct <= 20 and total_cals > 250):
            macro_profile.append("Low Carb")
        elif carbs_pct >= 60:
            macro_profile.append("High Carb")
        
        # Fat tagging
        if fats_pct <= 20 and total_cals > 200:
            macro_profile.append("Low Fat")
        elif fats_pct >= 50:
            macro_profile.append("High Fat")
        
        # Caloric density tagging
        if total_cals <= 350:
            macro_profile.append("Low Calorie")
        elif total_cals >= 700:
            macro_profile.append("High Calorie")
        
        # Sodium tagging based on daily value percentages
        sodium_daily_pct = (sodium_mg / self.DAILY_VALUES['sodium']) * 100
        if sodium_daily_pct <= 10:  # Less than 10% DV is considered low (230mg)
            macro_profile.append("Low Sodium")
        elif sodium_daily_pct >= 20:  # More than 20% DV is considered high (460mg)
            macro_profile.append("High Sodium")
        
        # Sugar tagging
        if sugar_g <= 8:  # More lenient threshold for low sugar
            macro_profile.append("Low Sugar")
        elif sugar_g >= 20:
            macro_profile.append("High Sugar")
        
        # Keto-friendly determination
        if carbs_g <= 15 and fats_pct >= 60 and carbs_pct <= 10:
            macro_profile.append("Keto Friendly")
        
        # Balanced macros (close to 40/30/30 or within reasonable range)
        is_balanced = (
            30 <= carbs_pct <= 50 and
            20 <= protein_pct <= 40 and
            20 <= fats_pct <= 40
        )
        if is_balanced and total_cals >= 300:
            macro_profile.append("Balanced")
        
        return macro_profile
    
    def enrich_menu_item(self, item: Dict[str, Any], force_regenerate: bool = False) -> Dict[str, Any]:
        """
        Enrich a single menu item with macronutrient profile.
        
        Args:
            item: Menu item dictionary
            force_regenerate: If True, regenerate even if profile exists
            
        Returns:
            Enriched item with macronutrient_profile field
        """
        # Check if nutritional values exist
        if 'nutritional_values' not in item:
            logger.debug(f"Skipping '{item.get('name', 'Unknown')}' - no nutritional data")
            return item
        
        # Skip if profile already exists and not forcing regeneration
        if item.get('macronutrient_profile') and not force_regenerate:
            logger.debug(f"Skipping '{item.get('name')}' - profile already exists")
            return item
        
        # Handle nested serving_sizes structure
        serving_sizes = item['nutritional_values'].get('serving_sizes', [])
        if serving_sizes:
            # Process each serving size
            for serving_size in serving_sizes:
                if 'nutrition' in serving_size:
                    profile = self._get_macronutrient_profile(serving_size['nutrition'])
                    serving_size['macronutrient_profile'] = profile
        else:
            # Handle direct nutrition data (if flat structure)
            nutrition = item['nutritional_values'].get('nutrition', {})
            if nutrition:
                profile = self._get_macronutrient_profile(nutrition)
                item['macronutrient_profile'] = profile
        
        return item
    
    def process_menu_items_file(
        self,
        input_file: str,
        output_file: Optional[str] = None,
        force_regenerate: bool = False,
        create_backup: bool = False,
        variations_file: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Process a JSON file containing menu items and add macronutrient profiles.
        
        Args:
            input_file: Path to input JSON file with menu items
            output_file: Path to output file (default: input with _enriched suffix)
            force_regenerate: If True, regenerate all profiles even if they exist
            create_backup: If True, create a backup of the input file
            variations_file: Path to variations file with nutrition data (optional)
            
        Returns:
            Dictionary with processing statistics
        """
        input_path = Path(input_file)
        
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        # Load menu items
        logger.info(f"Loading menu items from {input_file}")
        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Load variations if provided and create mapping: menu_item_id -> first variation
        variations_map = {}
        if variations_file:
            variations_path = Path(variations_file)
            if variations_path.exists():
                logger.info(f"Loading variations from {variations_file}")
                with open(variations_path, 'r', encoding='utf-8') as f:
                    variations = json.load(f)
                
                # Map menu_item_id to first variation with nutrition data
                for var in variations:
                    menu_item_id = var.get('menu_item_id')
                    if menu_item_id and menu_item_id not in variations_map:
                        if var.get('nutrition'):
                            variations_map[menu_item_id] = var
                logger.info(f"Mapped {len(variations_map)} menu items to variations")
            else:
                logger.warning(f"Variations file not found: {variations_file}")
        
        # Handle both list and dict formats
        is_nested_dict = isinstance(data, dict) and not isinstance(data, list)
        
        if is_nested_dict:
            # Nested restaurant dictionary format
            total_restaurants = len(data)
            logger.info(f"Processing {total_restaurants} restaurants (nested format)")
            items_list = []
            for restaurant_data in data.values():
                if 'items' in restaurant_data:
                    items_list.extend(restaurant_data['items'])
        else:
            # Flat list format
            if not isinstance(data, list):
                raise ValueError("Input file must contain a JSON array or nested restaurant dict")
            items_list = data
            logger.info(f"Processing {len(items_list)} menu items (flat format)")
        
        # Create backup if requested
        if create_backup:
            backup_file = input_path.parent / f"{input_path.stem}_backup{input_path.suffix}"
            logger.info(f"Creating backup at {backup_file}")
            with open(backup_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Stats tracking
        stats = {
            'total_items': len(items_list),
            'processed': 0,
            'skipped': 0,
            'no_nutrition_data': 0,
            'errors': 0
        }
        
        # Process items
        logger.info("Processing menu items...")
        
        if is_nested_dict:
            # Process nested structure
            for restaurant_name, restaurant_data in tqdm(data.items(), desc="Processing restaurants"):
                if 'items' not in restaurant_data:
                    continue
                
                for item in restaurant_data['items']:
                    try:
                        if 'nutritional_values' not in item:
                            stats['no_nutrition_data'] += 1
                            continue
                        
                        if not item.get('macronutrient_profile') or force_regenerate:
                            enriched = self.enrich_menu_item(item, force_regenerate)
                            stats['processed'] += 1
                        else:
                            stats['skipped'] += 1
                        
                        # Remove nutritional_values after processing (should only be in variations)
                        item.pop('nutritional_values', None)
                            
                    except Exception as e:
                        logger.error(f"Error processing item {item.get('name', 'unknown')}: {e}")
                        stats['errors'] += 1
        else:
            # Process flat list
            for item in tqdm(items_list, desc="Processing items"):
                try:
                    # Map nutrition from variations if available
                    if variations_map and item.get('_id') in variations_map:
                        variation = variations_map[item['_id']]
                        # Create nutritional_values structure from variation nutrition
                        item['nutritional_values'] = {
                            'nutrition': {
                                'calories': str(variation['nutrition'].get('calories', 0)),
                                'protein': str(variation['nutrition'].get('protein_g', 0)) + 'g',
                                'total_carbohydrates': str(variation['nutrition'].get('carbs_g', 0)) + 'g',
                                'total_fat': str(variation['nutrition'].get('fat_g', 0)) + 'g',
                                'sodium': str(variation['nutrition'].get('sodium_mg', 0)) + 'mg',
                                'sugar': str(variation['nutrition'].get('sugars_g', 0)) + 'g'
                            }
                        }
                    
                    if 'nutritional_values' not in item:
                        stats['no_nutrition_data'] += 1
                        continue
                    
                    if not item.get('macronutrient_profile') or force_regenerate:
                        enriched = self.enrich_menu_item(item, force_regenerate)
                        stats['processed'] += 1
                    else:
                        stats['skipped'] += 1
                    
                    # Remove nutritional_values after processing (should only be in variations)
                    item.pop('nutritional_values', None)
                        
                except Exception as e:
                    logger.error(f"Error processing item {item.get('name', 'unknown')}: {e}")
                    stats['errors'] += 1
        
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
        
        logger.info(f"Statistics: {stats}")
        
        return {
            'output_file': str(output_path),
            'stats': stats
        }


def main():
    """Main entry point for the script."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Add macronutrient profile tags to menu items based on nutritional data'
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
        help='Force regeneration of all profiles, even if they exist',
        action='store_true'
    )
    parser.add_argument(
        '-b', '--backup',
        help='Create a backup of the input file before processing',
        action='store_true'
    )
    
    args = parser.parse_args()
    
    try:
        logger.info("=" * 60)
        logger.info("Macronutrient Profile Generator (Dual Criteria)")
        logger.info("=" * 60)
        logger.info("Classification uses:")
        logger.info("  - Absolute values (grams)")
        logger.info("  - Relative percentages (calories)")
        logger.info("Output tags: High Protein, Low Carb, Balanced, Keto Friendly, etc.")
        logger.info("")
        
        generator = MacronutrientProfileGenerator()
        result = generator.process_menu_items_file(
            args.input_file,
            args.output,
            force_regenerate=args.force,
            create_backup=args.backup
        )
        
        print("\n✅ Processing complete!")
        print(f"📁 Output file: {result['output_file']}")
        print(f"📊 Statistics:")
        print(f"   Total items: {result['stats']['total_items']}")
        print(f"   Profiles generated: {result['stats']['processed']}")
        print(f"   Skipped (already had profiles): {result['stats']['skipped']}")
        print(f"   No nutrition data: {result['stats']['no_nutrition_data']}")
        print(f"   Errors: {result['stats']['errors']}")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"\n❌ Error: {e}")
        exit(1)


if __name__ == "__main__":
    main()