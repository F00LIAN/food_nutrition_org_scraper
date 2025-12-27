"""
Map Menu Item Images from Variations.

Takes the first image_url from menu_item_variations for each menu item
and adds it as menu_item_image_url to the corresponding menu item.
"""
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path
from tqdm import tqdm

# Configure logging
log_dir = Path('logging')
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'menu_item_images.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class MenuItemImageMapper:
    """
    Maps menu item images from variations to menu items.
    
    Process:
    1. Load menu items and variations
    2. Build a mapping of menu_item_id -> first image_url from variations
    3. Add menu_item_image_url field to each menu item
    """
    
    def __init__(self):
        """Initialize the image mapper."""
        logger.info("Initialized MenuItemImageMapper")
    
    def build_image_mapping(self, variations: List[Dict[str, Any]]) -> Dict[str, str]:
        """
        Build a mapping of menu_item_id to the first available image_url.
        
        Args:
            variations: List of menu item variations
            
        Returns:
            Dictionary mapping menu_item_id to image_url
        """
        image_map = {}
        
        for variation in variations:
            menu_item_id = variation.get('menu_item_id')
            image_url = variation.get('image_url')
            
            # Only map if we have both fields and haven't already mapped this menu item
            if menu_item_id and image_url and menu_item_id not in image_map:
                image_map[menu_item_id] = image_url
                logger.debug(f"Mapped image for menu_item_id: {menu_item_id[:8]}...")
        
        return image_map
    
    def process_files(
        self,
        menu_items_file: str,
        variations_file: str,
        output_file: Optional[str] = None,
        force_regenerate: bool = False
    ) -> Dict[str, Any]:
        """
        Process menu items and variations files to map images.
        
        Args:
            menu_items_file: Path to menu items JSON file
            variations_file: Path to variations JSON file
            output_file: Path to output file (default: input with _enriched suffix)
            force_regenerate: If True, overwrite existing menu_item_image_url
            
        Returns:
            Dictionary with processing statistics
        """
        menu_items_path = Path(menu_items_file)
        variations_path = Path(variations_file)
        
        if not menu_items_path.exists():
            raise FileNotFoundError(f"Menu items file not found: {menu_items_file}")
        
        if not variations_path.exists():
            raise FileNotFoundError(f"Variations file not found: {variations_file}")
        
        # Load data
        logger.info(f"Loading menu items from {menu_items_file}")
        with open(menu_items_path, 'r', encoding='utf-8') as f:
            menu_items = json.load(f)
        
        logger.info(f"Loading variations from {variations_file}")
        with open(variations_path, 'r', encoding='utf-8') as f:
            variations = json.load(f)
        
        # Build image mapping
        logger.info("Building image mapping from variations...")
        image_map = self.build_image_mapping(variations)
        logger.info(f"Built image mapping for {len(image_map)} menu items")
        
        # Stats tracking
        stats = {
            'total_items': len(menu_items),
            'images_mapped': 0,
            'already_had_image': 0,
            'no_image_found': 0
        }
        
        # Map images to menu items
        logger.info("Mapping images to menu items...")
        
        for item in tqdm(menu_items, desc="Mapping images"):
            item_id = item.get('_id')
            
            if not item_id:
                continue
            
            # Check if already has image and not forcing regeneration
            if item.get('menu_item_image_url') and not force_regenerate:
                stats['already_had_image'] += 1
                continue
            
            # Get image from mapping
            if item_id in image_map:
                item['menu_item_image_url'] = image_map[item_id]
                stats['images_mapped'] += 1
                logger.debug(f"Mapped image to: {item.get('name')}")
            else:
                stats['no_image_found'] += 1
                logger.debug(f"No image found for: {item.get('name')}")
        
        # Determine output file
        if output_file is None:
            name_parts = menu_items_path.stem.split('_')
            # Remove 'enriched' if present
            name_parts = [p for p in name_parts if p != 'enriched']
            
            if len(name_parts) >= 3 and name_parts[-1].isdigit():
                # Has timestamp: menu_items_20251226_112108
                base_name = '_'.join(name_parts[:-2]) + '_enriched_' + '_'.join(name_parts[-2:])
            else:
                base_name = '_'.join(name_parts) + '_enriched'
            
            output_file = menu_items_path.parent / f"{base_name}.json"
        
        output_path = Path(output_file)
        
        # Save enriched data
        logger.info(f"Saving enriched data to {output_path}")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(menu_items, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Statistics: {stats}")
        
        return {
            'output_file': str(output_path),
            'stats': stats
        }


def main():
    """Main entry point for the script."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Map menu item images from variations to menu items'
    )
    parser.add_argument(
        'menu_items_file',
        help='Path to menu items JSON file'
    )
    parser.add_argument(
        'variations_file',
        help='Path to variations JSON file'
    )
    parser.add_argument(
        '-o', '--output',
        help='Path to output file (default: input file with _enriched suffix)',
        default=None
    )
    parser.add_argument(
        '-f', '--force',
        help='Force regeneration, overwrite existing images',
        action='store_true'
    )
    
    args = parser.parse_args()
    
    try:
        logger.info("=" * 60)
        logger.info("Menu Item Image Mapper")
        logger.info("=" * 60)
        logger.info("Mapping first variation image to each menu item")
        logger.info("")
        
        mapper = MenuItemImageMapper()
        result = mapper.process_files(
            args.menu_items_file,
            args.variations_file,
            args.output,
            force_regenerate=args.force
        )
        
        print("\n✅ Processing complete!")
        print(f"📁 Output file: {result['output_file']}")
        print(f"📊 Statistics:")
        print(f"   Total items: {result['stats']['total_items']}")
        print(f"   Images mapped: {result['stats']['images_mapped']}")
        print(f"   Already had image: {result['stats']['already_had_image']}")
        print(f"   No image found: {result['stats']['no_image_found']}")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"\n❌ Error: {e}")
        exit(1)


if __name__ == "__main__":
    main()

