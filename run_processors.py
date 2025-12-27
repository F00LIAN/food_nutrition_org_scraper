"""
Processor Runner - Run enrichment processors on scraped data.

Automatically identifies the latest scraped files and runs them through
all processors in sequence.
"""
import sys
import subprocess
from pathlib import Path
import logging
import importlib.util
import shutil
from datetime import datetime

# Setup logging directory
def setup_logging(output_dir: str):
    """Setup logging directory - clears previous logs."""
    log_dir = Path('logging')
    
    # Clear and recreate logging directory
    if log_dir.exists():
        import shutil
        shutil.rmtree(log_dir)
    log_dir.mkdir(exist_ok=True)
    
    log_file = log_dir / 'run_processors.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ],
        force=True
    )
    logger = logging.getLogger(__name__)
    logger.info(f"Logging to: {log_file}")
    return logger

logger = logging.getLogger(__name__)


def load_processor_class(processor_file: str, class_name: str):
    """Dynamically load a processor class from a file."""
    spec = importlib.util.spec_from_file_location("module", processor_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, class_name)


class ProcessorPipeline:
    """Run all processors on scraped data in sequence."""
    
    def __init__(self, output_dir: str = "output"):
        """
        Initialize the processor pipeline.
        
        Args:
            output_dir: Directory containing scraped data files
        """
        self.output_dir = Path(output_dir)
        self.brands_file = None
        self.menu_items_file = None
        self.variations_file = None  # Track variations file for nutrition mapping
        self.original_menu_items_file = None  # Track original filename
        self.post_processed_dir = None  # Will be set when processing starts
        
    def _get_enriched_output_path(self, input_file: Path, step: int) -> Path:
        """
        Generate output filename in post_processed directory.
        
        Args:
            input_file: Input file path
            step: Processor step number (1-5)
            
        Returns:
            Output file path in post_processed directory
        """
        # Use original filename if this is first enrichment
        if self.original_menu_items_file:
            base_file = self.original_menu_items_file
        else:
            base_file = input_file
            self.original_menu_items_file = input_file
        
        # Parse filename: menu_items_20251226_112108.json
        name_parts = base_file.stem.split('_')
        
        # Remove 'enriched' if present
        name_parts = [p for p in name_parts if p != 'enriched']
        
        # Build new name with single 'enriched'
        if len(name_parts) >= 3 and name_parts[-1].isdigit():
            # Has timestamp: menu_items_20251226_112108
            base_name = '_'.join(name_parts[:-2]) + '_enriched_' + '_'.join(name_parts[-2:])
        else:
            base_name = '_'.join(name_parts) + '_enriched'
        
        return self.post_processed_dir / f"{base_name}.json"
        
    def find_latest_files(self) -> bool:
        """Find the latest scraped files."""
        # Look for restaurant_brands files
        brands_files = list(self.output_dir.rglob("*restaurant_brands*.json"))
        if brands_files:
            self.brands_file = max(brands_files, key=lambda p: p.stat().st_mtime)
            logger.info(f"Found brands file: {self.brands_file}")
        
        # Look for menu_items files (exclude variations)
        menu_files = list(self.output_dir.rglob("*menu_items*.json"))
        menu_files = [f for f in menu_files if 'variation' not in f.name.lower()]
        if menu_files:
            self.menu_items_file = max(menu_files, key=lambda p: p.stat().st_mtime)
            logger.info(f"Found menu items file: {self.menu_items_file}")
        
        # Look for variations files for nutrition mapping
        variations_files = list(self.output_dir.rglob("*menu_item_variations*.json"))
        if variations_files:
            self.variations_file = max(variations_files, key=lambda p: p.stat().st_mtime)
            logger.info(f"Found variations file: {self.variations_file}")
        
        return bool(self.brands_file or self.menu_items_file)
    
    def run_all_processors(self, skip_ai: bool = False) -> dict:
        """
        Run all processors in sequence.
        
        Args:
            skip_ai: If True, skip AI-based processors (faster, cheaper)
            
        Returns:
            Dictionary with results from each processor
        """
        results = {}
        
        # Create post_processed directory
        if self.brands_file:
            base_dir = self.brands_file.parent
        elif self.menu_items_file:
            base_dir = self.menu_items_file.parent
        else:
            base_dir = self.output_dir
        
        # Check if we're already in post_processed directory
        if base_dir.name == "post_processed":
            self.post_processed_dir = base_dir
        else:
            self.post_processed_dir = base_dir / "post_processed"
        
        self.post_processed_dir.mkdir(exist_ok=True)
        
        logger.info("=" * 70)
        logger.info("Starting Processor Pipeline")
        logger.info("=" * 70)
        logger.info(f"Post-processed files → {self.post_processed_dir}\n")
        
        # Process 1: Brand Category & Cuisine (AI)
        if self.brands_file and not skip_ai:
            logger.info("\n[1/5] Processing: Brand Category & Cuisine...")
            try:
                BrandCategoryCuisineGenerator = load_processor_class(
                    "processors/01_ai_brand_category_cuisine_.py",
                    "BrandCategoryCuisineGenerator"
                )
                processor = BrandCategoryCuisineGenerator()
                # Save to post_processed directory
                brands_output = self.post_processed_dir / f"{self.brands_file.stem}_enriched.json"
                result = processor.process_brands_file(
                    str(self.brands_file),
                    output_file=str(brands_output)
                )
                results['brand_category_cuisine'] = result
                logger.info(f"✅ Completed: {result['stats']}")
            except Exception as e:
                logger.error(f"❌ Error in processor 1: {e}")
                results['brand_category_cuisine'] = {'error': str(e)}
        
        # Process 2: Menu Item Enrichment - Combined (description, price, cuisine)
        if self.menu_items_file and not skip_ai:
            logger.info("\n[2/5] Processing: Menu Item Enrichment (Description, Price, Cuisine)...")
            try:
                MenuItemEnricher = load_processor_class(
                    "processors/02_ai_enrich_menu_items.py",
                    "MenuItemEnricher"
                )
                processor = MenuItemEnricher()
                output_path = self._get_enriched_output_path(self.menu_items_file, 2)
                result = processor.process_menu_items_file(
                    str(self.menu_items_file),
                    output_file=str(output_path)
                )
                results['menu_enrichment'] = result
                self.menu_items_file = output_path
                logger.info(f"✅ Completed: {result['stats']}")
            except Exception as e:
                logger.error(f"❌ Error in processor 2: {e}")
                results['menu_enrichment'] = {'error': str(e)}
        
        # Process 3: Macronutrient Profile
        if self.menu_items_file:
            logger.info("\n[3/5] Processing: Macronutrient Profile...")
            try:
                MacronutrientProfileGenerator = load_processor_class(
                    "processors/03_macronutrient_profile.py",
                    "MacronutrientProfileGenerator"
                )
                processor = MacronutrientProfileGenerator()
                output_path = self._get_enriched_output_path(self.menu_items_file, 4)
                result = processor.process_menu_items_file(
                    str(self.menu_items_file),
                    output_file=str(output_path),
                    variations_file=str(self.variations_file) if self.variations_file else None
                )
                results['macronutrient_profile'] = result
                self.menu_items_file = output_path
                logger.info(f"✅ Completed: {result['stats']}")
            except Exception as e:
                logger.error(f"❌ Error in processor 4: {e}")
                results['macronutrient_profile'] = {'error': str(e)}
        
        # Process 4: Golden Ratio
        if self.variations_file:
            logger.info("\n[4/5] Processing: Golden Ratio...")
            try:
                GoldenRatioGenerator = load_processor_class(
                    "processors/04_golden_ratio.py",
                    "GoldenRatioGenerator"
                )
                processor = GoldenRatioGenerator()
                # Save enriched variations to post_processed directory
                name_parts = self.variations_file.stem.split('_')
                name_parts = [p for p in name_parts if p != 'enriched']
                if len(name_parts) >= 3 and name_parts[-1].isdigit():
                    base_name = '_'.join(name_parts[:-2]) + '_enriched_' + '_'.join(name_parts[-2:])
                else:
                    base_name = '_'.join(name_parts) + '_enriched'
                variations_output = self.post_processed_dir / f"{base_name}.json"
                
                result = processor.process_menu_items_file(
                    str(self.variations_file),
                    output_file=str(variations_output),
                    generate_stats=True
                )
                results['golden_ratio'] = result
                self.variations_file = variations_output
                logger.info(f"✅ Completed: {result['stats']}")
            except Exception as e:
                logger.error(f"❌ Error in processor 5: {e}")
                results['golden_ratio'] = {'error': str(e)}
        
        # Process 5: Map Menu Item Images from Variations
        if self.menu_items_file and self.variations_file:
            logger.info("\n[5/5] Processing: Map Menu Item Images...")
            try:
                MenuItemImageMapper = load_processor_class(
                    "processors/05_map_menu_item_images.py",
                    "MenuItemImageMapper"
                )
                processor = MenuItemImageMapper()
                output_path = self._get_enriched_output_path(self.menu_items_file, 5)
                result = processor.process_files(
                    str(self.menu_items_file),
                    str(self.variations_file),
                    output_file=str(output_path)
                )
                results['menu_item_images'] = result
                self.menu_items_file = output_path
                logger.info(f"✅ Completed: {result['stats']}")
            except Exception as e:
                logger.error(f"❌ Error in processor 5 (images): {e}")
                results['menu_item_images'] = {'error': str(e)}
        
        logger.info("\n" + "=" * 70)
        logger.info("Pipeline Complete!")
        logger.info("=" * 70)
        logger.info(f"\n📁 Post-processed files saved to: {self.post_processed_dir}")
        if self.menu_items_file:
            logger.info(f"   Menu items: {self.menu_items_file.name}")
        if self.variations_file:
            logger.info(f"   Variations: {self.variations_file.name}")
        if self.brands_file:
            logger.info(f"   Brands: {self.brands_file.name}")
        
        # Store post_processed_dir for MongoDB upload
        results['post_processed_dir'] = str(self.post_processed_dir)
        
        return results


def main():
    """Main entry point."""
    import argparse
    import os
    
    parser = argparse.ArgumentParser(
        description='Run all processors on scraped data'
    )
    parser.add_argument(
        '-d', '--dir',
        help='Output directory containing scraped files (default: output)',
        default='output'
    )
    parser.add_argument(
        '--skip-ai',
        help='Skip AI-based processors (faster, no API costs)',
        action='store_true'
    )
    parser.add_argument(
        '--upload-mongodb',
        help='Upload post-processed files to MongoDB',
        action='store_true'
    )
    parser.add_argument(
        '--mongodb-connection',
        help='MongoDB connection string (or set MONGODB_CONNECTION_STRING env var)',
        default=os.getenv('MONGODB_CONNECTION_STRING')
    )
    parser.add_argument(
        '--mongodb-database',
        help='MongoDB database name (default: restaurant_v2)',
        default='restaurant_v2'
    )
    
    args = parser.parse_args()
    
    try:
        # Setup logging first
        logger = setup_logging(args.dir)
        
        pipeline = ProcessorPipeline(args.dir)
        
        if not pipeline.find_latest_files():
            logger.error("❌ No scraped files found in output directory")
            logger.info("Run the scraper first using example_usage.py")
            sys.exit(1)
        
        results = pipeline.run_all_processors(skip_ai=args.skip_ai)
        
        # Summary
        print("\n" + "=" * 70)
        print("📊 PROCESSING SUMMARY")
        print("=" * 70)
        for processor_name, result in results.items():
            if processor_name == 'post_processed_dir':
                continue
            if 'error' in result:
                print(f"❌ {processor_name}: {result['error']}")
            elif 'stats' in result:
                stats = result['stats']
                print(f"✅ {processor_name}:")
                print(f"   Processed: {stats.get('processed', stats.get('generated', 0))}")
                if 'errors' in stats:
                    print(f"   Errors: {stats['errors']}")
        
        # Upload to MongoDB if requested
        if args.upload_mongodb:
            if not args.mongodb_connection:
                logger.error("MongoDB connection string required for upload")
                print("\n❌ Use --mongodb-connection or set MONGODB_CONNECTION_STRING")
                sys.exit(1)
            
            try:
                from utils.mongodb_uploader import MongoDBUploader
                
                logger.info("\n" + "=" * 70)
                logger.info("Starting MongoDB Upload")
                logger.info("=" * 70)
                
                uploader = MongoDBUploader(args.mongodb_connection, args.mongodb_database)
                upload_results = uploader.upload_post_processed_directory(
                    Path(results['post_processed_dir'])
                )
                uploader.close()
                
                print("\n✅ MongoDB upload complete!")
                print("📊 Upload summary:")
                for collection, stats in upload_results.items():
                    print(f"   {collection}: {stats}")
                    
            except Exception as e:
                logger.error(f"MongoDB upload failed: {e}")
                print(f"\n⚠️  MongoDB upload failed: {e}")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
