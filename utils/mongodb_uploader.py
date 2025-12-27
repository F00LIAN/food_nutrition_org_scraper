"""
MongoDB Uploader for Post-Processed Restaurant Data.

Uploads enriched data to restaurant_v2 database with upsert logic.
Runs monthly - overwrites existing data, no historical tracking.
Includes Pydantic validation to prevent malformed data from reaching database.
"""
import json
import logging
from pathlib import Path
from typing import Dict, Any, List
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from datetime import datetime
from tqdm import tqdm

# Import validators
try:
    from .validators import (
        validate_brands,
        validate_menu_items,
        validate_variations,
        save_validation_report
    )
    VALIDATION_AVAILABLE = True
except ImportError:
    logger.warning("Validation module not available - uploading without validation")
    VALIDATION_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MongoDBUploader:
    """Uploads post-processed restaurant data to MongoDB with upsert logic."""
    
    def __init__(self, connection_string: str, database_name: str = "restaurant_v2"):
        """
        Initialize MongoDB uploader.
        
        Args:
            connection_string: MongoDB connection string
            database_name: Database name (default: restaurant_v2)
        """
        self.client = MongoClient(connection_string)
        self.db = self.client[database_name]
        logger.info(f"Connected to MongoDB database: {database_name}")
    
    def upsert_brands(self, brands_file: Path, skip_validation: bool = False) -> Dict[str, int]:
        """
        Upsert restaurant brands to restaurant_brands collection.
        
        Args:
            brands_file: Path to enriched brands JSON file
            skip_validation: Skip Pydantic validation (not recommended)
            
        Returns:
            Statistics dict with upserted/matched counts
        """
        logger.info(f"Loading brands from {brands_file}")
        with open(brands_file, 'r', encoding='utf-8') as f:
            brands = json.load(f)
        
        if not brands:
            logger.warning("No brands to upload")
            return {'upserted': 0, 'matched': 0, 'errors': 0, 'validation_errors': 0}
        
        # Validate data before upload
        validation_errors = 0
        if VALIDATION_AVAILABLE and not skip_validation:
            logger.info("Validating brands data...")
            brands, invalid = validate_brands(brands)
            validation_errors = len(invalid)
            
            if invalid:
                logger.warning(f"Found {len(invalid)} invalid brands - saving error report")
                save_validation_report({'brands': invalid}, brands_file.parent)
        
        if not brands:
            logger.error("No valid brands to upload after validation")
            return {'upserted': 0, 'matched': 0, 'errors': 0, 'validation_errors': validation_errors}
        
        collection = self.db['restaurant_brands']
        operations = []
        
        for brand in brands:
            # Upsert based on _id
            operations.append(
                UpdateOne(
                    {'_id': brand['_id']},
                    {'$set': brand},
                    upsert=True
                )
            )
        
        logger.info(f"Upserting {len(operations)} brands...")
        try:
            result = collection.bulk_write(operations, ordered=False)
            stats = {
                'upserted': result.upserted_count,
                'matched': result.matched_count,
                'modified': result.modified_count,
                'errors': 0
            }
            logger.info(f"Brands upsert complete: {stats}")
            return stats
        except BulkWriteError as e:
            logger.error(f"Bulk write error: {e.details}")
            return {'upserted': 0, 'matched': 0, 'errors': len(e.details['writeErrors'])}
    
    def upsert_menu_items(self, menu_items_file: Path, skip_validation: bool = False) -> Dict[str, int]:
        """
        Upsert menu items to menu_items collection.
        
        Args:
            menu_items_file: Path to enriched menu items JSON file
            skip_validation: Skip Pydantic validation (not recommended)
            
        Returns:
            Statistics dict with upserted/matched counts
        """
        logger.info(f"Loading menu items from {menu_items_file}")
        with open(menu_items_file, 'r', encoding='utf-8') as f:
            items = json.load(f)
        
        if not items:
            logger.warning("No menu items to upload")
            return {'upserted': 0, 'matched': 0, 'errors': 0, 'validation_errors': 0}
        
        # Validate data before upload
        validation_errors = 0
        if VALIDATION_AVAILABLE and not skip_validation:
            logger.info("Validating menu items data...")
            items, invalid = validate_menu_items(items)
            validation_errors = len(invalid)
            
            if invalid:
                logger.warning(f"Found {len(invalid)} invalid menu items - saving error report")
                save_validation_report({'menu_items': invalid}, menu_items_file.parent)
        
        if not items:
            logger.error("No valid menu items to upload after validation")
            return {'upserted': 0, 'matched': 0, 'errors': 0, 'validation_errors': validation_errors}
        
        collection = self.db['menu_items']
        operations = []
        
        for item in tqdm(items, desc="Preparing menu items"):
            # Upsert based on _id
            operations.append(
                UpdateOne(
                    {'_id': item['_id']},
                    {'$set': item},
                    upsert=True
                )
            )
        
        logger.info(f"Upserting {len(operations)} menu items...")
        try:
            result = collection.bulk_write(operations, ordered=False)
            stats = {
                'upserted': result.upserted_count,
                'matched': result.matched_count,
                'modified': result.modified_count,
                'validation_errors': validation_errors,
                'errors': 0
            }
            logger.info(f"Menu items upsert complete: {stats}")
            return stats
        except BulkWriteError as e:
            logger.error(f"Bulk write error: {e.details}")
            return {'upserted': 0, 'matched': 0, 'errors': len(e.details['writeErrors'])}
    
    def upsert_variations(self, variations_file: Path, skip_validation: bool = False) -> Dict[str, int]:
        """
        Upsert menu item variations to menu_item_variations collection.
        
        Args:
            variations_file: Path to enriched variations JSON file
            skip_validation: Skip Pydantic validation (not recommended)
            
        Returns:
            Statistics dict with upserted/matched counts
        """
        logger.info(f"Loading variations from {variations_file}")
        with open(variations_file, 'r', encoding='utf-8') as f:
            variations = json.load(f)
        
        if not variations:
            logger.warning("No variations to upload")
            return {'upserted': 0, 'matched': 0, 'errors': 0, 'validation_errors': 0}
        
        # Validate data before upload
        validation_errors = 0
        if VALIDATION_AVAILABLE and not skip_validation:
            logger.info("Validating variations data...")
            variations, invalid = validate_variations(variations)
            validation_errors = len(invalid)
            
            if invalid:
                logger.warning(f"Found {len(invalid)} invalid variations - saving error report")
                save_validation_report({'variations': invalid}, variations_file.parent)
        
        if not variations:
            logger.error("No valid variations to upload after validation")
            return {'upserted': 0, 'matched': 0, 'errors': 0, 'validation_errors': validation_errors}
        
        collection = self.db['menu_item_variations']
        operations = []
        
        for variation in tqdm(variations, desc="Preparing variations"):
            # Upsert based on _id
            operations.append(
                UpdateOne(
                    {'_id': variation['_id']},
                    {'$set': variation},
                    upsert=True
                )
            )
        
        logger.info(f"Upserting {len(operations)} variations...")
        try:
            result = collection.bulk_write(operations, ordered=False)
            stats = {
                'upserted': result.upserted_count,
                'matched': result.matched_count,
                'modified': result.modified_count,
                'validation_errors': validation_errors,
                'errors': 0
            }
            logger.info(f"Variations upsert complete: {stats}")
            return stats
        except BulkWriteError as e:
            logger.error(f"Bulk write error: {e.details}")
            return {'upserted': 0, 'matched': 0, 'errors': len(e.details['writeErrors'])}
    
    def upload_post_processed_directory(self, post_processed_dir: Path) -> Dict[str, Any]:
        """
        Upload all post-processed files from a directory.
        
        Args:
            post_processed_dir: Path to post_processed directory
            
        Returns:
            Dictionary with upload statistics for each collection
        """
        logger.info("=" * 70)
        logger.info("Starting MongoDB Upload")
        logger.info("=" * 70)
        logger.info(f"Source: {post_processed_dir}\n")
        
        results = {}
        
        # Find and upload brands
        brands_files = list(post_processed_dir.glob("*restaurant_brands*enriched*.json"))
        if brands_files:
            results['brands'] = self.upsert_brands(brands_files[0])
        
        # Find and upload menu items
        menu_items_files = list(post_processed_dir.glob("*menu_items_enriched*.json"))
        if menu_items_files:
            results['menu_items'] = self.upsert_menu_items(menu_items_files[0])
        
        # Find and upload variations
        variations_files = list(post_processed_dir.glob("*menu_item_variations_enriched*.json"))
        if variations_files:
            results['variations'] = self.upsert_variations(variations_files[0])
        
        logger.info("\n" + "=" * 70)
        logger.info("MongoDB Upload Complete!")
        logger.info("=" * 70)
        for collection, stats in results.items():
            logger.info(f"{collection}: {stats}")
        
        return results
    
    def close(self):
        """Close MongoDB connection."""
        self.client.close()
        logger.info("MongoDB connection closed")


def main():
    """Main entry point for MongoDB uploader."""
    import argparse
    import os
    
    parser = argparse.ArgumentParser(
        description='Upload post-processed restaurant data to MongoDB'
    )
    parser.add_argument(
        'post_processed_dir',
        help='Path to post_processed directory'
    )
    parser.add_argument(
        '--connection-string',
        help='MongoDB connection string (or set MONGODB_CONNECTION_STRING env var)',
        default=os.getenv('MONGODB_CONNECTION_STRING')
    )
    parser.add_argument(
        '--database',
        help='Database name (default: restaurant_v2)',
        default='restaurant_v2'
    )
    
    args = parser.parse_args()
    
    if not args.connection_string:
        print("❌ Error: MongoDB connection string required")
        print("   Use --connection-string or set MONGODB_CONNECTION_STRING environment variable")
        exit(1)
    
    try:
        uploader = MongoDBUploader(args.connection_string, args.database)
        results = uploader.upload_post_processed_directory(Path(args.post_processed_dir))
        uploader.close()
        
        print("\n✅ Upload complete!")
        print(f"📊 Summary:")
        for collection, stats in results.items():
            print(f"   {collection}: {stats}")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"\n❌ Error: {e}")
        exit(1)


if __name__ == "__main__":
    main()

