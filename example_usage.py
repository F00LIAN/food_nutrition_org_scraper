"""
Example usage scripts for the Fast Food Nutrition Scraper.

Three complete pipeline options:
1. Single Restaurant - Quick test
2. Five Restaurants - Medium test
3. Full Pipeline - Production run

Each option allows running: Scrape only / Post-Process only / Upload only / Full Pipeline
"""

from main import FastFoodNutritionScraper
from config.config import ScraperSettings
from run_processors import ProcessorPipeline
from pathlib import Path
import os
import logging
from dotenv import load_dotenv
import shutil

# Load environment variables from .env file
load_dotenv()


def cleanup_output_directory(output_dir: str, skip_confirmation: bool = False):
    """
    Clean up output directory before running pipeline.
    
    Args:
        output_dir: Directory to clean
        skip_confirmation: Skip user confirmation
    """
    output_path = Path(output_dir)
    
    if not output_path.exists():
        print(f"📁 Output directory doesn't exist yet: {output_dir}")
        return
    
    # Check if directory has files
    files = list(output_path.rglob('*.json'))
    if not files:
        print(f"📁 Output directory is already clean: {output_dir}")
        return
    
    print(f"\n🗑️  Found {len(files)} files in {output_dir}")
    
    if not skip_confirmation:
        confirm = input("   Delete all files and start fresh? (yes/no): ").strip().lower()
        if confirm != 'yes':
            print("   Keeping existing files...")
            return
    
    try:
        shutil.rmtree(output_path)
        output_path.mkdir(parents=True, exist_ok=True)
        print(f"✅ Cleaned output directory: {output_dir}")
    except Exception as e:
        print(f"⚠️  Failed to clean directory: {e}")


def run_scraper_only(settings: ScraperSettings):
    """
    Run scraper only (Step 1 of pipeline).
    
    Args:
        settings: Scraper configuration
    """
    # Clean output directory
    cleanup_output_directory(settings.output_dir)
    
    print("\n" + "="*70)
    print("📥 Scraping Restaurant Data")
    print("="*70)
    scraper = FastFoodNutritionScraper(settings)
    scraper.run_full_pipeline()
    
    print("\n" + "="*70)
    print("✅ SCRAPING COMPLETE!")
    print("="*70)
    print(f"📁 Output directory: {settings.output_dir}")
    print("\n💡 Next steps:")
    print("   - Run post-processing to enrich the data")
    print("   - Or skip to upload if data is already post-processed")


def run_post_processing_only(output_dir: str):
    """
    Run post-processing only (Step 2 of pipeline).
    
    Args:
        output_dir: Directory containing scraped data files
    """
    print("\n" + "="*70)
    print("📊 Running Post-Processors")
    print("="*70)
    
    pipeline = ProcessorPipeline(output_dir)
    
    if not pipeline.find_latest_files():
        print("❌ No files found to process")
        print(f"   Make sure scraped files exist in: {output_dir}")
        return None
    
    results = pipeline.run_all_processors(skip_ai=False)
    
    print("\n" + "="*70)
    print("✅ POST-PROCESSING COMPLETE!")
    print("="*70)
    print(f"📁 Post-processed files: {results['post_processed_dir']}")
    print("\n💡 Next step:")
    print("   - Upload to MongoDB")
    
    return results


def run_upload_only(output_dir: str, database_name: str):
    """
    Run MongoDB upload only (Step 3 of pipeline).
    
    Args:
        output_dir: Directory containing post-processed data files
        database_name: MongoDB database name to upload to
    """
    mongodb_connection = os.getenv('MONGODB_CONNECTION_STRING')
    
    if not mongodb_connection:
        print("\n" + "="*70)
        print("❌ MongoDB Connection String Not Found")
        print("="*70)
        print("Set MONGODB_CONNECTION_STRING environment variable to enable upload")
        print("\nExample:")
        print("   export MONGODB_CONNECTION_STRING='mongodb+srv://...'")
        return
    
    print("\n" + "="*70)
    print(f"📤 Uploading to MongoDB ({database_name})")
    print("="*70)
    
    # Find post_processed directory
    post_processed_dir = Path(output_dir) / "post_processed"
    
    if not post_processed_dir.exists():
        print(f"❌ Post-processed directory not found: {post_processed_dir}")
        print("   Make sure you've run post-processing first")
        return
    
    try:
        from utils.mongodb_uploader import MongoDBUploader
        
        uploader = MongoDBUploader(mongodb_connection, database_name)
        upload_results = uploader.upload_post_processed_directory(post_processed_dir)
        uploader.close()
        
        print("\n" + "="*70)
        print("✅ MONGODB UPLOAD COMPLETE!")
        print("="*70)
        print(f"💾 Database: {database_name}")
        print("📊 Upload summary:")
        for collection, stats in upload_results.items():
            print(f"   {collection}: {stats}")
            
    except Exception as e:
        print(f"\n❌ MongoDB upload failed: {e}")


def run_complete_pipeline(settings: ScraperSettings, database_name: str):
    """
    Run complete pipeline: Scrape → Post-Process → Upload to MongoDB.
    
    Args:
        settings: Scraper configuration
        database_name: MongoDB database name to upload to
    """
    output_dir = settings.output_dir
    
    # Clean output directory
    cleanup_output_directory(output_dir)
    
    # Step 1: Scrape
    print("\n" + "="*70)
    print("📥 STEP 1/3: Scraping Restaurant Data")
    print("="*70)
    scraper = FastFoodNutritionScraper(settings)
    scraper.run_full_pipeline()
    
    # Step 2: Post-Process
    print("\n" + "="*70)
    print("📊 STEP 2/3: Running Post-Processors")
    print("="*70)
    pipeline = ProcessorPipeline(output_dir)
    
    if not pipeline.find_latest_files():
        print("❌ No files found to process")
        return
    
    results = pipeline.run_all_processors(skip_ai=False)
    
    # Step 3: Upload to MongoDB
    mongodb_connection = os.getenv('MONGODB_CONNECTION_STRING')
    
    if not mongodb_connection:
        print("\n" + "="*70)
        print("⚠️  STEP 3/3: MongoDB Upload Skipped")
        print("="*70)
        print("Set MONGODB_CONNECTION_STRING environment variable to enable upload")
        print("Example: export MONGODB_CONNECTION_STRING='mongodb+srv://...'")
    else:
        print("\n" + "="*70)
        print(f"📤 STEP 3/3: Uploading to MongoDB ({database_name})")
        print("="*70)
        
        try:
            from utils.mongodb_uploader import MongoDBUploader
            
            uploader = MongoDBUploader(mongodb_connection, database_name)
            upload_results = uploader.upload_post_processed_directory(
                Path(results['post_processed_dir'])
            )
            uploader.close()
            
            print("\n✅ MongoDB upload complete!")
            print(f"📊 Database: {database_name}")
            print("📊 Upload summary:")
            for collection, stats in upload_results.items():
                print(f"   {collection}: {stats}")
                
        except Exception as e:
            print(f"\n⚠️  MongoDB upload failed: {e}")
    
    # Final Summary
    print("\n" + "="*70)
    print("✅ COMPLETE PIPELINE SUCCESS!")
    print("="*70)
    print(f"📁 Post-processed files: {results['post_processed_dir']}")
    if mongodb_connection:
        print(f"💾 Database: {database_name}")


def get_pipeline_settings(pipeline_type: str):
    """Get settings for a specific pipeline type."""
    if pipeline_type == "1":  # Single Restaurant
        return {
            "settings": ScraperSettings(
        specific_restaurants=["McDonald's"],  # Change restaurant here
        output_dir="output/example/single_restaurant",
        max_items_per_restaurant=50,
        log_level="INFO"
            ),
            "database_name": os.getenv('TEST_DATABASE_NAME', 'restaurant_v2_test'),
            "name": "Single Restaurant",
            "description": "Quick test (1 restaurant, 50 items)"
        }
    elif pipeline_type == "2":  # Five Restaurants
        return {
            "settings": ScraperSettings(
                specific_restaurants=["Honeygrow", "Chick-fil-A", "McDonald's", "Applebee's", "Taco Bell"],
        output_dir="output/example/five_restaurants",
        max_items_per_restaurant=100,
        log_level="INFO"
            ),
            "database_name": os.getenv('TEST_DATABASE_NAME', 'restaurant_v2_test'),
            "name": "Five Restaurants",
            "description": "Medium test (5 specific restaurants, 100 items each)"
        }
    elif pipeline_type == "3":  # Full Pipeline
        return {
            "settings": ScraperSettings(
                output_dir="output/production/full_pipeline",
                log_level="INFO"
            ),
            "database_name": os.getenv('PRODUCTION_DATABASE_NAME', 'restaurant_v2'),
            "name": "Full Pipeline",
            "description": "All restaurants (PRODUCTION)"
        }
    return None


def show_step_menu(pipeline_config):
    """Show menu for selecting which step to run."""
    print("\n" + "="*70)
    print(f"📋 {pipeline_config['name']} - Select Step")
    print("="*70)
    print(f"   {pipeline_config['description']}")
    print(f"   Output: {pipeline_config['settings'].output_dir}")
    print(f"   Database: {pipeline_config['database_name']}")
    print("="*70)
    print("\n  1. 📥 Scrape Only")
    print("     Run scraper to fetch restaurant data")
    print()
    print("  2. 📊 Post-Process Only")
    print("     Run AI enrichment and calculations on existing data")
    print()
    print("  3. 📤 Upload Only")
    print("     Upload post-processed data to MongoDB")
    print()
    print("  4. 🔄 Full Pipeline")
    print("     Run all steps: Scrape → Post-Process → Upload")
    print()
    print("  b. Back to main menu")
    print("="*70)
    
    step_choice = input("\nSelect step (1-4, or 'b' for back): ").strip()
    return step_choice


def main():
    """Main menu and execution logic."""
    
    while True:
        print("\n" + "="*70)
        print("🍔 Fast Food Nutrition Scraper - Pipeline Manager")
        print("="*70)
        print("\nSelect Pipeline Type:")
        print("="*70)
        print("  1. 🎯 Single Restaurant (~2 min)")
        print("     - Quick test run")
        print("     - 1 restaurant, 50 menu items")
        print("     - Output: output/example/single_restaurant")
        print()
        print("  2. 🔥 Five Restaurants (~10 min)")
        print("     - Medium test run")
        print("     - 5 restaurants, 100 items each")
        print("     - Output: output/example/five_restaurants")
        print()
        print("  3. 🚀 Full Pipeline (PRODUCTION - several hours)")
        print("     - Scrapes ALL restaurants")
        print("     - Complete dataset")
        print("     - Output: output/production/full_pipeline")
        print()
        print("  q. Quit")
        print("="*70)
        print("\n💡 Environment Variables:")
        print("   MONGODB_CONNECTION_STRING - Required for database upload")
        print("   TEST_DATABASE_NAME        - Test DB (default: restaurant_v2_test)")
        print("   PRODUCTION_DATABASE_NAME  - Prod DB (default: restaurant_v2)")
        print("\n   Example:")
        print("   export MONGODB_CONNECTION_STRING='mongodb+srv://...'")
        print("   export TEST_DATABASE_NAME='restaurant_v2_test'")
        print("   export PRODUCTION_DATABASE_NAME='restaurant_v2'")
        print("="*70)
        
        pipeline_choice = input("\nSelect option (1-3, or 'q' to quit): ").strip()
        
        if pipeline_choice.lower() == 'q':
            print("\nGoodbye! 👋")
            break
        
        if pipeline_choice not in ["1", "2", "3"]:
            print(f("\n⚠️  Invalid choice: {pipeline_choice}"))
            continue
        
        # Get pipeline configuration
        pipeline_config = get_pipeline_settings(pipeline_choice)
        
        # Production warning for option 3
        if pipeline_choice == "3":
            print(f"\n⚠️  WARNING: This will use PRODUCTION database '{pipeline_config['database_name']}'")
            print("   Full scraping may take several hours.")
            confirm = input("\nContinue? (yes/no): ").strip().lower()
            if confirm != 'yes':
                print("Cancelled.")
                continue
        
        # Show step selection menu
        step_choice = show_step_menu(pipeline_config)
        
        if step_choice == 'b':
            continue
        elif step_choice == '1':
            # Scrape only
            run_scraper_only(pipeline_config['settings'])
        elif step_choice == '2':
            # Post-process only
            run_post_processing_only(pipeline_config['settings'].output_dir)
        elif step_choice == '3':
            # Upload only
            run_upload_only(
                pipeline_config['settings'].output_dir,
                pipeline_config['database_name']
            )
        elif step_choice == '4':
            # Full pipeline
            run_complete_pipeline(
                pipeline_config['settings'],
                pipeline_config['database_name']
            )
        else:
            print(f"\n⚠️  Invalid choice: {step_choice}")
        
        # Ask if user wants to continue
        continue_choice = input("\n\nRun another pipeline? (y/n): ").strip().lower()
        if continue_choice != 'y':
            print("\nGoodbye! 👋")
            break


if __name__ == "__main__":
    main()
