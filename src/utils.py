"""
Utility functions for data persistence, checkpointing, and file operations.
"""

import json
import os
import glob
import logging
from typing import Dict, List, Any, Optional
from pathlib import Path
from datetime import datetime

from config.models import (
    RestaurantCollection,
    MenuItemCollection,
    MenuItemVariationCollection,
)


logger = logging.getLogger(__name__)


class DataPersistence:
    """
    Handles saving and loading data in various formats.
    
    Supports:
    - JSON serialization/deserialization
    - Checkpoint management
    - Incremental saves
    """
    
    def __init__(self, output_dir: str = "output"):
        """
        Initialize data persistence handler.
        
        Args:
            output_dir: Directory for saving output files
        """
        self.output_dir = Path(output_dir)
        self.checkpoint_dir = self.output_dir / "checkpoints"
        self._ensure_directories()
    
    def _ensure_directories(self) -> None:
        """Create output directories if they don't exist."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
    
    def save_json(
        self, 
        data: Any, 
        filename: str, 
        pretty: bool = True
    ) -> None:
        """
        Save data to JSON file.
        
        Args:
            data: Data to save (must be JSON-serializable)
            filename: Output filename
            pretty: Whether to pretty-print JSON
        """
        filepath = self.output_dir / filename
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                if pretty:
                    json.dump(data, f, ensure_ascii=False, indent=2, default=str)
                else:
                    json.dump(data, f, ensure_ascii=False, default=str)
            
            logger.info(f"💾 Saved: {filepath}")
        except Exception as e:
            logger.error(f"❌ Failed to save {filepath}: {e}")
            raise
    
    def load_json(self, filename: str) -> Any:
        """
        Load data from JSON file.
        
        Args:
            filename: Input filename
        
        Returns:
            Loaded data
        """
        filepath = self.output_dir / filename
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info(f"📂 Loaded: {filepath}")
            return data
        except Exception as e:
            logger.error(f"❌ Failed to load {filepath}: {e}")
            raise
    
    def save_checkpoint(
        self, 
        data: Any, 
        checkpoint_name: str
    ) -> None:
        """
        Save a checkpoint file.
        
        Args:
            data: Data to checkpoint
            checkpoint_name: Name for the checkpoint
        """
        # Sanitize checkpoint name
        safe_name = checkpoint_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
        filename = f"checkpoint_{safe_name}.json"
        filepath = self.checkpoint_dir / filename
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
            
            logger.debug(f"💾 Checkpoint saved: {filename}")
        except Exception as e:
            logger.warning(f"⚠️  Failed to save checkpoint {filename}: {e}")
    
    def load_checkpoints(self, pattern: str = "checkpoint_*.json") -> Dict[str, Any]:
        """
        Load all checkpoint files matching a pattern.
        
        Args:
            pattern: Glob pattern for checkpoint files
        
        Returns:
            Dictionary of checkpoint data
        """
        checkpoint_files = glob.glob(str(self.checkpoint_dir / pattern))
        logger.info(f"🔄 Found {len(checkpoint_files)} checkpoint files")
        
        merged_data = {}
        
        for checkpoint_file in checkpoint_files:
            try:
                with open(checkpoint_file, 'r', encoding='utf-8') as f:
                    checkpoint_data = json.load(f)
                    merged_data.update(checkpoint_data)
                    
                    # Extract restaurant name from checkpoint data
                    if checkpoint_data:
                        restaurant_name = list(checkpoint_data.keys())[0]
                        logger.info(f"  ✅ Loaded checkpoint: {restaurant_name}")
            except Exception as e:
                logger.warning(f"  ⚠️  Failed to load {checkpoint_file}: {e}")
        
        return merged_data
    
    def save_collections(
        self,
        restaurants: RestaurantCollection,
        menu_items: MenuItemCollection,
        variations: MenuItemVariationCollection,
        timestamp: Optional[str] = None
    ) -> None:
        """
        Save normalized data collections to separate JSON files.
        
        Args:
            restaurants: Restaurant collection
            menu_items: Menu item collection
            variations: Menu item variation collection
            timestamp: Optional timestamp for filenames
        """
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Convert Pydantic models to dicts (use by_alias=True to output _id)
        restaurant_data = [r.dict(by_alias=True) for r in restaurants.restaurants]
        menu_item_data = [m.dict(by_alias=True) for m in menu_items.items]
        variation_data = [v.dict(by_alias=True) for v in variations.variations]
        
        # Save each collection
        self.save_json(restaurant_data, f"restaurant_brands_{timestamp}.json")
        self.save_json(menu_item_data, f"menu_items_{timestamp}.json")
        self.save_json(variation_data, f"menu_item_variations_{timestamp}.json")
        
        logger.info(f"✅ Saved all collections with timestamp: {timestamp}")


class ProgressTracker:
    """
    Tracks and displays progress during scraping operations.
    """
    
    def __init__(self, total: int, description: str = "Processing"):
        """
        Initialize progress tracker.
        
        Args:
            total: Total number of items to process
            description: Description of the operation
        """
        self.total = total
        self.current = 0
        self.description = description
        self.start_time = datetime.now()
    
    def update(self, increment: int = 1) -> None:
        """
        Update progress.
        
        Args:
            increment: Number to increment by
        """
        self.current += increment
        self._display_progress()
    
    def _display_progress(self) -> None:
        """Display current progress."""
        if self.total == 0:
            percentage = 100.0
        else:
            percentage = (self.current / self.total) * 100
        
        elapsed = datetime.now() - self.start_time
        
        if self.current > 0:
            avg_time_per_item = elapsed.total_seconds() / self.current
            remaining_items = self.total - self.current
            eta_seconds = avg_time_per_item * remaining_items
            eta = f"{int(eta_seconds // 60)}m {int(eta_seconds % 60)}s"
        else:
            eta = "calculating..."
        
        logger.info(
            f"📊 {self.description}: {self.current}/{self.total} "
            f"({percentage:.1f}%) | ETA: {eta}"
        )
    
    def complete(self) -> None:
        """Mark operation as complete and display summary."""
        elapsed = datetime.now() - self.start_time
        elapsed_str = f"{int(elapsed.total_seconds() // 60)}m {int(elapsed.total_seconds() % 60)}s"
        
        logger.info(
            f"✅ {self.description} complete: {self.total} items "
            f"in {elapsed_str}"
        )


def validate_url(url: str) -> bool:
    """
    Validate if a URL is properly formatted.
    
    Args:
        url: URL string to validate
    
    Returns:
        True if valid, False otherwise
    """
    return url.startswith('http://') or url.startswith('https://')


def sanitize_filename(filename: str) -> str:
    """
    Sanitize a string to be safe for use as a filename.
    
    Args:
        filename: String to sanitize
    
    Returns:
        Safe filename string
    """
    # Remove or replace unsafe characters
    unsafe_chars = '<>:"/\\|?*'
    for char in unsafe_chars:
        filename = filename.replace(char, '_')
    
    # Remove leading/trailing spaces and dots
    filename = filename.strip('. ')
    
    # Limit length
    max_length = 200
    if len(filename) > max_length:
        filename = filename[:max_length]
    
    return filename

