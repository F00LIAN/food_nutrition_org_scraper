import json
import os
from typing import List, Dict, Tuple

class URLGenerator:
    """Generates URLs for scraping based on configuration"""

    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = os.path.json(os.path.dirname(__file__), 'scraper_config.json')

        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
    def generate_urls(self, site: str="fastfoodnutrition") -> List[Tuple[str, Dict]]:
        """
        Generate base URLs for scraping based on configuration

        Returns:
            List of tuples containing (base_url, params)
        """
        site_config = self.config[site]
        urls = []

        for restaurant in site_config["restaurants"]:
            restaurant_urls = self.

    def _generate_restaurant_urls(self, restaurant: Dict, base_url: str) -> List[str]:
        """Generate URLs for each restaurant chain off of the base URL grab""" 
        base_url = site_config["base_url"]
        