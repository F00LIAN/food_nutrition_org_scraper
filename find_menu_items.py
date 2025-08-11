import json
from typing import Dict, List
from urllib.parse import urljoin

import curl_cffi
from bs4 import BeautifulSoup as soup


BASE_URL = "https://fastfoodnutrition.org"
INPUT_JSON = "restaurants.json"
OUTPUT_JSON = "menu_items.json"


def load_restaurants(path: str = INPUT_JSON) -> List[Dict]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # expected: [{"name": str, "url": str}, ...]
    return data


def extract_menu_items(restaurant_url: str) -> List[Dict[str, str]]:
    session = curl_cffi.Session(impersonate="chrome")
    response = session.get(restaurant_url)

    document = soup(response.text, "html.parser")

    items: List[Dict[str, str]] = []

    # Find anchors under containers with classes: list, rest_item_list, ab1
    for anchor in document.select("ul.list.rest_item_list.ab1 a[href]"):
        # Item name is the first direct string within the anchor (exclude nested nodes)
        raw_name = anchor.find(string=True, recursive=False)
        item_name = raw_name.strip() if raw_name else anchor.get_text(" ", strip=True)
        item_url = urljoin(BASE_URL, anchor["href"])

        items.append({"name": item_name, "url": item_url})

    # de-duplicate while preserving order (by URL)
    seen = set()
    unique_items = []
    for itm in items:
        if itm["url"] in seen:
            continue
        seen.add(itm["url"])
        unique_items.append(itm)

    return unique_items


def build_menu_index(restaurants: List[Dict]) -> Dict[str, Dict]:
    index: Dict[str, Dict] = {}
    for entry in restaurants:
        name = entry.get("name")
        url = entry.get("url")
        if not name or not url:
            continue
        index[name] = {
            "url": url,
            "items": extract_menu_items(url),
        }
    return index


def save_to_json(data: Dict, output_path: str = OUTPUT_JSON) -> None:
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    restaurants = load_restaurants()
    menu_index = build_menu_index(restaurants)
    save_to_json(menu_index)
    print(f"Saved menu items for {len(menu_index)} restaurants to {OUTPUT_JSON}")






