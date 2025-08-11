import json
import curl_cffi
from bs4 import BeautifulSoup as soup
from urllib.parse import urljoin


BASE_URL = "https://fastfoodnutrition.org"
START_URL = f"{BASE_URL}/fast-food-restaurants"


def extract_restaurants() -> list:
    """Return a list of {name, url} for restaurants on the page."""
    session = curl_cffi.Session(impersonate="chrome")
    response = session.get(START_URL)

    document = soup(response.text, "html.parser")
    container = document.select_one(".rest_item_list.category")
    if not container:
        return []

    results_by_url = {}
    for card in container.select(".filter_target"):
        anchor = card.find("a", href=True)
        if not anchor:
            continue
        absolute_url = urljoin(BASE_URL, anchor["href"])

        label = card.find("div", class_="logo_box_text")
        # Prefer the first text node only (excludes the nested span " Nutrition")
        raw_name = label.find(string=True, recursive=False).strip() if label else None
        if not raw_name:
            # Fallback: remove trailing " Nutrition" if present
            text = label.get_text(" ", strip=True) if label else ""
            raw_name = text.replace(" Nutrition", "").strip() if text else None

        if raw_name:
            results_by_url[absolute_url] = {"name": raw_name, "url": absolute_url}

    return list(results_by_url.values())


def save_to_json(data: list, output_path: str = "restaurants.json") -> None:
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    restaurants = extract_restaurants()
    save_to_json(restaurants)
    print(f"Saved {len(restaurants)} restaurants to restaurants.json")

