### Fast Food Scraper
import curl_cffi
from bs4 import BeautifulSoup
import json
import requests

session = curl_cffi.Session(impersonate="chrome")

response = session.get("https://fastfoodnutrition.org/krispy-kreme/hot-chocolate-with-water/large#nutrition_label")

soup = BeautifulSoup(response.text, 'html.parser')

print(soup.prettify())



