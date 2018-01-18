import requests
from bs4 import BeautifulSoup

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_volcanoes_by_elevation"

req = requests.get(WIKI_URL)
soup = BeautifulSoup(req.text, 'lxml')
table_classes = {"class": ["sortable", "plainrowheaders"]}
wikitables = soup.findAll("table", table_classes)

print(wikitables)
