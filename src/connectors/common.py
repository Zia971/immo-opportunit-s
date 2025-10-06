# src/connectors/common.py
import time, re, requests
from bs4 import BeautifulSoup

UA = "Mozilla/5.0 (compatible; ImmoAgent971/1.0; +https://immo-opportunites.streamlit.app)"

def fetch(url, sleep=0.8, parser="html.parser"):
    time.sleep(sleep)  # throttling doux
    r = requests.get(url, headers={"User-Agent": UA}, timeout=20)
    r.raise_for_status()
    return BeautifulSoup(r.text, parser)

def iter_sitemap(url):
    """Itère sur les URLs d'annonces depuis un sitemap (si présent)."""
    soup = fetch(url, parser="xml")
    for loc in soup.find_all("loc"):
        href = loc.text.strip()
        if re.search(r"/(annonce|bien|vente)/", href):
            yield href
