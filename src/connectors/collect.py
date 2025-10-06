# src/connectors/collect.py
import re, math
from typing import List, Dict
from bs4 import BeautifulSoup
from .common import fetch, iter_sitemap

def parse_laforet_sitemap() -> List[Dict]:
    out = []
    for url in iter_sitemap("https://www.laforet.com/sitemap.xml"):
        if "guadeloupe" not in url.lower():
            continue
        try:
            soup = fetch(url)
            title = (soup.find("h1") or {}).get_text(strip=True)
            price = _num(soup.select_one("[class*=price]"))
            surface = _num(_first(soup, ["[class*=surface]", "li:contains('m²')"]))
            beds = _num(_first(soup, ["[class*=bedroom]", "li:contains('chambre')"]))
            photos = [img["src"] for img in soup.select("img") if "http" in img.get("src","")][:3]
            out.append(dict(
                id=url, url=url, title=title or "Bien Laforêt",
                price_total=price, surface_hab=surface, bedrooms=beds,
                photos=photos, source_name="Laforet"
            ))
        except Exception:
            continue
    return out

def parse_orpi_sitemap() -> List[Dict]:
    out = []
    for url in iter_sitemap("https://www.orpi.com/sitemap.xml"):
        if not any(k in url.lower() for k in ["guadeloupe","971"]):
            continue
        try:
            soup = fetch(url)
            title = (soup.find("h1") or {}).get_text(strip=True)
            price = _num(_first(soup, ["[data-testid=price]","[class*=price]"]))
            surface = _num(_first(soup, ["[class*=surface]","li:contains('m²')"]))
            beds = _num(_first(soup, ["[class*=chambre]","li:contains('chambre')"]))
            photos = [img["src"] for img in soup.select("img") if "http" in img.get("src","")][:3]
            out.append(dict(
                id=url, url=url, title=title or "Bien ORPI",
                price_total=price, surface_hab=surface, bedrooms=beds,
                photos=photos, source_name="ORPI"
            ))
        except Exception:
            continue
    return out

def parse_logicimmo_listing() -> List[Dict]:
    # Collecte légère des pages “liste” Guadeloupe (si dispo sans interdiction)
    # (Ici on laisse simple : on peut raffiner ensuite)
    return []

def parse_bienici_listing() -> List[Dict]:
    return []

def parse_domimmo_listing() -> List[Dict]:
    return []

# Helpers
def _num(node):
    if not node: return 0
    txt = node.get_text(" ", strip=True) if hasattr(node, "get_text") else str(node)
    m = re.search(r"(\d[\d\s]{1,})", txt)
    if not m: return 0
    return int(m.group(1).replace(" ", ""))

def _first(soup: BeautifulSoup, selectors):
    for sel in selectors:
        el = soup.select_one(sel)
        if el: return el
    return None

def collect_all() -> List[Dict]:
    data = []
    data += parse_laforet_sitemap()
    data += parse_orpi_sitemap()
    # Les trois suivants sont laissés vides pour l’instant (on les activera si autorisés):
    data += parse_logicimmo_listing()
    data += parse_bienici_listing()
    data += parse_domimmo_listing()
    return data
