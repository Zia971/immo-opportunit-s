# src/connectors/collect.py
import re
from typing import List, Dict
from bs4 import BeautifulSoup
from .common import fetch, iter_sitemap

def parse_laforet_sitemap() -> List[Dict]:
    out = []
    # Plusieurs sitemaps possibles, on essaie en cascade
    sitemaps = [
        "https://www.laforet.com/sitemap-annonces.xml",
        "https://www.laforet.com/sitemap.xml",
    ]
    for sm in sitemaps:
        for url in iter_sitemap(sm):
            if not any(k in url.lower() for k in ["971", "guadeloupe"]):
                continue
            try:
                soup = fetch(url)
                title = _text(soup.find("h1")) or "Bien Laforêt"
                price = _num(_first(soup, ["[class*=price]", "[data-testid*=price]"]))
                surface = _num(_first(soup, ["[class*=surface]", "li:contains('m²')"]))
                beds = _num(_first(soup, ["[class*=chambre]", "[class*=bedroom]", "li:contains('chambre')"]))
                photos = [img.get("src","") for img in soup.select("img") if "http" in img.get("src","")][:3]
                out.append(dict(
                    id=url, url=url, title=title,
                    price_total=price, surface_hab=surface, bedrooms=beds,
                    photos=photos, source_name="Laforet"
                ))
            except Exception:
                continue
    return out

def parse_orpi_sitemap() -> List[Dict]:
    out = []
    sm = "https://www.orpi.com/sitemap.xml"  # sans slash final
    for url in iter_sitemap(sm):
        if not any(k in url.lower() for k in ["guadeloupe", "971"]):
            continue
        try:
            soup = fetch(url)
            title = _text(soup.find("h1")) or "Bien ORPI"
            price = _num(_first(soup, ["[data-testid=price]", "[class*=price]"]))
            surface = _num(_first(soup, ["[class*=surface]", "li:contains('m²')"]))
            beds = _num(_first(soup, ["[class*=chambre]", "li:contains('chambre')"]))
            photos = [img.get("src","") for img in soup.select("img") if "http" in img.get("src","")][:3]
            out.append(dict(
                id=url, url=url, title=title,
                price_total=price, surface_hab=surface, bedrooms=beds,
                photos=photos, source_name="ORPI"
            ))
        except Exception:
            continue
    return out

def parse_logicimmo_listing() -> List[Dict]:
    # Connecteur JAUNE : on activera seulement des routes publiques permises.
    # Pour l’instant, on renvoie une liste vide proprement.
    return []

def parse_bienici_listing() -> List[Dict]:
    # Idem, on active uniquement des listings publics conformes.
    return []

def parse_domimmo_listing() -> List[Dict]:
    # Idem.
    return []

# Helpers
def _num(node):
    if not node: return 0
    txt = node.get_text(" ", strip=True) if hasattr(node, "get_text") else str(node)
    m = re.search(r"(\d[\d\s]{0,10})", txt)
    if not m: return 0
    try:
        return int(m.group(1).replace(" ", "").replace("\u202f",""))
    except Exception:
        return 0

def _first(soup: BeautifulSoup, selectors):
    for sel in selectors:
        try:
            el = soup.select_one(sel)
            if el: return el
        except Exception:
            continue
    return None

def _text(node):
    try:
        return node.get_text(strip=True)
    except Exception:
        return ""

def collect_all() -> List[Dict]:
    data = []
    data += parse_laforet_sitemap()
    data += parse_orpi_sitemap()
    # Les suivants restent vides tant qu’on n’a pas validé leurs routes publiques :
    data += parse_logicimmo_listing()
    data += parse_bienici_listing()
    data += parse_domimmo_listing()
    return data
