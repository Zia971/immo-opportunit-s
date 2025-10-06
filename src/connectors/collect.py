# src/connectors/collect.py
from typing import List, Dict
from .common import iter_sitemap, fetch
from .agencies import collect_agencies
from src.config_loader import load_sources_config

def parse_laforet_sitemap() -> List[Dict]:
    out = []
    for url in iter_sitemap("https://www.laforet.com/sitemap-annonces.xml"):
        if "971" not in url.lower() and "guadeloupe" not in url.lower():
            continue
        try:
            soup = fetch(url)
            title = (soup.find("h1") or {}).get_text(strip=True) if soup else "Bien Laforêt"
            price = _num_text(soup, ["[class*=price]", "[data-testid*=price]", ".price"])
            surface = _num_text(soup, ["[class*=surface]", "li:contains('m²')"])
            beds = _num_text(soup, ["[class*=chambre]", "[class*=bedroom]", "li:contains('chambre')"])
            photos = [img.get("src","") for img in soup.select("img") if "http" in img.get("src","")][:3]
            out.append(dict(
                id=url, url=url, title=title or "Bien Laforêt",
                price_total=price, surface_hab=surface, bedrooms=beds,
                photos=photos, source_name="laforet.com"
            ))
        except Exception:
            continue
    return out

def parse_orpi_sitemap() -> List[Dict]:
    out = []
    for url in iter_sitemap("https://www.orpi.com/sitemap.xml"):
        if "971" not in url.lower() and "guadeloupe" not in url.lower():
            continue
        try:
            soup = fetch(url)
            title = (soup.find("h1") or {}).get_text(strip=True) if soup else "Bien ORPI"
            price = _num_text(soup, ["[data-testid=price]", "[class*=price]", ".price"])
            surface = _num_text(soup, ["[class*=surface]", "li:contains('m²')"])
            beds = _num_text(soup, ["[class*=chambre]", "li:contains('chambre')"])
            photos = [img.get("src","") for img in soup.select("img") if "http" in img.get("src","")][:3]
            out.append(dict(
                id=url, url=url, title=title or "Bien ORPI",
                price_total=price, surface_hab=surface, bedrooms=beds,
                photos=photos, source_name="orpi.com"
            ))
        except Exception:
            continue
    return out

def _num_text(soup, selectors) -> int:
    import re
    if not soup:
        return 0
    for sel in selectors:
        try:
            el = soup.select_one(sel)
            if el:
                txt = el.get_text(" ", strip=True)
                m = re.search(r"(\d[\d\s]{0,10})", txt or "")
                if m:
                    return int(m.group(1).replace(" ", "").replace("\u202f",""))
        except Exception:
            continue
    return 0

def collect_all() -> List[Dict]:
    cfg = load_sources_config()
    data: List[Dict] = []

    # VERTS directs via sitemaps
    if any(s.get("name")=="Laforet971" and s.get("enabled") for s in cfg.get("sources", [])):
        data += parse_laforet_sitemap()
    if any(s.get("name")=="Orpi971" and s.get("enabled") for s in cfg.get("sources", [])):
        data += parse_orpi_sitemap()

    # Agences locales
    for s in cfg.get("sources", []):
        if s.get("name")=="AgencesLocales" and s.get("enabled"):
            base_urls = s.get("base_urls", [])
            data += collect_agencies(base_urls)

    # JAUNES (désactivés au départ ; on activera après validation CGU)
    # if any(s.get("name")=="BienIci" and s.get("enabled") for s in cfg.get("sources", [])):
    #     data += parse_bienici_listing()
    # ...

    return data
