# src/connectors/collect.py
from typing import List, Dict
from .common import iter_sitemap, fetch, is_asset_url
from .agencies import collect_agencies
from src.config_loader import load_sources_config

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

def parse_laforet_sitemap() -> List[Dict]:
    out = []
    for url in iter_sitemap("https://www.laforet.com/sitemap-annonces.xml"):
        if "971" not in url.lower() and "guadeloupe" not in url.lower():
            continue
        try:
            soup = fetch(url)
            if not soup: 
                continue
            title = (soup.find("h1") or {}).get_text(strip=True) or "Bien Laforêt"
            price = _num_text(soup, ["[class*=price]", "[data-testid*=price]", ".price"])
            surface = _num_text(soup, ["[class*=surface]", "li:contains('m²')"])
            beds = _num_text(soup, ["[class*=chambre]", "[class*=bedroom]", "li:contains('chambre')"])
            photos = [img.get("src","") for img in soup.select("img") if isinstance(img.get("src",""), str) and img.get("src","").startswith(("http://","https://"))][:3]
            if price <= 0:
                continue
            out.append(dict(
                id=url, url=url, title=title,
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
            if not soup:
                continue
            title = (soup.find("h1") or {}).get_text(strip=True) or "Bien ORPI"
            price = _num_text(soup, ["[data-testid=price]", "[class*=price]", ".price"])
            surface = _num_text(soup, ["[class*=surface]", "li:contains('m²')"])
            beds = _num_text(soup, ["[class*=chambre]", "li:contains('chambre')"])
            photos = [img.get("src","") for img in soup.select("img") if isinstance(img.get("src",""), str) and img.get("src","").startswith(("http://","https://"))][:3]
            if price <= 0:
                continue
            out.append(dict(
                id=url, url=url, title=title,
                price_total=price, surface_hab=surface, bedrooms=beds,
                photos=photos, source_name="orpi.com"
            ))
        except Exception:
            continue
    return out

def collect_all() -> List[Dict]:
    cfg = load_sources_config()
    data: List[Dict] = []

    if any(s.get("name")=="Laforet971" and s.get("enabled") for s in cfg.get("sources", [])):
        data += parse_laforet_sitemap()
    if any(s.get("name")=="Orpi971" and s.get("enabled") for s in cfg.get("sources", [])):
        data += parse_orpi_sitemap()

    for s in cfg.get("sources", []):
        if s.get("name")=="AgencesLocales" and s.get("enabled"):
            data += collect_agencies(s.get("base_urls", []))

    # filtre final : pas d’asset, prix > 0
    cleaned = []
    for r in data:
        if not r:
            continue
        if is_asset_url(r.get("url","")):
            continue
        try:
            if int(r.get("price_total", 0)) <= 0:
                continue
        except Exception:
            continue
        cleaned.append(r)
    return cleaned
