# src/connectors/agencies.py
import re
from typing import List, Dict
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from .common import fetch

MAX_PER_SITE = 40  # plafond doux par run

# Heuristiques génériques pour trouver les pages "ventes" sur un site d'agence
CANDIDATE_LIST_PATHS = [
    "vente", "ventes", "nos-biens", "nos-biens/vente", "annonces", "biens",
    "acheter", "achat", "immobilier/vente", "biens-a-vendre"
]

def _absolutize(base, href):
    if not href:
        return None
    if href.startswith("mailto:") or href.startswith("tel:"):
        return None
    return urljoin(base, href)

def _looks_like_listing_url(url: str) -> bool:
    u = url.lower()
    return any(k in u for k in ["vente", "annonce", "biens", "acheter", "achat"])

def _looks_like_detail_url(url: str) -> bool:
    u = url.lower()
    return any(k in u for k in ["/bien", "/annonce", "/maison", "/appartement", "/terrain"])

def _num_from_text(text: str) -> int:
    if not text:
        return 0
    m = re.search(r"(\d[\d\s]{0,10})", text)
    if not m:
        return 0
    try:
        return int(m.group(1).replace(" ", "").replace("\u202f",""))
    except Exception:
        return 0

def discover_listing_pages(home_url: str) -> List[str]:
    """Tente de trouver 1-2 pages 'liste des biens' à partir de la homepage d'agence."""
    pages = set()
    soup = fetch(home_url)
    for a in soup.select("a[href]"):
        href = _absolutize(home_url, a.get("href"))
        if href and _looks_like_listing_url(href):
            pages.add(href)
    # Ajoute chemins candidats
    for path in CANDIDATE_LIST_PATHS:
        pages.add(urljoin(home_url, path))
        pages.add(urljoin(home_url, f"{path}/"))
    return list(pages)[:5]

def extract_detail_urls(listing_url: str) -> List[str]:
    """Extrait des liens de fiches depuis une page listing (pagination basique)."""
    urls, seen = [], set()
    to_visit = [listing_url]
    while to_visit and len(urls) < MAX_PER_SITE:
        url = to_visit.pop(0)
        soup = fetch(url)
        # Liens vers détails
        for a in soup.select("a[href]"):
            href = _absolutize(url, a.get("href"))
            if not href or href in seen:
                continue
            seen.add(href)
            if _looks_like_detail_url(href):
                urls.append(href)
        # Pagination simple
        next_a = soup.find("a", string=re.compile(r"(Suivant|Next|>|\»)", re.I))
        if next_a:
            nxt = _absolutize(url, next_a.get("href"))
            if nxt and nxt not in seen:
                to_visit.append(nxt)
    return urls[:MAX_PER_SITE]

def parse_detail(url: str) -> Dict:
    """Parse une fiche bien avec heuristiques génériques (titre, prix, surface, chambres, photos)."""
    soup = fetch(url)
    title = (soup.find("h1") or soup.find("h2") or soup.title)
    title = (title.get_text(strip=True) if title else "Bien à vendre")
    price_el = soup.select_one("[class*=price], .price, [data-price], .prix")
    surface_el = soup.find(string=re.compile(r"\b(\d+)\s?m[²2]\b"))
    beds_el = soup.find(string=re.compile(r"(\d+)\s?(ch|chambres|chambre)", re.I))
    imgs = [img.get("src","") for img in soup.select("img") if "http" in img.get("src","")]
    return dict(
        id=url,
        url=url,
        title=title,
        price_total=_num_from_text(price_el.get_text(" ", strip=True) if price_el else ""),
        surface_hab=_num_from_text(surface_el or ""),
        bedrooms=_num_from_text(beds_el or ""),
        photos=imgs[:3],
        source_name=urlparse(url).netloc
    )

def collect_agencies(base_urls: List[str]) -> List[Dict]:
    data = []
    for base in base_urls:
        try:
            listing_pages = discover_listing_pages(base)
            for lp in listing_pages:
                for durl in extract_detail_urls(lp):
                    try:
                        data.append(parse_detail(durl))
                        if len(data) >= MAX_PER_SITE:
                            break
                    except Exception:
                        continue
                if len(data) >= MAX_PER_SITE:
                    break
        except Exception:
            continue
    return data
