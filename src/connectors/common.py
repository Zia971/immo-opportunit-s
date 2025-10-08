# src/connectors/common.py
import time, re, requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse

UA = "Mozilla/5.0 (compatible; ImmoAgent971/1.0; +https://immo-opportunites.streamlit.app)"

ASSET_EXT_RE = re.compile(r"\.(jpg|jpeg|png|gif|webp|svg|avif|pdf|css|js)(\?|$)", re.I)
CDN_HOST_RE  = re.compile(r"(static|cdn|cloudfront|akamai|fastly)", re.I)

def is_asset_url(url: str) -> bool:
    if not isinstance(url, str) or not url.startswith(("http://","https://")):
        return True
    if ASSET_EXT_RE.search(url):
        return True
    host = urlparse(url).netloc
    if CDN_HOST_RE.search(host):
        return True
    return False

def fetch(url, sleep=0.8, parser="html.parser"):
    """Requête douce + parser robuste; renvoie un soup vide si erreur."""
    time.sleep(sleep)
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=20, allow_redirects=True)
        if not r.ok:
            raise Exception(f"HTTP {r.status_code} on {url}")
        return BeautifulSoup(r.text, parser)
    except Exception:
        return BeautifulSoup("", parser if parser != "xml" else "html.parser")

def iter_sitemap(url):
    """Itère des URLs d’annonces depuis un sitemap (fallback HTML)."""
    url = url.replace("sitemap.xml/", "sitemap.xml").rstrip("/")
    soup = fetch(url, parser="xml")
    if not soup or not soup.find_all:
        soup = fetch(url, parser="html.parser")
    for loc in soup.find_all("loc"):
        href = (loc.text or "").strip()
        if not href or is_asset_url(href):
            continue
        if re.search(r"/(annonce|annonces|bien|vente|achat)/", href, re.IGNORECASE):
            yield href
