# src/connectors/common.py
import time, re, requests
from bs4 import BeautifulSoup

UA = "Mozilla/5.0 (compatible; ImmoAgent971/1.0; +https://immo-opportunites.streamlit.app)"

def fetch(url, sleep=0.8, parser="html.parser"):
    """Requête douce + parseur robuste (ne casse pas le pipeline en cas de 404)."""
    time.sleep(sleep)  # throttling doux
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=20, allow_redirects=True)
        if not r.ok:
            raise Exception(f"HTTP {r.status_code} on {url}")
        return BeautifulSoup(r.text, parser)
    except Exception:
        # retourne un soup vide pour laisser le collecteur continuer
        return BeautifulSoup("", parser if parser != "xml" else "html.parser")

def iter_sitemap(url):
    """
    Itère sur les URLs d'annonces depuis un sitemap (si présent).
    - supprime les slashs parasites
    - bascule en 'html.parser' si lxml indisponible
    - ne jette pas d'exception bloquante
    """
    url = url.replace("sitemap.xml/", "sitemap.xml").rstrip("/")
    soup = fetch(url, parser="xml")
    if not soup or not soup.find_all:
        soup = fetch(url, parser="html.parser")
    for loc in soup.find_all("loc"):
        href = (loc.text or "").strip()
        if not href:
            continue
        if re.search(r"/(annonce|annonces|bien|vente|achat)/", href, re.IGNORECASE):
            yield href
