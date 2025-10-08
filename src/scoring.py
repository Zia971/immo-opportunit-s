
# src/scoring.py
from typing import Dict, Tuple
import numpy as np
import pandas as pd

def load_calibration():
    """Renvoie la pondération par catégorie."""
    return {
        "Localisation & Urbanisme": 0.4,
        "Caractéristiques du bien": 0.35,
        "Travaux & Potentiel": 0.25,
    }

def build_targets():
    """Renvoie les règles de score cibles."""
    return {
        "Risques naturels": {"cat": "Localisation & Urbanisme", "poids": 20},
        "Zonage PLU": {"cat": "Localisation & Urbanisme", "poids": 15},
        "Distance commerces": {"cat": "Localisation & Urbanisme", "poids": 5},
        "Nombre de lots": {"cat": "Caractéristiques du bien", "poids": 15},
        "Quote-part annuelle": {"cat": "Caractéristiques du bien", "poids": 10},
        "Taxe foncière": {"cat": "Caractéristiques du bien", "poids": 10},
        "Travaux": {"cat": "Travaux & Potentiel", "poids": 25},
        "Rentabilité": {"cat": "Travaux & Potentiel", "poids": 25},
    }

def score_listing(row: Dict, targets: Dict, cat_weights: Dict) -> Tuple[float, str]:
    """Applique les règles de pondération pour calculer un score global."""

    def apply_rule(ok: bool, target: Dict, cat: str):
        nonlocal score, excl, logs
        poids = target["poids"]
        if not ok:
            logs.append(f"❌ Exclusion ({cat})")
            excl = True
            score -= poids * 0.5
        else:
            score += poids
            logs.append(f"✅ OK {cat} (+{poids})")

    score, excl, logs = 0.0, False, []

    # --- Localisation & Urbanisme ---
    t = targets.get("Risques naturels")
    if t:
        ppr = str(row.get("ppr_zone", "") or "").strip().lower()
        ok = True if ppr == "" else (ppr.startswith("hors") or ppr in ["zone blanche","zone bleue","bleue","blanche"])
        apply_rule(ok, t, "Localisation & Urbanisme")

    t = targets.get("Zonage PLU")
    if t:
        plu = str(row.get("plu_zone", "") or "").strip().upper()
        ok = True if plu == "" else (plu in ["U", "AU"])
        apply_rule(ok, t, "Localisation & Urbanisme")

    t = targets.get("Distance commerces")
    if t:
        try:
            dist = float(row.get("dist_amen_min", ""))
            ok = dist <= 10
        except Exception:
            ok = True
        apply_rule(ok, t, "Localisation & Urbanisme")

    # --- Caractéristiques du bien ---
    t = targets.get("Nombre de lots")
    if t:
        lots = row.get("copro_lots", None)
        try:
            lots_i = int(lots) if lots not in [None, ""] else 0
        except Exception:
            lots_i = 0
        ok = (lots_i == 0) or (lots_i <= 40)
        apply_rule(ok, t, "Caractéristiques du bien")

    t = targets.get("Quote-part annuelle")
    if t:
        try:
            lots_i = int(row.get("copro_lots", 0) or 0)
        except Exception:
            lots_i = 0
        if lots_i > 0:
            try:
                ok = float(row.get("charges_copro_an", 1e9)) <= 1400
            except Exception:
                ok = True
            apply_rule(ok, t, "Caractéristiques du bien")

    t = targets.get("Taxe foncière")
    if t:
        try:
            tf = float(row.get("taxe_fonciere", 0) or 0)
            ok = tf <= 2500 or tf == 0
        except Exception:
            ok = True
        apply_rule(ok, t, "Caractéristiques du bien")

    # --- Travaux & Potentiel ---
    t = targets.get("Travaux")
    if t:
        try:
            capex = float(row.get("capex_ratio", ""))
        except Exception:
            capex = None
        y = float(row.get("yield_net", 0) or 0)
        if capex is None:
            ok = True
        else:
            ok = (capex <= 0.25) or (capex > 0.25 and y >= 8.5)
        apply_rule(ok, t, "Travaux & Potentiel")

    t = targets.get("Rentabilité")
    if t:
        try:
            y = float(row.get("yield_net", 0) or 0)
            ok = y >= 5.0
        except Exception:
            ok = True
        apply_rule(ok, t, "Travaux & Potentiel")

    # --- Score final ---
    final = 0.0 if excl else max(0.0, min(100.0, score))
    return final, "; ".join(logs)
