
# src/scoring.py
from typing import Dict, Tuple
import os
import pandas as pd

# --------------------------------------------------------------------
# 1) Calibration : lit (facultatif) l'Excel, sinon valeurs par défaut
# --------------------------------------------------------------------
def load_calibration(xlsx_path: str | None = None):
    """
    Retourne (crit_df, cat_weights)
    - crit_df : DataFrame des critères (si Excel dispo) sinon DF vide
    - cat_weights : pondération par catégorie
    """
    # Pondérations par défaut (cohérentes avec le reste du code)
    cat_weights = {
        "Localisation & Urbanisme": 0.40,
        "Caractéristiques du bien": 0.35,
        "Travaux & Potentiel":     0.25,
    }

    crit_df = pd.DataFrame()
    if xlsx_path and os.path.exists(xlsx_path):
        try:
            # On lit la 1ère feuille par défaut ; si tu as une feuille dédiée tu peux la nommer ici
            crit_df = pd.read_excel(xlsx_path)
            # Si une table de pondération par catégorie existe, on l'utilise
            # colonnes attendues éventuelles: "categorie", "poids"
            possible_cols = {c.lower(): c for c in crit_df.columns}
            if "categorie" in possible_cols and "poids" in possible_cols:
                cat_weights = {}
                for _, r in crit_df.iterrows():
                    try:
                        cat = str(r[possible_cols["categorie"]]).strip()
                        w   = float(r[possible_cols["poids"]])
                        if cat:
                            cat_weights[cat] = w
                    except Exception:
                        continue
                # Normalisation douce si nécessaire
                s = sum(cat_weights.values()) or 1.0
                cat_weights = {k: v/s for k, v in cat_weights.items()}
        except Exception:
            # En cas de souci de lecture, on garde les valeurs par défaut
            crit_df = pd.DataFrame()

    return crit_df, cat_weights


# --------------------------------------------------------------------
# 2) Cibles/règles : on construit la structure utilisée par le scoring
# --------------------------------------------------------------------
def build_targets(_crit_df: pd.DataFrame | None = None) -> Dict[str, Dict]:
    """
    Renvoie la définition des cibles/règles. On peut plus tard
    surcharger dynamiquement avec _crit_df si besoin.
    """
    return {
        # Localisation & Urbanisme
        "Risques naturels":   {"cat": "Localisation & Urbanisme", "poids": 20},
        "Zonage PLU":         {"cat": "Localisation & Urbanisme", "poids": 15},
        "Distance commerces": {"cat": "Localisation & Urbanisme", "poids":  5},

        # Caractéristiques du bien
        "Nombre de lots":     {"cat": "Caractéristiques du bien", "poids": 15},
        "Quote-part annuelle":{"cat": "Caractéristiques du bien", "poids": 10},
        "Taxe foncière":      {"cat": "Caractéristiques du bien", "poids": 10},

        # Travaux & Potentiel
        "Travaux":            {"cat": "Travaux & Potentiel",      "poids": 25},
        "Rentabilité":        {"cat": "Travaux & Potentiel",      "poids": 25},
    }


# --------------------------------------------------------------------
# 3) Scoring : applique les règles (champs manquants = NEUTRE)
# --------------------------------------------------------------------
def score_listing(row: Dict, targets: Dict, cat_weights: Dict) -> Tuple[float, str]:
    """Calcule un score global (0–100) + explications lisibles."""

    def apply_rule(ok: bool, target: Dict, cat: str):
        nonlocal score, excl, logs
        poids = float(target.get("poids", 0))
        if not ok:
            logs.append(f"❌ Exclusion ({cat})")
            excl = True
            score -= poids * 0.5  # petit malus si exclusion
        else:
            score += poids
            logs.append(f"✅ OK {cat} (+{poids})")

    score, excl, logs = 0.0, False, []

    # --- Localisation & Urbanisme ---
    t = targets.get("Risques naturels")
    if t:
        ppr = str(row.get("ppr_zone", "") or "").strip().lower()
        # Vide = neutre ; exclusion seulement si info clairement défavorable
        ok = True if ppr == "" else (ppr.startswith("hors") or ppr in ["zone blanche","zone bleue","bleue","blanche"])
        apply_rule(ok, t, "Localisation & Urbanisme")

    t = targets.get("Zonage PLU")
    if t:
        plu = str(row.get("plu_zone", "") or "").strip().upper()
        # Vide = neutre ; OK si U ou AU
        ok = True if plu == "" else (plu in ["U", "AU"])
        apply_rule(ok, t, "Localisation & Urbanisme")

    t = targets.get("Distance commerces")
    if t:
        try:
            dist = float(row.get("dist_amen_min", ""))
            ok = dist <= 10
        except Exception:
            ok = True  # neutre si inconnu
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
                ok = True  # neutre si inconnu
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
        y = 0.0
        try:
            y = float(row.get("yield_net", 0) or 0)
        except Exception:
            pass
        ok = True if capex is None else ((capex <= 0.25) or (capex > 0.25 and y >= 8.5))
        apply_rule(ok, t, "Travaux & Potentiel")

    t = targets.get("Rentabilité")
    if t:
        try:
            y = float(row.get("yield_net", 0) or 0)
            ok = y >= 5.0
        except Exception:
            ok = True
        apply_rule(ok, t, "Travaux & Potentiel")

    # --- Score final borné 0–100 (exclusion = 0) ---
    final = 0.0 if excl else max(0.0, min(100.0, score))
    return final, "; ".join(logs)
