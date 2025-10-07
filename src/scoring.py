
import pandas as pd
import unicodedata


# =========================================================
# 🔹 1. Détection automatique des feuilles du fichier Excel
# =========================================================
def _norm(s: str) -> str:
    """Normalise une chaîne (supprime accents, majuscules, espaces inutiles)."""
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode()
    return s.strip().lower()


def load_calibration(path: str):
    """Charge automatiquement les onglets 'Critères' et 'Calibration IA' du fichier Excel,
    même si les noms changent (ex: Criteres, Calibrage, Poids, etc.)."""
    xls = pd.ExcelFile(path)

    crit_sheet = None
    calib_sheet = None
    for name in xls.sheet_names:
        n = _norm(name)
        if crit_sheet is None and ("crit" in n or "criter" in n):
            crit_sheet = name
        if calib_sheet is None and ("calib" in n or "poids" in n):
            calib_sheet = name

    if crit_sheet is None:
        crit_sheet = xls.sheet_names[0]
    if calib_sheet is None:
        calib_sheet = xls.sheet_names[1] if len(xls.sheet_names) > 1 else xls.sheet_names[0]

    crit = pd.read_excel(xls, sheet_name=crit_sheet).fillna("")
    calib = pd.read_excel(xls, sheet_name=calib_sheet)

    crit.columns = [c.strip() for c in crit.columns]
    calib.columns = [c.strip() for c in calib.columns]

    # On repère automatiquement la colonne Catégorie et Poids (%)
    cat_col = next((c for c in calib.columns if "cat" in _norm(c)), None)
    poids_col = next((c for c in calib.columns if "%" in _norm(c) or "poids" in _norm(c)), None)

    cat_weights = {}
    if cat_col and poids_col:
        for _, row in calib.iterrows():
            cat_name = str(row[cat_col]).strip()
            try:
                weight = float(row[poids_col]) / 100.0
            except Exception:
                weight = 0.0
            cat_weights[cat_name] = weight

    return crit, cat_weights


# =========================================================
# 🔹 2. Construction des cibles (critères)
# =========================================================
def _get_target(crit_df: pd.DataFrame, name_contains: str):
    rows = crit_df[crit_df["Critère"].str.contains(name_contains, case=False, na=False)]
    if rows.empty:
        return None
    r = rows.iloc[0]
    try:
        w = float(r.get("Pondération IA (%)", 0) or 0) / 100.0
    except Exception:
        w = 0.0
    return dict(
        value=str(r.get("Valeur cible", "")),
        rule=str(r.get("Type de règle", "")),
        weight=w,
        priority=str(r.get("Priorité (1-5)", "")),
        source=str(r.get("Source (DVF/Annonce/IA/PLU)", "")),
    )


def build_targets(crit_df: pd.DataFrame):
    """Construit les cibles à partir du tableau des critères."""
    names = [
        "Budget max",
        "Nombre de lots",
        "Quote-part annuelle",
        "Taxe foncière",
        "Rendement locatif net",
        "Cash-flow",
        "Risques naturels",
        "Zonage PLU",
        "chambres",
        "Surface habitable",
        "Division possible",
        "Potentiel colocation",
        "baisse de prix",
        "Ancienneté",
        "Remis en vente",
        "Distance commerces",
        "Extérieur",
        "Travaux",
    ]
    return {n: _get_target(crit_df, n) for n in names}


# =========================================================
# 🔹 3. Fonction de scoring principale
# =========================================================
def score_listing(row, targets, cat_weights):
    """Applique les règles de pondération pour calculer un score global."""

    def apply_rule(ok, target, cat):
        nonlocal score, excl, logs
        if not target:
            return
        w = target["weight"]
        rule = (target["rule"] or "").lower()
        cat_w = cat_weights.get(cat, 0.0)
        contrib = w * cat_w * 100.0
        if rule == "exclusion":
            if not ok:
                excl = True
                logs.append(f"❌ Exclusion ({cat})")
            else:
                logs.append(f"✅ OK exclu ({cat})")
        elif rule == "indispensable":
            if ok:
                score += contrib
                logs.append(f"✅ Indisp (+{contrib:.1f})")
            else:
                malus = contrib * 0.8
                score -= malus
                logs.append(f"⚠️ Indisp non atteint (−{malus:.1f})")
        elif rule == "bonus":
            if ok:
                score += contrib
                logs.append(f"➕ Bonus (+{contrib:.1f})")

    score, excl, logs = 0.0, False, []

   # --- Localisation & Urbanisme ---
t = targets.get("Risques naturels")
if t:
    ppr = str(row.get("ppr_zone", "") or "").strip().lower()
    # ✅ Vide = NEUTRE (pas d'exclusion). Exclusion seulement si clairement en zone rouge.
    ok = True if ppr == "" else (ppr.startswith("hors") or ppr in ["zone blanche","zone bleue","bleue","blanche"])
    apply_rule(ok, t, "Localisation & Urbanisme")

t = targets.get("Zonage PLU")
if t:
    plu = str(row.get("plu_zone", "") or "").strip().upper()
    # ✅ Vide = NEUTRE. OK si U / AU, sinon neutre.
    ok = True if plu == "" else (plu in ["U", "AU"])
    apply_rule(ok, t, "Localisation & Urbanisme")

t = targets.get("Distance commerces")
if t:
    # ✅ Vide / NaN = NEUTRE (pas de malus). OK si <= 10 quand dispo.
    try:
        dist = float(row.get("dist_amen_min", ""))
        ok = dist <= 10
    except Exception:
        ok = True  # neutre
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

    # --- Rentabilité & Finance ---
    t = targets.get("Budget max")
    if t:
        apply_rule(float(row.get("price_total", 1e12)) <= 250000, t, "Rentabilité & Finance")

    t = targets.get("Rendement locatif net")
    if t:
        apply_rule(float(row.get("yield_net", 0)) >= 7.0, t, "Rentabilité & Finance")

    t = targets.get("Cash-flow")
    if t:
        apply_rule(float(row.get("cashflow", -1e9)) >= 0.0, t, "Rentabilité & Finance")

    t = targets.get("Taxe foncière")
    if t:
        apply_rule(float(row.get("taxe_fonciere", 1e9)) <= 4500, t, "Rentabilité & Finance")

    # --- Travaux & Potentiel ---
t = targets.get("Travaux")
if t:
    try:
        capex = float(row.get("capex_ratio", ""))
    except Exception:
        capex = None
    y = float(row.get("yield_net", 0) or 0)
    if capex is None:
        ok = True  # neutre si inconnu
    else:
        ok = (capex <= 0.25) or (capex > 0.25 and y >= 8.5)
    apply_rule(ok, t, "Travaux & Potentiel")

    # --- Historique & Dynamique ---
    t = targets.get("baisse de prix")
    if t:
        apply_rule(float(row.get("price_drop_pct", 0)) >= 10, t, "Historique & Dynamique")

    t = targets.get("Ancienneté")
    if t:
        apply_rule(int(row.get("age_days", 0)) > 90, t, "Historique & Dynamique")

    t = targets.get("Remis en vente")
    if t:
        apply_rule(bool(row.get("is_returned", False)), t, "Historique & Dynamique")

    final = 0.0 if excl else max(0.0, min(100.0, score))
    return final, "; ".join(logs)
