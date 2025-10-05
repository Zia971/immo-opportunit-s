
import pandas as pd
def load_calibration(path: str):
    xls = pd.ExcelFile(path)
    crit = pd.read_excel(xls, sheet_name="Critères").fillna("")
    calib = pd.read_excel(xls, sheet_name="Calibration IA")
    crit.columns = [c.strip() for c in crit.columns]
    cat_weights = {row["Catégorie"]: float(row["Poids (%)"])/100.0
                   for _, row in calib.iterrows() if str(row.get("Catégorie","")).strip()}
    return crit, cat_weights
def _get_target(crit_df: pd.DataFrame, name_contains: str):
    rows = crit_df[crit_df["Critère"].str.contains(name_contains, case=False, na=False)]
    if rows.empty: return None
    r = rows.iloc[0]
    try: w = float(r.get("Pondération IA (%)", 0) or 0)/100.0
    except: w = 0.0
    return dict(value=str(r.get("Valeur cible","")), rule=str(r.get("Type de règle","")),
                weight=w, priority=str(r.get("Priorité (1-5)","")),
                source=str(r.get("Source (DVF/Annonce/IA/PLU)","")))
def build_targets(crit_df: pd.DataFrame):
    names = ["Budget max","Nombre de lots","Quote-part annuelle","Taxe foncière","Rendement locatif net",
             "Cash-flow","Risques naturels","Zonage PLU","chambres","Surface habitable",
             "Division possible","Potentiel colocation","baisse de prix","Ancienneté","Remis en vente",
             "Distance commerces","Extérieur","Travaux"]
    return {n: _get_target(crit_df, n) for n in names}
def score_listing(row, targets, cat_weights):
    def apply_rule(ok, target, cat):
        nonlocal score, excl, logs
        if not target: return
        w = target["weight"]; rule=(target["rule"] or "").lower()
        cat_w = cat_weights.get(cat, 0.0); contrib = w*cat_w*100.0
        if rule=="exclusion":
            if not ok: excl=True; logs.append(f"❌ Exclusion ({cat})"); 
            else: logs.append(f"✅ OK exclu ({cat})")
        elif rule=="indispensable":
            if ok: score+=contrib; logs.append(f"✅ Indisp (+{contrib:.1f})")
            else: malus=contrib*0.8; score-=malus; logs.append(f"⚠️ Indisp non atteint (−{malus:.1f})")
        elif rule=="bonus":
            if ok: score+=contrib; logs.append(f"➕ Bonus (+{contrib:.1f})")
    score, excl, logs = 0.0, False, []
    t=targets.get("Risques naturels"); 
    if t: apply_rule(str(row.get("ppr_zone","")).lower().startswith("hors"), t, "Localisation & Urbanisme")
    t=targets.get("Zonage PLU");      
    if t: apply_rule(str(row.get("plu_zone","")).upper() in ["U","AU"], t, "Localisation & Urbanisme")
    t=targets.get("Distance commerces"); 
    if t: apply_rule(float(row.get("dist_amen_min", 999))<=10, t, "Localisation & Urbanisme")
    t=targets.get("Nombre de lots");  
    if t: apply_rule((int(row.get("copro_lots",0))==0) or (int(row.get("copro_lots",0))<=40), t, "Caractéristiques du bien")
    t=targets.get("Quote-part annuelle"); 
    if t and int(row.get("copro_lots",0))>0: apply_rule(float(row.get("charges_copro_an",1e9))<=1400, t, "Caractéristiques du bien")
    t=targets.get("chambres");        
    if t: apply_rule(int(row.get("bedrooms",0))>=3, t, "Caractéristiques du bien")
    t=targets.get("Surface habitable"); 
    if t: apply_rule(float(row.get("surface_hab",0))>=65, t, "Caractéristiques du bien")
    t=targets.get("Extérieur");       
    if t: apply_rule(bool(row.get("outdoor", False)), t, "Caractéristiques du bien")
    t=targets.get("Budget max");      
    if t: apply_rule(float(row.get("price_total",1e12))<=250000, t, "Rentabilité & Finance")
    t=targets.get("Rendement locatif net"); 
    if t: apply_rule(float(row.get("yield_net",0))>=7.0, t, "Rentabilité & Finance")
    t=targets.get("Cash-flow");       
    if t: apply_rule(float(row.get("cashflow",-1e9))>=0.0, t, "Rentabilité & Finance")
    t=targets.get("Taxe foncière");   
    if t: apply_rule(float(row.get("taxe_fonciere",1e9))<=4500, t, "Rentabilité & Finance")
    t=targets.get("Travaux");         
    if t:
        capex=float(row.get("capex_ratio",1.0)); y=float(row.get("yield_net",0))
        apply_rule((capex<=0.25) or (capex>0.25 and y>=8.5), t, "Travaux & Potentiel")
    t=targets.get("Division possible"); 
    if t: apply_rule(bool(row.get("division_possible", False)), t, "Travaux & Potentiel")
    t=targets.get("Potentiel colocation"); 
    if t: apply_rule(bool(row.get("colocation_ready", False)) and int(row.get("bedrooms",0))>=3, t, "Travaux & Potentiel")
    t=targets.get("baisse de prix");  
    if t: apply_rule(float(row.get("price_drop_pct",0))>=10, t, "Historique & Dynamique")
    t=targets.get("Ancienneté");      
    if t: apply_rule(int(row.get("age_days",0))>90, t, "Historique & Dynamique")
    t=targets.get("Remis en vente");  
    if t: apply_rule(bool(row.get("is_returned", False)), t, "Historique & Dynamique")
    final = 0.0 if excl else max(0.0, min(100.0, score))
    return final, "; ".join(logs)
