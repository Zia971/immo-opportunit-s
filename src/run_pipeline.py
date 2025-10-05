
import io, os, pandas as pd
from src.scoring import load_calibration, build_targets, score_listing
from src.normalizer import normalize
def load_sources_data():
    return pd.DataFrame(columns=["id","title","url","price_total","surface_hab","bedrooms","copro_lots",
                                 "charges_copro_an","taxe_fonciere","ppr_zone","plu_zone","age_days",
                                 "price_drop_pct","status","rent_potential","capex_ratio","yield_net","cashflow",
                                 "division_possible","colocation_ready","outdoor","sanitation","dist_amen_min","photos"])
def main():
    crit_df, cat_weights = load_calibration("criteres_recherche_immo_FINAL.xlsx")
    targets = build_targets(crit_df)
    df = load_sources_data()
    df = normalize(df)
    scores, logs = [], []
    for _, row in df.iterrows():
        s, e = score_listing(row, targets, cat_weights)
        scores.append(s); logs.append(e)
    if len(df):
        df["score"] = scores; df["explications"] = logs
        df = df.sort_values("score", ascending=False).reset_index(drop=True)
    else:
        df["score"] = []; df["explications"] = []
    os.makedirs("output", exist_ok=True); os.makedirs("reports", exist_ok=True)
    df.head(10).to_excel("output/top10.xlsx", index=False)
    with open("reports/top10.html","w",encoding="utf-8") as f:
        f.write("<html><body><h2>Top 10</h2><p>(Généré — connecteurs activés lors du déploiement.)</p></body></html>")
    print("Pipeline OK")
if __name__ == "__main__":
    main()
