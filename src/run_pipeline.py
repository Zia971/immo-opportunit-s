
# src/run_pipeline.py
import os, json, time, pandas as pd
from datetime import datetime, timezone
from src.scoring import load_calibration, build_targets, score_listing
from src.normalizer import normalize
from src.connectors.collect import collect_all

DATA_DIR = "data"
HISTORY_CSV = f"{DATA_DIR}/history.csv"
SNAPSHOT_CSV = f"{DATA_DIR}/snapshot.csv"

def _utcnow_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def load_sources_data():
    rows = collect_all()
    return pd.DataFrame(rows)

def ensure_dirs():
    os.makedirs("output", exist_ok=True)
    os.makedirs("reports", exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)

def update_history(df_now: pd.DataFrame) -> pd.DataFrame:
    """Crée/Met à jour l'historique pour détecter baisse %, ancienneté, remis en vente, etc."""
    ensure_dirs()
    if os.path.exists(SNAPSHOT_CSV):
        prev = pd.read_csv(SNAPSHOT_CSV)
    else:
        prev = pd.DataFrame(columns=["id","first_seen","last_seen","last_price","status"])

    snap = prev.set_index("id") if len(prev) else pd.DataFrame().set_index(pd.Index([]))

    out = []
    for _, r in df_now.iterrows():
        rid = str(r.get("id") or r.get("url"))
        price = float(r.get("price_total", 0))
        status = "available"
        now = _utcnow_iso()

        if rid in snap.index:
            first_seen = snap.loc[rid, "first_seen"]
            last_price = float(snap.loc[rid, "last_price"] or 0)
            price_drop_pct = 0.0
            if price and last_price and price < last_price:
                price_drop_pct = round((last_price - price) / last_price * 100, 2)
            out.append(dict(
                id=rid, first_seen=first_seen, last_seen=now,
                last_price=price, status=status, price_drop_pct=price_drop_pct
            ))
        else:
            out.append(dict(
                id=rid, first_seen=now, last_seen=now,
                last_price=price, status=status, price_drop_pct=0.0
            ))

    snap_new = pd.DataFrame(out)
    snap_new.to_csv(SNAPSHOT_CSV, index=False)

    # Détection “remis en vente” : si le bien existait avec status != available et redevient available
    # (placeholder, on affinerait quand une source expose “sous compromis”)
    return snap_new

def enrich_with_history(df: pd.DataFrame, hist: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df.assign(price_drop_pct=0, age_days=0, is_returned=False, status="available")
    merged = df.merge(hist[["id","first_seen","last_seen","last_price","price_drop_pct","status"]],
                      on="id", how="left")
    merged["price_drop_pct"] = merged["price_drop_pct"].fillna(0).astype(float)
    merged["age_days"] = merged["first_seen"].fillna(merged["last_seen"]).apply(
        lambda x: 0 if pd.isna(x) else (pd.Timestamp.utcnow() - pd.to_datetime(x, utc=True)).days
    )
    merged["is_returned"] = False  # sera vrai si on observe un cycle under_offer->available (à venir)
    return merged

def main():
    ensure_dirs()
    crit_df, cat_weights = load_calibration("criteres_recherche_immo_FINAL.xlsx")
    targets = build_targets(crit_df)

    raw = load_sources_data()
    # Sécurisation de la colonne ID
if "id" not in raw.columns:
    # Crée un identifiant unique basé sur l'URL si absent
    raw["id"] = raw["url"].fillna("").apply(lambda x: hash(x))

else:
    # Si la colonne existe, on complète les valeurs manquantes
    raw["id"] = raw["id"].fillna(raw["url"].apply(lambda x: hash(x)))

    hist = update_history(raw)
    df = normalize(raw)
    df = enrich_with_history(df, hist)

    scores, logs = [], []
    for _, row in df.iterrows():
        s, e = score_listing(row, targets, cat_weights)
        scores.append(s); logs.append(e)

    if len(df):
        df["score"] = scores; df["explications"] = logs
        df = df.sort_values("score", ascending=False).reset_index(drop=True)
    else:
        df["score"] = []; df["explications"] = []

    # Exports
    df.head(10).to_excel("output/top10.xlsx", index=False)
    df.to_csv("output/all_listings.csv", index=False)
    with open("reports/top10.html","w",encoding="utf-8") as f:
        f.write("<html><body><h2>Top 10 — Guadeloupe</h2><p>Généré automatiquement.</p></body></html>")
    print("Pipeline OK —", len(df), "annonces traitées")

if __name__ == "__main__":
    main()
