
# src/run_pipeline.py
import os
import pandas as pd
from datetime import datetime, timezone

from src.scoring import load_calibration, build_targets, score_listing
from src.normalizer import normalize
from src.connectors.collect import collect_all

DATA_DIR = "data"
HISTORY_CSV = f"{DATA_DIR}/history.csv"
SNAPSHOT_CSV = f"{DATA_DIR}/snapshot.csv"


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_dirs():
    os.makedirs("output", exist_ok=True)
    os.makedirs("reports", exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)


def load_sources_data() -> pd.DataFrame:
    """
    Récupère les annonces depuis les connecteurs.
    Doit retourner un DataFrame avec au minimum: url, title, price_total, surface_hab, bedrooms, photos.
    """
    rows = collect_all()  # liste de dicts
    base_cols = [
        "id", "url", "title", "price_total", "surface_hab", "bedrooms",
        "copro_lots", "charges_copro_an", "taxe_fonciere", "ppr_zone",
        "plu_zone", "age_days", "price_drop_pct", "status", "rent_potential",
        "capex_ratio", "yield_net", "cashflow", "division_possible",
        "colocation_ready", "outdoor", "sanitation", "dist_amen_min", "photos",
        "source_name"
    ]
    if not rows:
        return pd.DataFrame(columns=base_cols)
    df = pd.DataFrame(rows)
    # Garantit la présence des colonnes clés dès maintenant
    for c in base_cols:
        if c not in df.columns:
            df[c] = None
    return df[base_cols]


def update_history(df_now: pd.DataFrame) -> pd.DataFrame:
    """
    Crée/Met à jour l'historique pour calculer:
    - first_seen / last_seen
    - last_price
    - price_drop_pct
    """
    ensure_dirs()

    if os.path.exists(SNAPSHOT_CSV):
        prev = pd.read_csv(SNAPSHOT_CSV)
    else:
        prev = pd.DataFrame(columns=["id", "first_seen", "last_seen", "last_price", "status", "price_drop_pct"])

    # filets de sécurité sur df_now
    if "id" not in df_now.columns:
        df_now["id"] = df_now.get("url", pd.Series(dtype=str)).fillna("").apply(lambda x: hash(x))
    if "price_total" not in df_now.columns:
        df_now["price_total"] = 0
    if "status" not in df_now.columns:
        df_now["status"] = "available"

    out_rows = []
    prev = prev.set_index("id") if len(prev) else pd.DataFrame().set_index(pd.Index([]))

    for _, r in df_now.iterrows():
        rid = str(r.get("id") or r.get("url") or "")
        if rid == "":
            continue

        price = float(r.get("price_total", 0) or 0)
        status = str(r.get("status", "available") or "available")
        now = _utcnow_iso()

        if rid in prev.index:
            first_seen = prev.loc[rid, "first_seen"]
            last_price = float(prev.loc[rid, "last_price"] or 0)
            price_drop_pct = 0.0
            if price and last_price and price < last_price:
                try:
                    price_drop_pct = round((last_price - price) / last_price * 100, 2)
                except Exception:
                    price_drop_pct = 0.0
            out_rows.append(dict(
                id=rid, first_seen=first_seen, last_seen=now,
                last_price=price, status=status, price_drop_pct=price_drop_pct
            ))
        else:
            out_rows.append(dict(
                id=rid, first_seen=now, last_seen=now,
                last_price=price, status=status, price_drop_pct=0.0
            ))

    snap_new = pd.DataFrame(out_rows)
    # s'assure que la colonne price_drop_pct est bien là
    if "price_drop_pct" not in snap_new.columns:
        snap_new["price_drop_pct"] = 0.0
    snap_new.to_csv(SNAPSHOT_CSV, index=False)
    return snap_new


def enrich_with_history(df: pd.DataFrame, hist: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.assign(price_drop_pct=0.0, age_days=0, is_returned=False, status="available")

    # colonnes attendues côté historique
    cols = ["id", "first_seen", "last_seen", "last_price", "price_drop_pct", "status"]
    hist = hist.reindex(columns=cols, fill_value=pd.NA)  # garantit price_drop_pct présent
    merged = df.merge(hist, on="id", how="left")

    # normalisations robustes
    merged["price_drop_pct"] = pd.to_numeric(merged["price_drop_pct"], errors="coerce").fillna(0.0)
    merged["status"] = merged["status"].fillna("available")
    merged["first_seen"] = merged["first_seen"].fillna(merged["last_seen"])
    merged["age_days"] = merged["first_seen"].apply(
        lambda x: 0 if pd.isna(x) else (pd.Timestamp.utcnow() - pd.to_datetime(x, utc=True)).days
    )
    merged["is_returned"] = False  # activer quand on trackera under_offer -> available
    return merged


def main():
    ensure_dirs()

    # 1) Calibration (depuis ton Excel)
    crit_df, cat_weights = load_calibration("criteres_recherche_immo_FINAL.xlsx")
    targets = build_targets(crit_df)

    # 2) Collecte sources
    raw = load_sources_data()

    # 🧱 Sécurisation des colonnes essentielles
    if "id" not in raw.columns:
        raw["id"] = raw.get("url", pd.Series(dtype=str)).fillna("").apply(lambda x: hash(x))
    else:
        raw["id"] = raw["id"].where(raw["id"].notna(), raw.get("url", pd.Series(dtype=str)).fillna("").apply(lambda x: hash(x)))
    if "price_drop_pct" not in raw.columns:
        raw["price_drop_pct"] = 0.0
    if "status" not in raw.columns:
        raw["status"] = "available"

    # 3) Historique (baisse %, ancienneté)
    hist = update_history(raw)

    # 4) Normalisation + enrichissement
    df = normalize(raw)
    # filet: si le normalizer n'a pas encore la colonne, on la crée
    if "price_drop_pct" not in df.columns:
        df["price_drop_pct"] = 0.0
    df = enrich_with_history(df, hist)

    # 5) Scoring
    scores, logs = [], []
    for _, row in df.iterrows():
        s, e = score_listing(row, targets, cat_weights)
        scores.append(s); logs.append(e)

    if len(df):
        df["score"] = scores
        df["explications"] = logs
        df = df.sort_values("score", ascending=False).reset_index(drop=True)
    else:
        df["score"] = []
        df["explications"] = []

    # 6) Exports
    df.head(10).to_excel("output/top10.xlsx", index=False)
    df.to_csv("output/all_listings.csv", index=False)
    with open("reports/top10.html", "w", encoding="utf-8") as f:
        f.write("<html><body><h2>Top 10 — Guadeloupe</h2><p>Généré automatiquement.</p></body></html>")

    print("Pipeline OK —", len(df), "annonces traitées")


if __name__ == "__main__":
    main()
