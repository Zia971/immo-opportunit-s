# src/run_pipeline.py
import os
import pandas as pd
from datetime import datetime, timezone

from src.scoring import load_calibration, build_targets, score_listing
from src.normalizer import normalize
from src.connectors.collect import collect_all

DATA_DIR = "data"
SNAPSHOT_CSV = f"{DATA_DIR}/snapshot.csv"


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_dirs():
    os.makedirs("output", exist_ok=True)
    os.makedirs("reports", exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)


def load_sources_data() -> pd.DataFrame:
    """Collecte brut des annonces depuis les connecteurs."""
    rows = collect_all()  # liste de dicts
    base_cols = [
        "id","url","title","price_total","surface_hab","bedrooms",
        "copro_lots","charges_copro_an","taxe_fonciere","ppr_zone","plu_zone",
        "age_days","price_drop_pct","status","rent_potential","capex_ratio",
        "yield_net","cashflow","division_possible","colocation_ready","outdoor",
        "sanitation","dist_amen_min","photos","source_name"
    ]
    if not rows:
        df = pd.DataFrame(columns=base_cols)
    else:
        df = pd.DataFrame(rows)
        for c in base_cols:
            if c not in df.columns:
                df[c] = None
        df = df[base_cols]
    # garde-fous
    if "id" not in df.columns:
        df["id"] = df.get("url", pd.Series(dtype=str)).fillna("").apply(lambda x: hash(x))
    if "price_drop_pct" not in df.columns:
        df["price_drop_pct"] = 0.0
    if "status" not in df.columns:
        df["status"] = "available"
    return df


def read_snapshot() -> pd.DataFrame:
    """Lit le snapshot historique et garantit les colonnes requises."""
    cols = ["id","first_seen","last_seen","last_price","status","price_drop_pct"]
    if os.path.exists(SNAPSHOT_CSV):
        snap = pd.read_csv(SNAPSHOT_CSV)
    else:
        snap = pd.DataFrame(columns=cols)
    # ajoute colonnes manquantes si ancien fichier
    for c in cols:
        if c not in snap.columns:
            snap[c] = pd.NA
    # types
    snap["price_drop_pct"] = pd.to_numeric(snap["price_drop_pct"], errors="coerce").fillna(0.0)
    snap["status"] = snap["status"].fillna("available")
    return snap[cols]


def write_snapshot(df: pd.DataFrame) -> None:
    cols = ["id","first_seen","last_seen","last_price","status","price_drop_pct"]
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    df["price_drop_pct"] = pd.to_numeric(df["price_drop_pct"], errors="coerce").fillna(0.0)
    df["status"] = df["status"].fillna("available")
    df[cols].to_csv(SNAPSHOT_CSV, index=False)


def update_history(df_now: pd.DataFrame) -> pd.DataFrame:
    """Met à jour first_seen / last_seen / last_price / price_drop_pct."""
    ensure_dirs()
    prev = read_snapshot().set_index("id")

    out = []
    for _, r in df_now.iterrows():
        rid = str(r.get("id") or r.get("url") or "")
        if not rid:
            continue
        price = float(r.get("price_total", 0) or 0)
        status = str(r.get("status", "available") or "available")
        now = _utcnow_iso()

        if rid in prev.index:
            first_seen = prev.loc[rid, "first_seen"]
            last_price = float(prev.loc[rid, "last_price"] or 0)
            drop = 0.0
            if price and last_price and price < last_price:
                try:
                    drop = round((last_price - price) / last_price * 100, 2)
                except Exception:
                    drop = 0.0
            out.append(dict(
                id=rid, first_seen=first_seen, last_seen=now,
                last_price=price, status=status, price_drop_pct=drop
            ))
        else:
            out.append(dict(
                id=rid, first_seen=now, last_seen=now,
                last_price=price, status=status, price_drop_pct=0.0
            ))

    snap_new = pd.DataFrame(out)
    write_snapshot(snap_new)
    return snap_new


def enrich_with_history(df: pd.DataFrame, hist: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.assign(price_drop_pct=0.0, age_days=0, is_returned=False, status="available")

    # garantir colonnes hist
    cols = ["id","first_seen","last_seen","last_price","status","price_drop_pct"]
    for c in cols:
        if c not in hist.columns:
            hist[c] = pd.NA
    hist["price_drop_pct"] = pd.to_numeric(hist["price_drop_pct"], errors="coerce").fillna(0.0)
    hist["status"] = hist["status"].fillna("available")

    merged = df.merge(hist[cols], on="id", how="left")
    # si price_drop_pct a disparu pendant le merge (ne devrait pas), on recrée
    if "price_drop_pct" not in merged.columns:
        merged["price_drop_pct"] = 0.0
    merged["price_drop_pct"] = pd.to_numeric(merged["price_drop_pct"], errors="coerce").fillna(0.0)

    merged["first_seen"] = merged["first_seen"].fillna(merged["last_seen"])
    merged["age_days"] = merged["first_seen"].apply(
        lambda x: 0 if pd.isna(x) else (pd.Timestamp.utcnow() - pd.to_datetime(x, utc=True)).days
    )
    merged["is_returned"] = False
    merged["status"] = merged["status"].fillna("available")
    return merged


def main():
    ensure_dirs()

    # 1) Calibration (Excel)
    crit_df, cat_weights = load_calibration("criteres_recherche_immo_FINAL.xlsx")
    targets = build_targets(crit_df)

    # 2) Collecte
    raw = load_sources_data()

    # 3) Historique
    hist = update_history(raw)

    # 4) Normalisation + enrichissement
    df = normalize(raw)
    if "price_drop_pct" not in df.columns:  # filet de sécurité
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
