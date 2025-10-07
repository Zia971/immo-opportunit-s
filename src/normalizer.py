
# src/normalizer.py
import pandas as pd

WANTED_COLS = [
    "id","title","url","price_total","surface_hab","bedrooms","copro_lots","charges_copro_an",
    "taxe_fonciere","ppr_zone","plu_zone","age_days","price_drop_pct","status","rent_potential",
    "capex_ratio","yield_net","cashflow","division_possible","colocation_ready","outdoor",
    "sanitation","dist_amen_min","photos","source_name"
]

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    # Garantit la présence des colonnes
    for c in WANTED_COLS:
        if c not in df.columns:
            df[c] = None

    # Conversions numériques sûres
    num_cols = [
        "price_total","surface_hab","bedrooms","copro_lots","charges_copro_an","taxe_fonciere",
        "age_days","price_drop_pct","capex_ratio","yield_net","cashflow","dist_amen_min"
    ]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # Types spécifiques
    df["bedrooms"] = df["bedrooms"].astype(int)
    df["copro_lots"] = df["copro_lots"].astype(int)

    for b in ["division_possible","colocation_ready","outdoor"]:
        df[b] = df[b].fillna(False).astype(bool)

    # Photos en liste
    def _to_list(x):
        if isinstance(x, list):
            return x
        if pd.isna(x) or x is None:
            return []
        return [x]
    df["photos"] = df["photos"].apply(_to_list)

    # Statut par défaut
    df["status"] = df["status"].fillna("available")

    return df[WANTED_COLS]
