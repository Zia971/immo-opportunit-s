
import pandas as pd
WANTED_COLS = ["id","title","url","price_total","surface_hab","bedrooms","copro_lots","charges_copro_an",
               "taxe_fonciere","ppr_zone","plu_zone","age_days","price_drop_pct","status","rent_potential",
               "capex_ratio","yield_net","cashflow","division_possible","colocation_ready","outdoor",
               "sanitation","dist_amen_min","photos"]
def normalize(df: pd.DataFrame) -> pd.DataFrame:
    for c in WANTED_COLS:
        if c not in df.columns: df[c] = None
    num_cols = ["price_total","surface_hab","bedrooms","copro_lots","charges_copro_an","taxe_fonciere",
                "age_days","price_drop_pct","capex_ratio","yield_net","cashflow","dist_amen_min"]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df["bedrooms"] = df["bedrooms"].astype(int)
    df["copro_lots"] = df["copro_lots"].astype(int)
    for b in ["division_possible","colocation_ready","outdoor"]:
        df[b] = df[b].fillna(False).astype(bool)
    df["photos"] = df["photos"].apply(lambda x: x if isinstance(x, list) else [])
    return df[WANTED_COLS]
