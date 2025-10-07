# streamlit_app.py
import streamlit as st, pandas as pd, os

st.set_page_config(page_title="opportunité immobilière Guadeloupe", layout="wide")
st.title("opportunité immobilière Guadeloupe")
st.caption("Top 10 mis à jour automatiquement selon vos critères (Excel).")

TOP_PATH = "output/top10.xlsx"
ALL_PATH = "output/all_listings.csv"

def badge(txt):
    st.markdown(f"<span style='background:#1f2937;color:#fff;padding:4px 8px;border-radius:12px;margin-right:6px;display:inline-block'>{txt}</span>", unsafe_allow_html=True)

def fmt_price(x):
    try:
        return f"{int(x):,} €".replace(",", " ")
    except Exception:
        return "—"

def card(row):
    st.divider()
    cols = st.columns([4,3,3])
    with cols[0]:
        title = row.get("title") or "Bien à vendre"
        url = row.get("url") or "#"
        st.markdown(f"### [{title}]({url})")
        st.write(f"**Score : {row.get('score',0):.1f}/100**")
        st.write(f"**Prix** : {fmt_price(row.get('price_total',0))}")
        st.write(f"**Surface** : {int(row.get('surface_hab',0))} m²  ·  **Chambres** : {int(row.get('bedrooms',0))}")
        # Badges
        if float(row.get("price_drop_pct",0)) >= 10:
            badge(f"↓ {row['price_drop_pct']:.0f}%")
        if int(row.get("age_days",0)) >= 90:
            badge(f"> {int(row['age_days'])} j")
        status = (row.get("status") or "available").lower()
        if status != "available":
            badge(status)
        if row.get("source_name"):
            st.caption(f"Source : {row['source_name']}")
    with cols[1]:
        # Explications du score
        if row.get("explications"):
            with st.expander("🧠 Pourquoi ce score ?"):
                st.write(row["explications"])
        # Caractéristiques utiles
        with st.expander("📋 Détails utiles"):
            st.write(f"- **Copropriété (lots)** : {int(row.get('copro_lots',0))}")
            if int(row.get('copro_lots',0)) > 0:
                st.write(f"- **Charges/an** : {fmt_price(row.get('charges_copro_an',0))}")
            tf = row.get("taxe_fonciere",0)
            if float(tf) > 0:
                st.write(f"- **Taxe foncière** : {fmt_price(tf)}")
            st.write(f"- **Zonage PLU** : {row.get('plu_zone') or '—'}")
            st.write(f"- **PPR** : {row.get('ppr_zone') or '—'}")
            dist = row.get("dist_amen_min")
            st.write(f"- **Commerces** : {('≤ '+str(int(dist))+' min') if pd.notna(dist) and dist not in [None,''] and float(dist)>0 else '—'}")
    with cols[2]:
        photos = row.get("photos") or []
        if photos:
            st.image(photos[0], use_column_width=True)
        if len(photos) > 1:
            st.image(photos[1:3], use_column_width=True)

def show_top():
    if not os.path.exists(TOP_PATH):
        st.warning("Le classement n’a pas encore été généré.")
        return
    df = pd.read_excel(TOP_PATH)
    if df.empty:
        st.info("Aucune annonce chargée pour l’instant. Les connecteurs s’exécutent — repasse plus tard.")
        return
    for _, r in df.iterrows():
        card(r)

show_top()

st.divider()
with st.expander("🔎 Toutes les annonces (CSV)"):
    if os.path.exists(ALL_PATH):
        try:
            df_all = pd.read_csv(ALL_PATH)
            st.dataframe(df_all.head(200))
        except Exception:
            st.caption("CSV indisponible pour le moment.")
st.caption("© Agent IA — Guadeloupe")
