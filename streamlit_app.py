# streamlit_app.py
import os, pandas as pd, streamlit as st

st.set_page_config(page_title="opportunité immobilière Guadeloupe", layout="wide")
st.title("opportunité immobilière Guadeloupe")
st.caption("Top 10 mis à jour automatiquement selon vos critères (Excel).")

TOP_PATH = "output/top10.xlsx"
ALL_PATH = "output/all_listings.csv"

def badge(txt):
    st.markdown(
        f"<span style='background:#1f2937;color:#fff;padding:4px 8px;border-radius:12px;margin-right:6px;display:inline-block'>{txt}</span>",
        unsafe_allow_html=True,
    )

def fmt_price(x):
    try:
        return f"{int(float(x)):,} €".replace(",", " ")
    except Exception:
        return "—"

def _valid_photo_urls(photos, limit=2):
    """Garde uniquement des URLs http(s) non vides, limite leur nombre."""
    urls = []
    if isinstance(photos, list):
        for u in photos:
            if isinstance(u, str) and u.startswith(("http://", "https://")):
                urls.append(u.strip())
    elif isinstance(photos, str) and photos.startswith(("http://", "https://")):
        urls = [photos.strip()]
    return urls[:limit]

def _safe_show_images(urls):
    """N'échoue jamais : essaye st.image, sinon affiche des liens cliquables."""
    if not urls:
        st.caption("Aperçu photo indisponible.")
        return
    try:
        if len(urls) == 1:
            st.image(urls[0], use_column_width=True)
        else:
            st.image(urls, use_column_width=True)
    except Exception:
        # Certains hôtes bloquent le hotlinking : on affiche des liens à la place.
        st.caption("Prévisualisation bloquée par le site source.")
        for i, u in enumerate(urls, 1):
            st.markdown(f"- [Photo {i}]({u})")

def card(row):
    st.divider()
    cols = st.columns([4, 3, 3])
    with cols[0]:
        title = row.get("title") or "Bien à vendre"
        url = row.get("url") or "#"
        st.markdown(f"### [{title}]({url})")
        st.write(f"**Score : {row.get('score',0):.1f}/100**")
        st.write(f"**Prix** : {fmt_price(row.get('price_total',0))}")
        st.write(f"**Surface** : {int(float(row.get('surface_hab',0) or 0))} m²  ·  "
                 f"**Chambres** : {int(float(row.get('bedrooms',0) or 0))}")
        # Badges
        try:
            drop = float(row.get("price_drop_pct", 0) or 0)
            if drop >= 10:
                badge(f"↓ {drop:.0f}%")
        except Exception:
            pass
        try:
            age = int(float(row.get("age_days", 0) or 0))
            if age >= 90:
                badge(f"> {age} j")
        except Exception:
            pass
        status = (row.get("status") or "available").lower()
        if status != "available":
            badge(status)
        if row.get("source_name"):
            st.caption(f"Source : {row['source_name']}")
    with cols[1]:
        if row.get("explications"):
            with st.expander("🧠 Pourquoi ce score ?"):
                st.write(row["explications"])
        with st.expander("📋 Détails utiles"):
            copro = int(float(row.get("copro_lots", 0) or 0))
            if copro > 0:
                st.write(f"- **Copropriété (lots)** : {copro}")
                st.write(f"- **Charges/an** : {fmt_price(row.get('charges_copro_an',0))}")
            tf = row.get("taxe_fonciere", 0)
            try:
                if float(tf) > 0:
                    st.write(f"- **Taxe foncière** : {fmt_price(tf)}")
            except Exception:
                pass
            st.write(f"- **Zonage PLU** : {row.get('plu_zone') or '—'}")
            st.write(f"- **PPR** : {row.get('ppr_zone') or '—'}")
            try:
                dist = float(row.get("dist_amen_min", "") or 0)
                st.write(f"- **Commerces** : {'≤ '+str(int(dist))+' min' if dist>0 else '—'}")
            except Exception:
                st.write(f"- **Commerces** : —")
    with cols[2]:
        urls = _valid_photo_urls(row.get("photos"))
        _safe_show_images(urls)

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
