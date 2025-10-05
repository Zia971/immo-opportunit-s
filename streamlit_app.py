
import streamlit as st, pandas as pd, os
st.set_page_config(page_title="opportunité immobilière Guadeloupe", layout="wide")
st.title("opportunité immobilière Guadeloupe")
st.caption("Top 10 mis à jour automatiquement selon vos critères (Excel).")
if os.path.exists("output/top10.xlsx"):
    df = pd.read_excel("output/top10.xlsx")
    if len(df)==0:
        st.info("Aucune annonce chargée pour l’instant. Les connecteurs seront activés lors du déploiement.")
    else:
        for _, r in df.iterrows():
            st.markdown(f"### [{r.get('title','(sans titre)')}]({r.get('url','#')}) — **Score: {r.get('score',0):.1f}/100**")
            with st.expander("Détails"):
                st.write(r.to_dict())
else:
    st.warning("Le classement n’a pas encore été généré.")
st.divider(); st.write("© Agent IA — Guadeloupe")
