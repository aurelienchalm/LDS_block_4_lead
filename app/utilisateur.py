import streamlit as st
import pandas as pd
import requests
import os
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=env_path, override=True)

# Adresse de ton backend FastAPI
API_HOST = os.getenv("API_HOST")

# Configuration de la page
st.set_page_config(page_title="Estimation immobilière", page_icon="🏡", layout="centered")
st.title("🏡 Estimation du prix d'un bien immobilier avec Est'immo!")

# 📥 Formulaire utilisateur
with st.form("prediction_form"):
    square_feet = st.number_input("📐 Surface (m²)", min_value=10.0, max_value=1000.0, step=1.0)
    num_bedrooms = st.number_input("🛏️ Nombre de chambres", min_value=0, max_value=10, step=1)
    num_bathrooms = st.number_input("🛁 Nombre de salles de bain", min_value=0, max_value=10, step=1)
    num_floors = st.number_input("🏢 Nombre d'étages", min_value=0, max_value=10, step=1)
    year_built = st.number_input("📅 Année de construction", min_value=1800, max_value=2025, step=1)
    has_garden = st.checkbox("🌳 Jardin")
    has_pool = st.checkbox("🏊 Piscine")
    garage_size = st.number_input("🚗 Taille du garage (m²)", min_value=0, max_value=100, step=1)
    location_score = st.slider("📍 Note de localisation", min_value=0.0, max_value=5.0, step=0.1)
    distance_to_center = st.number_input("🗺️ Distance au centre-ville (km)", min_value=0.0, max_value=50.0, step=0.1)

    submitted = st.form_submit_button("💾 Soumettre")

# 📤 Traitement formulaire
if submitted:
    payload = {
        "square_feet": square_feet,
        "num_bedrooms": num_bedrooms,
        "num_bathrooms": num_bathrooms,
        "num_floors": num_floors,
        "year_built": year_built,
        "has_garden": has_garden,
        "has_pool": has_pool,
        "garage_size": garage_size,
        "location_score": location_score,
        "distance_to_center": distance_to_center
    }

    try:
        # 🔹 Insertion du bien via l'API
        insert_resp = requests.post(f"{API_HOST}/insert", json=payload)
        insert_resp.raise_for_status()
        row_id = insert_resp.json()["id"]
        st.success(f"✅ Bien inséré avec l'ID {row_id}.")

        # 🔹 Prédiction via l'API
        with st.spinner("🔮 Prédiction en cours..."):
            predict_resp = requests.post(f"{API_HOST}/predict")
            predict_resp.raise_for_status()
            result = predict_resp.json()
            st.success(f"📈 Prix prédit pour le bien #{result['id']}: **{round(result['predicted_price'], 2)} €**")
    except requests.RequestException as e:
        st.error(f"❌ Erreur API : {e}")

# 📊 Affichage des dernières prédictions
st.markdown("---")
st.subheader("📊 Dernières prédictions")

try:
    predictions_resp = requests.get(f"{API_HOST}/predictions?limit=5")
    predictions_resp.raise_for_status()
    df = pd.DataFrame(predictions_resp.json())

    if not df.empty:
        df_display = df.rename(columns={
            "id": "ID",
            "square_feet": "Surface (m²)",
            "num_bedrooms": "Chambres",
            "num_bathrooms": "Salles de bain",
            "num_floors": "Étages",
            "year_built": "Année",
            "has_garden": "Jardin",
            "has_pool": "Piscine",
            "garage_size": "Garage (m²)",
            "location_score": "Localisation",
            "distance_to_center": "Distance (km)",
            "price_predict": "Prix prédit (€)"
        })
        df_display["Jardin"] = df_display["Jardin"].map({0: "❌", 1: "✅", False: "❌", True: "✅"})
        df_display["Piscine"] = df_display["Piscine"].map({0: "❌", 1: "✅", False: "❌", True: "✅"})
        st.dataframe(df_display, use_container_width=True)
    else:
        st.info("Aucune prédiction disponible.")
except Exception as e:
    st.error(f"❌ Erreur lors de la récupération des prédictions : {e}")