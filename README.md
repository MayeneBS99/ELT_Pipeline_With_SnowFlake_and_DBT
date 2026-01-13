# 🚲 Paris-EcoTrack: End-to-End ELT Pipeline
**A Cloud Data Engineering project correlating Velib' (Bike-sharing) usage with real-time weather data in Paris.**

---

## 📌 Overview
This project demonstrates a complete **ELT (Extract, Load, Transform)** pipeline. It automates the collection of bike availability data from the JCDecaux API and weather conditions from OpenWeatherMap, loads them into **Snowflake**, and transforms them using **dbt** into an Analytics-ready Star Schema.

## 🏗 Architecture


1. **Extract & Load (Python)**: Custom scripts fetch JSON data from REST APIs and load it into Snowflake `RAW` tables using the `write_pandas` connector.
2. **Transform (dbt)**: 
   - **Staging Layer**: Cleaning, casting, and renaming raw fields into clean Views.
   - **Marts Layer**: Modeling a **Star Schema** with a unified Fact table (`fact_eco_mobility`) and a Dimension table (`dim_stations`).
3. **Storage (Snowflake)**: Cloud Data Warehouse utilizing multi-layered schemas (`RAW`, `STAGING`, `ANALYTICS`).
4. **Visualization**: (Optional) Ready to be connected to Power BI or Tableau.

## 🛠 Tech Stack
- **Language**: Python 3.x (Pandas, Requests)
- **Data Warehouse**: Snowflake
- **Transformation**: dbt (Data Build Tool)
- **Orchestration**: Environment variables (.env) & Git
- **APIs**: JCDecaux Open Data, OpenWeatherMap

## 📂 Project Structure
```text
Paris-EcoTrack/
├── .env                       <-- clés API et accès Snowflake
├── .gitignore                 <-- Liste les fichiers à NE PAS envoyer sur GitHub
├── requirements.txt
├── src/
│   ├── extract_velib.py
│   └── extract_weather.py
├── dbt_project/               <-- Ton dossier dbt (créé via 'dbt init')
│   ├── dbt_project.yml        <-- Le cerveau (config des tables/vues)
│   
│   ├── models/
│   │   ├── sources.yml        <-- Déclare les tables RAW créées par Python
│   │   ├── staging/           <-- Tes vues SQL de nettoyage
│   │   └── marts/             <-- Tes tables SQL finales (Faits/Dimensions)
└── README.md
