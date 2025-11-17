# Next‑Gen Football Data Pipeline & Dashboard

A fully‑containerized, end‑to‑end modern data engineering project built inside **GitHub Codespaces** using **Python, PostgreSQL, Docker, dbt, Airflow, and Streamlit**. The goal is to design a production‑grade pipeline powering a football analytics dashboard with automated ingestion, transformation, and serving of multi‑season player statistics.

---

## 📌 Project Overview

This project builds a complete data platform for football data, starting from raw CSV season statistics all the way to a live interactive dashboard.

### Pipeline Stages

1. **Raw Data Acquisition** – Downloading season CSV files.
2. **Data Cleaning & Normalization** – Standardizing columns across seasons.
3. **Staging Schema** – Loading cleaned data into PostgreSQL.
4. **dbt Transformations** – Building dimensional models.
5. **Airflow Orchestration** – Automating ingestion → staging → warehouse.
6. **Analytics Dashboard (Streamlit)** – Surfacing KPIs and insights.

---

## 🏗 Architecture Diagram

```
Raw CSVs → Cleaning Script → staging schema → dbt models → mart tables → Streamlit Dashboard
           (Python)         (PostgreSQL)       (dbt)          (PostgreSQL)        (Python)
```

Everything runs using Docker‑based services.

---

## 🧱 Project Structure

```
nextgen-football-pipeline/
│
├── data/                # Raw + cleaned data
├── scripts/             # Python loaders + cleaning
├── docker/              # Docker configs
├── dbt_project/         # dbt models
├── airflow/             # Airflow DAGs
├── app/                 # Streamlit dashboard
└── README.md
```

---

## 🧼 Step 1 — Structural Normalization & Cleaning

Before loading anything into the database, we:

* Standardized all season CSV files.
* Unified column names.
* Ensured consistent data types.
* Removed corrupted rows.

### Output

All cleaned files now share a single schema.

---

## 🏗 Step 2 — Staging Schema Design

We created a PostgreSQL schema named **staging**.

Inside it, we loaded **merged multi‑season tables**:

* `staging.stg_stats_standard` → UNION ALL of all *standard stats* seasons
* `staging.stg_stats_shooting`
* `staging.stg_stats_passing`
* etc.

The staging layer stores **cleaned but untarnsformed** data.

---

## 🛠 Step 3 — dbt Transformations

Using dbt, we:

* Profiled staging tables
* Created **models/base** to standardize naming
* Built **intermediate** logic (e.g., team/player dimension linking)
* Built **marts** containing:

  * `fact_player_season` tables
  * `dim_player`
  * `dim_team`

These models power the dashboard.

---

## 🔄 Step 4 — Airflow Orchestration

Airflow automates the pipeline:

1. Extract raw CSVs
2. Clean & normalize
3. Load to staging
4. Run dbt transformations
5. Notify dashboard service

DAGs live in:

```
airflow/dags/
```

---

## 📊 Step 5 — Streamlit Analytics Dashboard

The dashboard reads the **mart tables** using a read‑only connection.

Key features:

* Player comparison
* Seasonal performance trends
* Leaderboards
* KPIs (xG, goals, key passes, etc.)

---

## 🐳 Docker Services

All services run in containers:

* **PostgreSQL** → Data warehouse
* **Airflow** → Orchestration
* **dbt** → Transformations
* **Streamlit** → Analytics
* **PgAdmin** → DB UI

---

## 📦 Running the Project

```sh
docker-compose up --build
```

Then access:

* Airflow UI → localhost:8080
* Streamlit → localhost:8501
* PgAdmin → localhost:5050

---

## 🧪 Testing

Test scripts live in `tests/`.

* Data quality checks (dbt tests)
* Freshness checks
* Schema matching

---

## 🚀 Future Enhancements

* Add ingestion from APIs
* Add ML models (rating prediction)
* Add match‑level datasets

---

#