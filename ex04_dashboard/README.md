# Exercice 4 - Data Visualization (Streamlit)

## Consignes (partie 4)
- Concevoir une restitution de donnees (EDA ou dashboard).
- Connecter l'outil de visualisation au datamart (Postgres).
- Produire des visualisations metier exploitables.

## Implementation retenue
Dashboard Streamlit connecte a `bigdata_dwh` (Postgres) et alimente par `fact_trip` + dimensions.

Structure de la page:
- Titre + description
- 5 KPI
- 4 graphiques (2x2)
- Top 10 pickup zones (table + bar chart)

## Prerequis
- Services lances (`postgres` au minimum):

```bash
docker compose up -d
```

- Datamart deja alimente (ex02 + ex03 executes).

## Lancer le dashboard
Depuis la racine du projet:

```bash
cd ex04_dashboard
uv run streamlit run app.py
```


Puis ouvrir:
- `http://localhost:8501`

## Source de donnees
Connexion SQLAlchemy definie dans `app.py`:
- `postgresql+psycopg2://bigdata:bigdata123@localhost:5432/bigdata_dwh`

## KPIs et graphiques
- KPI:
  - Total trips
  - Revenue
  - Average fare
  - Average distance
  - Tip rate
- Graphiques:
  - Trips by day of week
  - Revenue by hour
  - Average fare by payment type
  - Average tip rate by vendor
- Bottom section:
  - Top 10 pickup zones (table + bar chart)

## Depannage rapide
- Si erreur connexion Postgres:
  - verifier `docker compose ps`
  - verifier que `postgres` ecoute sur `5432`
- Si page vide:
  - verifier que les tables du datamart contiennent des donnees
- Si Streamlit ne recharge pas:
  - relancer `uv run streamlit run app.py`
