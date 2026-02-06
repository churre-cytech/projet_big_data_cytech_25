# Exercice 6 - Airflow

## Consignes partie bonus
L'objectif de cette partie additionnel est de pouvoir proposer un système entièrement automatisé avec Airflow.
1. Pour cela, nous devez ajouter un nouveau service dans le docker-compose nommé Airflow
2. Ensuite vous devez concevoir votre DAG afin d'automatiser sous la forme de pipelines l'ensemble des exercices 


## Procedure reproductible

### 1) Demarrer l'infra
Depuis la racine du projet:

```bash
docker compose up -d
```

### 2) Generer les jars Scala (ex01 + ex02)
Depuis la racine du projet:

```bash
chmod +x ./scripts/build_jars.sh
./scripts/build_jars.sh
```

### 3) Verifier le DAG Airflow
```bash
docker compose exec airflow airflow dags list
docker compose exec airflow airflow dags list-import-errors
```

Le DAG attendu est `tp_bigdata_pipeline`.

### 4) Executer le pipeline
- Ouvrir Airflow UI: `http://localhost:8085`
- Trigger le DAG `tp_bigdata_pipeline`
- Suivre les tasks dans l'ordre:
  - `wait_services`
  - `ex03_init_schema`
  - `ex03_seed_dimensions`
  - `ex01_retrieval`
  - `ex02_ingestion`
  - `ex05_training`

### 5) Notes importantes
- Le script `scripts/build_jars.sh` doit etre relance apres une modification de code Scala (ex01/ex02).
- En local, la commande recommandee est la commande relative depuis la racine: `./scripts/build_jars.sh`.
- Le chemin absolu est aussi possible (exemple): `/home/churre/ING3/Big_data/projet_big_data_cytech_25/scripts/build_jars.sh`.
