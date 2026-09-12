#!/bin/bash
# Inicializa DB Airflow
docker-compose up airflow-init

# Inicia servicos em background
docker-compose up -d

# Aguarda webserver iniciar
sleep 15

# Ativa e dispara DAG principal
docker exec -it $(docker-compose ps -q airflow-webserver) airflow dags unpause pipeline_fraud_analysis
docker exec -it $(docker-compose ps -q airflow-webserver) airflow dags trigger pipeline_fraud_analysis

echo "Airflow: http://localhost:8080"
echo "Observabilidade (Streamlit): http://localhost:8501"