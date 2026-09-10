#!/bin/bash
# Inicializa o banco de dados do Airflow
docker-compose up airflow-init

# Sobe todos os servicos em segundo plano
docker-compose up -d

echo "Ambiente iniciado com sucesso! Acesse http://localhost:8080"