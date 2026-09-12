# Pipeline de Engenharia de Dados - Desafio Técnico

Pipeline de dados desenvolvida com arquitetura em camadas (Medalhão), orquestrada via **Apache Airflow**, processada com **PySpark** e conteinerizada com **Docker**. A arquitetura conta com controle rigoroso de qualidade de dados (Data Quality), roteamento de anomalias (Dead Letter Queue) e portal de observabilidade via **Streamlit** e **DuckDB**.

## 🛠️ Tecnologias Utilizadas
* **Python 3.10** & **PySpark** (Processamento distribuído e Data Quality nativo)
* **Apache Airflow** (Orquestração de pipelines e Circuit Breaker)
* **Docker & Docker Compose** (Conteinerização e reprodutibilidade)
* **Streamlit & DuckDB** (Interface visual de metadados e consultas Parquet)
* **Pytest & GitHub Actions** (Testes de integração e esteira de CI/CD)

## 📁 Estrutura do Projeto
```text
.
├── .github/workflows/       # CI/CD (GitHub Actions)
├── dags/                    
│   ├── modules/             # Funcoes puras (Ingestao, Transformacao, Agregacao)
│   └── pipeline_fraud_analysis.py # Controller DAG Principal
├── data/                    # Data Lake (Bronze, Silver, Gold, Quarentena)
├── tests/                   # Suite automatizada (Pytest)
├── app.py                   # Portal de Observabilidade (Streamlit)
├── data_catalog.json        # Dicionario de metadados dinamico
├── start.sh                 # Entrypoint
├── Dockerfile               # Custom Image
└── docker-compose.yml       # Infraestrutura
```

## 🚀 Como Executar o Projeto

1. Certifique-se de que o arquivo de dados (`df_fraud_credit.csv`) está posicionado na pasta `data/`.
2. Na raiz do projeto, execute o script de inicialização automatizada:
   ```bash
   ./start.sh
   ```
3. A pipeline iniciará automaticamente. Acesse:
   * Observabilidade (Streamlit): http://localhost:8501
   * Airflow UI: http://localhost:8080
      * **Usuário:** admin
      * **Senha:** admin


## 📈 Próximos Passos
* [x] Setup do ambiente Docker e Airflow
* [x] Ingestão e salvamento da Camada Bronze em formato Parquet
* [x] Camada Silver (Limpeza, Tratamento de Dados e Schema Enforcement)
* [x] Implementação do Data Quality Automatizado (PySpark Nativo, DLQ e Relatório JSON)
* [x] Testes Unitários e Esteira de CI/CD (GitHub Actions)
* [x] Camada Gold (Agregações por dimensão de negócio)