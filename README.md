# Pipeline de Engenharia de Dados - Desafio Técnico

Pipeline de dados desenvolvida com arquitetura em camadas (Medalhão), orquestrada via **Apache Airflow**, processada com **PySpark** e conteinerizada com **Docker**.

## 🛠️ Tecnologias Utilizadas
* **Python 3.10** & **PySpark** (Processamento distribuído)
* **Apache Airflow** (Orquestração de pipelines)
* **Docker & Docker Compose** (Conteinerização e reprodutibilidade)
* **Great Expectations** (Data Quality Automatizado - *Em breve*)
* **PostgreSQL** (Backend do Airflow e Camada Gold / Data Warehouse)

## 📁 Estrutura do Projeto
```text
.
├── .github/workflows/       # Esteira de CI (GitHub Actions)
├── dags/                    # DAGs do Apache Airflow
│   └── 01_bronze_ingestion.py # Pipeline de Ingestão (Camada Bronze)
├── data/                    # Data Lake Local (Bronze, Silver, Gold)
├── start.sh                 # Script de inicialização automatizada
├── Dockerfile               # Imagem customizada com Java e Airflow
├── docker-compose.yml       # Orquestração dos serviços
└── requirements.txt         # Dependências do projeto
```

## 🚀 Como Executar o Projeto

Garantimos a execução fácil em qualquer ambiente através de containers Docker. 

1. Certifique-se de que o arquivo de dados (`df_fraud_credit.csv`) está posicionado na pasta `data/`.
2. Na raiz do projeto, execute o script de inicialização automatizada:
   ```bash
   ./start.sh
   ```
3. Acesse a interface web do Airflow no navegador:
   * **URL:** http://localhost:8080
   * **Usuário:** admin
   * **Senha:** admin
4. Ative e execute a DAG `01_bronze_ingestion`.

## 📈 Próximos Passos
* [x] Setup do ambiente Docker e Airflow
* [x] Ingestão e salvamento da Camada Bronze em formato Parquet
* [ ] Implementação do Data Quality Automatizado (Great Expectations)
* [ ] Camada Silver (Limpeza e Tratamento de Dados)
* [ ] Camada Gold (Agregações e Carga no PostgreSQL)
* [ ] Testes Unitários e Esteira de CI/CD (GitHub Actions)