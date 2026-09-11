# Pipeline de Engenharia de Dados - Desafio Técnico

Pipeline de dados desenvolvida com arquitetura em camadas (Medalhão), orquestrada via **Apache Airflow**, processada com **PySpark** e conteinerizada com **Docker**. A arquitetura conta com controle rigoroso de qualidade de dados (Data Quality) e roteamento de anomalias (Dead Letter Queue).

## 🛠️ Tecnologias Utilizadas
* **Python 3.10** & **PySpark** (Processamento distribuído e Data Quality nativo)
* **Apache Airflow** (Orquestração de pipelines e Circuit Breaker)
* **Docker & Docker Compose** (Conteinerização e reprodutibilidade)
* **Pytest** (Testes unitários, de integridade de DAGs e de integração)
* **GitHub Actions** (Esteira de CI/CD com validação de formatação e testes)
* **PostgreSQL** (Backend do Airflow e futura Camada Gold / Data Warehouse)

## 📁 Estrutura do Projeto
```text
.
├── .github/workflows/       # Esteira de CI (GitHub Actions)
├── dags/                    # DAGs do Apache Airflow[cite: 6]
│   ├── 01_bronze_ingestion.py # Ingestão e validação inicial de schema
│   └── 02_silver_transform.py # Data Quality, Tipagem e roteamento para DLQ
├── data/                    # Data Lake Local (Bronze, Silver e Quarentena)
├── tests/                   # Suíte de testes automatizados (Pytest)
│   ├── data/                # Dados mockados para simulação de cenários
│   └── test_*.py            # Validação de integridade e lógica de negócio
├── start.sh                 # Script de inicialização automatizada[cite: 6]
├── Dockerfile               # Imagem customizada com Java e Airflow[cite: 6]
├── docker-compose.yml       # Orquestração dos serviços[cite: 6]
└── requirements.txt         # Dependências do projeto[cite: 6]
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
4. Ative e execute as DAGs em ordem: `01_bronze_ingestion` e depois a `02_silver_transform`.

## 📈 Próximos Passos
* [x] Setup do ambiente Docker e Airflow
* [x] Ingestão e salvamento da Camada Bronze em formato Parquet
* [x] Camada Silver (Limpeza, Tratamento de Dados e Schema Enforcement)
* [x] Implementação do Data Quality Automatizado (PySpark Nativo, DLQ e Relatório JSON)
* [x] Testes Unitários e Esteira de CI/CD (GitHub Actions)
* [ ] Camada Gold (Agregações por dimensão de negócio)
* [ ] Carga final no banco de dados PostgreSQL