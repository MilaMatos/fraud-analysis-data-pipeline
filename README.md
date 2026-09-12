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
├── Dockerfile               # Custom Image
└── docker-compose.yml       # Infraestrutura
```

## 🚀 Como Executar o Projeto (Windows, Linux ou macOS)

### Pré-requisitos
1. **Docker Desktop** (ou Docker Engine + Docker Compose v2) instalado e rodando.
   * *Usuários de Windows:* Certifique-se de que a Virtualização está ativada na BIOS e o WSL (Windows Subsystem for Linux) está instalado (`wsl --install`).
2. O arquivo de dados bruto (`df_fraud_credit.csv`) deve estar posicionado dentro da pasta `data/` na raiz do projeto.

### Passo a Passo de Inicialização

**1. Inicializar o Banco de Dados do Airflow**
Cria as tabelas de metadados e o usuário administrador nativo.
```bash
docker compose up airflow-init
```
*Aguarde o processamento até o terminal retornar a mensagem indicando que o container finalizou (`exited with code 0`).*

**2. Subir a Infraestrutura**
Inicia os serviços do Postgres, Airflow (Webserver e Scheduler) e Streamlit em segundo plano. O container `setup-permissions` ajustará as permissões da pasta `data/` automaticamente.
```bash
docker compose up -d
```

**3. Ativar e Executar a Pipeline**
Aguarde de 15 a 30 segundos para a inicialização completa do webserver e execute os comandos abaixo para tirar a DAG da pausa e engatilhá-la:
```bash
docker compose exec airflow-webserver airflow dags unpause pipeline_fraud_analysis
docker compose exec airflow-webserver airflow dags trigger pipeline_fraud_analysis
```
*Alternativa:* Você também pode acessar a interface web do Airflow e ativar a DAG manualmente alterando o botão de "Pause/Unpause" e clicando em "Trigger DAG".

### 🔗 Acessos
Com os serviços em execução, acompanhe a pipeline pelo navegador:
* **Airflow UI:** [http://localhost:8080](http://localhost:8080)
  * **Usuário:** admin
  * **Senha:** admin
* **Observabilidade (Streamlit):** [http://localhost:8501](http://localhost:8501)

## 📈 Próximos Passos
* [x] Setup do ambiente Docker e Airflow
* [x] Ingestão e salvamento da Camada Bronze em formato Parquet
* [x] Camada Silver (Limpeza, Tratamento de Dados e Schema Enforcement)
* [x] Implementação do Data Quality Automatizado (PySpark Nativo, DLQ e Relatório JSON)
* [x] Testes Unitários e Esteira de CI/CD (GitHub Actions)
* [x] Camada Gold (Agregações por dimensão de negócio)