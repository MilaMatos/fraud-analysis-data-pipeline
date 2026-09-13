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
├── data/                    
│   ├── examples/            # Lotes de dados mockados para testes
│   └── ...                  # Data Lake local gerado em tempo de execução
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
docker-compose up airflow-init
```
*Aguarde o processamento até o terminal retornar a mensagem indicando que o container finalizou (`exited with code 0`).*

**2. Subir a Infraestrutura**
Inicia os serviços em segundo plano e ajusta as permissões de pastas.
```bash
docker-compose up -d
```

### 🧪 Testando com Dados de Exemplo (Simulação)
Neste repositório possui dois lotes de arquivos na pasta `data/examples/` para você testar a resiliência da pipeline e o monitoramento visual.

1. **Cenário Ideal:** Copie o arquivo `df_fraud_credit_lote_aprovado.csv` para a raiz da pasta `data/` e renomeie-o para `df_fraud_credit.csv`.
2. **Engatilhar Pipeline:**
   ```bash
   docker-compose exec airflow-webserver airflow dags unpause pipeline_fraud_analysis
   ```
   ```bash
   docker-compose exec airflow-webserver airflow dags trigger pipeline_fraud_analysis
   ```
3. **Cenário de Anomalia (Data Drift e Circuit Breaker):** Repita o processo copiando o `df_fraud_credit_lote_reprovado.csv`, renomeando-o para `df_fraud_credit.csv` e disparando a DAG novamente. Isso forçará a quebra da qualidade para demonstrar os alertas no Streamlit.

### 🔗 Acessos
Com os serviços em execução, acompanhe pelo navegador:
* **Observabilidade (Streamlit):** [http://localhost:8501](http://localhost:8501)
* **Airflow UI:** [http://localhost:8080](http://localhost:8080) (Usuário: `admin` | Senha: `admin`)

## 📈 Próximos Passos
* [x] Setup do ambiente Docker e Airflow
* [x] Ingestão e salvamento da Camada Bronze em formato Parquet
* [x] Camada Silver (Limpeza, Tratamento de Dados e Schema Enforcement)
* [x] Implementação do Data Quality Automatizado (PySpark Nativo, DLQ e Relatório JSON)
* [x] Testes Unitários e Esteira de CI/CD (GitHub Actions)
* [x] Portal de Observabilidade (Streamlit)
* [x] Camada Gold (Agregações por dimensão de negócio)