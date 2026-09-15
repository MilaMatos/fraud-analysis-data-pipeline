# Pipeline de Engenharia de Dados - Observabilidade e Data Lakehouse

Pipeline de dados baseada na arquitetura Medalhão (Bronze, Silver, Gold), orquestrada via **Apache Airflow**, processada com **PySpark** e conteinerizada com **Docker**. O projeto conta com controle rigoroso de qualidade (Data Quality), roteamento de anomalias (Quarentena) e portal de observabilidade.

### 🔄 Fluxo da Pipeline
1. **Bronze:** Ingestão dos dados brutos e salvamento em formato Parquet.
2. **Silver:** Limpeza, tipagem e validação de regras de negócio (Data Quality). Registros inválidos são isolados (Quarentena/DLQ) e relatórios de métricas são gerados em JSON.
3. **Circuit Breaker:** O Airflow avalia o relatório da Silver. Se a conformidade global cair abaixo do limiar (threshold), o fluxo é interrompido.
4. **Gold:** Geração de agregações prontas para consumo a partir dos dados validados.
5. **Observabilidade:** O painel em Streamlit + DuckDB consome os relatórios JSON e os arquivos físicos Parquet para exibir KPIs, auditoria de colunas e estatísticas.

## 🛠️ Tecnologias Utilizadas
* **Python 3.10** & **PySpark** (Processamento distribuído)
* **Apache Airflow** (Orquestração de dependências)
* **Docker & Docker Compose** (Conteinerização)
* **Streamlit & DuckDB** (Interface visual e motor analítico)
* **Pytest & GitHub Actions** (Testes de integração e CI/CD)

## 📁 Estrutura do Projeto
```text
.
├── .github/workflows/       # CI/CD (GitHub Actions)
├── dags/                    
│   ├── modules/             # Funcoes puras (Ingestao, Transformacao, Agregacao)
│   └── pipeline_fraud_analysis.py # Controller DAG Principal
├── data/                    
│   └── examples/            # Lotes de dados mockados para testes
├── tests/                   # Suite automatizada (Pytest)
├── app.py                   # Portal de Observabilidade (Streamlit)
├── data_catalog.json        # Dicionario de metadados dinamico
├── Dockerfile               # Imagem customizada
└── docker-compose.yml       # Infraestrutura local
```

## 🚀 Como Executar o Projeto

### Pré-requisitos
1. **Docker Desktop** (ou Docker Engine + Docker Compose v2) instalado e rodando.
2. Baixe o arquivo de dados base do projeto: [📥 Download df_fraud_credit.csv](INSERIR_SEU_LINK_AQUI)
3. Coloque o arquivo baixado (`df_fraud_credit.csv`) dentro da pasta `data/` na raiz do projeto.

### Passo a Passo de Inicialização

**1. Inicializar o Banco de Dados do Airflow**
```bash
docker-compose up airflow-init
```
*Aguarde o processamento até o terminal retornar a mensagem `exited with code 0`.*

**2. Subir a Infraestrutura**
```bash
docker-compose up -d
```

### 🧪 Testando com Dados de Exemplo (Simulação)
O repositório possui dois lotes na pasta `data/examples/` para você testar a resiliência e a observabilidade da pipeline.

1. **Cenário Ideal:** Copie o arquivo `df_fraud_credit_lote_aprovado.csv` para a raiz da pasta `data/`, renomeie-o para `df_fraud_credit.csv`.
2. **Engatilhar Pipeline:**
   ```bash
   docker-compose exec airflow-webserver airflow dags unpause pipeline_fraud_analysis
   docker-compose exec airflow-webserver airflow dags trigger pipeline_fraud_analysis
   ```
3. **Cenário de Anomalia (Circuit Breaker):** Copie o arquivo `df_fraud_credit_lote_reprovado.csv`, renomeando-o para `df_fraud_credit.csv`. Dispare a DAG novamente. Isso forçará uma reprovação na qualidade para demonstrar os alertas e KPIs no Streamlit.

### 🔗 Acessos
Com os serviços em execução, acesse pelo navegador:
* **Observabilidade (Streamlit):** [http://localhost:8501](http://localhost:8501)
* **Airflow UI:** [http://localhost:8080](http://localhost:8080) (Usuário: `admin` | Senha: `admin`)