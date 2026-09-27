# Pipeline de Engenharia de Dados - Observabilidade e Data Lakehouse

Pipeline de dados baseada na **arquitetura Medalhão** (Bronze, Silver, Gold), orquestrada via **Apache Airflow**, processada com **PySpark** e conteinerizada com **Docker**. O projeto conta com controle rigoroso de qualidade (Data Quality), roteamento de anomalias (Quarentena) e portal de observabilidade.

### 🎯 Objetivos do Projeto

O objetivo principal deste projeto é construir uma infraestrutura completa de **Data Lakehouse resiliente e observável** para o processamento de dados transacionais. 

A pipeline foi desenvolvida para atender aos seguintes requisitos de negócio e engenharia:

1. **Arquitetura de Dados em Camadas (Medalhão):** Estruturar o processamento distribuído em etapas isoladas (**Bronze, Silver e Gold**) para garantir rastreabilidade, imutabilidade e reuso dos dados.
2. **Geração de Insights de Negócio (Camada Gold):**
   * **Análise de Risco Geográfico:** Consolidação do índice médio de risco de fraude (`risk_score`) categorizado por região (`location_region`).
   * **Mapeamento de Principais Transações:** Identificação do **Top 3 maiores vendas (`amount`)** para transações do tipo `sale`, aplicando deduplicação temporal para selecionar apenas a transação mais recente por endereço recebedor (`receiving_address`).
3. **Data Quality &amp; Resiliência Automatizada:** Implementar validações estritas de esquema e regras de negócio, com isolamento de anomalias em uma zona de **Quarentena (Dead Letter Queue)** e interrupção automatizada (**Circuit Breaker**) caso o lote apresente índice de conformidade inferior a 80%.
4. **Observabilidade em Tempo Real:** Disponibilizar um portal visual analítico para monitoramento contínuo da qualidade dos lotes, saúde do esquema (*schema drift*) e volumes processados.
5. **Portabilidade &amp; Qualidade de Software:** Garantir execução simplificada via ambientes totalmente conteinerizados (**Docker Compose**), suíte de testes automatizados (**Pytest**) e esteira de integração contínua (**GitHub Actions**).

---

###

### 🔄 Fluxo da Pipeline
1. **Bronze:** Ingestão dos dados brutos e salvamento em formato Parquet.
2. **Silver:** Limpeza, tipagem e validação de regras de negócio (Data Quality). Registros inválidos são isolados (Quarentena/DLQ) e relatórios de métricas são gerados em JSON.
3. **Circuit Breaker:** Avalia o relatório da Silver. Se a conformidade global cair abaixo do limiar (threshold), o fluxo é interrompido.
4. **Gold:** Geração de agregações prontas para consumo a partir dos dados validados.
5. **Observabilidade:** O painel em Streamlit + DuckDB consome os relatórios JSON e os arquivos físicos Parquet para exibir KPIs, auditoria de colunas e estatísticas.

## 🛠️ Tecnologias Utilizadas
* **Python 3.10** & **PySpark** (Processamento distribuído)
* **Apache Airflow** (Orquestração de dependências)
* **Docker & Docker Compose** (Conteinerização)
* **Streamlit & DuckDB** (Interface visual e motor analítico)
* **Pytest & GitHub Actions** (Testes de integração)

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
2. Baixe o arquivo de dados base do projeto: [📥 Download df_fraud_credit.csv](https://drive.google.com/drive/folders/1joufA6g9pN86BuvAfG3fNThTy6k51x_p?hl)
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
3. **Cenário de Anomalia:** Copie o arquivo `df_fraud_credit_lote_reprovado.csv`, renomeando-o para `df_fraud_credit.csv`. Dispare a DAG novamente. Isso forçará uma reprovação na qualidade para demonstrar os alertas e KPIs no Streamlit.

### 🔗 Acessos
Com os serviços em execução, acesse pelo navegador:
* **Observabilidade (Streamlit):** [http://localhost:8501](http://localhost:8501)
* **Airflow UI:** [http://localhost:8080](http://localhost:8080) (Usuário: `admin` | Senha: `admin`)

---

### 🧠 Decisões de Design e Arquitetura
Durante a concepção e desenvolvimento dessa pipeline de dados me deparei com diferentes caminhos técnicos. A escolha das ferramentas e abordagens deste projeto foi feita considerando fatores como escalabilidade, manutenibilidade, isolamento de falhas e boas práticas de engenharia de software. 

Abaixo estão registradas as principais decisões de design tomadas ao longo do projeto e suas justificativas:

* **Arquitetura Medalhão (Bronze, Silver, Gold):** Escolhida para garantir imutabilidade da fonte na camada Bronze, isolamento e sanitização na Silver, e entrega de visões analíticas de alta performance na Gold. Essa separação assegura total auditabilidade e permite reprocessar regras de negócio sem a necessidade de re-ingerir a fonte original.
* **PySpark no Core da Pipeline:** Optou-se pelo PySpark (em vez de Pandas ou DuckDB) para a etapa de ETL por conta da sua capacidade nativa de processamento distribuído. Pensado para escalar para volumes massivos de dados em um ambiente corporativo.
* **DuckDB + Streamlit para Observabilidade:** Enquanto o PySpark gerencia o processamento do Lakehouse, o **DuckDB** foi adotado no portal de observabilidade por consultar arquivos Parquet e JSON locais com altíssima velocidade in-memory via SQL nativo. Combinado ao **Streamlit**, para entregar dashboards analíticos leves e sem a necessidade de provisionar um banco relacional pesado.
* **Resiliência com Quarentena (DLQ) &amp; Circuit Breaker:** A pipeline foi desenhada sob o princípio de *Design for Failure*. Registros que violam regras estritas (*Hard Rules*) são desviados para a Quarentena com o motivo da rejeição para auditoria. Caso a conformidade global do lote caia abaixo de 80%, o **Circuit Breaker** interrompe a DAG no Airflow, impedindo a contaminação das tabelas analíticas.
* **Qualidade de Software com Pytest &amp; GitHub Actions (CI/CD):** Para garantir o ciclo de vida e a estabilidade do código, foi implementada uma suíte de testes automatizados cobrindo a integridade das DAGs, transformações e agregações. A integração com o **GitHub Actions** valida automaticamente cada push, servindo como uma aplicação prática de cultura de testes e automação de CI/CD no fluxo.