import streamlit as st
import json
import pandas as pd
import duckdb
import glob
import os

st.set_page_config(page_title="Lakehouse Observability", layout="wide")


# ==========================================
# FUNCOES DE CARREGAMENTO
# ==========================================
@st.cache_data
def load_json(filepath):
    try:
        with open(filepath, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return None


def query_parquet(path):
    try:
        return duckdb.query(
            f"SELECT * FROM read_parquet('{path}/*.parquet') LIMIT 1000"
        ).to_df()
    except Exception as e:
        return pd.DataFrame()


# ==========================================
# NAVEGACAO E LAYOUT
# ==========================================
st.sidebar.title("Data Engineering")
menu = st.sidebar.radio(
    "Navegação", ["Monitoramento DQ", "Catálogo de Dados", "Exploração (DuckDB)"]
)

# 1. MONITORAMENTO DQ
if menu == "Monitoramento DQ":
    st.header("Saúde da Pipeline (Camada Silver)")
    
    # Busca arquivos no diretorio e ordena do mais recente pro mais antigo
    report_files = sorted(glob.glob("data/silver/dq_reports/*.json"), reverse=True)
    
    if report_files:
        # Interface de selecao de historico
        selected_report = st.selectbox(
            "Selecione a execução (Histórico):", 
            report_files, 
            format_func=lambda x: os.path.basename(x)
        )
        
        dq_data = load_json(selected_report)
        
        st.write(f"**Data do Relatório:** {dq_data['execution_date']}")
        
        m = dq_data["metrics"]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total de Registros", f"{m['total_records']:,}")
        c2.metric("Erros Capturados", f"{m['total_errors']:,}")
        c3.metric("Taxa de Erro", f"{m['error_rate_pct']}%")
        c4.metric("Conformidade", f"{m['conformity_rate_pct']}%")
        
        st.divider()
        st.subheader("Qualidade por Coluna")
        df_col = pd.DataFrame.from_dict(dq_data["column_quality"], orient="index")
        st.dataframe(df_col, use_container_width=True)
    else:
        st.warning("Nenhum relatório de DQ encontrado. Execute a pipeline Silver primeiro.")

# 2. CATALOGO DE DADOS
elif menu == "Catálogo de Dados":
    st.header("Dicionário de Metadados")
    catalog = load_json("data_catalog.json")

    if catalog:
        for layer, columns in catalog.items():
            with st.expander(layer, expanded=True):
                st.table(pd.DataFrame(columns))
    else:
        st.warning("Arquivo data_catalog.json não encontrado.")

# 3. EXPLORACAO (DUCKDB)
elif menu == "Exploração (DuckDB)":
    st.header("Consulta Direta no Data Lake (Parquet)")

    opcoes_tabelas = [
        ("Bronze Raw", "data/bronze/fraud_data"),
        ("Silver Clean", "data/silver/fraud_data_clean"),
        ("Silver Quarantine", "data/silver/quarantine"),
        ("Gold (Region Risk)", "data/gold/region_risk"),
        ("Gold (Top Sales)", "data/gold/top_sales"),
    ]

    # format_func exibe apenas o indice 0 (o nome) na interface
    table_choice = st.selectbox(
        "Selecione o Data Mart / Camada:", opcoes_tabelas, format_func=lambda x: x[0]
    )

    layer_name, path = table_choice
    st.subheader(f"Visualizando: {layer_name}")

    df_result = query_parquet(path)

    if not df_result.empty:
        st.dataframe(df_result, use_container_width=True)

        # Logica dinamica para o texto de linhas
        num_linhas = len(df_result)
        if num_linhas == 1000:
            st.caption(
                f"Mostrando as primeiras {num_linhas} linhas (limite de visualização atingido)."
            )
        else:
            st.caption(
                f"Mostrando todas as {num_linhas} linhas disponíveis nesta tabela."
            )
    else:
        st.warning(
            "Nenhum dado encontrado neste diretório. Verifique se a DAG foi executada."
        )
