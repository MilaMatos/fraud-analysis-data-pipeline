import streamlit as st
import pandas as pd
import duckdb
import json
import glob
import os

st.set_page_config(page_title="Data Lakehouse Observability", layout="wide", initial_sidebar_state="expanded")

def load_json(path):
    with open(path, "r") as f:
        return json.load(f)

def get_duckdb_conn():
    return duckdb.connect(database=':memory:')

# Callback de Feedback Visual
def notify_update():
    st.toast("Relatório carregado e atualizado na interface!", icon="🔄")

def colored_progress_bar(pct, threshold):
    if pct >= threshold:
        color = "#28a745" # Verde
    elif pct >= (threshold - 5.0):
        color = "#ffc107" # Amarelo
    else:
        color = "#dc3545" # Vermelho
        
    return f"""
    <div style="width: 100%; background-color: #333333; border-radius: 4px; margin-top: 5px; margin-bottom: 10px;">
        <div style="width: {pct}%; background-color: {color}; padding-right: 5px; text-align: right; color: white; border-radius: 4px; font-size: 12px; height: 18px; line-height: 18px; font-weight: bold;">
            {pct}%
        </div>
    </div>
    """

EXPECTED_CATEGORIES = {
    "transaction_type": ["transfer", "purchase", "sale", "phishing", "scam"],
    "location_region": ["Europe", "South America", "Asia", "Africa", "North America"],
    "anomaly": ["low_risk", "moderate_risk", "high_risk"],
    "age_group": ["established", "veteran", "new"],
    "purchase_pattern": ["focused", "high_value", "random"]
}

# ==========================================
# NAVEGAÇÃO GLOBAL (SIDEBAR)
# ==========================================
st.sidebar.title("Navegação")
menu = st.sidebar.radio(
    "Ir para:",
    ["1. Monitoramento DQ (Histórico)", "2. Catálogo de Dados", "3. Exploração (DuckDB)"]
)

# ==========================================
# PÁGINA 1: MONITORAMENTO DQ
# ==========================================
if menu == "1. Monitoramento DQ (Histórico)":
    st.title("Monitoramento de Qualidade de Dados")
    
    report_files = sorted(glob.glob("data/silver/dq_reports/*.json"), reverse=True)

    if not report_files:
        st.warning("Nenhum relatório de Data Quality encontrado. Execute a pipeline Silver primeiro.")
        st.stop()

    st.sidebar.divider()
    st.sidebar.write("**Controle de Visualização**")
    selected_report = st.sidebar.selectbox(
        "Selecione a execução:", 
        report_files, 
        format_func=lambda x: os.path.basename(x),
        on_change=notify_update
    )

    dq_data = load_json(selected_report)
    threshold = dq_data["metrics"].get("circuit_breaker_threshold_pct", 95.0)

    tab1, tab2 = st.tabs(["📊 Visão Geral DQ", "🔍 Exploratória & Drift"])

    # --- ABA 1: VISÃO GERAL DE QUALIDADE ---
    with tab1:
        st.markdown(f"**Data do Relatório:** {dq_data['execution_date']} &nbsp;|&nbsp; **Meta (Circuit Breaker):** {threshold}%")
        
        m = dq_data["metrics"]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total de Registros (Linhas)", f"{m['total_records']:,}")
        c2.metric("Erros Capturados (DLQ)", f"{m['total_errors']:,}")
        c3.metric("Taxa de Erro", f"{m['error_rate_pct']}%")
        c4.metric("Conformidade Global", f"{m['conformity_rate_pct']}%")
        
        with st.expander("ℹ️ Entenda as Métricas de Qualidade"):
            st.markdown("""
            * **Erros Capturados:** Total de linhas enviadas para a Quarentena. Se um registro possui erro em uma ou mais colunas, a linha inteira é invalidada.
            * **Completude (Completeness):** Mede a ausência de nulos. Se a informação existe, é considerada completa.
            * **Validade (Validity):** Mede se a informação está de acordo com as regras de negócio.
            """)
        
        st.divider()
        st.subheader("Qualidade por Coluna")
        
        df_col = pd.DataFrame.from_dict(dq_data["column_quality"], orient="index")
        
        for col_name, row in df_col.iterrows():
            comp_pct = float(row.get('completeness_pct', 0.0))
            val_pct = float(row.get('validity_pct', 0.0))
            
            col1, col2, col3 = st.columns([2, 4, 4])
            with col1:
                st.write(f"**{col_name}**")
                # Aumento de contraste e peso na fonte secundaria
                st.markdown(f"<span style='color: #CCCCCC; font-size: 14px; font-weight: 500;'>Inválidos: {row['invalid']} | Nulos: {row['null']}</span>", unsafe_allow_html=True)
            with col2:
                st.markdown(f"<span style='font-size:12px;'>Completude</span>", unsafe_allow_html=True)
                st.markdown(colored_progress_bar(comp_pct, threshold), unsafe_allow_html=True)
            with col3:
                st.markdown(f"<span style='font-size:12px;'>Validade</span>", unsafe_allow_html=True)
                st.markdown(colored_progress_bar(val_pct, threshold), unsafe_allow_html=True)

    # --- ABA 2: EXPLORATÓRIA & DATA DRIFT ---
    with tab2:
        st.header("Análise Exploratória e Data Drift (Silver)")
        eda = dq_data.get("eda", {})
        
        st.subheader("Estatísticas Numéricas")
        num_data = eda.get("numeric", {})
        if num_data:
            # Estruturacao em DataFrame para melhor UI e escaneamento visual
            df_num = pd.DataFrame.from_dict(num_data, orient='index')
            df_num.rename(columns={'min': 'Mínimo', 'max': 'Máximo', 'avg': 'Média'}, inplace=True)
            st.dataframe(df_num, use_container_width=True)
                
        st.divider()
        st.subheader("Mapeamento de Categorias (Data Drift)")
        
        cat_data = eda.get("categorical", {})
        for col_name, found_categories in cat_data.items():
            expected = set(EXPECTED_CATEGORIES.get(col_name, []))
            found = set(found_categories)
            
            drifted = found - expected
            
            st.markdown(f"**Coluna:** `{col_name}`")
            if drifted:
                st.error(f"⚠️ **Alerta de Drift:** Novas categorias não mapeadas encontradas: {list(drifted)}")
            else:
                st.success("✅ Todas as categorias encontradas correspondem ao contrato esperado.")
                
            st.caption(f"Valores identificados no lote: {list(found)}")
            st.write("---")

# ==========================================
# PÁGINA 2: CATÁLOGO DE DADOS
# ==========================================
elif menu == "2. Catálogo de Dados":
    st.title("Dicionário de Variáveis")
    st.markdown("Documentação centralizada do Data Lakehouse, apresentando o estado e as regras de qualidade aplicadas em cada etapa.")
    st.divider()
    
    catalog = load_json("data_catalog.json")
    
    # Exibe todas as tabelas sequencialmente
    for layer_name, schema_list in catalog.items():
        st.subheader(f"Camada: {layer_name}")
        df_catalog = pd.DataFrame(schema_list)
        st.dataframe(df_catalog, use_container_width=True, hide_index=True)
        st.write("")

# ==========================================
# PÁGINA 3: EXPLORAÇÃO (DUCKDB)
# ==========================================
elif menu == "3. Exploração (DuckDB)":
    st.title("Exploração Direta de Arquivos (Parquet)")
    st.info("ℹ️ **Contexto:** Os dados exibidos abaixo refletem **exclusivamente o estado físico atual** dos arquivos no Data Lake (última execução concluída).")
    
    layer_map = {
        "Bronze (Raw)": "data/bronze/fraud_data/*.parquet",
        "Silver (Clean)": "data/silver/fraud_data_clean/*.parquet",
        "Silver (Quarentena)": "data/silver/quarantine/*.parquet",
        "Gold (Region Risk)": "data/gold/region_risk/*.parquet",
        "Gold (Top Sales)": "data/gold/top_sales/*.parquet"
    }
    
    col1, col2 = st.columns([3, 1])
    with col1:
        selected_layer = st.selectbox("Selecione a camada para visualização:", list(layer_map.keys()))
    with col2:
        row_limit = st.select_slider("Limite de Linhas (Performance)", options=[100, 500, 1000, 5000], value=1000)
    
    try:
        conn = get_duckdb_conn()
        query = f"SELECT * FROM read_parquet('{layer_map[selected_layer]}') LIMIT {row_limit}"
        df_preview = conn.execute(query).df()
        
        st.write(f"Exibindo amostra de `{len(df_preview)}` registros:")
        st.dataframe(df_preview, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error("Dados não encontrados para esta camada. Verifique se a pipeline já foi executada.")
        st.caption(f"Erro original: {e}")