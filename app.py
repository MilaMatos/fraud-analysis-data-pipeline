import streamlit as st
import pandas as pd
import duckdb
import json
import glob
import os
from datetime import datetime, timedelta
import plotly.graph_objects as go

st.set_page_config(page_title="Data Quality Scorecard", layout="wide", initial_sidebar_state="expanded")

def load_json(path):
    with open(path, "r") as f:
        return json.load(f)

def get_duckdb_conn():
    return duckdb.connect(database=':memory:')

def notify_update():
    st.toast("Painel atualizado com sucesso!", icon="📊")

def colored_progress_bar(pct, threshold):
    color = "#28a745" if pct >= threshold else ("#ffc107" if pct >= (threshold - 5.0) else "#dc3545")
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

HARD_COLS = ["amount", "risk_score", "timestamp", "transaction_type", "location_region", "receiving_address"]

# ==========================================
# NAVEGAÇÃO
# ==========================================
st.sidebar.title("Navegação")
menu = st.sidebar.radio("Ir para:", ["1. Visão Geral DQ", "2. Catálogo de Dados", "3. Exploração (DuckDB)"])

# ==========================================
# PÁGINA 1: VISÃO GERAL DQ
# ==========================================
if menu == "1. Visão Geral DQ":
    st.title("Painel de Desempenho (KPIs) - Qualidade de Dados")
    
    report_files = sorted(glob.glob("data/silver/dq_reports/*.json"), reverse=True)
    if not report_files:
        st.warning("Nenhum relatório encontrado.")
        st.stop()

    st.sidebar.divider()
    selected_report = st.sidebar.selectbox(
        "Selecione o Lote (Arquivo):", 
        report_files, 
        format_func=lambda x: os.path.basename(x),
        on_change=notify_update
    )

    dq_data = load_json(selected_report)
    m = dq_data["metrics"]
    threshold = m.get("circuit_breaker_threshold_pct", 95.0)
    
    try:
        dt_obj = datetime.fromisoformat(dq_data['execution_date'])
        dt_obj_utc3 = dt_obj - timedelta(hours=3)
        data_formatada = dt_obj_utc3.strftime("%d/%m/%Y %H:%M:%S")
    except:
        data_formatada = dq_data['execution_date']

    tab1, tab2 = st.tabs(["📊 Visão Geral", "🔍 Exploratória e Drift"])

    # --- ABA 1: VISÃO GERAL ---
    with tab1:
        st.markdown(f"**Data de Processamento do Lote:** {data_formatada} (UTC-3)")
        
        # 1. KPIs Principais com calculo de percentual
        total_ingested = m['total_records'] + m.get('duplicate_records', 0)
        duplicates = m.get('duplicate_records', 0)
        quarantine = m['total_errors']
        clean_records = m['total_records'] - quarantine
        
        pct_dup = round((duplicates / total_ingested) * 100, 2) if total_ingested else 0.0
        pct_quar = round((quarantine / total_ingested) * 100, 2) if total_ingested else 0.0
        pct_clean = round((clean_records / total_ingested) * 100, 2) if total_ingested else 0.0
        
        kpi_style = "padding: 15px; border-radius: 8px; text-align: center; background-color: #1e272e; border: 1px solid #333;"
        
        c1, c2, c3, c4 = st.columns([2, 1, 1, 2])
        with c1:
            st.markdown(f'<div style="{kpi_style} border-top: 4px solid #4da6ff;"><span style="color:#a4b0be; font-size:14px;">Registros Recebidos</span><br><span style="color:#4da6ff; font-size:28px; font-weight:bold;">{f"{total_ingested:,}".replace(",", ".")}</span></div>', unsafe_allow_html=True)
        with c2:
            st.markdown(f'<div style="{kpi_style} border-top: 4px solid #facc15;"><span style="color:#a4b0be; font-size:14px;">Duplicidade</span><br><span style="color:#facc15; font-size:28px; font-weight:bold;">{f"{duplicates:,}".replace(",", ".")} <span style="font-size:14px; opacity:0.8;">({f"{pct_dup:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") }%)</span></span></div>', unsafe_allow_html=True)
        with c3:
            st.markdown(f'<div style="{kpi_style} border-top: 4px solid #dc3545;"><span style="color:#a4b0be; font-size:14px;">Quarentena</span><br><span style="color:#dc3545; font-size:28px; font-weight:bold;">{f"{quarantine:,}".replace(",", ".")} <span style="font-size:14px; opacity:0.8;">({f"{pct_quar:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") }%)</span></span></div>', unsafe_allow_html=True)
        with c4:
            st.markdown(f'<div style="{kpi_style} border-top: 4px solid #28a745;"><span style="color:#a4b0be; font-size:14px;">Registros Válidos</span><br><span style="color:#28a745; font-size:28px; font-weight:bold;">{f"{clean_records:,}".replace(",", ".")} <span style="font-size:14px; opacity:0.8;">({f"{pct_clean:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") }%)</span></span></div>', unsafe_allow_html=True)
        st.write("") 
        
        with st.expander("ℹ️ Entenda as Métricas e Regras de Qualidade"):
            st.markdown("""
            **Métricas Gerais:**
            * **Registros Recebidos:** Volume total de linhas brutas ingeridas no lote antes das validações de qualidade.
            * **Duplicidade:** Quantidade de registros 100% idênticos que foram descartados para evitar contagem dupla.
            * **Quarentena:** Quantidade de registros que falharam em uma ou mais regras bloqueantes de qualidade e foram separados na camada Silver (Quarentena).
            * **Registros Válidos:** Quantidade de registros que passaram por todas as validações obrigatórias e estão prontos para consumo na Gold.
            * **Taxa de Aproveitamento do Lote:** Percentual de registros recebidos que sobreviveram às validações e foram aprovados (Registros Válidos em relação ao Total Recebido).

            **Auditoria e Ocorrências:**
            > **Importante:** os valores abaixo representam ocorrências de problemas identificadas nas colunas. Um mesmo registro pode apresentar múltiplas ocorrências.
            * **Alertas:** Ocorrências de Nulos ou Erros de validação detectados em colunas não obrigatórias.
            * **Erros de Validação:** Ocorrência de um valor que não atende às regras estabelecidas em colunas obrigatórias. (ex: valores negativos em `amount`) . A linha é descartada para Quarentena.
            * **Valores Nulos:** Ocorrência de falta de informação em colunas obrigatórias. A linha é descartada para Quarentena.
         
            **Qualidade por Coluna:**
            * **Completude:** Percentual de valores preenchidos em uma coluna.
            * **Validade:** Percentual de valores que atendem às regras de formato, domínio e/ou negócio estabelecidas para a coluna.
            """)

        st.divider()

        # 2. Avaliação de Qualidade
        col_gauge, spacer, col_cards = st.columns([4, 1, 5])
        
        with col_gauge:
            conformidade = m['conformity_rate_pct']
            cor_gauge = "#28a745" if conformidade >= threshold else "#dc3545"
            
            fig_gauge = go.Figure(go.Indicator(
                mode="gauge+number",
                value=conformidade,
                number={'suffix': "%", 'font': {'size': 40}},
                domain={'x': [0, 1], 'y': [0, 1]},
                title={'text': "Taxa de Aproveitamento do Lote", 'font': {'size': 18}},
                gauge={
                    'axis': {'range': [0, 100], 'tickwidth': 1},
                    'bar': {'color': cor_gauge},
                    'bgcolor': "white",
                    'borderwidth': 2,
                    'bordercolor': "gray",
                    'threshold': {
                        'line': {'color': "black", 'width': 4},
                        'thickness': 0.75,
                        'value': threshold
                    }
                }
            ))
            fig_gauge.update_layout(height=320, margin=dict(l=20, r=20, t=50, b=20))
            st.plotly_chart(fig_gauge, use_container_width=True)

        with spacer:
            st.empty()

        with col_cards:
            st.write("### Auditoria de Qualidade — Ocorrências ")
            
            alerts = m.get('total_alerts', 0)
            nulls_errors = dq_data["anomalies"].get("missing_values", 0)
            logic_errors = dq_data["anomalies"].get("invalid_values", 0)

            html_cards = f"""
                <div style="display: flex; gap: 15px; margin-bottom: 15px;">
                    <!-- Card Erros de Validacao -->
                    <div style="flex: 1; background-color: #1a222d; padding: 16px; border-radius: 8px; border-left: 4px solid #fb923c;">
                        <div style="font-size: 13px; font-weight: 500; color: #94a3b8; margin-bottom: 6px;">Erros de Validação</div>
                        <div style="font-size: 26px; font-weight: 700; color: #fb923c;">{f"{logic_errors:,}".replace(",", ".")}</div>
                    </div>
                    <!-- Card Valores Nulos -->
                    <div style="flex: 1; background-color: #1a222d; padding: 16px; border-radius: 8px; border-left: 4px solid #f87171;">
                        <div style="font-size: 13px; font-weight: 500; color: #94a3b8; margin-bottom: 6px;">Valores Nulos</div>
                        <div style="font-size: 26px; font-weight: 700; color: #f87171;">{f"{nulls_errors:,}".replace(",", ".")}</div>
                    </div>
                </div>
                <!-- Card Alertas -->
                <div style="background-color: #1a222d; padding: 16px; border-radius: 8px; border-left: 4px solid #facc15;">
                    <div style="font-size: 13px; font-weight: 500; color: #94a3b8; margin-bottom: 6px;">Alertas</div>
                    <div style="font-size: 26px; font-weight: 700; color: #facc15;">{f"{alerts:,}".replace(",", ".")}</div>
                </div>
                """
            st.markdown(html_cards, unsafe_allow_html=True)

        st.divider()

        # 3. Qualidade por Coluna
        df_col = pd.DataFrame.from_dict(dq_data["column_quality"], orient="index")
        df_col['total_issues'] = df_col['invalid'] + df_col['null']
        df_col = df_col.sort_values(by='total_issues', ascending=False)
        
        df_hard = df_col[df_col.index.isin(HARD_COLS)]
        df_soft = df_col[~df_col.index.isin(HARD_COLS)]

        def render_col_quality(df_subset, total_rows):
            for col_name, row in df_subset.iterrows():
                comp_pct = float(row.get('completeness_pct', 0.0))
                
                non_null_count = total_rows - int(row['null'])
                if non_null_count > 0:
                    val_pct = (int(row['valid']) / non_null_count) * 100.0
                else:
                    val_pct = 100.0
                
                c_name, c_comp, c_val = st.columns([3, 4, 4])
                with c_name:
                    st.write(f"**{col_name}**")
                    st.markdown(f"<span style='color: #fd7e14; font-size: 15px; font-weight: 600;'>Inválidos: {int(row['invalid'])}</span> &nbsp;|&nbsp; <span style='color: #dc3545; font-size: 15px; font-weight: 600;'>Nulos: {int(row['null'])}</span>", unsafe_allow_html=True)
                with c_comp:
                    st.markdown(f"<span style='font-size:12px;'>Completude</span>", unsafe_allow_html=True)
                    st.markdown(colored_progress_bar(comp_pct, threshold), unsafe_allow_html=True)
                with c_val:
                    st.markdown(f"<span style='font-size:12px;'>Validade</span>", unsafe_allow_html=True)
                    st.markdown(colored_progress_bar(val_pct, threshold), unsafe_allow_html=True)
                
                st.write("") 

        st.subheader("🔴 Colunas Obrigatórias")
        st.caption("Erros nestas colunas reprovam o registro.")
        render_col_quality(df_hard, m['total_records']) 
        
        st.write("---")
            
        st.subheader("🟡 Colunas Opcionais")
        st.caption("Erros nestas colunas geram alertas, mas não reprovam o registro.")
        render_col_quality(df_soft, m['total_records'])

    # --- ABA 2: EXPLORATÓRIA & DRIFT ---
    with tab2:
        unmapped = m.get("unmapped_columns", [])
        if unmapped:
            st.error(f"🚨 **Alerta de Schema Drift:** {len(unmapped)} coluna(s) não mapeada(s): `{unmapped}`.")
        
        st.subheader("Estatísticas Numéricas")
        eda = dq_data.get("eda", {})
        num_data = eda.get("numeric", {})
        if num_data:
            df_num = pd.DataFrame.from_dict(num_data, orient='index')
            df_num.rename(columns={'min': 'Mínimo', 'max': 'Máximo', 'avg': 'Média'}, inplace=True)
            st.dataframe(df_num, use_container_width=True)
                
        st.subheader("Mapeamento de Categorias (Data Drift)")
        cat_data = eda.get("categorical", {})
        for col_name, found_categories in cat_data.items():
            expected = set(EXPECTED_CATEGORIES.get(col_name, []))
            found = set(found_categories)
            drifted = found - expected
            
            st.markdown(f"**Coluna:** `{col_name}`")
            if drifted:
                st.error(f"⚠️ **Alerta de Drift:** Novas categorias encontradas: {list(drifted)}")
            else:
                st.success("✅ Todas as categorias correspondem ao contrato.")
            st.write("---")

# ==========================================
# PÁGINA 2: CATÁLOGO DE DADOS
# ==========================================
elif menu == "2. Catálogo de Dados":
    st.title("Dicionário de Variáveis")
    catalog = load_json("data_catalog.json")
    
    layer_names = list(catalog.keys())
    tabs = st.tabs(layer_names)
    
    def color_obrigatorio(val):
        val_str = str(val).strip().lower()
        if val_str == 'sim':
            return 'color: #f87171; font-weight: bold;'
        elif val_str == 'não' or val_str == 'nao':
            return 'color: #facc15; font-weight: bold;'
        return ''

    for idx, tab in enumerate(tabs):
        with tab:
            df_catalog = pd.DataFrame(catalog[layer_names[idx]])
            
            # Tratamento de erro: só aplica a cor se a coluna existir nesta aba/camada
            if 'obrigatorio' in df_catalog.columns:
                styled_df = df_catalog.style.map(
                    color_obrigatorio, subset=['obrigatorio']
                ) if hasattr(df_catalog.style, 'map') else df_catalog.style.applymap(
                    color_obrigatorio, subset=['obrigatorio']
                )
                st.dataframe(styled_df, use_container_width=True, hide_index=True)
            else:
                st.dataframe(df_catalog, use_container_width=True, hide_index=True)

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
    
    c1, c2 = st.columns([3, 1])
    with c1: 
        selected_layer = st.selectbox("Selecione a camada para visualização:", list(layer_map.keys()))
    with c2: 
        row_limit = st.select_slider("Limite de Linhas (Performance)", options=[100, 250, 500, 1000, 5000], value=250)
    
    st.write("")
    
    try:
        conn = get_duckdb_conn()
        df_preview = conn.execute(f"SELECT * FROM read_parquet('{layer_map[selected_layer]}') LIMIT {row_limit}").df()
        
        st.write(f"Exibindo amostra de `{len(df_preview)}` registros:")
        st.dataframe(df_preview, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error("Dados não encontrados para esta camada.")