import os
from pathlib import Path

import pandas as pd
import streamlit as st
from databricks import sql
from dotenv import load_dotenv

# Carrega variaveis do .env na raiz do projeto e sobrescreve as ja carregadas
PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(PROJECT_ROOT / ".env", override=True)


def get_connection_params():
    """Limpa e retorna os parametros de conexao."""
    host = os.environ.get("DATABRICKS_HOST", "").strip()
    host = host.removeprefix("https://").removeprefix("http://").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN", "").strip()
    http_path = os.environ.get("DATABRICKS_HTTP_PATH", "").strip()
    return host, token, http_path


def test_connection():
    """Testa a conexao com uma query simples."""
    host, token, http_path = get_connection_params()
    with sql.connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            return cur.fetchone()


def run_query(query):
    """Executa uma consulta no Databricks SQL e retorna um DataFrame pandas."""
    host, token, http_path = get_connection_params()

    if not all([host, token, http_path]):
        st.error("Preencha DATABRICKS_HOST, DATABRICKS_TOKEN e DATABRICKS_HTTP_PATH no .env")
        st.stop()

    with sql.connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            return pd.DataFrame(rows, columns=columns)


st.set_page_config(page_title="Marathon Majors", layout="wide")

st.title("World Marathon Majors — Dashboard")
st.markdown("Analise de resultados das maratonas de Chicago, Londres, Nova York e Berlin.")

with st.sidebar:
    with st.expander("Diagnostico de conexao"):
        host, token, http_path = get_connection_params()
        st.write(f"Host: {host[:40]}..." if host else "Host nao configurado")
        st.write(f"Token: {token[:8]}..." if token else "Token nao configurado")
        st.write(f"HTTP path: {http_path}")
        if st.button("Testar conexao"):
            try:
                test_connection()
                st.success("Conexao OK!")
            except Exception as e:
                st.error(f"Falha na conexao: {e}")


# Sidebar com filtros
with st.sidebar:
    st.header("Filtros")
    try:
        years_df = run_query("SELECT DISTINCT year FROM gold.finishers_by_year ORDER BY year")
        available_years = sorted(years_df["year"].dropna().astype(int).tolist())
    except Exception as e:
        st.warning(f"Nao foi possivel carregar anos: {e}")
        available_years = []

    selected_years = st.multiselect("Anos", available_years, default=available_years[-5:] if available_years else [])
    selected_marathon = st.selectbox("Maratona", ["Todas", "Chicago", "London", "New York", "Berlin"])


# KPIs
try:
    kpi = run_query("""
        SELECT 
            SUM(total_athletes) AS total_finishers,
            COUNT(DISTINCT year) AS total_editions,
            COUNT(DISTINCT source) AS total_marathons
        FROM gold.kpi_summary
    """)
    cols = st.columns(4)
    if not kpi.empty:
        cols[0].metric("Conclusoes", int(kpi["total_finishers"].iloc[0] or 0))
        cols[1].metric("Edicoes", int(kpi["total_editions"].iloc[0] or 0))
        cols[2].metric("Maratonas", int(kpi["total_marathons"].iloc[0] or 0))
except Exception as e:
    st.warning(f"Nao foi possivel carregar KPIs: {e}")


tab1, tab2, tab3, tab4, tab5 = st.tabs(["Visao Geral", "Paises", "Tempos", "Demografia", "Clima"])

with tab1:
    st.subheader("Conclusoes por ano")
    try:
        finishers = run_query("""
            SELECT year, SUM(total_finishers) AS total_finishers
            FROM gold.finishers_by_year
            GROUP BY year
            ORDER BY year
        """)
        if selected_years:
            finishers = finishers[finishers["year"].isin(selected_years)]
        if selected_marathon != "Todas":
            finishers = finishers[finishers["source"] == selected_marathon.lower()]
        st.line_chart(finishers.set_index("year"))
    except Exception as e:
        st.error(f"Erro ao carregar conclusoes: {e}")

    st.subheader("Comparacao entre maratonas")
    try:
        comparison = run_query("""
            SELECT source, year, avg_finish_time_sec
            FROM gold.kpi_summary
            ORDER BY year, source
        """)
        if selected_years:
            comparison = comparison[comparison["year"].isin(selected_years)]
        if selected_marathon != "Todas":
            comparison = comparison[comparison["source"] == selected_marathon.lower()]
        if not comparison.empty:
            pivot = comparison.pivot_table(index="year", columns="source", values="avg_finish_time_sec", aggfunc="mean")
            st.line_chart(pivot)
    except Exception as e:
        st.error(f"Erro ao carregar comparacao: {e}")

with tab2:
    st.subheader("Top paises")
    try:
        top = run_query("""
            SELECT country, SUM(total_athletes) AS total_athletes
            FROM gold.top_countries
            GROUP BY country
            ORDER BY total_athletes DESC
            LIMIT 20
        """)
        st.bar_chart(top.set_index("country"))
    except Exception as e:
        st.error(f"Erro ao carregar paises: {e}")

    st.subheader("Atletas por pais")
    try:
        athletes = run_query("""
            SELECT country, total_athletes, avg_finish_time_sec
            FROM gold.athletes_by_country
            ORDER BY total_athletes DESC
            LIMIT 20
        """)
        st.dataframe(athletes, use_container_width=True)
    except Exception as e:
        st.error(f"Erro ao carregar atletas: {e}")

with tab3:
    st.subheader("Tempos por faixa etaria e genero")
    try:
        times = run_query("""
            SELECT age_group, gender, AVG(mean) AS mean_time
            FROM gold.times_distribution
            GROUP BY age_group, gender
            ORDER BY age_group
        """)
        if not times.empty:
            pivot = times.pivot_table(index="age_group", columns="gender", values="mean_time", aggfunc="mean")
            st.bar_chart(pivot)
    except Exception as e:
        st.error(f"Erro ao carregar tempos: {e}")

    st.subheader("Estatisticas de tempos")
    try:
        stats = run_query("""
            SELECT age_group, gender, AVG(min) AS minimo, AVG(mean) AS media, AVG(median) AS mediana, AVG(max) AS maximo
            FROM gold.times_distribution
            GROUP BY age_group, gender
            ORDER BY age_group
        """)
        st.dataframe(stats, use_container_width=True)
    except Exception as e:
        st.error(f"Erro ao carregar estatisticas: {e}")

with tab4:
    st.subheader("Perfil de idade e genero")
    try:
        profile = run_query("""
            SELECT age_group, gender, SUM(total_athletes) AS total_athletes
            FROM gold.age_gender_profile
            GROUP BY age_group, gender
            ORDER BY age_group
        """)
        if not profile.empty:
            pivot = profile.pivot_table(index="age_group", columns="gender", values="total_athletes", aggfunc="sum")
            st.bar_chart(pivot)
            st.dataframe(profile, use_container_width=True)
    except Exception as e:
        st.error(f"Erro ao carregar demografia: {e}")

with tab5:
    st.subheader("Impacto do clima")
    try:
        weather = run_query("""
            SELECT source, year, temperature_mean_c, avg_finish_time_sec
            FROM gold.weather_impact
            ORDER BY year, source
        """)
        if not weather.empty:
            st.scatter_chart(weather, x="temperature_mean_c", y="avg_finish_time_sec", color="source")
            st.dataframe(weather, use_container_width=True)
        else:
            st.info("Tabela gold.weather_impact vazia. Verifique se o weather enrichment foi executado.")
    except Exception as e:
        st.error(f"Erro ao carregar clima: {e}")
