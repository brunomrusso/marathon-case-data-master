import os
from pathlib import Path

import pandas as pd
import streamlit as st
from databricks import sql
from dotenv import load_dotenv

# Carrega variaveis do .env na raiz do projeto
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
        years_df = run_query("SELECT DISTINCT year FROM marathon.gold.marathon_comparison ORDER BY year")
        available_years = sorted(years_df["year"].dropna().astype(int).tolist())
    except Exception as e:
        st.warning(f"Nao foi possivel carregar anos: {e}")
        available_years = []

    selected_years = st.multiselect("Anos", available_years, default=available_years[-5:] if available_years else [])
    selected_marathon = st.selectbox("Maratona", ["Todas", "Chicago", "London", "New York", "Berlin"])


# KPIs
try:
    kpi = run_query("SELECT * FROM marathon.gold.kpi_summary LIMIT 1")
    cols = st.columns(4)
    cols[0].metric("Conclusoes", int(kpi["total_finishers"].iloc[0]))
    cols[1].metric("Paises", int(kpi["total_countries"].iloc[0]))
    cols[2].metric("Edicoes", int(kpi["total_editions"].iloc[0]))
    if "avg_finish_time" in kpi.columns:
        cols[3].metric("Tempo medio", str(kpi["avg_finish_time"].iloc[0]))
except Exception as e:
    st.warning(f"Nao foi possivel carregar KPIs: {e}")


tab1, tab2, tab3, tab4, tab5 = st.tabs(["Visao Geral", "Paises", "Tempos", "Demografia", "Clima"])

with tab1:
    st.subheader("Conclusoes por ano")
    try:
        finishers = run_query("SELECT year, total_finishers FROM marathon.gold.finishers_by_year ORDER BY year")
        if selected_years:
            finishers = finishers[finishers["year"].isin(selected_years)]
        st.line_chart(finishers.set_index("year"))
    except Exception as e:
        st.error(f"Erro ao carregar conclusoes: {e}")

with tab2:
    st.subheader("Top paises")
    try:
        top = run_query("SELECT country, total_finishers FROM marathon.gold.top_countries ORDER BY total_finishers DESC LIMIT 20")
        st.bar_chart(top.set_index("country"))
    except Exception as e:
        st.error(f"Erro ao carregar paises: {e}")

    st.subheader("Atletas por pais")
    try:
        athletes = run_query(
            "SELECT country, COUNT(*) as total FROM marathon.gold.athletes_by_country GROUP BY country ORDER BY total DESC LIMIT 20"
        )
        st.dataframe(athletes, use_container_width=True)
    except Exception as e:
        st.error(f"Erro ao carregar atletas: {e}")

with tab3:
    st.subheader("Distribuicao de tempos")
    try:
        times = run_query("SELECT finish_time_seconds FROM marathon.gold.times_distribution")
        bins = pd.cut(times["finish_time_seconds"], bins=30).value_counts().sort_index()
        bin_df = pd.DataFrame({"intervalo": [f"{int(i.left)}-{int(i.right)}" for i in bins.index], "atletas": bins.values})
        st.bar_chart(bin_df.set_index("intervalo"))
    except Exception as e:
        st.error(f"Erro ao carregar tempos: {e}")

    st.subheader("Comparacao entre maratonas")
    try:
        comparison = run_query("SELECT * FROM marathon.gold.marathon_comparison")
        if selected_years:
            comparison = comparison[comparison["year"].isin(selected_years)]
        if selected_marathon != "Todas":
            comparison = comparison[comparison["marathon"] == selected_marathon]
        if "marathon" in comparison.columns and "avg_finish_time_seconds" in comparison.columns:
            pivot = comparison.pivot_table(index="year", columns="marathon", values="avg_finish_time_seconds", aggfunc="mean")
            st.bar_chart(pivot)
        else:
            st.dataframe(comparison, use_container_width=True)
    except Exception as e:
        st.error(f"Erro ao carregar comparacao: {e}")

with tab4:
    st.subheader("Perfil de idade e genero")
    try:
        profile = run_query("SELECT * FROM marathon.gold.age_gender_profile")
        if selected_marathon != "Todas":
            profile = profile[profile["marathon"] == selected_marathon]
        st.dataframe(profile, use_container_width=True)
        if "age_group" in profile.columns and "finishers" in profile.columns:
            st.bar_chart(profile.set_index("age_group")[["finishers"]])
    except Exception as e:
        st.error(f"Erro ao carregar demografia: {e}")

with tab5:
    st.subheader("Impacto do clima")
    try:
        weather = run_query("SELECT * FROM marathon.gold.weather_impact")
        if selected_marathon != "Todas":
            weather = weather[weather["marathon"] == selected_marathon]
        st.dataframe(weather, use_container_width=True)
        if "temperature" in weather.columns and "avg_finish_time_seconds" in weather.columns:
            st.scatter_chart(weather, x="temperature", y="avg_finish_time_seconds", color="marathon" if "marathon" in weather.columns else None)
    except Exception as e:
        st.error(f"Erro ao carregar clima: {e}")
