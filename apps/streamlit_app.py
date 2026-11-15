"""Dashboard Streamlit/Plotly de restitution LahjaLens."""

from __future__ import annotations

import io
import os
from typing import Any, Sequence

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st


st.set_page_config(
    page_title="LahjaLens Analytics",
    page_icon="🔎",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource(show_spinner=False)
def _connection_config() -> dict[str, Any]:
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5434")),
        "dbname": os.getenv("POSTGRES_DB", "lahjalens_db"),
        "user": os.getenv("POSTGRES_USER", "lahja_user"),
        "password": os.getenv("POSTGRES_PASSWORD", "change_me"),
    }


def query_df(sql: str, params: Sequence[Any] | None = None) -> pd.DataFrame:
    connection = psycopg2.connect(**_connection_config())
    try:
        return pd.read_sql_query(sql, connection, params=params)
    finally:
        connection.close()


def safe_query(sql: str, params: Sequence[Any] | None = None) -> pd.DataFrame:
    try:
        return query_df(sql, params)
    except Exception as error:  # pragma: no cover - rendered by Streamlit
        st.error(
            "PostgreSQL est indisponible ou le schéma LahjaLens n'est pas initialisé. "
            f"Détail : {type(error).__name__}: {error}"
        )
        st.stop()


def _filters() -> tuple[str, list[Any]]:
    categories = safe_query(
        "SELECT DISTINCT article_topic FROM lahjalens.v_latest_predictions "
        "WHERE article_topic IS NOT NULL ORDER BY article_topic"
    )["article_topic"].tolist()
    sources = safe_query(
        "SELECT DISTINCT source FROM lahjalens.v_latest_predictions ORDER BY source"
    )["source"].tolist()
    models = safe_query(
        "SELECT DISTINCT model_version FROM lahjalens.v_latest_predictions ORDER BY model_version"
    )["model_version"].tolist()

    st.sidebar.subheader("Filtres analytiques")
    category = st.sidebar.multiselect("Catégorie article", categories)
    source = st.sidebar.multiselect("Source", sources)
    sentiment = st.sidebar.multiselect(
        "Sentiment prédit", ["negative", "neutral", "positive"]
    )
    model = st.sidebar.multiselect("Version modèle", models)
    clauses: list[str] = []
    params: list[Any] = []
    for field, values in (
        ("article_topic", category),
        ("source", source),
        ("predicted_sentiment", sentiment),
        ("model_version", model),
    ):
        if values:
            clauses.append(f"{field} = ANY(%s)")
            params.append(values)
    return (" WHERE " + " AND ".join(clauses)) if clauses else "", params


def _filtered_query(base_sql: str, suffix: str, params: Sequence[Any]) -> pd.DataFrame:
    return safe_query(base_sql + suffix, params)


def render_header() -> None:
    st.title("LahjaLens Analytics")
    st.caption(
        "Analyse descriptive des prédictions E2 sur Hespress — les sentiments affichés sont des sorties de modèle, pas des annotations humaines."
    )


def page_overview() -> None:
    render_header()
    overview = safe_query("SELECT * FROM lahjalens.v_overview").iloc[0]
    columns = st.columns(6)
    cards = (
        ("Commentaires analysés", int(overview.comments_analyzed)),
        ("Prédits positifs", int(overview.positive_predictions)),
        ("Prédits négatifs", int(overview.negative_predictions)),
        ("Prédits neutres", int(overview.neutral_predictions)),
        ("Catégories", int(overview.categories_covered)),
        ("Versions modèle", int(overview.model_versions)),
    )
    for column, (label, value) in zip(columns, cards):
        column.metric(label, f"{value:,}")

    left, right = st.columns(2)
    distribution = safe_query("SELECT * FROM lahjalens.v_sentiment_distribution")
    with left:
        st.subheader("Répartition globale")
        if distribution.empty:
            st.info("Aucune prédiction disponible.")
        else:
            st.plotly_chart(
                px.pie(
                    distribution,
                    names="sentiment",
                    values="rows",
                    color="sentiment",
                    color_discrete_map={
                        "negative": "#e45756", "neutral": "#7b8794", "positive": "#2ca58d"
                    },
                ),
                use_container_width=True,
            )
    with right:
        topic = safe_query(
            "SELECT article_topic, COUNT(*) AS rows "
            "FROM lahjalens.v_latest_predictions GROUP BY article_topic ORDER BY rows DESC"
        )
        st.subheader("Volumes par catégorie")
        st.plotly_chart(px.bar(topic, x="article_topic", y="rows"), use_container_width=True)

    st.info(
        "Les catégories affichées sont les catégories des articles Hespress. "
        "Elles ne constituent pas une détection automatique de thèmes dans le texte des commentaires."
    )


def page_sentiment() -> None:
    render_header()
    suffix, params = _filters()
    base = (
        "SELECT article_topic, source, predicted_sentiment AS sentiment, COUNT(*) AS rows "
        "FROM lahjalens.v_latest_predictions"
    )
    if suffix:
        suffix += " GROUP BY article_topic, source, predicted_sentiment ORDER BY article_topic, sentiment"
    else:
        suffix = " GROUP BY article_topic, source, predicted_sentiment ORDER BY article_topic, sentiment"
    data = _filtered_query(base, suffix, params)
    if data.empty:
        st.info("Aucune donnée pour ces filtres.")
        return
    left, right = st.columns(2)
    with left:
        st.subheader("Sentiments par catégorie")
        st.plotly_chart(
            px.bar(data, x="article_topic", y="rows", color="sentiment", barmode="group"),
            use_container_width=True,
        )
    with right:
        st.subheader("Tableau catégorie × sentiment")
        pivot = data.pivot_table(index="article_topic", columns="sentiment", values="rows", fill_value=0)
        st.dataframe(pivot, use_container_width=True)


def page_explorer() -> None:
    render_header()
    suffix, params = _filters()
    search = st.text_input("Recherche dans le commentaire", placeholder="texte libre")
    page_size = st.selectbox("Lignes par page", [25, 50, 100], index=0)
    page = st.number_input("Page", min_value=1, value=1, step=1)
    clauses = ["1=1"]
    query_params: list[Any] = []
    if suffix:
        clauses.append(suffix.removeprefix(" WHERE "))
        query_params.extend(params)
    if search.strip():
        clauses.append("comment_text ILIKE %s")
        query_params.append(f"%{search.strip()}%")
    where_sql = " WHERE " + " AND ".join(clauses)
    offset = (int(page) - 1) * int(page_size)
    query_params.extend([int(page_size), offset])
    data = safe_query(
        "SELECT event_id, article_id, article_topic, source, comment_text, "
        "predicted_sentiment AS sentiment, prob_negative, prob_neutral, prob_positive, "
        "model_version, predicted_at "
        "FROM lahjalens.v_latest_predictions" + where_sql +
        " ORDER BY predicted_at DESC, event_id LIMIT %s OFFSET %s",
        query_params,
    )
    show_text = st.checkbox("Afficher le texte des commentaires", value=False)
    if not show_text and "comment_text" in data:
        data = data.drop(columns=["comment_text"])
    st.dataframe(data, use_container_width=True, hide_index=True)
    st.download_button(
        "Exporter la page CSV",
        data.to_csv(index=False).encode("utf-8-sig"),
        file_name="lahjalens_filtered_predictions.csv",
        mime="text/csv",
    )


def page_monitoring() -> None:
    render_header()
    st.subheader("Runs d'inférence")
    runs = safe_query("SELECT * FROM lahjalens.v_model_runs")
    st.dataframe(runs, use_container_width=True, hide_index=True)
    st.caption(
        "Les performances AfriSenti/MAC doivent être consultées dans les rapports d'évaluation vérifiés. "
        "Elles ne sont pas présentées comme des scores Hespress."
    )


def page_quality() -> None:
    render_header()
    st.subheader("Contrôles de qualité des données chargées")
    quality = safe_query("SELECT * FROM lahjalens.v_data_quality ORDER BY check_name")
    st.dataframe(quality, use_container_width=True, hide_index=True)
    runs = safe_query(
        "SELECT run_id, status, completed_at, row_count FROM lahjalens.inference_runs "
        "ORDER BY started_at DESC LIMIT 10"
    )
    st.subheader("Dernières exécutions")
    st.dataframe(runs, use_container_width=True, hide_index=True)


def main() -> None:
    page = st.sidebar.radio(
        "Navigation",
        ["Overview", "Sentiment Analysis", "Data Explorer", "Model Monitoring", "Data Quality"],
    )
    if page == "Overview":
        page_overview()
    elif page == "Sentiment Analysis":
        page_sentiment()
    elif page == "Data Explorer":
        page_explorer()
    elif page == "Model Monitoring":
        page_monitoring()
    else:
        page_quality()


if __name__ == "__main__":
    main()
