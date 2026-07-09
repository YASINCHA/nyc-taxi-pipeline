# ==============================================================================
# chatbot_nyc_taxi.py — Chatbot RAG sur les données NYC Taxi
#
# Utilise LangChain + Ollama (llama3.2) + PostgreSQL
# Interface web via Streamlit
#
# Lancement : streamlit run chatbot_nyc_taxi.py
# ==============================================================================

import streamlit as st
import psycopg2
import pandas as pd
from langchain_ollama import OllamaLLM
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from sqlalchemy import create_engine


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────


OLLAMA_MODEL = "llama3.2"

# ─────────────────────────────────────────────────────────────────────────────
# Connexion PostgreSQL
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_db_connection():
    return psycopg2.connect(**PG_CONFIG)


def run_query(sql: str) -> pd.DataFrame:
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{PG_CONFIG['user']}:{PG_CONFIG['password']}"
            f"@{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['dbname']}"
        )
        with engine.connect() as conn:
            return pd.read_sql(sql, conn)
    except Exception as e:
        return pd.DataFrame({"Erreur": [str(e)]})


# ─────────────────────────────────────────────────────────────────────────────
# Contexte des données pour le LLM
# ─────────────────────────────────────────────────────────────────────────────
def get_data_context() -> str:
    """Récupère un résumé des données pour enrichir le contexte du LLM."""
    summary_sql = """
        SELECT
            COUNT(*)                              AS total_courses,
            ROUND(SUM(total_amount)::numeric, 2)  AS revenu_total,
            ROUND(AVG(total_amount)::numeric, 2)  AS revenu_moyen,
            ROUND(AVG(trip_distance)::numeric, 2) AS distance_moyenne,
            MIN(tpep_pickup_datetime)::date        AS date_debut,
            MAX(tpep_pickup_datetime)::date        AS date_fin
        FROM final_taxi_data
    """
    weekly_sql = """
        SELECT day_of_week, avg_daily_revenue, avg_daily_trips
        FROM weekly_performance
        ORDER BY dow_number
    """
    top_zones_sql = """
        SELECT pickup_location_id, trip_count, total_revenue
        FROM top_pickup_zones
        LIMIT 5
    """
    streaming_sql = """
        SELECT COUNT(*) AS nb_fenetres,
               ROUND(SUM(total_revenue)::numeric, 2) AS revenu_streaming
        FROM streaming_metrics
    """

    summary = run_query(summary_sql)
    weekly  = run_query(weekly_sql)
    zones   = run_query(top_zones_sql)
    stream  = run_query(streaming_sql)

    context = f"""
DONNÉES NYC TAXI DISPONIBLES :

Résumé global :
{summary.to_string(index=False)}

Performance par jour de la semaine :
{weekly.to_string(index=False)}

Top 5 zones de pickup (par revenu) :
{zones.to_string(index=False)}

Données streaming temps réel :
{stream.to_string(index=False)}
"""
    return context


# ─────────────────────────────────────────────────────────────────────────────
# LangChain — Prompt + LLM
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_llm_chain():
    llm = OllamaLLM(model=OLLAMA_MODEL, temperature=0.1, base_url="http://host.docker.internal:11434")

    prompt = PromptTemplate(
        input_variables=["context", "question"],
        template="""Tu es un assistant expert en analyse de données NYC Taxi.
Tu as accès aux données réelles suivantes :

{context}

Réponds à la question suivante de façon claire, précise et en français.
Si la question concerne les données, cite les chiffres exacts.
Si tu ne sais pas, dis-le honnêtement.

Question : {question}

Réponse :"""
    )

    return LLMChain(llm=llm, prompt=prompt)


# ─────────────────────────────────────────────────────────────────────────────
# Questions suggérées
# ─────────────────────────────────────────────────────────────────────────────
SUGGESTED_QUESTIONS = [
    "Quel jour de la semaine génère le plus de revenus ?",
    "Combien de courses ont été effectuées au total ?",
    "Quelle est la zone de pickup la plus rentable ?",
    "Quel est le revenu moyen par course ?",
    "Quelle est la distance moyenne des courses ?",
    "Quelles sont les données de streaming temps réel ?",
    "Quel est le revenu total généré en janvier 2026 ?",
    "Compare les revenus du lundi vs vendredi",
]


# ─────────────────────────────────────────────────────────────────────────────
# Interface Streamlit
# ─────────────────────────────────────────────────────────────────────────────
def main():
    st.set_page_config(
        page_title="NYC Taxi — Chatbot IA",
        page_icon="🚕",
        layout="wide",
    )

    # Header
    st.title("🚕 NYC Taxi — Chatbot IA")
    st.markdown(
        "Posez des questions en langage naturel sur les données NYC Taxi. "
        "Propulsé par **LangChain + Ollama (llama3.2) + PostgreSQL**."
    )
    st.divider()

    # Sidebar — métriques clés
    with st.sidebar:
        st.header("📊 Métriques clés")
        try:
            df = run_query("""
                SELECT
                    COUNT(*) AS courses,
                    ROUND(SUM(total_amount)::numeric, 0) AS revenu
                FROM final_taxi_data
            """)
            st.metric("Total courses", f"{df['courses'][0]:,}")
            st.metric("Revenu total", f"${df['revenu'][0]:,}")
        except:
            st.warning("Connexion PostgreSQL en cours...")

        st.divider()
        st.header("💡 Questions suggérées")
        for q in SUGGESTED_QUESTIONS[:4]:
            if st.button(q, use_container_width=True):
                st.session_state["question"] = q

    # Historique des messages
    if "messages" not in st.session_state:
        st.session_state.messages = []

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Input question
    question = st.chat_input("Posez votre question sur les données NYC Taxi...")

    # Récupère la question depuis le sidebar si cliquée
    if "question" in st.session_state:
        question = st.session_state.pop("question")

    if question:
        # Affiche la question
        st.session_state.messages.append({"role": "user", "content": question})
        with st.chat_message("user"):
            st.markdown(question)

        # Génère la réponse
        with st.chat_message("assistant"):
            with st.spinner("Analyse des données en cours..."):
                try:
                    context = get_data_context()
                    chain   = get_llm_chain()
                    response = chain.invoke({
                        "context":  context,
                        "question": question,
                    })
                    answer = response["text"] if isinstance(response, dict) else str(response)
                except Exception as e:
                    answer = f"❌ Erreur : {e}"

            st.markdown(answer)
            st.session_state.messages.append({
                "role": "assistant",
                "content": answer,
            })

    # Footer
    st.divider()
    st.caption(
        "Stack : LangChain · Ollama llama3.2 · PostgreSQL · Spark · dbt · Airflow · Kafka"
    )


if __name__ == "__main__":
    main()
