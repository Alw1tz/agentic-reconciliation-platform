"""
RAG chat over "Learning Spark, 2nd Edition", backed by the BOOK_RAG_EXAMPLE
Cortex Search Service (created via SYSTEM$CREATE_SEARCH_SERVICE_FROM_STAGE
directly from the staged PDF — voyage-multimodal-3 embeddings, 399 chunks).

Streamlit-in-Snowflake app: multi-turn chat, not just single Q&A. Deploy via
Snowsight (Streamlit > Create, paste this file) or `snow streamlit deploy`.
"""

import json

import streamlit as st
from snowflake.snowpark.context import get_active_session

DATABASE = "RAG_SANDBOX"
SCHEMA = "DEMO"
SEARCH_SERVICE = "BOOK_RAG_EXAMPLE"
FULL_SERVICE_NAME = f"{DATABASE}.{SCHEMA}.{SEARCH_SERVICE}"
SEARCH_COLUMNS = ["TEXT", "RELATIVE_PATH"]
# Check which COMPLETE models are enabled for your account/region before
# relying on this — availability varies.
MODEL = "llama3.1-70b"
TOP_K = 5
HISTORY_TURNS = 6  # how many prior messages to feed back into the prompt

session = get_active_session()


def search_book(query: str, limit: int = TOP_K) -> list[dict]:
    """Query the Cortex Search Service via SQL — no snowflake.core package
    needed, just the Snowpark session every Streamlit-in-Snowflake app
    already has."""
    payload = json.dumps({"query": query, "columns": SEARCH_COLUMNS, "limit": limit})
    row = session.sql(
        "SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(?, ?) AS result",
        params=[FULL_SERVICE_NAME, payload],
    ).collect()[0]
    return json.loads(row["RESULT"])["results"]

st.title("Learning Spark — RAG Chat")
st.caption(
    "Multi-turn chat grounded in Learning Spark, 2nd Edition via Cortex "
    "Search. Retrieval and generation both run inside Snowflake."
)

if "messages" not in st.session_state:
    st.session_state.messages = []

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])

if question := st.chat_input("Ask about Learning Spark..."):
    st.session_state.messages.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.write(question)

    results = search_book(question)

    context = "\n\n---\n\n".join(r["TEXT"] for r in results)
    history = "\n".join(
        f"{m['role']}: {m['content']}"
        for m in st.session_state.messages[-HISTORY_TURNS:]
    )

    prompt = f"""You are a chat assistant answering questions about the book
"Learning Spark, 2nd Edition". Use ONLY the context below — if it doesn't
contain the answer, say so rather than guessing.

Conversation so far:
{history}

Context:
{context}

Question: {question}
Answer:"""

    answer = session.sql(
        "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?) AS answer",
        params=[MODEL, prompt],
    ).collect()[0]["ANSWER"]

    st.session_state.messages.append({"role": "assistant", "content": answer})
    with st.chat_message("assistant"):
        st.write(answer)

    with st.expander(f"Retrieved chunks ({len(results)})"):
        for r in results:
            st.markdown(f"**{r['RELATIVE_PATH']}**")
            st.write(r["TEXT"])
            st.divider()
