import streamlit as st
from kafka import KafkaConsumer
from poll_utility import get_survey_questions, footer_html
import threading
import json
import pandas as pd
import time
import plotly.express as px

# Shared state between threads
records = []
answer_ids = set()
lock = threading.Lock()

def kafka_listener():
    """Background thread that listens to Kafka messages and updates shared data."""
    consumer = KafkaConsumer(
        "poll-responses-topic",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        enable_auto_commit=True,
        session_timeout_ms=45000,
    )

    for msg in consumer:
        poll_data = json.loads(msg.value)
        row = {"answer_id": poll_data["answer_id"]}
        for qa in poll_data["answer_array"]:
            row.update(qa)

        with lock:
            # Ignore duplicate answer_id
            if row["answer_id"] in answer_ids:
                continue
            answer_ids.add(row["answer_id"])
            records.append(row)


def main():
    st.set_page_config(page_title="Live Poll Dashboard", page_icon="📊", layout="centered")

    st.title("📊 Live Poll Dashboard")
    st.caption("Real-time visualization of responses from Kafka.")
    st.divider()

    survey_questions = get_survey_questions()

    # Start background Kafka listener thread once
    if "listener_started" not in st.session_state:
        thread = threading.Thread(target=kafka_listener, daemon=True)
        thread.start()
        st.session_state.listener_started = True
        st.toast("✅ Kafka listener started in background.")

    
    placeholder = st.empty()
    charts = {q["Q"]: st.empty() for q in survey_questions}
    st_all_orders = st.empty()
    st_all_records = st.empty()
    st.markdown(footer_html, unsafe_allow_html=True)
    
    

    # Streamlit loop for live updates
    while True:
        with lock:
            df = pd.DataFrame(records)

        if not df.empty:
            total_response = df["answer_id"].nunique()
            placeholder.metric("Total Responses", total_response)
            for ques in survey_questions:
                qtext = ques["Q"]
                chart_container = charts[qtext]
                with chart_container:
                    # col1=barchart, col2=piechart
                    col1, col2 = st.columns([3, 1])
                    col1.subheader(f"Responses for: {qtext}")
                    value_counts = df[[qtext]].value_counts().reset_index()
                    value_counts.columns = ["answer","count"]
                    fig1 = px.bar(
                        value_counts,
                        x="answer",
                        y="count"
                    )
                    col1.plotly_chart(fig1, use_container_width=True)
                    fig2 = px.pie(value_counts,values='count',names='answer')
                    col2.subheader(f"Proportions: {qtext}")
                    col2.plotly_chart(fig2, use_container_width=True)
            
            st_all_orders.dataframe(df,use_container_width=True)
        



        time.sleep(0.5)  # refresh intervals
    


if __name__ == "__main__":
    main()
    
