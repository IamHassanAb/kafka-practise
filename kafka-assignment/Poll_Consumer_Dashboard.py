import streamlit as st
from kafka import KafkaConsumer
from poll_utility import get_survey_questions
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
    st.title("📊 Live Poll Dashboard")
    st.divider()

    survey_questions = get_survey_questions()

    # Start background Kafka listener thread once
    if "listener_started" not in st.session_state:
        thread = threading.Thread(target=kafka_listener, daemon=True)
        thread.start()
        st.session_state.listener_started = True
        st.toast("✅ Kafka listener started in background.")

    placeholder = st.empty()
    st_all_orders = st.empty()
    st_all_records = st.empty()

    # Streamlit loop for live updates
    while True:
        st.toast('Fetching')
        with lock:
            df = pd.DataFrame(records)


        total_response = len(df)
        placeholder.metric("Total Responses", total_response)
        # st.divider()

        if not df.empty:
            
            with st_all_records.expander("Click to Expand JSON Output"):
                st_all_orders.dataframe(df)
                st.write(records)
        #     # Display charts for each question
            questions = [ques["Q"] for ques in survey_questions]
            st.columns(len(questions))
            # for question in survey_questions:
            #     ques = question["Q"]
                
        #         options = question["A"]
        #         fig = px.bar(
        #             df[[ques]].value_counts().reset_index(),
        #             x="index",
        #             y=ques,
        #             labels={'index':'Conference Rating',ques:'Count'},
        #             title=f"Responses for: {ques}",
        #         )
        #         st.plotly_chart(fig, use_container_width=True)



        time.sleep(2)  # refresh interval


if __name__ == "__main__":
    main()
