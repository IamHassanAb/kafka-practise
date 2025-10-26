import streamlit as st
from poll_utility import poll_response_producer
from kafka import KafkaProducer
import json
import time

def produce_order():
    response = poll_response_producer()
    topic = 'poll-responses-topic'
    p = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    p.send(topic, response)
    p.flush()
    return response

def main():
    st.title("Poll Producer Dashboard")
    
    send_once = st.button("Send Poll Once")
    send_continuous = st.button("Start Continuous Sending")
    stop_continuous = st.button("Stop Continuous Sending")

    # Use session state to control continuous sending
    if "sending" not in st.session_state:
        st.session_state.sending = False

    if send_once:
        produce_order()
        st.success("One poll response generated and sent to Kafka topic.")

    if send_continuous:
        st.session_state.sending = True

    if stop_continuous:
        st.session_state.sending = False

    # Continuous sending loop
    if st.session_state.sending:
        st.warning("Continuous sending is ON. Click 'Stop Continuous Sending' to stop.")
        for _ in range(5):  # Limit to 5 messages per rerun to avoid infinite loop in Streamlit
            if not st.session_state.sending:
                break
            print(produce_order())
            st.info("Poll message sent.")

            time.sleep(1)
        st.rerun()

if __name__ == "__main__":
    main()
