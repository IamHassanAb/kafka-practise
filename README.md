# Live Poll Dashboard — Kafka + Streamlit

A small real-time streaming demo: synthetic poll responses are published to Kafka, and a
Streamlit dashboard consumes them live, updating KPIs, bar charts, and a raw data view as
messages arrive.

## How it works

- `poll_topic_create.py` creates the Kafka topic.
- `poll_response_api.py` generates simulated poll responses (conference-feedback style
  questions) — there's no real survey behind this, it's synthetic data standing in for a
  live source.
- `KAFKA_STREAMLIT_PRODUCER_HASSANABBAS.py` publishes those responses to Kafka.
- `STREAMLIT_CONSUMER_HASSANABBAS.py` runs the dashboard: a background thread listens to
  Kafka continuously, while Streamlit's main thread re-renders the UI every couple of
  seconds. The two threads share state, so a `threading.Lock` guards every read/write to
  prevent the UI from reading mid-update. Details in [`optimized-kafka.md`](./optimized-kafka.md).

## Running it

```bash
docker-compose up -d        # starts Kafka
pip install -r requirements.txt
python poll_topic_create.py
python KAFKA_STREAMLIT_PRODUCER_HASSANABBAS.py
streamlit run STREAMLIT_CONSUMER_HASSANABBAS.py
