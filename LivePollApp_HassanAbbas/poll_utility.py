from poll_response_api import PollResponseAPI
import json

footer_html = """
        <style>
        .footer {
            position: fixed;
            left: 0;
            bottom: 0;
            width: 100%;
            background-color: rgba(240, 240, 240, 0.95);
            color: #333333;
            text-align: center;
            font-size: 14px;
            padding: 10px 0;
            border-top: 1px solid #e0e0e0;
            z-index: 100;
        }
        .footer a {
            color: #FF4B4B;
            text-decoration: none;
            font-weight: 500;
        }
        </style>

        <div class="footer">
            📊 <b>Live Poll Dashboard</b> — Real-time analytics powered by Kafka & Streamlit <br>
            Built by Hassan Abbas | © 2025
        </div>
        """

# def produce_order(order_details):
#     # p = Producer(read_ccloud_config("client.properties"))
#     p = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=lambda v: json.dumps(v).encode('utf-8'))
#     p.send('orders-topic', order_details)
#     p.flush()

def get_survey_questions():
    return PollResponseAPI().survey_questions

print(PollResponseAPI().survey_questions)

# print(json.dumps(PollResponseAPI().survey_questions, indent=4))
def poll_response_producer():
    # questions = get_survey_questions()
    return PollResponseAPI().poll_response_api()
# print(PollResponseAPI().poll_response_api())