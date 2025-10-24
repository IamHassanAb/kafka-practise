from poll_response_api import PollResponseAPI
import json



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