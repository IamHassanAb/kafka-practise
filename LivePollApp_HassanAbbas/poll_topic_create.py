from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id='python-admin'
)

# Create a topic
topic_name = "poll-responses-topic"
topic = NewTopic(name=topic_name, num_partitions=3, replication_factor=1)
admin_client.create_topics(new_topics=[topic], validate_only=False)
print(f"Topic '{topic_name}' created!")

# List topics
print("Topics now:", admin_client.list_topics())

# Describe topic
metadata = admin_client.describe_topics([topic_name])
for t in metadata:
    print(f"Topic: {t.topic}")
    for p in t.partitions:
        print(f"  Partition {p.partition}, Leader: {p.leader}, Replicas: {p.replicas}")

# # Delete topic
# admin_client.delete_topics([topic_name])
# print(f"Topic '{topic_name}' deleted!")
