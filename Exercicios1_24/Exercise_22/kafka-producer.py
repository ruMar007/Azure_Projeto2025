from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import TopicAlreadyExistsError
import time
import random
import json

def create_topic():
    """
    Create a Kafka topic if it does not already exist.
    
    This function uses the KafkaAdminClient to check if a topic exists, and
    if it does not, creates it with 2 partitions and a replication factor of 1.
    """
    admin_client = KafkaAdminClient(
        bootstrap_servers='localhost:9092',
        client_id='test-admin'
    )

    topic_list = [NewTopic(name="test-topic", num_partitions=2, replication_factor=1)]
    
    # Check if topic exists before creating
    existing_topics = admin_client.list_topics()
    if "test-topic" in existing_topics:
        print("Topic 'test-topic' already exists.")
    else:
        try:
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print("Topic 'test-topic' created.")
        except TopicAlreadyExistsError:
            print("Topic 'test-topic' already exists (caught by exception).")
        except Exception as e:
            print(f"An error occurred: {e}")

def sendMessages():
    # Create the topic before producing messages
    create_topic()
    
    # Create the Kafka producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    # Generate 100 events
    for _ in range(10000):
        event = createRandomEvent()
        print(event)
        # Send event to topic
        producer.send(topic='test-topic', value=event)
    producer.flush()

# Generate the JSON message
def createRandomEvent():
    event = {}
    event['ts'] = int(time.time() * 1000) # timestamp in milliseconds
    event['user_id'] = random.randint(1,101) # random user_id between 1 and 100
    event['product_name'] = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for x in range(5)) # random 5-letter product name
    event['rank'] = random.randint(0,4) # random rank from 0 to 4 
    return event

sendMessages()





