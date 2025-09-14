from kafka.admin import KafkaAdminClient, NewTopic 
from kafka.errors import TopicAlreadyExistsError,NoBrokersAvailable

BOOTSTRAP_SERVERS='localhost:9092'
#creates topic 
TOPIC_NAME='user-login-events'
PARTITIONS=3
REPLICATION_FACTOR=1

if __name__=="__main__":
    print("attempting to connect to kafka admin client ...")
    try:
        admin_client=KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            client_id='my_kafka_app'
        )
        print("successfully connected to kafka")

        topic=NewTopic(
            name=TOPIC_NAME,
            num_partitions=PARTITIONS,
            replication_factor=REPLICATION_FACTOR
        )
        print(f"Attempting to create topic '{TOPIC_NAME}'...")
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{TOPIC_NAME}' created successfully!")

    except NoBrokersAvailable:
        print(f"error could not connect to any kafka brokers at {BOOTSTRAP_SERVERS} ")  
        print("Please ensure Kafka is running and accessible.")
    except TopicAlreadyExistsError:
        print(f"Topic '{TOPIC_NAME}' already exists. No action taken.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}") 

    finally:
        if 'admin_client' in locals() and admin_client:
            admin_client.close()
            print("admin client closed connection ...")     