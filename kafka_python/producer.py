import time
import json 
from kafka import KafkaProducer

def json_Serializer(data):
    return json.dumps(data).encode('utf-8')


producer=KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=json_Serializer
)


if __name__=="__main__":
    messageCount=0
    ##the loop logic may change according to the faker data 
    
    while True:
        #in the faker 
        message = {'message_id':messageCount,'content':"hello kafka"}
        producer.send("test_topics", message)
        print("hello")
        time.sleep(3)
        messageCount += 1
