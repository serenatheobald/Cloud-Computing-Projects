from google.cloud import pubsub_v1
from google.cloud.logging import Client as LoggingClient


#create GC Pub/Sub subscriber that listens to messages published by the first app 
#and handles messages related to banned countries


# Create a logger instance
client = LoggingClient()
logger = client.logger('homework3_logger')
subscriber = pubsub_v1.SubscriberClient()

project_id = "ds-561-first-project"
subscription_id = "serena_topic-sub"
subscription_path = subscriber.subscription_path(project_id, subscription_id)


def callback(message):
    try:
        print(f"Received {message}.")
        message.ack()
    except Exception as e:
        print(f"Error processing message: {str(e)}")


with subscriber:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
        
    print(f"Listening for messages on {subscription_path}...")
            
    try:
        streaming_pull_future.result()
    except Exception as e:
        print(f"Error receiving messages: {str(e)}")
        streaming_pull_future.cancel()
        
        

