from google.cloud import pubsub_v1
from google.cloud.logging import Client as LoggingClient


#create GC Pub/Sub subscriber that listens to messages published by the first app 
#and handles messages related to banned countries


# Create a logger instance
client = LoggingClient()
logger = client.logger('second app')


project_id = "ds-561-first-project"
subscription_id = "serena_topic-sub"

def callback(message):
    try:
        # Process the message content
        message_data = message.data.decode("utf-8")
        
        # Check if the message indicates a forbidden request
        if "Forbidden request" in message_data:
            country = message_data.split("from ")[1]
            #print(f"Received forbidden request from {country}")
            logger.info(f"Received forbidden request from {country}")

        # acknowledge the message
        message.ack()
    except Exception as e:
        #print(f"Error processing message: {str(e)}")
        logger.error(f"Error processing message: {str(e)}")

def receive_messages():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...")

    try:
        # Wait for the subscriber to finish receiving messages
        streaming_pull_future.result()
    except Exception as e:
        streaming_pull_future.cancel()
        print(f"Error receiving messages: {str(e)}")

if __name__ == "__main__":
    receive_messages()