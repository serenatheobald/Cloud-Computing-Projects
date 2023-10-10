from google.cloud import pubsub_v1
from google.cloud.logging import Client as LoggingClient


#create GC Pub/Sub subscriber that listens to messages published by the first app 
#and handles messages related to banned countries


# Create a logger instance
client = LoggingClient()
logger = client.logger('homework3_logger')


project_id = "ds-561-first-project"
subscription_id = "serena_topic-sub"

def callback(message):
    try:
        message_data = message.data.decode("utf-8")
        if "Forbidden request" in message_data:
            country = message_data.split("from ")[1]
            logger.log_text(f"Received forbidden request from {country}", severity='INFO')
        message.ack()
    except Exception as e:
        logger.log_text(f"Error processing message: {str(e)}", severity='ERROR')

def subscribe_and_listen():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    
    logger.log_text(f"Listening for messages on {subscription_path}...", severity='INFO')
    
    try:
        streaming_pull_future.result()
    except Exception as e:
        streaming_pull_future.cancel()
        logger.log_text(f"Error receiving messages: {str(e)}", severity='ERROR')



if __name__ == "__main__":
    subscribe_and_listen()
