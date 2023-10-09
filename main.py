import os
import functions_framework
from flask import request, send_file, Response
from google.cloud import storage
from google.cloud import pubsub_v1
from google.cloud.logging import Client as LoggingClient
import logging as std_logging
import traceback
from urllib.parse import urlparse


# Initialize the Google Cloud Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()
project_id = "ds-561-first-project"
topic_id = "serena_topic"
topic_path = publisher.topic_path(project_id, topic_id)
subscription_id = "serena_topic-sub"

# Initialize the Google Cloud Pub/Sub subscriber client
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)


# Initialize the Google Cloud Logging client
client = LoggingClient()
logger = client.logger('homework3_logger')


Banned_Countries = ["north korea", "iran", "cuba", "myanmar", "iraq", "libya", "sudan", "zimbabwe", "syria"]


def callback(future):
    try:
        message_id = future.result()
        print(f"Message {message_id} published.")
    except Exception as e:
        print(f"Failed to publish message due to {e}")

# Listens for HTTP GET requests and serves files from Google Cloud Storage bucket
# based on the file name in the request
@functions_framework.http
def accept_requests(request):
    
    # Log the entire request 
    logger.log_text(f"Received request: {request}", severity='INFO')
    logger.log_text(f"Received request path: {request.path}", severity='INFO')



    # Requests for other HTTP methods other than GET should return 501 status
    if request.method != 'GET':
        # Log unexpected method
        logger.log_text(f"Received unexpected method {request.method}", severity='ERROR')
        return 'Not implemented', 501
    
    # Retrieve the country from the request
    country = request.args.get('country', '').lower()

    if country in Banned_Countries:
        logger.log_text(f'Forbidden Country: {country.title()}', severity='ERROR')
        message = f"Forbidden request from {country.title()}"
        publish_future = publisher.publish(topic_path, message.encode("utf-8"))
        publish_future.add_done_callback(callback)
        return "FORBIDDEN COUNTRY", 400
    
    

     # Getting file name from request
    #file_name = request.args.get('file_name')
    #logger.log_text(f"Retrieved file_name value: {file_name}", severity='INFO')
    
    # Stripping the path and only keeping the file name
    #base_name =  file_name.split("/")[-1]
    #logger.log_text(f"Stripped file name from request: {base_name}", severity='INFO')

    # Extract the file name from the URL path using request.path
    logger.log_text(f"Raw Path: {request.path}", severity='INFO')
    path_elements = request.path.split("/")
    logger.log_text(f"Split Path: {path_elements}", severity='INFO')
    
    bucket_name = path_elements[1]
    subdirectory = "/".join(path_elements[2:4])  # Joining the elements to form the subdirectory
    file_name = path_elements[-1] 
    

    
    logger.log_text(f"Bucket Name: {bucket_name}", severity='INFO')
    logger.log_text(f"Subdirectory: {subdirectory}", severity='INFO')
    logger.log_text(f"File Name: {file_name}", severity='INFO')
    logger.log_text(f"Full Path: {request.path}", severity='INFO')


    
    try:
        # Initialize connection to Google Cloud Storage bucket
        storage_client = storage.Client()
        #bucket_name = "serena_ds561_hw2_bucket"
        #blob_path = f"Serena_Directory/ds561_hw2_pythonfiles/{file_name}"
        blob_path = f"{subdirectory}/{file_name}"
       
        # Get file from Google Cloud Storage bucket
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_path)
        
       
        # Checking if blob exists
        if not blob.exists():
            logger.log_text("File not found", severity='ERROR')
            return "File not found", 404
       
        # Get blob contents as text and send it as a response
        file_contents = blob.download_as_text()
        
        # Log successful request
        logger.log_text(f"File {file_name} served successfully", severity='INFO')
        
        return Response(file_contents, content_type="text/html"), 200

    except Exception as e:
        # Log the error message and traceback
        error_message = f"An error occurred: {str(e)}"
        logger.log_text(error_message, severity='ERROR')
        traceback_str = traceback.format_exc()
        logger.log_text(traceback_str, severity='ERROR')
        return "An error occurred while processing the request", 500
    

# tells the subscriber to listen to the specified Pub/Sub subscription 
#listening for and processing messages sent by second app via the Pub/Sub topic and subscription
if __name__ == "__main__":
    # Start the Pub/Sub subscriber to listen for messages
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for Pub/Sub messages on {subscription_path}...")

    try:
        # Wait for the subscriber to finish receiving messages
        streaming_pull_future.result()
    except Exception as e:
        streaming_pull_future.cancel()
        print(f"Error receiving Pub/Sub messages: {str(e)}")
    
    
    
