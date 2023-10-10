import os
import functions_framework
from flask import request, send_file, Response
from google.cloud import storage
from google.cloud import pubsub_v1
from google.cloud.logging import Client as LoggingClient
import traceback
import google.cloud.logging

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
        logger.log_text(f"Message {message_id} published.", severity='INFO')  
    except Exception as e:
        logger.log_text(f"Failed to publish message due to {str(e)}", severity='ERROR')  
        logger.log_text(traceback.format_exc(), severity='ERROR') 



# Listens for HTTP GET requests and serves files from Google Cloud Storage bucket
# based on the file name in the request
@functions_framework.http
def accept_requests(request):
    
    # Log the entire request 
    logger.log_text(f"Received request: {request}", severity='INFO')
    logger.log_text(f"Received request path: {request.path}", severity='INFO')
    
    logger.log_text(f"Received headers: {request.headers}", severity='INFO')
    # Extract and log the original IP from the header
    original_ip = request.headers.get('X-client-IP', '').split(',')[0]
    logger.log_text(f"Original IP Address: {original_ip}", severity='INFO')
    
    # retrieve the country directly from the header
    country = request.headers.get('X-country', '').lower().strip()
    logger.log_text(f"Received Country via Header: {country}", severity='INFO')

    # Debug log: Checking against banned countries
    logger.log_text(f"Checking against banned countries: {Banned_Countries}", severity='INFO')


    # Requests for other HTTP methods other than GET should return 501 status
    if request.method != 'GET':
        # Log unexpected method
        logger.log_text(f"Received unexpected method {request.method}. Responding with 501.", severity='ERROR')
        return Response("Not implemented", content_type="text/html", status = 501)


    if country.lower().strip() in Banned_Countries:
        logger.log_text(f'Forbidden Country: {country}', severity='ERROR')
        message = f"Forbidden request from {country}"
        logger.log_text("Attempting to publish message...", severity='INFO')
        publish_future = publisher.publish(topic_path, message.encode("utf-8"))
        publish_future.add_done_callback(callback)
        logger.log_text("Message publishing initiated", severity='INFO') 
        return "FORBIDDEN COUNTRY", 400
    

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
            logger.log_text("Error 404: File not found", severity='ERROR')
            return Response("File not found", content_type="text/html", status = 404)
       
        # Get blob contents as text and send it as a response
        file_contents = blob.download_as_text()
        
        # Log successful request
        logger.log_text(f"File {file_name} served successfully. Responding with 200.", severity='INFO')
        return Response(file_contents, content_type="text/html", status = 200)

    except Exception as e:
        # Log the error message and traceback
        error_message = f"An error occurred: {str(e)}. Responding with 500."
        logger.log_text(error_message, severity='ERROR')
        traceback_str = traceback.format_exc()
        logger.log_text(traceback_str, severity='ERROR')
        return Response("An error occured while processing the request", content_type="text/html", status = 500)
    

