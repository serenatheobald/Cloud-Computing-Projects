import os
import functions_framework
from flask import request, send_file, Response
from google.cloud import storage
from google.cloud import pubsub_v1
from google.cloud.logging import Client as LoggingClient
import logging as std_logging
import traceback

# Initialize the Google Cloud Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()
project_id = "ds-561-first-project"
topic_id = "serena_topic"
topic_path = publisher.topic_path(project_id, topic_id)

# Initialize the Google Cloud Logging client
client = LoggingClient()
logger = client.logger('homework3_logger')

# Listens for HTTP GET requests and serves files from Google Cloud Storage bucket
# based on the file name in the request
@functions_framework.http
def accept_requests(request):
    
    # Requests for other HTTP methods other than GET should return 501 status
    if request.method != 'GET':
        # Log unexpected method
        logger.log_text(f"Received unexpected method {request.method}", severity='ERROR')
        return 'Not implemented', 501

    # Getting file name from request
    file_name = request.args.get('file_name')
    
    # Check if file name is provided or missing
    if not file_name:
        logger.log_text("File name is missing", severity='ERROR')
        return "File name is missing", 400
    
    try:
        # Initialize connection to Google Cloud Storage bucket
        storage_client = storage.Client()
        bucket_name = "serena_ds561_hw2_bucket"
        blob_path = f"Serena_Directory/ds561_hw2_pythonfiles/{file_name}"
       
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