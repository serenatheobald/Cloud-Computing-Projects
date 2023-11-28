from flask import Flask, request, Response
from google.cloud import storage
from google.cloud import pubsub_v1
from google.cloud.logging import Client as LoggingClient
import traceback
from waitress import serve


# Initialize the Google Cloud Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()
project_id = "ds-561-first-project"
topic_id = "serena_topic"
topic_path = publisher.topic_path(project_id, topic_id)




# Initialize the Google Cloud Logging client
client = LoggingClient()
logger = client.logger('homework4_logger')


Banned_Countries = ["north korea", "iran", "cuba", "myanmar", "iraq", "libya", "sudan", "zimbabwe", "syria"]


app = Flask(__name__)

# Initialize connection to GCS bucket
def initialize_storage_client(bucket_name):
    client = storage.Client.create_anonymous_client()
    return client.bucket(bucket_name)



# Extract file content from the bucket
def get_file_content(filename, subdirectory, bucket):
    try:
        print(f"Received filename: {filename}")
        blob_path = f"{subdirectory}/{filename}"
        print(f"Constructed blob path: {blob_path}")

        blob = bucket.get_blob(blob_path)

        if blob:
            return blob.download_as_text()
        else:
            return None
    except Exception as e:
        print(f"Error occurred: {e}")

    

@app.route('/<bucket>/<dir_name>/<dir2_name>/<file_name>', methods=['GET', 'PUT', 'POST', 'DELETE', 'HEAD', 'CONNECT', 'OPTIONS', 'TRACE', 'PATCH'])
def serve_file(bucket,dir_name, dir2_name, file_name):
    
    bucket_name = bucket
    directory = dir_name + "/" + dir2_name
    
    filename = file_name
    bucket = initialize_storage_client(bucket_name)
    
    
    
    # Extract the country directly from the header
    country = request.headers.get('X-country', '').lower().strip()
    print("file name:", filename)
    
    # If the request method is not GET, log and return 501 status
    if request.method != 'GET':
        logger.log_text("Received unexpected method {}. Responding with 501.".format(request.method), severity='ERROR')
        return Response("Not implemented", content_type="text/html", status=501)

    # If the country is banned, publish a message and return a 400 status
    if country in Banned_Countries:
        logger.log_text('Forbidden Country: {}'.format(country), severity='ERROR')
        message = "Forbidden request from {}".format(country)
        try:
            publish_future = publisher.publish(topic_path, message.encode("utf-8"))
            publish_future.result()
            logger.log_text("Message publishing initiated", severity='INFO') 
            logger.log_text("Message with ID {} published successfully.".format(publish_future.result()), severity='INFO')
        except Exception as e:
            logger.log_text("Error publishing message: {}".format(e), severity='ERROR')
            logger.log_text(traceback.format_exc(), severity='ERROR')
        return "FORBIDDEN COUNTRY", 400
    
    # Check if file exists and return it, else log error and return 404
    content = get_file_content(filename, directory, bucket)

    if content:
        logger.log_text("Served file {} successfully with 200 OK.".format(filename), severity='INFO')
        return content
    else:
        logger.log_text("File {} not found. Responding with 404.".format(filename), severity='ERROR')
        return Response("File Not Found", content_type="text/html", status=404)


mode = 'dev'
if __name__ == "__main__":
    if mode == 'dev':
        app.run(host='0.0.0.0', port=80, debug=True, use_reloader=False)
    else:
        serve(app, host='0.0.0.0', port=80, threads = 2)
        
    
    
    
    
    
    
    
    
    
