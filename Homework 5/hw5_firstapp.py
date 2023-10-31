from flask import Flask, request, Response
from google.cloud import storage
from google.cloud import pubsub_v1
from google.cloud.logging import Client as LoggingClient
import traceback
from waitress import serve
import pymysql

project_id = "ds-561-first-project"
region = 'us-central1'
instance_name = 'serena-mysql-instance'

# Cloud SQL configurations
INSTANCE_CONNECTION_NAME = f"{project_id}:{region}:{instance_name}"
DB_USER = "root"
DB_PASS = "%(\\n9OqkXz\\^k'0["
DB_NAME = "serena-database"
DB_HOST = '34.30.250.225'



# Function to return the database connection object
def getconn():
    conn = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME
    )
    if not conn:
        raise Exception("Could not establish a database connection.")
    return conn

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
def serve_file(bucket, dir_name, dir2_name, file_name):
    
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
        log_failed_request(filename, 501)
        return Response("Not implemented", content_type="text/html", status=501)

    # Retrieve headers 
    client_ip = request.headers.get('X-client-ip', '')
    gender = request.headers.get('X-gender', '')
    age = request.headers.get('X-age')
    income = request.headers.get('X-income')
    time_of_day = request.headers.get('X-time')  
    

    # If the country is banned, publish a message and return a 400 status
    if country in Banned_Countries:
        logger.log_text('Forbidden Country: {}'.format(country), severity='ERROR')
        message = "Forbidden request from {}".format(country)
        log_failed_request(filename, 400)
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
        inserting_into_table(country, client_ip, gender, age, income, False, time_of_day, filename)
        return content
    else:
        logger.log_text("File {} not found. Responding with 404.".format(filename), severity='ERROR')
        log_failed_request(filename, 404)
        return Response("File Not Found", content_type="text/html", status=404)


def inserting_into_table(country, client_ip, gender, age, income, is_banned, time_of_day, requested_file):
    
    log_entry = (f"Inserting data - Country: {country}, IP: {client_ip}, Gender: {gender}, "
                 f"Age: {age}, Income: {income}, Is Banned: {is_banned}, "
                 f"Time: {time_of_day}, File: {requested_file}")
    
    logger.log_text(log_entry, severity='INFO')

    
    conn = getconn()
    with conn.cursor() as cursor:
        insert_stmt = """
            INSERT INTO serena_hw5_Requests
            (country, client_ip, gender, age, income, is_banned, time_of_day, requested_file)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            cursor.execute(
                insert_stmt,
                (country, client_ip, gender, age, income, is_banned, time_of_day, requested_file)
            )
            conn.commit()
            logger.log_text("Data inserted successfully.", severity='INFO')
        except Exception as e:
            log_error = f"Error inserting data: {e}"
            logger.log_text(log_error, severity='ERROR')
    conn.close()

def log_failed_request(requested_file, error_code):
    conn = getconn()
    with conn.cursor() as cursor:
        insert_stmt = """
            INSERT INTO serena_hw5_Failed_Requests
            (time_of_request, requested_file, error_code)
            VALUES (NOW(), %s, %s)
        """
        cursor.execute(insert_stmt, (requested_file, error_code))
        conn.commit()
    conn.close()               

mode = 'dev'
if __name__ == "__main__":
    if mode == 'dev':
        app.run(host='0.0.0.0', port=80, debug=True, use_reloader=False)
    else:
        serve(app, host='0.0.0.0', port=80, threads = 2)
        
    
    
    
    
    
    
    
    

    
    
    
    
    
