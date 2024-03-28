
In BU's DS 561 cloud computing class, I focused on integrating Python code with Google Cloud across nine projects (#2-#10). The course emphasizes addressing organizational technology needs through public cloud providers, which offer computing, storage, and networking solutions. Unlike traditional infrastructure setup, where companies had to invest significant time and money, cloud services allow renting resources instantly. Through hands-on experience, I explored various compute, storage, and networking options, learning to solve real-world problems using Python and Google Cloud Platform.

#Project 2:
In this project, I am analyzing the link structure of a collection of web pages, hosted on a Google Cloud Storage bucket. The primary data source for this project is a collection of 10,000 web pages hosted on a Google Cloud Storage bucket. Using the Google Cloud Console, a connection is established to the aforementioned bucket. An anonymous client in the Python code is used for connecting to the bucket, which avoids the need for service account credentials. My Python client creates a graph of these web pages, calculates several properties related to incoming and outgoing links, and computes PageRank scores for each web page.

#Project 3:
In this project, I aimed to create an application that listens for HTTP GET requests, accesses files from a Google Cloud Storage bucket, serves them based on the filename in the request, and logs erroneous requests. If a request originates from a banned country, a message gets published to a Google Cloud Pub/Sub topic. My second application serves as a subscriber to this topic, printing out instances where forbidden requests were made.

#Project 4:
In this project, I focused on setting up various Virtual Machine (VM) instances, running web applications, and performing stress tests. Instead of using cloud functions, I used VMs. This project was a continuation of project 3.

#Project 5:
Project 5 focuses on modifying my web server from Project #4 to extract the information from the incoming requests, create a schema that adheres to 2nd normal form and can hold information (country, client ip, gender, age, income, is_banned, time of day, and requested file)  about the requests that our web server is receiving, and inserting it into our SQL database. Requests that fail (return codes other than 200) should be logged into a separate table that also obeys 2nd normal form but only contains the time of request, the requested file, and the error code.

#Project 6:
Project 6 implements 2 models that retrieve data from the SQL database I created and populated in Project #5. One model uses client IP to predict the country from which the request originated. The second model should use any of the available fields to predict income. In my case, I used gender, age, and client IP as my predictor variables.

#Project 7:
Project 7 uses Apache Beam and Cloud Data Flow to do some data processing on the files in your cloud bucket. In particular, I am finding and printing out the top 5 files with the most incoming links, and top 5 files with the most outgoing links.

#Project 8:
Project 8 is about creating two VMs running our web server, ensuring they are in different zones (in the same region), and placing them behind a load balancer. I modified my web server code to return the name of the zone the server is running in as a response header. I then modified the given helper client to extract and print the new response header. Lastly, I calculated failover timing measurements and the ratio of requests served by each backend VM.

#Project 9:
Project 9 is about porting our web server from Homework 4 to our container image under Google Kubernetes Engine. I achieved this by creating a Docker container for the server and deploying it to a GKE cluster. I then demonstrate the functionality of our app by using the provided http helper client to request a few hundred of our bucket files and use a series of curl commands to demonstrate one request for each response case. Lastly, I used my previously created second app to track requests from banned countries.

#Project 10:
In this project, I used Google Deployment Manager (GDM) to deploy components from Project 5, including Service Accounts, GCS buckets, VM web server, Cloud SQL database, PubSub, and a VM with a PubSub listener. After deploying, I tested the functionality using an HTTP client and curl commands to demonstrate various HTTP response codes. Lastly, I verified verify database contents and ensure proper cleanup of resources after completion.

