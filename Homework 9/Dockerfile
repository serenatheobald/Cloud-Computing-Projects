# Use an official Python runtime as a parent image
FROM python:3.9-slim-buster

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the current directory contents into the container at /usr/src/app
COPY . .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 80 available to the world outside this container
EXPOSE 80

# Set an environment variable to enable Application Default Credentials (ADC)
ENV GOOGLE_CLOUD_PROJECT ds-561-first-project

# Run app.py when the container launches
CMD ["python", "hw4_firstapp.py"]
