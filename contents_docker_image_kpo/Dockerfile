# Dockerfile for tjanif/docker-sandbox:snowpark-example
FROM python:3.8.14

WORKDIR /

# installing packages
COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip
RUN pip install -r requirements.txt

# creating the file to write XComs to
RUN mkdir -p airflow/xcom
RUN echo "" > airflow/xcom/return.json

# copy the script into the Docker container
COPY script.py ./

# run the script
CMD ["python", "./script.py"]