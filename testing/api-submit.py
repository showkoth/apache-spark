import requests
import json

# Define the Spark master URL
spark_master_url = "http://localhost:6066/v1/submissions/create"

# Define the job payload
payload = {
    "action": "CreateSubmissionRequest",
    "appArgs": [],
    "appResource": "/opt/spark/work-dir/test-script.py",
    "clientSparkVersion": "3.5.4",
    "mainClass": "",
    "environmentVariables": {
        "PYSPARK_PYTHON": "/usr/bin/python3",
    },
    "sparkProperties": {
        "spark.app.name": "PySparkApp",
        "spark.master": "spark://<spark-master>:7077",
    }
}

# Make the POST request to submit the job
response = requests.post(spark_master_url, data=json.dumps(payload), headers={'Content-Type': 'application/json'})

# Check the response
if response.status_code == 200:
    print(f"Job successfully submitted: {response.json()}")
else:
    print(f"Error submitting job: {response.text}")
