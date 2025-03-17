import os
import paho.mqtt.client as mqtt
import pandas as pd
import time
import json
import ssl
import boto3  # AWS SDK for Python
from io import BytesIO
import base64
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# AWS IoT details
ENDPOINT = "a1amrksqm7ehvm-ats.iot.eu-north-1.amazonaws.com"  # Replace with your IoT Core endpoint
THING_NAME = "basicPubSub"  # Y nour IoT Thing name
TOPIC = "sdk/test/python"  # Topic to publish data
CERTIFICATE_PATH = "../foodsurakhsha247.cert.pem"
PRIVATE_KEY_PATH = "../foodsurakhsha247.private.key"
ROOT_CA_PATH = "../root-CA.crt"

# AWS DynamoDB setup
DYNAMODB_TABLE_NAME = "TestResults"  # Replace with your table name
dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_DB"),
    aws_secret_access_key=os.getenv("AWS_SECRET_KEY_DB"),
    region_name=os.getenv("AWS_REGION_DB"),
)

dynamo_table = dynamodb.Table(DYNAMODB_TABLE_NAME)


# MQTT Client setup
client = mqtt.Client(client_id=THING_NAME)
client.tls_set(ca_certs=ROOT_CA_PATH, certfile=CERTIFICATE_PATH, keyfile=PRIVATE_KEY_PATH, tls_version=ssl.PROTOCOL_TLSv1_2)
print("TLS setup completed")

# Connect to AWS IoT Core
client.connect(ENDPOINT, 8883, 60)

# Function to encode an image
def encode_image(image_path):
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode("utf-8")

# Function to clear all data in DynamoDB
def clear_dynamodb():
    response = dynamo_table.scan()
    with dynamo_table.batch_writer() as batch:
        for item in response.get('Items', []):
            batch.delete_item(Key={
                'id_number': item['id_number']  # Replace 'PrimaryKey' with your table's actual primary key
            })
    print("Cleared all data from DynamoDB.")

# Function to read data from Excel and send to AWS IoT
def publish_data():
    df = pd.read_excel("../food_data.xlsx")

    # Ensure column names are correctly formatted (strip spaces)
    df.columns = df.columns.str.strip()

    for _, row in df.iterrows():
        payload = {
            "hotel_name": row["Hotel Name"],
            "id_number": str(row["ID Number"]),  # Convert ID to string if needed
            "sanitation": str(row["Sanitation"]),
            "image_name": row["ImageName"],
            "image_data": encode_image(os.path.join("../images", os.path.basename(row["Image"]))),  # Use relative paths
        }

        # Publish to AWS IoT Core
        client.publish(TOPIC, json.dumps(payload))
        print(f"Published: {payload['id_number']}")
        time.sleep(5)  # Adjust interval as needed

    # Wait for 300 seconds after sending all rows
    print("All rows processed. Waiting for 300 seconds before clearing DynamoDB.")
    time.sleep(300)

    # Clear DynamoDB data
    clear_dynamodb()

try:
    publish_data()
except KeyboardInterrupt:
    print("Stopped sending data.")
    client.disconnect()