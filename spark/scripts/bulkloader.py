import boto3
import requests
import json
import os, sys

# Define your AWS configuration
aws_access_key_id = sys.argv[1]
aws_secret_access_key = sys.argv[2]
region_name = "eu-west-3"

# Define the Neptune configuration
neptune_endpoint = "growsmart-neptune.cluster-custom-cgogeml0cuty.eu-west-3.neptune.amazonaws.com"
neptune_port = "8182"

# Define S3 configuration
bucket_name = "growsmartformattedzone"
prefix = sys.argv[3]

# Define the IAM role ARN for the Neptune bulk load
iam_role_arn = sys.argv[4]

# Create a boto3 session
session = boto3.Session(
    aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name
)

# Iterate over the CSV files in the S3 bucket with the specified prefix
s3 = session.resource("s3")
bucket = s3.Bucket(bucket_name)

for obj in bucket.objects.filter(Prefix=prefix):
    if obj.key.endswith(".csv"):
        file_key = obj.key

        # Get the S3 object URL
        url = f"s3://{bucket_name}/{file_key}"

        print("Processing", obj.key, "uploading to", url)

        # Define the payload for the Neptune loader API
        payload = {
            "source": url,
            "format": "opencypher",
            "iamRoleArn": iam_role_arn,
            "region": region_name,
            "failOnError": "FALSE",
            "parallelism": "OVERSUBSCRIBE",
            "updateSingleCardinalityProperties": "FALSE",
            "queueRequest": "TRUE",
        }

        print(payload)

        # Make a POST request to the Neptune loader API
        response = requests.post(
            f"https://{neptune_endpoint}:{neptune_port}/loader",
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload),
        )

        # Print the response from the Neptune loader API
        print(response.json())
