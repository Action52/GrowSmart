import boto3
import requests
import json
import os, sys

# Define your AWS configuration
aws_access_key_id = sys.argv[1]
aws_secret_access_key = sys.argv[2]
region_name = 'eu-west-3'

# Define the Neptune configuration
neptune_endpoint = 'growsmart-neptune.cluster-custom-cgogeml0cuty.eu-west-3.neptune.amazonaws.com'
neptune_port = '8182'

# Define S3 configuration
bucket_name = 'growsmartformattedzone'
prefix = sys.argv[3]

# Define the IAM role ARN for the Neptune bulk load
iam_role_arn = sys.argv[4]

# Create a boto3 session
session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)

# Create an S3 client
s3 = session.client('s3')

# Iterate over the CSV files in the S3 bucket with the specified prefix
paginator = s3.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
    for obj in page['Contents']:
        if obj['Key'].endswith('.csv'):
            file_key = obj['Key']

            # Get the presigned URL of the S3 object
            url = s3.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': bucket_name,
                    'Key': file_key
                }
            )

            print("Processing ", obj['Key'], " uploading to ", url)


            # Define the payload for the Neptune loader API
            payload = {
                "source": url,
                "format": "csv",
                "iamRoleArn": iam_role_arn,
                "region": region_name,
                "failOnError": "FALSE",
                "parallelism": "OVERSUBSCRIBE",
                "updateSingleCardinalityProperties": "FALSE",
                "queueRequest": "TRUE"
            }

            print(payload)
            # Make a POST request to the Neptune loader API
            response = requests.post(
                f'https://{neptune_endpoint}:{neptune_port}/loader',
                headers={'Content-Type': 'application/json'},
                data=json.dumps(payload)
            )

            # Print the response from the Neptune loader API
            print(response.json())
