import boto3
import requests
import json
import os

def lambda_handler(event, context):

    # Define your AWS configuration
    region_name = os.environ['AWS_REGION']  # AWS Lambda sets this environment variable by default

    # Define the Neptune configuration
    neptune_endpoint = os.environ['NEPTUNE_ENDPOINT']
    neptune_port = '8182'

    # Define S3 configuration
    bucket_name = os.environ['BUCKET_NAME']
    prefix = os.environ['PREFIX']

    # Define the IAM role ARN for the Neptune bulk load
    iam_role_arn = os.environ['IAM_ROLE_ARN']

    # Create an S3 client
    s3 = boto3.client('s3')

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
