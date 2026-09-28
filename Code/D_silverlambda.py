import json
import boto3

def lambda_handler(event, context):
    client = boto3.client('s3')
    glue_client = boto3.client('glue')
    start_job_run = glue_client.start_job_run(
        JobName='silver-job',
        Arguments={
            '--run_date': '2020-10-10'
        }
    )

    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }