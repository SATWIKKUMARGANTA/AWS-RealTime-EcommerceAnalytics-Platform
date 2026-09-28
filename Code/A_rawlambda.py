import boto3
import json

s3 = boto3.client('s3')
kinesis = boto3.client('kinesis', region_name='ap-southeast-2')
STREAM = "e-commerce"

def lambda_handler(event, context):
    raw_bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    print(f"RAW nunchi file vachindi: {key}")

    obj = s3.get_object(Bucket=raw_bucket, Key=key)
    lines = obj['Body'].read().decode('utf-8').splitlines()
    header = lines[0].split(',')

    batch = []
    count = 0
    for line in lines[1:]:
        row = dict(zip(header, line.split(',')))
        row['_source'] = key
        batch.append({'Data': json.dumps(row)+"\n", 'PartitionKey': key})

        # 100 records okasari pampu - fast!
        if len(batch) == 100:
            kinesis.put_records(StreamName=STREAM, Records=batch)
            count += len(batch)
            batch = []

    if batch:
        kinesis.put_records(StreamName=STREAM, Records=batch)
        count += len(batch)

    print(f"{count} records Kinesis ki vellayi ma!")
    return "DONE"