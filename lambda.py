from __future__ import print_function
from boto3.dynamodb.conditions import Key, Attr
import boto3
import time

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')
rek_client = boto3.client('rekognition')
bucket_name = 'iot-project-0000'
dynamodb = boto3.resource('dynamodb')
sns = boto3.resource('sns')


def SNS(s1, s2):
    topic = sns.Topic('arn:aws:sns:us-east-1:264539880516:Final_project')
    print('Status')
    print(s1, s2)
    if (s1 == False and s2 == True):
        topic.publish(Message='Arrival')
    elif (s1 == True and s2 == False):
        topic.publish(Message='Removal')


def analyze_video_rekog(file_name, bucket_name, result):
    response = rek_client.start_label_detection(
        Video={
            'S3Object': {
                'Bucket': bucket_name,
                'Name': file_name,
            }
        },
        MinConfidence=55,
    )
    job_id = response['JobId']

    while True:
        response = rek_client.get_label_detection(
            JobId=job_id
        )
        if response['JobStatus'] == 'SUCCEEDED':
            found = False
            for label in response['Labels']:
                result.append(label['Label']['Name'])
                if label['Label']['Name'] == 'Box':
                    found = True
                    break
            print("SUCCEEDED, found box" if found else "SUCCEEDED, didn't find box")
            break
        elif response['JobStatus'] == 'FAILED':
            print('FAILED')
            break
        time.sleep(1)
    print('All the objects found')
    return found


def lambda_handler(event, context):
    objects = s3_client.list_objects(Bucket=bucket_name)['Contents']
    print(objects)
    file_name = []
    for object in objects:
        name = object['Key']
        file_name.append(name)
    file_name.sort(reverse=True)
    print('Current video name')
    print(file_name[0])
    result = []
    time1 = int(file_name[0].split('.')[0])
    currentStatus = analyze_video_rekog(file_name[0], bucket_name, result)
    time2 = int(str(time.time()).split('.')[0])
    result = set(result)
    table = dynamodb.Table('Result')
    response = table.scan()
    records = len(response['Items'])
    formerStatus = False

    for i in response['Items']:
        if i['Index'] == file_name[1]:
            formerStatus = i['Status']
            break


    response2 = table.put_item(Item={'Index': file_name[0], 'Result': str(result), 'Status': currentStatus, 'Time to Analyze': time2 - time1})

    SNS(formerStatus, currentStatus)
    return currentStatus
