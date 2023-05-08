import json
import boto3

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb').Table('random_numbers')
    numbers = dynamodb.scan()['Items']
    resp = [int(i['number']) for i in numbers]
    return {
        'statusCode': 200,
        'body': resp
    }
