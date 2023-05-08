import boto3
import json

def get_credentials():
    dyn = boto3.resource('dynamodb')
    table = dyn.Table('credentials')

    response = table.scan()
    items = response['Items']

    credentials = {}

    for item in items:
        credentials[int(item['employee_id'])] = item['password']

    return credentials

def lambda_handler(event, context):
    # TODO implement
    return {
        'statusCode': 200,
        # 'headers': {
        #     'Access-Control-Allow-Origin': '*',
        #     'Access-Control-Allow-Headers': '*',
        #     'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        # },
        # 'headers': {
        #     "Access-Control-Allow-Origin" : "*",
        #     "Access-Control-Allow-Credentials" : "true"
        # },
        'body': json.dumps(get_credentials())
    }