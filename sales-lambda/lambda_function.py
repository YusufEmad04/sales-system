import json

import boto3


# import pandas as pd

class DynamoDBCounter:
    # primary_key = sales_num
    def __init__(self, table_name):
        self.table_name = table_name
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(self.table_name)

    def increment_employee_counter(self, employee_id):
        # check if employee_id exists
        response = self.table.get_item(Key={'employee_id': employee_id})
        if 'Item' in response:
            # increment counter
            self.table.update_item(
                Key={'employee_id': employee_id},
                UpdateExpression='SET sales_num = sales_num + :val',
                ExpressionAttributeValues={':val': 1}
            )
            return response['Item']['sales_num'] + 1
        else:
            # create new item
            self.table.put_item(Item={'employee_id': employee_id, 'sales_num': 1})
            return 1

    def get_employee_counter(self, employee_id):
        response = self.table.get_item(Key={'employee_id': employee_id})
        if 'Item' in response:
            return response['Item']['sales_num']
        else:
            return 0


class DynamoDBSales:

    def __init__(self, table_name):
        self.table_name = table_name
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(self.table_name)

    def add_sale(
            self,
            employee_id,
            customer_name,
            customer_number
    ):
        sale_id = str(hash(str(employee_id) + customer_name + customer_number))
        employee_sale_num = DynamoDBCounter('counter').increment_employee_counter(str(employee_id))
        self.table.put_item(
            Item={
                'sale_id': sale_id,
                'employee_id': employee_id,
                'customer_name': customer_name,
                'customer_number': customer_number,
                'employee_sale_num': employee_sale_num
            }
        )
        return sale_id

    def get_employee_sales(self, employee_id):
        response = self.table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr('employee_id').eq(employee_id)
        )

        if 'Items' in response:
            return response['Items']
        else:
            return []


def lambda_handler(event, context):
    # get employee_id, customer_name, customer_number from event

    body = json.loads(event["body"])

    employee_id = body['employee_id']
    customer_name = body['customer_name']
    customer_number = body['customer_number']

    db = DynamoDBSales('sales')
    # add sale
    sale_id = db.add_sale(
        employee_id=int(employee_id),
        customer_name=customer_name,
        customer_number=customer_number
    )

    return {
        'statusCode': 200,
        'body': {
            'sale_id': sale_id
        }
    }

