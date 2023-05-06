import boto3
import pandas as pd


def get_excel_url(db):
    # items = db.table.scan()['Items']
    items = db.scan()['Items']

    """
    excel file format
    employee_id,customer_name,customer_number,employee_sale_num
    """
    # create dataframe and order columns
    df = pd.DataFrame(items)
    df = df[['employee_id', 'customer_name', 'customer_number', 'employee_sale_num']]
    # order by employee_id
    df = df.sort_values(by=['employee_id'])
    # order each employee's sales by employee_sale_num
    df = df.groupby('employee_id', group_keys=True).apply(lambda x: x.sort_values('employee_sale_num'))
    print(df)
    # save to excel file
    df.to_excel('/tmp/sales.xlsx', index=True)
    # upload to s3
    s3 = boto3.client('s3')
    s3.upload_file('/tmp/sales.xlsx', 'sales-excel', 'sales.xlsx')

    # make object public
    # s3.put_object_acl(
    #     ACL='public-read',
    #     Bucket='sales-excel',
    #     Key='sales.xlsx'
    # )

    # get object url
    url = s3.generate_presigned_url(
        ClientMethod='get_object',
        Params={
            'Bucket': 'sales-excel',
            'Key': 'sales.xlsx'
        }
    )
    return url


def lambda_handler(event, context):
    return {
        'statusCode': 200,
        # 'body': print_table(DynamoDBSales('sales'))
        'body': get_excel_url(boto3.resource('dynamodb').Table('sales'))
    }
