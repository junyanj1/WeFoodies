import json
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('user')


def lambda_handler(event, context):
    # print(event)
    response = table.get_item(Key={'id':event["queryStringParameters"]["q"]})
    print(type(response))
    return {
        'statusCode': 200,
        'headers': {
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
        },
        'body': json.dumps(response["Item"])
    }