import boto3
import json
from boto3.dynamodb.conditions import Key
import decimal

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('restaurant')


def queryDB(rid):
    res = []
    response = table.get_item(Key = {
        "id" : rid
    })
    print(response)
    item = response.get('Item')
    if not item:
        # raise RuntimeError('No restaurantId: ' + rid + ' in restaurant db')
        return {
            'statusCode': 400,
            'body': 'No such restaurant Id, try again',
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
            },
            'isBase64Encoded': False
        }
    else:
        res.append(item)

    return res

def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError


def lambda_handler(event, context):

    print(event)
    restaurant = queryDB(event["queryStringParameters"]["q"])
    return {
        'statusCode': 200,
        'body': json.dumps(restaurant, default=decimal_default),
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
        },
        'isBase64Encoded': False
    }
