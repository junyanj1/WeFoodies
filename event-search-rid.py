import boto3
import json
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('event')

def queryDB(rid):
    res = []
    response = table.query(
        IndexName = 'restaurantId-index',
        KeyConditionExpression=Key('restaurantId').eq(str(rid))
    )
    items = response.get('Items')
    if not items:
        raise RuntimeError('No restaurantId: ' + rid + ' in event db')
    else:
        for item in items:
            res.append(item['id'])

    return res



def lambda_handler(event, context):
    rid = event['queryStringParameters'].get("q")
    eventIds = queryDB(rid)
    return eventIds