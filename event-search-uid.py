import boto3
import json
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('event')

def queryDB(uid):
    res = []
    response = table.query(
        # IndexName = 'restaurantId-index',
        KeyConditionExpression=Key('userId').eq(str(uid))
        # only one partitionkey (it is like hash table)
    )
    items = response.get('Items')
    if not items:
        raise RuntimeError('No userId: ' + uid + ' in event db')
    else:
        for item in items:
            res.append(item['id'])

    return res



def lambda_handler(event, context):
    uid = event['queryStringParameters'].get("q")
    eventIds = queryDB(uid)
    return eventIds