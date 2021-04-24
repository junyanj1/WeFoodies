"""input zipcode / userid 
give event ids based on them"""

import boto3
import json
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import datetime

client_db = boto3.resource('dynamodb')

HOST = 'search-event-tmtmaeqngbomjv37brze5kqpfa.us-west-2.es.amazonaws.com'
REGION = 'us-west-2'

service = 'es'
region = REGION
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

es = Elasticsearch(
    hosts = [{'host': HOST, 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)


def search(keys):
    results = []
    for key in keys:
        r = es.search(index="event", body={"from":0,"size":5,"query":{"match":{"labels":key}}})
        for r in r["hits"]["hits"]:
            print('find', r)
            esId = r['_id']
            eventTime = r['_source'].get('eventTime')
            # print(eventTime)
        
            if not eventTime or datetime.datetime.strptime(eventTime, '%Y-%m-%d %H:%M:%S.%f') <  datetime.datetime.now():
                es.delete(index="event", id=esId)
                print('delete', r)
                continue
            
            eventId = r['_source']['eventId']
            print(eventId)
            results.append(eventId)
            print('put', r)
            # print(results)
        # print(results)
    return results


def getFollowing(userId):
    table = client_db.Table('user')
    response = table.get_item(
        Key={
            'id': userId
            }
        )

    item = response['Item']
    followingLst = []
    if item:
        followingLst = item.get('following')

    else:
        raise RuntimeError('No userId: ' + userId + 'in user db')

    return followingLst


def lambda_handler(event, context):
    zipcode = event['queryStringParameters'].get("zipcode")
    userId = event['queryStringParameters'].get("userId")
    friendIds = getFollowing(userId)
    # print(friendIds)
    eventIds = []
    if friendIds:
        eventIds = search(friendIds)
        # print(eventIds)
    if zipcode:
        eventIds += search(zipcode)
        # print(eventIds)

    if not eventIds:
        eventIds += search('10027')
    print(eventIds)
    response = {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
            },
            'body': json.dumps({ 
                "eventIds": eventIds
            }),
            'isBase64Encoded': False,
        }
                    
    return response