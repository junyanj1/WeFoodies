"""input zipcode 
give restaurants based on them"""

import boto3
import json
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import datetime
import decimal

client_db = boto3.resource('dynamodb')
HOST = 'search-restaurant-d6jgpyhmyollvvj6wnlmokc5q4.us-west-2.es.amazonaws.com'
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

def getDBitem(input):
    client_db = boto3.resource('dynamodb')
    # table = client_db.Table('restaurant')
    response = client_db.batch_get_item(
    RequestItems={
        'restaurant': {
            'Keys': 
                [{'id': str(rid)} for rid in input]
            }
        })
    return response['Responses']['restaurant']

def search(keys):
    results = set()
    for key in keys:
        # r = es.search(index="restaurant", body={"from":0,"size":5,"query":{"match":{"labels":key}}})
        r = es.search(index="restaurant", body={"from":0,"size":5,"query":{"match_all":{}}})
        for r in r["hits"]["hits"]:
            print('find', r)
            rId = r['_source']['rId']
            print(rId)
            results.add(rId)
            print('put', r)
    return results
    
def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError

def lambda_handler(event, context):
    print(event)
    zipcode = event['queryStringParameters'].get("zipcode")
    rids = search(zipcode+'-1' if not zipcode else '10036-1')
    restaurants = []
    if rids:
        restaurants = getDBitem(rids)


    response = {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
            },
            'body': json.dumps(restaurants, default=decimal_default),
            
            'isBase64Encoded': False,
        }
                    
    return response