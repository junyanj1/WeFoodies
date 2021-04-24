import json
from yelpapi import YelpAPI
import os
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import datetime
import boto3
from decimal import Decimal
import decimal

HOST = 'search-restaurant-d6jgpyhmyollvvj6wnlmokc5q4.us-west-2.es.amazonaws.com'
service = 'es'
region = 'us-west-2'
# headers = {"Content-Type" : "application/json"}
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
client_db = boto3.resource('dynamodb')
    

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
        response = es.search(index="restaurant", body={"from":0,"size":5,"query":{"match":{"labels":key}}})
        
        for r in response["hits"]["hits"]:
            if r['_score'] < 0.6:
                print("Not confident enough, ")
                continue
            print('find', r)
            rId = r['_source']['rId']
            print(rId)
            results.append(rId)
            print('put', r)
    return list(set(results))

def uploadToEs(input):
     mydata={
         "rId": input['rId'],
         "labels": input['labels']
         }
     # print(mydata)
     mydata_json = json.dumps(mydata)
     print("Mydata:", mydata_json)
 
     es.index(index="restaurant", doc_type="_doc", body=mydata_json)
     print('upload restaurant es successfully')

def uploadToDB(input):
    table = client_db.Table('restaurant')
    table.put_item(
        Item=input
        )
    print('upload event db successfully')

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
    
def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError

api_key = os.environ['yelpApiKey']
yelp_api = YelpAPI(api_key)


def lambda_handler(event, context):
    print(event)
    print(event["multiValueQueryStringParameters"])
    queryparam = event["multiValueQueryStringParameters"]
    q = queryparam.get("q")
    zipcode = queryparam.get("zipcode")
    if not q or not zipcode:
        return {
            'statusCode': 400,
            'body': "Missing query parameter",
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
            }
        }
    esSearchResult = search(q[0]+","+zipcode[0])
    print("es", esSearchResult)
    if len(esSearchResult)<5:
        resp = yelp_api.search_query(term=q, location=zipcode, sort_by='best_match')
        for i in resp["businesses"][:5]:
            categories = []
            for j in i["categories"]:
                categories.append(j["alias"])
            print("Categories", categories)
            labels = [i["name"]]+categories+[zipcode[0]+"-1"]
            uploadToEs({"rId": i["id"],"labels": labels})
            i2 = json.loads(json.dumps(i), parse_float=Decimal)
            uploadToDB(i2)
        resp = resp["businesses"][:5]
    else:
        # esSearchResult
        # esSearchResult = list(set(esSearchResult))
        print(esSearchResult)
        resp = getDBitem(esSearchResult)
        resp = json.loads(json.dumps(resp, default=decimal_default))

    
    return {
        'statusCode': 200,
        'body': json.dumps(resp),
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
        },
        'isBase64Encoded': False
    }
