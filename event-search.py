import boto3
import json
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import datetime

HOST = 'search-event-tmtmaeqngbomjv37brze5kqpfa.us-west-2.es.amazonaws.com'
def search(keys):
    service = 'es'
    region = 'us-west-2'
    # headers = {"Content-Type" : "application/json"}
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
        

    es = Elasticsearch(
        hosts = [{'host': HOST, 'port': 443}],
        http_auth = awsauth,
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
    )
    
    results = []
    
    for key in keys:
        # print(key)
        r = es.search(index="event", body={"from":0,"size":10,"query":{"match":{"labels":key}}})
        for r in r["hits"]["hits"]:
            print('find', r)
            esId = r['_id']
            eventTime = r['_source'].get('eventTime')
            # print(eventTime)
            
            try:
                if not eventTime or datetime.datetime.strptime(eventTime, '%Y-%m-%d %H:%M:%S.%f') <  datetime.datetime.now():
                    es.delete(index="event",id=esId)
                    print('delete', r)
                    continue
            except:
                es.delete(index="event",id=esId)
                print('delete', r)
                continue
            
            eventId = r['_source']['eventId']
            results.append(eventId)
            print('put', r)

    return results


def lambda_handler(event, context):
    
    print("event", event)
    print("queryStringParameters",event['multiValueQueryStringParameters'])
    query = event['multiValueQueryStringParameters']['q']
    print("query",query)

    
    #return {
    #     'statusCode': 200,
    #     'body': json.dumps(query)
    #}
    
    #query = event['queryStringParameters'].get("q")
    if not query:
        return {
            'statusCode': 400,
            'body': json.dumps('q is None')
        }
    keys=[]
    for q in query:
        print(q)
    keys=[]
    eventIds = search([q.lower() for q in query])
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


# if __name__ == '__main__' :
#     event = {'queryStringParameters':{'q': "['chipotle']"}}
#     res = lambda_handler(event, '')
#     print(res)
