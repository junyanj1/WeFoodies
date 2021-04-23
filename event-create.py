import boto3
import requests
import datetime
import json
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
client_db = boto3.resource('dynamodb')

HOST = 'search-event-tmtmaeqngbomjv37brze5kqpfa.us-west-2.es.amazonaws.com'
REGION = 'us-west-2'

def getESLabels(input):
    labels = []
    client_db = boto3.resource('dynamodb')
    table1 = client_db.Table('restaurant')
    response = table1.get_item(
        Key={
            'id': input['restaurantId']
            }
        )

    item = response['Item']
    # print(item)
    if item:
        rName = item['name']
        rZipcode = item['zipcode']
        labels = [rName, rZipcode]
    else:
        raise RuntimeError('No restaurantId: ' + input['restaurantId'] + ' in restaurant db')

    client_db = boto3.resource('dynamodb')
    table2 = client_db.Table('user')
    response = table2.get_item(
        Key={
            'id': input['userId']
                }
            )
    item = response['Item']
    # print(item)
    if item:
        labels.append(item['name']) #Caution: do not use labels += uName
        labels.append(item['id'])
    else:
        raise RuntimeError('No restaurantId: ' + input['restaurantId'] + ' in restaurant db')

    return labels



def uploadToEs(input, labels):
    service = 'es'
    region = REGION

    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

    mydata={
        "eventId": input['eventId'],
        "eventTime": input['time'],
        "labels": input['labels']
        }
    # print(mydata)
    mydata_json = json.dumps(mydata)   
    es = Elasticsearch(
    hosts = [{'host': HOST, 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
    )

    es.index(index="event", doc_type="_doc", body=mydata_json)
    print('upload event es successfully')


def uploadToDB(input):
    table = client_db.Table('event')
    eventId = str(datetime.datetime.now())
    table.put_item(
        Item={  'id' : eventId,
                'userId': input.get('userId'),
                'restaurantId': input.get('restaurantId'),
                'time' : input.get('time'),
                'numPeople' : input.get('numPeople'),
                'participants': [input.get('userId')]
            }
        )
    print('upload event db successfully')
    return eventId


def uploadToUserDB(uid, eid):
    table = client_db.Table('user')
    response = table.get_item(Key={'id': uid})
    item = response.get('Item')
    events = []
    
    if not item:
        raise RuntimeError('No userId: ' + uid + ' in user db')

    elif item.get('events'):
        events = item.get('events')
    
    events.append(eid)
    
    response = table.update_item(
        Key={
            'id': uid
        },
        UpdateExpression="set events=:e",
        ExpressionAttributeValues={
            ':e': events
        },
        ReturnValues="UPDATED_NEW"
        )

    print('update user db successfully')



def lambda_handler(event, context):
    userId = event.get('userId')
    restaurantId = event.get('restaurantId')
    time = event.get('time')
    numPeople = event.get('numPeople')

    sc = 200
    message = ""
    
    if not userId:
        sc = 400
        message += "userId is None "
    if not restaurantId:
        sc = 400
        message += "restaurant is None "
    if not time:
        sc = 400
        message += "time is None "
    if not numPeople:
        sc = 400
        message += "numPeople is None "

    if sc == 400:
        return {
            'statusCode': sc,
            'body': json.dumps(message)
        }

    input = {'userId': userId, 'restaurantId': restaurantId, 'time': time, 'numPeople':numPeople}
    eventId = uploadToDB(input)
    uploadToUserDB(userId, eventId)
    input['eventId'] = eventId
    labels = getESLabels(input)
    input['labels'] = labels
    uploadToEs(input, labels)


if __name__ == '__main__' :
    event = {'userId': 'uid1', 
            'restaurantId': 'rid1', 
            'time': '2021-04-30 13:21:56.550545', 
            'numPeople': 4}
    lambda_handler(event, '')

# dt = datetime.datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S.%f')
# dt < datetime.datetime.now()