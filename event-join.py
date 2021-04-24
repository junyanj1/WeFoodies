'''
event db: participant
user db: 
'''
import boto3
import json
client_db = boto3.resource('dynamodb')

def updateEventDB(uid, eid):
    table = client_db.Table('event')
    response = table.get_item(Key={'id': eid})
    item = response.get('Item')
    if not item:
        raise RuntimeError('No eventId: ' + eid + ' in event db')

    elif item.get('participants'):
        participants = item.get('participants')
    
    if not uid in participants:
        participants.append(uid)


    response = table.update_item(
        Key={
            'id': eid
        },
        UpdateExpression="set participants=:p",
        ExpressionAttributeValues={
            ':p': participants
        },
        ReturnValues="UPDATED_NEW"
        )

    print('update event db successfully')


def upDateUserDB(uid, eid):
    table = client_db.Table('user')
    response = table.get_item(Key={'id': uid})
    item = response.get('Item')
    events = []
    
    if not item:
        raise RuntimeError('No userId: ' + uid + ' in user db')

    elif item.get('events'):
        events = item.get('events')
    
    if not eid in events:
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
    
    print("event", event)
    print("Event_Id",event['params']['querystring']['eid'])
    print("User_Id",event['params']['querystring']['userId'])
    
    uid = event['params']['querystring']['userId']
    eid = event['params']['querystring']['eid']

    
    return {
         'statusCode': 200,
         'body': json.dumps(eid)
    }
    
    
    uid = event.get('userId')
    eid = event.get('eventId')

    sc = 200
    message = ""
    if not uid:
        sc = 400
        message += "userId is None "
    if not eid:
        sc = 400
        message += "eventId is None "
    if sc == 400:
        return {
            'statusCode': sc,
            'body': json.dumps(message)
        }

    
    updateEventDB(uid, eid)
    upDateUserDB(uid, eid)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Join Successfully')
    }
