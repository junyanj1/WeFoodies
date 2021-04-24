import boto3
import json
client_db = boto3.resource('dynamodb')

#%%
# client_db = boto3.resource('dynamodb')
# table = client_db.Table('event')
# response = client_db.batch_get_item(
#     RequestItems={
#         'event': {
#             'Keys': [    
#                 {'id':  '2021-04-05 13:13:09.334110'},
#                 {'id': '2021-04-05 12:29:11.401135'}
#                 ]
#         }
#     })
# response
#%%
def getEvent(input):
    res = []
    print("input",input)
    for eid in input:
        print("eid",eid)
        event = {}
        
        table = client_db.Table('event')
        response = table.get_item(
            Key={
                'id': eid
                }
            )
        print(eid)
        item = response['Item']
        if item:
            event['eventId'] = item.get('id')
            event['userId'] = item.get('userId')
            event['restaurantId'] = item.get('restaurantId')
            event['time'] = item.get('time')
            event['numPeople'] = int(item.get('numPeople'))

        else:
            raise RuntimeError('No eventId: ' + input['eventId'] + 'in event db') from exc


        table = client_db.Table('user')
        response = table.get_item(
            Key={
                'id': event.get('userId')
                }
            )

        item = response['Item']
        if item:
            event['userName'] = item.get('name')
            event['gender'] = item.get('gender')

        else:
            raise RuntimeError('No userId: ' + event.get('userId') + 'in user db') from exc


        
        table = client_db.Table('restaurant')
        response = table.get_item(
            Key={
                'id': event.get('restaurantId')
                }
            )

        item = response['Item']
        if item:
            event['userName'] = item.get('name')
            event['gender'] = item.get('gender')
        
        else:
            raise RuntimeError('No restaurantId: ' + event.get('restaurantId') + 'in user db') from exc

        res.append(event)
    return res

def lambda_handler(event, context):
    print("event",event)
    query = event['multiValueQueryStringParameters']['q']
    print("query",query)
    query = [w.replace("_", " ") for w in query]
    print("query",query)
    #return {
    #    'statusCode': 200,
    #    'body': json.dumps(query)
    #}
    
    #query = event['queryStringParameters'].get("q")
    if not query:
        return {
            'statusCode': 400,
            'body': json.dumps('q is None')
        }

    #events = getEvent(eval(query))
    events = getEvent(query)
    response = {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
            },
            'body': json.dumps({ 
                "eventIds": events
            }),
            'isBase64Encoded': False,
        }
                    
    return response


# if __name__ == '__main__' :
#     event = {'queryStringParameters':{'q': "['2021-04-05 13:13:09.334110', '2021-04-05 12:29:11.401135']"}}
#     res = lambda_handler(event, '')
#     print(res)