# Input: userid, username, gender(dropdown menu), sexualOrientation (dropdown menu),
# email, description, phoneNumber, vegan(boolean), allergies(drop menu),
# cuisinePreferences (dropdown menu), pricePoint ($/$$/$$$/$$$$)

import json
import boto3

dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    # TODO implement
    table = dynamodb.Table('user')
    ue = "set "
    ea = {}
    count = 0
    for i in event.keys():
        if i != "userId":
            event[i] = str(event[i])
            ue += i+"=:"+i+","
            ea[":"+i] = event[i]

    ue = ue[:-1]

    print("/ea", ea)
    print("/ue", ue)

    response = table.update_item(
        Key={
            'id': event["userId"]
        },
        UpdateExpression=ue,
        ExpressionAttributeValues=ea,
        ReturnValues="UPDATED_NEW"
    )
    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }

