import json
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('user')

def lambda_handler(event, context):
    follower = event.get("followerId")
    followee = event.get("followeeId")
    r1 = table.get_item(Key={'id':follower})
    flist = r1.get("Item").get("following")
    if not flist:
        flist = [followee]
    else:
        flist.append(followee)
    response = table.update_item(
        Key={
            'id': follower
        },
        UpdateExpression="set following = :f",
        ExpressionAttributeValues={
            ':f': flist,
        },
        ReturnValues="UPDATED_NEW"
    )
    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }