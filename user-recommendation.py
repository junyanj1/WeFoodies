# Return 10 recommended users; must match SO
import json
import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('user')
# sexual orientation matching dictionary
sorules = {(0,1):[(1,0)],(0,0):[(0,0)],(1,1):[(1,1)],(2,2):[(i,j) for i in range(3) for j in range(3)],(0,2):[(1,0),(0,0)],(1,2):[(0,1),(1,1)],(2,0):[(0,1),(0,0)],(2,1):[(1,0),(1,1)]}

# class User:
#     def __init__(self, )

def lambda_handler(event, context):
    r1 = table.get_item(Key={"id":event["queryStringParameters"]["q"]})
    uinfo = r1["Item"]
    if uinfo.get("so") is not None:
        t = (int(uinfo["gender"]),int(uinfo["so"]))
        print(t)
        recso = sorules.get(t)
    else:
        recso = [(i,j) for i in range(3) for j in range(3)]
    ## Check if vegan
    candidates = []
    for i in recso:
        r2 = table.query(
            IndexName='gender-so-index',
            KeyConditionExpression=Key('gender').eq(str(recso[0][0])) & Key('so').eq(str(recso[0][1]))
        )
        candidates.extend(r2["Items"])
    scores = {}
    candidate_dict = {}
    for c in candidates:
        score = 0
        if c.get("vegan") == uinfo.get("vegan"):
            score += 5
        if c.get("allergies") == uinfo.get("allergies"):
            score += 5
        if c.get("pricePoint") == uinfo.get("pricePoint"):
            score += 3
        if c.get("cuisinePreferences") is not None and uinfo.get("cuisinePreferences") is not None:
            t = set(list(c.get("cuisinePreferences"))).intersection(uinfo.get("cuisinePreferences"))
            if len(t) > 5:
                score += 5
            if len(t) > 3:
                score += 2
        scores[c.get("id")] = score
        candidate_dict[c.get("id")] = c
    sortedCandi = sorted(scores.items(), key=lambda item: -item[1])[:5]
    result = [candidate_dict[r] for (r,v) in sortedCandi]
        
    return {
        'statusCode': 200,
        'headers': {
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
        },
        'body': json.dumps(result)
    }
