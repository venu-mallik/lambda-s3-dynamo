import boto3
import json
import pandas as pd
import awswrangler as wr

table_name = 'omnirio-0'
s3 =boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(table_name)

def lambda_handler(event, context):
    http_method = event.get('httpMethod','').lower()
    success = { 'body' : json.dumps(dict()),  "statusCode": 200, "isBase64Encoded": False}
    fail = { 'body' : { 'detail' : "error or not found" , "success" : False } ,   "isBase64Encoded": False ,"statusCode": 500}
    if http_method == 'post':
        try:
            query_string = event.get('queryStringParameters')
            headers = event.get('headers')
            body = event.get('body')
            csv_public_path = body.get("presignedUrl")
            df = pd.read_csv(csv_public_path)
            df.columns = ['SKU','product_name','product_image','status','category']
            df['SKU'] = df['SKU'].astype(str)
            wr.dynamodb.put_df(
                df=df,
                table_name=table_name
            )
            return success
        except Exception:
            return fail
    if http_method == 'get':
        try:
            query_string = event.get('queryStringParameters')
            SKU = '1'
            if query_string:
                SKU = query_string.get('SKU','1')
            response = table.get_item(Key={
              "SKU": str(SKU)
            })
            success['body'] = json.dumps(response['Item'])
            return success
        except Exception as e:
            return fail
    return { 
        'body' : json.dumps({ 'detail' : "Method Not Found"}),
        "statusCode": 404, 
        "isBase64Encoded": False 
        
    }
        
    
