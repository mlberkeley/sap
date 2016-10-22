import boto3

client = boto3.client('dynamodb')

response = client.describe_table(TableName='stock-v1')

count = str(response['Table']['ItemCount'])

print("Total count is: " + count)