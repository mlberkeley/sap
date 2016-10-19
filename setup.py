import boto3

# Get the service resource.
dynamodb = boto3.resource('dynamodb')

# Create the DynamoDB table.
table = dynamodb.create_table(
    TableName='stock-v1',
    KeySchema=[
        {
            'AttributeName': 'symbol',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'dateTime',
            'KeyType': 'RANGE'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'symbol',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'dateTime',
            'AttributeType': 'S'
        },

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
)

# Wait until the table exists.
table.meta.client.get_waiter('table_exists').wait(TableName='users')

print("table created.")