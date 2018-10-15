import base64
import json
import boto3


def lambda_handler(event, context):
    """
    Receive a batch of events from Kinesis and insert as-is into our DynamoDB table if invoked asynchronously,
    otherwise perform an asynchronous invocation of this Lambda and immediately return
    """
    #if not event.get('async'):
    #    invoke_self_async(event, context)
    #    return


    # Get a handle to the table
    dynamo_db = boto3.resource('dynamodb')
    curr_pos_table = dynamo_db.Table('current_position')

    # Decode the data from base 64 and then put it into JSON
    decoded_record_data = [base64.b64decode(record['kinesis']['data']) for record in event['Records']]
    deserialized_data = [json.loads(decoded_record) for decoded_record in decoded_record_data]

    # Insert each item in to the database
    with curr_pos_table.batch_writer() as batch_writer:
        for movement_event in deserialized_data:
            batch_writer.put_item(Item=movement_event)


def invoke_self_async(event, context):
    """
    Have the Lambda invoke itself asynchronously, passing the same event it received originally,
    and tagging the event as 'async' so it's actually processed
    """
    event['async'] = True
    called_function = context.invoked_function_arn
    boto3.client('lambda').invoke(
        FunctionName=called_function,
        InvocationType='Event',
        Payload=bytes(json.dumps(event))
    )
