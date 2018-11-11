import base64
import json
import boto3
import logging


def lambda_handler(event, context):
    """
    Receive a batch of events from Kinesis and insert as-is into our DynamoDB table if invoked asynchronously,
    otherwise perform an asynchronous invocation of this Lambda and immediately return
    """
    # if not event.get('async'):
    #    invoke_self_async(event, context)
    #    return

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Decode the bytes to base64
    decoded_record_data = []
    for record in event['Records']:
        try:
            decoded_record_data.append(base64.b64decode(record['kinesis']['data']))
        except Exception as e:
            logger.error('%s - %s', "Error decoding record", e)

    # Deserialize the data
    deserialized_data = []
    for decoded_record in decoded_record_data:
        try:
            deserialized_data.append(json.loads(decoded_record))
        except Exception as e:
            logger.error('%s - %s', "Error deserializing data", e)

    # Try opening a connection to DynamoDB
    try:
        # Get a handle to the table
        dynamo_db = boto3.resource('dynamodb')
        curr_pos_table = dynamo_db.Table('current_position')
    except Exception as e:
        logger.error('%s - %s', "Error connecting to DynamoDB", e)
        return

    # Try sending the data
    transmit_data(curr_pos_table, deserialized_data, 0)


def transmit_data(curr_pos_table, deserialized_data, depth):

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if depth > 3:
        logger.error("Depth exceeded trying to split data.")
        return

    delimiter = '-%-'

    try :
        # Insert each item in to the database
        with curr_pos_table.batch_writer() as batch_writer:

            # For each queued event
            for movement_event in deserialized_data:

                # Generate the unique partition key
                movement_event['pKey'] = str(movement_event['eventId']) + delimiter \
                                         + str(movement_event['regionId']) + delimiter \
                                         + movement_event['uuid']

                # Insert or remove from table
                if movement_event['entering']:
                    batch_writer.put_item(Item=movement_event)
                else:
                    batch_writer.delete_item(
                        Key={
                            'pKey': movement_event['pKey']
                        }
                    )

    except Exception as e:
        logger.info('%s - %s', "Error bulk transmitting - retrying in halves", e)

        halfway = len(deserialized_data) // 2
        transmit_data(curr_pos_table, deserialized_data[:halfway], depth+1)
        transmit_data(curr_pos_table, deserialized_data[halfway:], depth+1)




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
