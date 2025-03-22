import os
import boto3
from datetime import datetime
from urllib.parse import unquote_plus
from boto3.dynamodb.conditions import Key

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])
dst_bucket = os.environ['DST_BUCKET']

def lambda_handler(event, context):
    logger.info("🔔 Lambda triggered.")
    logger.info("📦 Received event: %s", event)

    for record in event.get('Records', []):
        event_name = record['eventName']
        src_bucket = record['s3']['bucket']['name']
        src_key = unquote_plus(record['s3']['object']['key'])

        logger.info("➡️ Event type: %s | src_bucket: %s | src_key: %s", event_name, src_bucket, src_key)

        if event_name.startswith('ObjectCreated:'):
            handle_put(src_bucket, src_key)
        elif event_name.startswith('ObjectRemoved:'):
            handle_delete(src_key)
        else:
            logger.warning("⚠️ Unhandled event type: %s", event_name)

def handle_put(src_bucket, src_key):
    timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%S')
    copy_key = f"{src_key}_copy_{timestamp}"

    try:
        logger.info("🚀 Copying object to dst bucket: %s as %s", dst_bucket, copy_key)

        s3.copy_object(
            Bucket=dst_bucket,
            CopySource={'Bucket': src_bucket, 'Key': src_key},
            Key=copy_key
        )

        logger.info("✅ Copy succeeded.")

        table.put_item(Item={
            'objectName': src_key,
            'sortKey': copy_key,
            'copyKey': copy_key,
            'timestamp': timestamp,
            'disowned': 'false'
            # ✅ 不再写入空字符串的 disownTime
        })

        logger.info("📄 Record inserted into DynamoDB: %s", copy_key)

        response = table.query(
            KeyConditionExpression=Key('objectName').eq(src_key)
        )
        items = sorted(response['Items'], key=lambda x: x['timestamp'])

        logger.info("🔍 Total copies: %d", len(items))

        if len(items) > 3:
            to_delete = items[0]
            logger.info("🗑️ Too many copies, deleting oldest: %s", to_delete['copyKey'])

            s3.delete_object(Bucket=dst_bucket, Key=to_delete['copyKey'])
            table.delete_item(
                Key={'objectName': src_key, 'sortKey': to_delete['sortKey']}
            )
            logger.info("✅ Oldest copy deleted from S3 and DynamoDB.")

    except Exception as e:
        logger.error("❌ Error during handle_put: %s", str(e), exc_info=True)

def handle_delete(src_key):
    try:
        logger.info("❌ Handling DELETE for %s", src_key)

        response = table.query(
            KeyConditionExpression=Key('objectName').eq(src_key)
        )
        now = datetime.utcnow().isoformat()

        for item in response['Items']:
            logger.info("🔖 Marking %s as disowned", item['copyKey'])

            table.update_item(
                Key={'objectName': item['objectName'], 'sortKey': item['sortKey']},
                UpdateExpression="SET disowned = :d, disownTime = :t",
                ExpressionAttributeValues={
                    ':d': 'true',
                    ':t': now
                }
            )
        logger.info("✅ All copies marked as disowned.")
    except Exception as e:
        logger.error("❌ Error during handle_delete: %s", str(e), exc_info=True)
