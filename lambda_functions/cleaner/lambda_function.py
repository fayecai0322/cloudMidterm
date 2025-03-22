import os
import time
import boto3
from datetime import datetime, timezone
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
    logger.info("🧹 Cleaner Lambda triggered")
    logger.info("⏱️ Running cleanup for 60s with 5s interval...")

    start_time = time.time()
    while time.time() - start_time < 60:
        clean_disowned_copies()
        time.sleep(5)

def clean_disowned_copies():
    try:
        response = table.query(
            IndexName='DisownedIndex',
            KeyConditionExpression=Key('disowned').eq('true')
        )

        now = datetime.now(timezone.utc)

        for item in response.get('Items', []):
            disown_time_str = item.get('disownTime')
            if not disown_time_str:
                logger.warning("⚠️ disownTime missing in item: %s", item)
                continue

            try:
                disown_time = datetime.fromisoformat(disown_time_str)
            except Exception as parse_err:
                logger.error("❌ Failed to parse disownTime '%s': %s", disown_time_str, parse_err)
                continue

            age = (now - disown_time).total_seconds()

            if age > 10:
                logger.info("🗑️ Deleting from S3: %s (age %.2fs)", item['copyKey'], age)
                s3.delete_object(Bucket=dst_bucket, Key=item['copyKey'])

                logger.info("🧾 Deleting from DynamoDB: (%s, %s)", item['objectName'], item['sortKey'])
                table.delete_item(
                    Key={'objectName': item['objectName'], 'sortKey': item['sortKey']}
                )
                logger.info("✅ Deleted successfully.")
            else:
                logger.info("⏳ Still too new: %s (%.2fs)", item['copyKey'], age)

    except Exception as e:
        logger.error("❌ Error in clean_disowned_copies: %s", str(e), exc_info=True)
