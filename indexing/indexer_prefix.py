import urllib.parse
import argparse
import logging
import boto3
import sys

from botocore.handlers import disable_signing
from pygelf import GelfTcpHandler

parser = argparse.ArgumentParser(description='Index a s3 bucket and send it to graylog')
parser.add_argument("bucket_name", help="The name of the bucket")
parser.add_argument("--host", help="The graylog instance (default: localhost)", default="localhost")
parser.add_argument("--port", "-p", help="The input port at the graylog (default: 12201)", default=12201)

args = parser.parse_args()



logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(GelfTcpHandler(host=args.host, port=args.port, include_extra_fields=True))


bucket_name = args.bucket_name
MaxItems = 10000000
MaxPrefixes = 1000
files = 0
prefixes = 0

client = boto3.client('s3')
client.meta.events.register('choose-signer.s3.*', disable_signing)
paginator = client.get_paginator('list_objects_v2')
result = paginator.paginate(Bucket=bucket_name, Delimiter='/', PaginationConfig={'MaxItems': MaxItems})


# Fetch Files in Root of S3 Bucket
for content in result.search('Contents'):
    if not content:
        print("No files on root-level")
        continue

    print(f"Fetching root-file {content['Key']}")

    log = {
        "last_modified": content["LastModified"].isoformat(),
        "size": content["Size"],
        "bucket": bucket_name,
        "key": content["Key"],
        "short_message": "Hi!",
    }

    if "." in content["Key"]:
        log["file_extension"] = content["Key"].split(".")[-1]

    if content["StorageClass"] != "STANDARD":
        log['storage_class'] = content["StorageClass"]
        print(f"Found storage_class {log['storage_class']} in {content['Key']}")

    files += 1
    url = url = "http://"+bucket_name+".s3.amazonaws.com/"+urllib.parse.quote(content["Key"])
    logger.info(url, extra=log)

# Fetch Common Prefixes of S3 Bucket
for index, prefix in enumerate(result.search('CommonPrefixes')):
    if not prefix:
        print("No common prefixes")
        continue

    if prefixes == MaxPrefixes:
        print(f"Fetched {prefixes}, stopping here...")
        break

    prefixes += 1
    prefix = prefix.get('Prefix')
    print(f"Fetching prefix {prefix}")
    prefix_paginator = client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix, PaginationConfig={'MaxItems': MaxItems})
    for page in page_iterator:
        for item in page['Contents']:

            log = {
                "last_modified": item["LastModified"].isoformat(),
                "size": item["Size"],
                "bucket": bucket_name,
                "key": item["Key"],
                "short_message": "Hi!",
            }

            if "." in item["Key"]:
                extension = item["Key"].split(".")[-1]
                if "_" not in extension and "/" not in extension and not "-" in extension and not "." in extension:
                    log['file_extension'] = extension

            if item["StorageClass"] != "STANDARD":
                log['storage_class'] = item["StorageClass"]
                print(f"Found storage_class {log['storage_class']} in {item['Key']}")


            files += 1
            url = "http://"+bucket_name+".s3.amazonaws.com/"+urllib.parse.quote(item["Key"])
            logger.info(url, extra=log)

# Obsolete due to fetching root files
# if files == 0:
#     print("Prefix fetch yielded no results, using simple iteration now")
#     paginator = client.get_paginator('list_objects_v2')
#     page_iterator = paginator.paginate(Bucket=bucket_name, PaginationConfig={'MaxItems': MaxItems})
#     for page in page_iterator:
#         for item in page['Contents']:
#             log = {
#                 "last_modified": item["LastModified"].isoformat(),
#                 "size": item["Size"],
#                 "bucket": bucket_name,
#                 "key": item["Key"],
#                 "short_message": "Hi!",
#             }

#             if "." in item["Key"]:
#                 log['file_extension'] = item["Key"].split(".")[-1]

#             files += 1
#             url = "http://"+bucket_name+".s3.amazonaws.com/"+urllib.parse.quote(item["Key"])
#             logger.info(url, extra=log)

print(f"Fetched {files} files")

