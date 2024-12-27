# S3 Bucket Indexing

This folder contains the neccesary code to ingest the list of files of an open AWS s3 bucket into Graylog


```
usage: indexer_prefix.py [-h] [--host HOST] [--port PORT] bucket_name

Index a s3 bucket and send it to graylog

positional arguments:
  bucket_name           The name of the bucket

options:
  -h, --help            show this help message and exit
  --host HOST           The graylog instance (default: localhost)
  --port PORT, -p PORT  The input port at the graylog (default: 12201)
```


