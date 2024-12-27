# s3-Analysis
tooling for bucket analysis

# What is in here?
1) There is a script to ingest the index of an open (indexing without auth) s3 bucket into Graylog. This script does not download the files. It only grabs all keys and pushes each of them as one log into Graylog. For huge buckets you might need to adjust some of the variables to index everything.
2) There is a content pack for Graylog (Version 6.1) which will allow you to analyze the bucket by filetype, size, timeframe and other keywords. The Graylog UI will be the tool to search and understand the buckt in reasonable time to estimate if the bucket does contain relevant data.


