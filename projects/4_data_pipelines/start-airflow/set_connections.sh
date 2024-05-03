#!/bin/bash
# udacity-project4
# TO-DO: run the follwing command and observe the JSON output: 
airflow connections get aws_credentials -o json 
airflow connections add aws_credentials --conn-uri 'aws://XXXX:XXXX@'

# user: admin
# pass: XXXX
# TO-DO: run the follwing command and observe the JSON output: 
# airflow connections get redshift -o json
airflow connections get redshift -o json 
airflow connections add redshift --conn-uri 'redshift://admin:XXXX@default-workgroup.517611425250.us-east-1.redshift-serverless.amazonaws.com:5439/dev'
#
# TO-DO: update the following bucket name to match the name of your S3 bucket and un-comment it:
airflow variables set s3_stage_bucket fuong-cao
airflow variables set s3_stage_events_key log-data
airflow variables set s3_json_format_file log_json_path.json
airflow variables set s3_stage_songs_key song-data
