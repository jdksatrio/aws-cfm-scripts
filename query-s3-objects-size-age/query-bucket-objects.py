import boto3
import csv
from datetime import datetime

# Initialize STS client
sts_client = boto3.client('sts')

# Initialize S3 client with temporary credentials
s3_client = boto3.client('s3', region_name='ap-southeast-3')


# Specify the bucket name
bucket_name = 'aws-glue-assets-121e52980caf-ap-southeast-3'

# List to store object metadata
object_metadata_list = []

# List all objects in the bucket
paginator = s3_client.get_paginator('list_objects_v2')
operation_parameters = {'Bucket': bucket_name}
page_iterator = paginator.paginate(**operation_parameters)

# Iterate through each object in the bucket
for page in page_iterator:
    if 'Contents' in page:
        for obj in page['Contents']:
            # Get object key (filename)
            object_key = obj['Key']
            
            # Get last modified date
            last_modified_date = obj['LastModified']
            
            # Get size in bytes
            size_bytes = obj['Size']
            
            # Convert last modified date to a readable format
            last_modified_date_str = last_modified_date.strftime('%Y-%m-%d %H:%M:%S')
            
            # Append object metadata to the list
            object_metadata_list.append([object_key, last_modified_date_str, size_bytes])

# Specify the CSV file path
csv_file_path = 's3_bucket_objects.csv'

# Write object metadata to CSV file
with open(csv_file_path, 'w', newline='') as csv_file:
    csv_writer = csv.writer(csv_file)
    
    # Write header row
    csv_writer.writerow(['Filename', 'Last Modified Date', 'Size (bytes)'])
    
    # Write object metadata rows
    csv_writer.writerows(object_metadata_list)

print(f"Object metadata exported to CSV file: {csv_file_path}")
