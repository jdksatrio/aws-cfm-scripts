import boto3
import pandas as pd

def check_bucket_lifecycle(bucket_name):
    try:
        # Get the lifecycle configuration for the bucket
        response = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
        lifecycle_rules = response.get('Rules', [])
        
        # Get the bucket size
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        size_bytes = sum(obj['Size'] for obj in response.get('Contents', []))
        size_gb = size_bytes / (1024 * 1024 * 1024)
        
        # Check if the bucket has a lifecycle configuration
        if lifecycle_rules:
            return {"Bucket": bucket_name, "Lifecycle configuration": lifecycle_rules, "Storage (GB)": size_gb}
        else:
            return {"Bucket": bucket_name, "Lifecycle configuration": None, "Storage (GB)": size_gb}
    except Exception as e:
        return {"Bucket": bucket_name, "Lifecycle configuration": None, "Storage (GB)": None}

# Create an S3 client
s3_client = boto3.client('s3')

# Get a list of all S3 buckets in your account
buckets = [bucket['Name'] for bucket in s3_client.list_buckets()['Buckets']]

# Loop through each bucket and check lifecycle configuration
results = []
for bucket in buckets:
    result = check_bucket_lifecycle(bucket)
    results.append(result)

# Convert results to DataFrame
df = pd.DataFrame(results)
df.to_csv('s3-lifecycle-list.csv')


# Display DataFrame
print(df)
