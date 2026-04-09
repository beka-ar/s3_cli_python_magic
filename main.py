
import argparse
import magic
import boto3
import os
import sys
from dotenv import load_dotenv
from botocore.exceptions import ClientError, NoCredentialsError

# Load .env file. Boto3 will automatically pick up the credentials from the environment.
load_dotenv()

def get_content_type(file_path):
    mime = magic.Magic(mime=True)
    content_type = mime.from_file(file_path)
    return content_type

def upload_to_s3(file_path, bucket_name):
    # File existence checks
    if not os.path.exists(file_path):
        print(f"Error: File not found at path: {file_path}")
        return
    if not os.path.isfile(file_path):
        print(f"Error: Path provided is a directory, not a file: {file_path}")
        return

    try:
        # Let boto3 find credentials from the environment (loaded by load_dotenv)
        s3 = boto3.client('s3')
        # Make a test call to verify credentials early
        s3.list_buckets()
    except NoCredentialsError:
        print("Error: AWS credentials not found.")
        print("Please make sure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set in your .idx/week4/.env file.")
        sys.exit(1)
    except ClientError as e:
        # Catch specific credential errors from the test call
        if e.response['Error']['Code'] == 'InvalidAccessKeyId':
            print("FATAL: The AWS Access Key ID you provided does not exist in AWS records.")
            print("Please go to your AWS IAM console, generate a new key, and update your .idx/week4/.env file.")
        elif e.response['Error']['Code'] == 'SignatureDoesNotMatch':
            print("FATAL: The AWS Secret Access Key is incorrect.")
            print("Please double-check the AWS_SECRET_ACCESS_KEY in your .idx/week4/.env file.")
        else:
            print(f"An unexpected AWS client error occurred: {e}")
        sys.exit(1)

    content_type = get_content_type(file_path)
    folder = content_type.split('/')[0]
    file_name = os.path.basename(file_path)
    s3_key = f"{folder}/{file_name}"

    print(f"Uploading {file_name} to s3://{bucket_name}/{s3_key}")

    try:
        s3.upload_file(file_path, bucket_name, s3_key)
        print("Upload successful!")
    except ClientError as e:
        print(f"Failed to upload file to S3: {e}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload files to S3 based on content type.")
    parser.add_argument("file_path", help="The path to the file to upload.")
    parser.add_argument("bucket_name", help="The name of the S3 bucket.")

    args = parser.parse_args()

    upload_to_s3(args.file_path, args.bucket_name)
