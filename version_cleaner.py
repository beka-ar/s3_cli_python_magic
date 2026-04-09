
import typer
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import List

# Load environment variables from .env file
load_dotenv()

app = typer.Typer(help="A CLI tool to clean up old versions of S3 objects.")

def get_s3_client():
    """Initializes and verifies the S3 client."""
    try:
        # Let boto3 find credentials from the environment
        s3 = boto3.client('s3')
        # A simple API call to verify credentials early
        s3.list_buckets()
        return s3
    except NoCredentialsError:
        typer.secho("Fatal: AWS credentials not found.", fg=typer.colors.RED)
        typer.echo("Please ensure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are in your .idx/week4/.env file.")
        raise typer.Exit(code=1)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == 'InvalidAccessKeyId':
            typer.secho("Fatal: The AWS Access Key ID is invalid.", fg=typer.colors.RED)
            typer.echo("Please generate a new key in the AWS IAM console and update your .env file.")
        elif error_code == 'SignatureDoesNotMatch':
            typer.secho("Fatal: The AWS Secret Access Key is incorrect.", fg=typer.colors.RED)
            typer.echo("Please double-check the AWS_SECRET_ACCESS_KEY in your .env file.")
        else:
            typer.secho(f"An unexpected AWS client error occurred: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

@app.command()
def clean(
    bucket: str = typer.Option(..., "--bucket", "-b", help="The S3 bucket name where the files are located."),
    object_keys: List[str] = typer.Argument(..., help="A list of file paths (S3 object keys) to clean."),
    months_old: int = typer.Option(6, "--months", "-m", help="Delete versions older than this many months.")
):
    """
    Checks for and deletes object versions older than a specified number of months.
    """
    s3 = get_s3_client()
    # Calculate the cutoff date. Any version last modified before this date will be deleted.
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=months_old * 30)
    
    typer.echo(f"Searching for versions older than {cutoff_date.strftime('%Y-%m-%d')}...")

    for key in object_keys:
        typer.secho(f"\nProcessing: s3://{bucket}/{key}", fg=typer.colors.CYAN)
        versions_to_delete = []

        try:
            # Use a paginator to handle objects with a large number of versions
            paginator = s3.get_paginator('list_object_versions')
            pages = paginator.paginate(Bucket=bucket, Prefix=key)

            for page in pages:
                # Combine normal versions and delete markers for comprehensive cleanup
                all_versions = page.get('Versions', []) + page.get('DeleteMarkers', [])
                for version in all_versions:
                    # Ensure we are only processing the specified object, not others with a similar prefix
                    if version['Key'] == key and version['LastModified'] < cutoff_date:
                        versions_to_delete.append({
                            'Key': key,
                            'VersionId': version['VersionId']
                        })
                        typer.echo(f"  - Queued for deletion: VersionId {version['VersionId']} (Created: {version['LastModified'].strftime('%Y-%m-%d')})")
            
            if not versions_to_delete:
                typer.echo(f"  No versions found older than {months_old} months.")
                continue

            # Perform a single bulk delete operation for efficiency
            typer.echo(f"  Attempting to delete {len(versions_to_delete)} old version(s)...")
            response = s3.delete_objects(
                Bucket=bucket,
                Delete={'Objects': versions_to_delete}
            )

            # Report any errors from the delete operation
            if response.get('Errors'):
                typer.secho("  Errors occurred during deletion:", fg=typer.colors.RED)
                for error in response['Errors']:
                    typer.echo(f"    - VersionId: {error.get('VersionId')}, Code: {error['Code']}, Message: {error['Message']}")
            else:
                typer.secho(f"  Successfully deleted {len(response.get('Deleted', []))} version(s).", fg=typer.colors.GREEN)

        except ClientError as e:
            if e.response['Error']['Code'] == 'AccessDenied':
                typer.secho(f"Error: Access Denied. Ensure you have 's3:ListObjectVersions' and 's3:DeleteObjectVersion' permissions for the bucket '{bucket}'.", fg=typer.colors.RED)
            else:
                 typer.secho(f"An AWS error occurred: {e}", fg=typer.colors.RED)
        except Exception as e:
            typer.secho(f"An unexpected error occurred: {e}", fg=typer.colors.RED)

if __name__ == "__main__":
    app()
