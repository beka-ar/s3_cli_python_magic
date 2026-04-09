
# s3_cli.py

import typer
import boto3
import magic
import os
import json
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from typing import List

# Load environment variables from .env file
load_dotenv()

app = typer.Typer(
    help="A comprehensive CLI tool for S3 file uploads and version management.",
    rich_markup_mode="rich",
)

# --- UTILITY FUNCTIONS ---

def get_s3_client():
    """Initializes and verifies the S3 client, handling credentials and common errors."""
    try:
        s3 = boto3.client('s3')
        # Verify credentials by making a lightweight API call
        s3.list_buckets()
        return s3
    except NoCredentialsError:
        typer.secho("Fatal: AWS credentials not found.", fg=typer.colors.RED)
        typer.echo("Please ensure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are in your .env file.")
        raise typer.Exit(code=1)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == 'InvalidAccessKeyId':
            typer.secho("Fatal: The AWS Access Key ID is invalid.", fg=typer.colors.RED)
        elif error_code == 'SignatureDoesNotMatch':
            typer.secho("Fatal: The AWS Secret Access Key is incorrect.", fg=typer.colors.RED)
        else:
            typer.secho(f"An unexpected AWS client error occurred: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

# --- CLI COMMANDS ---

@app.command()
def upload(
    file_path: str = typer.Argument(..., help="The path to the local file to upload."),
    bucket: str = typer.Option(..., "--bucket", "-b", help="The S3 bucket name.")
):
    """
    Uploads a file to a folder in an S3 bucket named after the file's type (e.g., 'image').
    """
    if not os.path.exists(file_path):
        typer.secho(f"Error: File not found at path: {file_path}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    s3 = get_s3_client()

    mime = magic.Magic(mime=True)
    content_type = mime.from_file(file_path)
    file_type = content_type.split('/')[0]
    file_name = os.path.basename(file_path)

    s3_key = f"{file_type}/{file_name}"

    typer.echo(f"File type determined as: [bold]{file_type}[/bold]")
    typer.echo(f"Uploading [cyan]{file_path}[/cyan] to [yellow]s3://{bucket}/{s3_key}[/yellow]...")

    try:
        s3.upload_file(file_path, bucket, s3_key)
        typer.secho("\nUpload successful!", fg=typer.colors.GREEN)
    except ClientError as e:
        typer.secho(f"Error during upload: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

@app.command()
def status(
    bucket: str = typer.Option(..., "--bucket", "-b", help="The S3 bucket name to check.")
):
    """Checks if versioning is enabled on an S3 bucket."""
    s3 = get_s3_client()
    try:
        versioning_status = s3.get_bucket_versioning(Bucket=bucket)
        status = versioning_status.get('Status', 'Not Enabled')
        typer.echo(f"Versioning status for bucket '{bucket}': [bold {'green' if status == 'Enabled' else 'red'}]{status}[/bold]")
    except ClientError as e:
        typer.secho(f"Error checking bucket status: {e}", fg=typer.colors.RED)

@app.command(name="list-versions")
def list_versions(
    bucket: str = typer.Option(..., "--bucket", "-b", help="The S3 bucket name."),
    object_key: str = typer.Argument(..., help="The full path (key) of the file in the bucket.")
):
    """Lists all versions of a specific file in an S3 bucket."""
    s3 = get_s3_client()
    try:
        results = s3.list_object_versions(Bucket=bucket, Prefix=object_key)
        versions = results.get('Versions', [])
        if not versions:
            typer.secho(f"No versions found for '{object_key}'.", fg=typer.colors.YELLOW)
            return

        typer.echo(f"Versions for [cyan]{object_key}[/cyan]:")
        for v in versions:
            is_latest = "(latest)" if v['IsLatest'] else ""
            typer.echo(
                f"  - Version ID: {v['VersionId']}\n"
                f"    Last Modified: {v['LastModified']}\n"
                f"    Size: {v['Size']} bytes {is_latest}"
            )
    except ClientError as e:
        typer.secho(f"Error listing versions: {e}", fg=typer.colors.RED)

@app.command(name="restore-previous")
def restore_previous(
    bucket: str = typer.Option(..., "--bucket", "-b", help="The S3 bucket name."),
    object_key: str = typer.Argument(..., help="The full path (key) of the file to restore.")
):
    """Restores the second-to-last version of a file as the new, current version."""
    s3 = get_s3_client()
    try:
        versions = s3.list_object_versions(Bucket=bucket, Prefix=object_key).get('Versions', [])
        if len(versions) < 2:
            typer.secho("Error: At least two versions are required to restore.", fg=typer.colors.RED)
            raise typer.Exit(code=1)

        # The API returns versions sorted by LastModified date, latest first.
        previous_version = versions[1]
        version_id_to_restore = previous_version['VersionId']

        typer.echo(f"Restoring version [bold]{version_id_to_restore}[/bold]...")

        s3.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': object_key, 'VersionId': version_id_to_restore},
            Key=object_key
        )
        typer.secho("Successfully restored previous version.", fg=typer.colors.GREEN)

    except ClientError as e:
        typer.secho(f"An error occurred during restore: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

@app.command()
def clean(
    bucket: str = typer.Option(..., "--bucket", "-b", help="The S3 bucket name."),
    object_keys: List[str] = typer.Argument(..., help="A list of file paths (S3 object keys) to clean."),
    months_old: int = typer.Option(6, "--months", "-m", help="Delete versions older than this many months.")
):
    """Deletes object versions older than a specified number of months."""
    s3 = get_s3_client()
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=months_old * 30)

    typer.echo(f"Searching for versions older than {cutoff_date.strftime('%Y-%m-%d')}...")

    versions_to_delete = []
    for key in object_keys:
        try:
            versions = s3.list_object_versions(Bucket=bucket, Prefix=key).get('Versions', [])
            for v in versions:
                if not v['IsLatest'] and v['LastModified'] < cutoff_date:
                    versions_to_delete.append({'Key': key, 'VersionId': v['VersionId']})
        except ClientError as e:
            typer.secho(f"Could not process '{key}': {e}", fg=typer.colors.RED)
            continue

    if not versions_to_delete:
        typer.secho("No old versions found to delete.", fg=typer.colors.GREEN)
        return

    typer.echo(f"Found {len(versions_to_delete)} old versions to delete. Proceeding...")

    try:
        response = s3.delete_objects(
            Bucket=bucket,
            Delete={'Objects': versions_to_delete}
        )
        deleted = response.get('Deleted', [])
        errors = response.get('Errors', [])

        if deleted:
            typer.secho(f"Successfully deleted {len(deleted)} versions.", fg=typer.colors.GREEN)
        if errors:
            typer.secho(f"Could not delete {len(errors)} versions:", fg=typer.colors.RED)
            for err in errors:
                typer.echo(f"  - Key: {err['Key']}, VersionId: {err['VersionId']}, Message: {err['Message']}")

    except ClientError as e:
        typer.secho(f"An error occurred during deletion: {e}", fg=typer.colors.RED)

@app.command(name="host-static-site")
def host_static_site(
    file_path: str = typer.Argument(..., help="The path to the local file to host (e.g., index.html)."),
    bucket: str = typer.Option(..., "--bucket", "-b", help="The S3 bucket name.")
):
    """
    Uploads a file and configures the S3 bucket for static website hosting.
    """
    if not os.path.exists(file_path):
        typer.secho(f"Error: File not found at path: {file_path}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    s3 = get_s3_client()
    file_name = os.path.basename(file_path)

    # 1. Upload the file
    typer.echo(f"Uploading [cyan]{file_path}[/cyan] to [yellow]s3://{bucket}/{file_name}[/yellow]...")
    try:
        s3.upload_file(file_path, bucket, file_name, ExtraArgs={'ContentType': 'text/html'})
        typer.secho("Upload successful!", fg=typer.colors.GREEN)
    except ClientError as e:
        typer.secho(f"Error during upload: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    # 2. Configure bucket for static website hosting
    typer.echo(f"Configuring bucket [yellow]{bucket}[/yellow] for static website hosting...")
    try:
        website_configuration = {
            'ErrorDocument': {'Key': file_name},
            'IndexDocument': {'Suffix': file_name},
        }
        s3.put_bucket_website(Bucket=bucket, WebsiteConfiguration=website_configuration)
        typer.secho("Bucket configured successfully!", fg=typer.colors.GREEN)
    except ClientError as e:
        typer.secho(f"Error configuring bucket website: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)
        
    # 3. Set a public read bucket policy
    typer.echo(f"Setting public read policy on bucket [yellow]{bucket}[/yellow]...")
    try:
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "PublicReadGetObject",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{bucket}/*"
                }
            ]
        }
        s3.put_bucket_policy(Bucket=bucket, Policy=json.dumps(policy))
        typer.secho("Bucket policy set successfully!", fg=typer.colors.GREEN)
    except ClientError as e:
        typer.secho(f"Error setting bucket policy: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)


    # 4. Display the public URL
    try:
        location = s3.get_bucket_location(Bucket=bucket)['LocationConstraint']
        # For us-east-1, the location constraint is None.
        if location is None:
            region = 'us-east-1'
            website_url = f"http://{bucket}.s3-website-{region}.amazonaws.com"
        else:
            website_url = f"http://{bucket}.s3-website.{location}.amazonaws.com"
        
        typer.echo("\n" + "="*40)
        typer.secho("  Static Website URL:", fg=typer.colors.CYAN, bold=True)
        typer.echo(f"  [link={website_url}]{website_url}[/link]")
        typer.echo("="*40 + "\n")

    except ClientError as e:
        typer.secho(f"Could not determine bucket location: {e}", fg=typer.colors.RED)

@app.command(name="allow-public-access")
def allow_public_access(
    bucket: str = typer.Option(..., "--bucket", "-b", help="The S3 bucket name.")
):
    """
    Disables the public access block for a given S3 bucket.
    """
    s3 = get_s3_client()
    typer.echo(f"Disabling public access block for bucket [yellow]{bucket}[/yellow]...")
    try:
        s3.put_public_access_block(
            Bucket=bucket,
            PublicAccessBlockConfiguration={
                'BlockPublicAcls': False,
                'IgnorePublicAcls': False,
                'BlockPublicPolicy': False,
                'RestrictPublicBuckets': False
            },
        )
        typer.secho("Public access block disabled successfully!", fg=typer.colors.GREEN)
    except ClientError as e:
        typer.secho(f"Error disabling public access block: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

if __name__ == "__main__":
    app()
