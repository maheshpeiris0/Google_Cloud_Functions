from google.cloud import storage
import os

def get_last_modified_file(bucket_name, prefix):
    """Gets the most recently modified file within a GCS 'folder' (prefix)."""

    # Set up GCP environment (authentication will be handled by cloud SDK)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'path/to/your/credentials.json' 

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=prefix)
    most_recent = max(blobs, key=lambda blob: blob.time_created)

    return most_recent.name, most_recent.time_created

if __name__ == "__main__":
    bucket_name = 'your-bucket-name'
    prefix = 'your-folder-prefix/'  # Include trailing slash
    last_modified_file, modified_time = get_last_modified_file(bucket_name, prefix)

    print("Last modified file:", last_modified_file)
    print("Modified time:", modified_time)
