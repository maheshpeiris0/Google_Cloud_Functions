import pandas as pd
from google.cloud import storage

def hello_world(request):
    google_cloud_storage_bucket = 'Bucket-Name'
    selected_type = '2023-01' # searching word
    save_bucket_path = 'location to save (bucket path)'
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(google_cloud_storage_bucket)
    blobs = bucket.list_blobs()
    file_list= []
    for blob in blobs:
        file_list.append(blob.name)
    selected_file = [file for file in file_list if selected_type in file]
    full_file_path = ["gs://"+google_cloud_storage_bucket+"/"+file for file in selected_file]
    dfs = [pd.read_parquet(file) for file in full_file_path]
    df = pd.concat(dfs, ignore_index=True)
    df.to_parquet(save_bucket_path)
    return f'Files saved in {save_bucket_path}'
