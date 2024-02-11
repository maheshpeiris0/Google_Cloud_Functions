from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timedelta

def file_search():
    google_cloud_storage_bucket = 'bucket-name' # bucket name
    save_bucket_path = 'gs://bucket/location/price_data/'  # Full path including 'gs://'
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(google_cloud_storage_bucket)

    thirty_days_ago = datetime.now() - timedelta(days=30)
    blobs = bucket.list_blobs()
    today = datetime.now()
    dates = [(today - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(30)]
    
    selected_file_list = []
    for blob in blobs:
        for date in dates:
            if date in blob.name:
                selected_file_list.append(blob.name)
                break  # Once a match is found, no need to check other dates for this blob

    # Correctly building the full file paths
    full_file_path = ["gs://" + google_cloud_storage_bucket + "/" + file for file in selected_file_list]
    
    # Assuming you have the necessary permissions and setup to read these files directly
    dfs = [pd.read_parquet(file) for file in full_file_path]
    df = pd.concat(dfs, ignore_index=True)
    
    df.to_parquet(save_bucket_path + 'aggregated_data.parquet')  # Save to the specified path
    df.rename(columns={'T':'Ticker','o':'day_open','h':'day_high','l':'day_low','c':'day_close','v':'day_volume','vw':'day_vw','t':'unix_date'}, inplace=True)
    df['datetime'] = pd.to_datetime(df['unix_date'], unit='ms', utc=True)  
    df['datetime_est'] = df['datetime'].dt.tz_convert('America/New_York')
    df['datetime_est'] = df['datetime_est'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['datetime_est'] = pd.to_datetime(df['datetime_est'])
    df.drop(columns=['unix_date','datetime','n'], inplace=True)
    df['otc'] =df['otc'].astype('str')
    client = bigquery.Client()
    table_id = "nimble-nexus-308502.test_tables.last_thirty_day__share_price"
    job_config = bigquery.LoadJobConfig(schema=[
            bigquery.SchemaField("Ticker", "STRING"),
            bigquery.SchemaField("day_open", "FLOAT"),
            bigquery.SchemaField("day_high", "FLOAT"),
            bigquery.SchemaField("day_low", "FLOAT"),
            bigquery.SchemaField("day_close", "FLOAT"),
            bigquery.SchemaField("day_volume", "FLOAT"),
            bigquery.SchemaField("day_vw", "FLOAT"),
            bigquery.SchemaField("otc", "STRING"),
            bigquery.SchemaField("datetime_est", "TIMESTAMP")
        ],write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    table = client.get_table(table_id)
    print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
                )
        )
    
    
    return f'Files saved in {save_bucket_path}'

if __name__ == '__main__':
    print(file_search())
