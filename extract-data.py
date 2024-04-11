import boto3
import pyarrow.parquet as pq
from io import BytesIO

s3 = boto3.client('s3', aws_access_key_id='your id', aws_secret_access_key='your key', region_name='us-east-1')

bucket_name = 'demo-parquet-dex'
bsc_key='bsc/2020-09-12_425000_7BE703446B28941F_5000.parquet'

bsc_local_path = '/Users/divyasshree/Downloads/2020-09-12_425000_7BE703446B28941F_5000.parquet'
file_obj = s3.download_file(bucket_name, bsc_key, bsc_local_path)


compressed_data = open(bsc_local_path, 'rb').read()


buffer = BytesIO(compressed_data)


parquet_file = pq.ParquetFile(buffer)

print('parquet_file read')

start_row = 0
end_row = 999


df = parquet_file.read_row_group(0)

print(df)
