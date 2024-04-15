import os
import xarray as xr
import pandas as pd
import boto3
import json
from datetime import datetime

def lambda_handler(event, context):
    
    # Parâmetros de carga
    lat_min = float(os.environ["LATMIN"])
    lat_max = float(os.environ["LATMAX"])
    lon_min = float(os.environ["LNGMIN"])
    lon_max = float(os.environ["LNGMAX"])
    start_date = os.environ["STARTDATE"]
    end_date = os.environ["ENDDATE"]

	# Carga da origem
    sst_ds = xr.open_zarr('https://surftemp-sst.s3.us-west-2.amazonaws.com/data/sst.zarr')
    
    # Transformação para data frame
    df = sst_ds["analysed_sst"].loc[start_date:end_date].sel(lat=slice(lat_min,lat_max),lon=slice(lon_min,lon_max)).to_dataframe()
    
    s3 = boto3.client("s3")
    sqs = boto3.client("sqs")

	# Ingestão na camada 
    for date, new_df in df.groupby(level=0):
        data = new_df.droplevel(0)
        data2 = data.reset_index([0,1])
        dateStr = f"{date.strftime("%Y%m%d")}"
        jsonFile = data2.to_json(orient="records")
        fileName = f"{dateStr}.json"
        s3Response = s3.put_object(Bucket=os.environ["RAWBUCKET"], Key=fileName, Body=jsonFile)
        sqsResponse = sqs.send_message(QueueUrl=os.environ["RAWSQS"], MessageBody=dateStr)

    return {
        'statusCode': 200,
        'body': json.dumps('Step 1 finished!')
    }
