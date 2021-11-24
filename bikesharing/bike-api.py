import pandas as pd
import boto3
import botocore
import io
import json
import os
import requests
import time
import re
import pytz
  
from botocore.client import Config
from datetime import datetime

from prometheus_client import multiprocess
from prometheus_client import CollectorRegistry, Counter, Summary, REGISTRY, generate_latest,CONTENT_TYPE_LATEST

#config
# INTERVAL='1h'

API_USERNAME = os.environ.get('API_USERNAME')
API_PASSWORD = os.environ.get('API_PASSWORD')
S3_ENDPOINT = os.environ.get('S3_ENDPOINT')
S3_ACCESS_KEY = os.environ.get('S3_ACCESS_KEY')
S3_SECRET_KEY = os.environ.get('S3_SECRET_KEY')
S3_BUCKET = os.environ.get('S3_BUCKET')

#static setup 
ENDPOINT_BIKE="https://os.smartcommunitylab.it/core.mobility/bikesharing/{0}"
CITIES=['trento', 'rovereto', 'pergine_valsugana', 'lavis', 'mezzocorona', 'mezzolombardo', 'sanmichelealladige']
PARTITIONED=True

#prometheus metrics
REQUEST_TIME = Summary('bike_api_request_processing_seconds', 'Time spent processing request')
COUNTER_TOTAL = Counter('bike_api_total', 'Number of data frames read')


def read_df_from_url(url, context):
    context.logger.info("read from "+url)
    response = requests.get(url)
    context.logger.info("response code "+str(response.status_code))
    if(response.status_code == 200):
        return pd.read_json(io.BytesIO(response.content), orient='records')
    else:
        return pd.DataFrame()


def metrics(context, event):
    context.logger.info('called metrics')
    #use multiprocess metrics otherwise data collected from different processors is not included
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    output = generate_latest(registry).decode('UTF-8')

    return context.Response(body=output,
        headers={},
        content_type=CONTENT_TYPE_LATEST,
        status_code=200)       

def handler(context, event):
    try:
        # check if metrics called
        if event.trigger.kind == 'http' and event.method == 'GET':
            if event.path == '/metrics':
                return metrics(context, event)
            else:
                return context.Response(body='Error not supported',
                        headers={},
                        content_type='text/plain',
                        status_code=405)  
        else:
            return process(context, event)

        
    except Exception as e:
        context.logger.error('Error: '+str(e))        
        return context.Response(body='Error '+str(e),
                        headers={},
                        content_type='text/plain',
                        status_code=500)   
 
@REQUEST_TIME.time()
def process(context, event): 
    #params - expect json
    cities = CITIES
    
    if(event.content_type == 'application/json'):
        if 'city' in event.body:
            cities = [event.body['city']]

    else:
        citystring = event.body.decode('utf-8').strip()
        if citystring:
            cities = [citystring]
           

    context.logger.info('fetch data from API')
    
    now = datetime.today().astimezone(pytz.UTC)
    day = now.strftime('%Y%m%d')
    hour = now.strftime('%H')

    # init counter for gauge
    df_total = 0

    #init s3 client
    s3 = boto3.client('s3',
                    endpoint_url=S3_ENDPOINT,
                    aws_access_key_id=S3_ACCESS_KEY,
                    aws_secret_access_key=S3_SECRET_KEY,
                    config=Config(signature_version='s3v4'),
                    region_name='us-east-1')    


    for city in cities:
        context.logger.info('fetch data for city '+city)
        if(PARTITIONED):
            daypath = 'year='+now.strftime('%Y')+'/month='+now.strftime('%m')+'/day='+now.strftime('%d')+'/hour='+now.strftime('%H')
            filename='city={}/{}/bike-{}-{}'.format(city,daypath,city,now.strftime('%Y%m%dT%H%M%S'))
        else:
            filename='{}/{}/{}/bike-{}-{}'.format(city,day,hour,city,now.strftime('%Y%m%dT%H%M%S'))

        #load traffic
        context.logger.info('read bikes for '+city)
        df = read_df_from_url(ENDPOINT_BIKE.format(city), context)
        df_count = len(df)
        context.logger.info('num read '+str(df_count))            

        if(df_count > 0):
            # add to total
            df_total = df_total + df_count
           
            # extract coordinates
            df[['latitude','longitude']] = pd.DataFrame(df.position.tolist(), columns=['latitude', 'longitude'])

            # add time column with now as value
            df['timestamp'] = now

            #rename and drop columns
            df = df[['timestamp','id','name','address','bikes','slots','totalSlots','latitude','longitude']]

            if( not PARTITIONED):
                # add city column
                df['city'] = city

            # write to io buffer
            context.logger.info('write parquet to buffer')
            parquetio = io.BytesIO()
            df.to_parquet(parquetio, engine='pyarrow',allow_truncated_timestamps=True)
            # seek to start otherwise upload will be 0
            parquetio.seek(0)

            context.logger.info('upload to s3 as '+filename+'.parquet')
            s3.upload_fileobj(parquetio, S3_BUCKET, filename+'.parquet')

            # cleanup
            del parquetio
            
        del df
    #end cities loop

    #add total to counter
    COUNTER_TOTAL.inc(df_total)

    context.logger.info('done.')

    return context.Response(body='Done. Read at '+str(now)+' for cities '+str(cities),
                            headers={},
                            content_type='text/plain',
                            status_code=200)



