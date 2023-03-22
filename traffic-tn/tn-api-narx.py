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
  
from requests.auth import HTTPBasicAuth
from botocore.client import Config
from datetime import datetime
from datetime import timedelta    

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
INTERVAL = os.environ.get('INTERVAL')

#static setup 
ENDPOINT_TRAFFIC="https://tn.smartcommunitylab.it/trento.mobilitydatawrapper/traffic/{0}/{1}/{2}/{3}"
ENDPOINT_POSITIONS="https://tn.smartcommunitylab.it/trento.mobilitydatawrapper/positions/{0}"
AGGREGATE="By5m"
AGGREGATE_TIME=5*60*1000
TYPE=['Narx','RDT','Spot']

#prometheus metrics
REQUEST_TIME = Summary('tn_api_request_processing_seconds', 'Time spent processing request')
COUNTER_NARX = Counter('tn_api_narx', 'Number of Narx data frames read')
COUNTER_RDT = Counter('tn_api_rdt', 'Number of RDT data frames read')
COUNTER_SPOT = Counter('tn_api_spot', 'Number of Spot data frames read')
COUNTER_TOTAL = Counter('tn_api_total', 'Number of data frames read')


def read_df_from_url(url, context):
    context.logger.info("read from "+url)
    response = requests.get(url, auth=HTTPBasicAuth(API_USERNAME, API_PASSWORD))
    context.logger.info("response code "+str(response.status_code))
    if(response.status_code == 200):
        return pd.read_json(io.BytesIO(response.content), orient='records')
    else:
        return pd.DataFrame()



timeregex = re.compile(r'^((?P<days>[\.\d]+?)d)?((?P<hours>[\.\d]+?)h)?((?P<minutes>[\.\d]+?)m)?((?P<seconds>[\.\d]+?)s)?$')

def parse_time(time_str):
    """
    Parse a time string e.g. (2h13m) into a timedelta object.

    :param time_str: A string identifying a duration.  (eg. 2h13m)
    :return datetime.timedelta: A datetime.timedelta object
    """
    parts = timeregex.match(time_str)
    assert parts is not None, "Could not parse any time information from '{}'.  Examples of valid strings: '8h', '2d8h5m20s', '2m4s'".format(time_str)
    time_params = {name: float(param) for name, param in parts.groupdict().items() if param}
    return timedelta(**time_params)            


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
    message = {}
    datatypes = TYPE
    interval = INTERVAL

    #default dates
    now = datetime.today().astimezone(pytz.UTC)
    date_start = None
    date_end = None

    # parse event as json
    if(event.content_type == 'application/json'):
        message = event.body
    else:
        jsstring = event.body.decode('utf-8').strip()
        if jsstring:
            message = json.loads(jsstring)

    if 'date_start' in message:
        date_start =  datetime.fromisoformat(message['start'])
    if 'date_end' in message:
        date_end =  datetime.fromisoformat(message['end'])
    if 'type' in message:
        datatypes = [message['type']]
    if 'interval' in message:
        interval = message['interval']

    # build interval from params
    if date_end is None:
        date_end = now.replace(minute=00,second=00, microsecond=00)

    if date_start is None:
        #derive interval
        interval_delta = parse_time(interval)
        # get interval as minutes for mapping function
        interval_minutes = (interval_delta.seconds//60)%60
        date_start = date_end - interval_delta    
        #subtract 1s from end to avoid overlap
        date_end = date_end - timedelta(seconds=1)
            

    context.logger.info('fetch data from API for interval '+str(date_start)+' => '+str(date_end))

    # calculate date interval as ms for API call
    time_start = int(datetime.timestamp(date_start)*1000)
    time_end = int(datetime.timestamp(date_end)*1000)
    day = date_start.strftime('%Y%m%d')

    # init counter for gauge
    df_total = 0

    #init s3 client
    s3 = boto3.client('s3',
                    endpoint_url=S3_ENDPOINT,
                    aws_access_key_id=S3_ACCESS_KEY,
                    aws_secret_access_key=S3_SECRET_KEY,
                    config=Config(signature_version='s3v4'),
                    region_name='us-east-1')    


    for datatype in datatypes:
        context.logger.info('fetch data for type '+datatype)
        filename='{}/{}/{}/traffic-{}_{}-{}-{}-{}'.format(
            interval,datatype,day,datatype,AGGREGATE,interval,
            date_start.strftime('%Y%m%dT%H%M%S'),date_end.strftime('%Y%m%dT%H%M%S'))

        #check if files already exist
        exists = False
        try:
            s3.head_object(Bucket=S3_BUCKET, Key=filename+'.parquet')
            exists = True
        except botocore.exceptions.ClientError:
            # Not found
            exists = False
            pass


        if not exists:
            #load positions
            context.logger.info('read positions...')
            df_positions = read_df_from_url(ENDPOINT_POSITIONS.format(datatype), context)
            context.logger.info('num positions '+str(len(df_positions)))
            if(len(df_positions) > 0):
                df_positions[['latitude','longitude']] = pd.DataFrame(df_positions.coordinates.tolist(), columns=['latitude', 'longitude'])
                df_positions['place'] = df_positions['place'].str.strip()
            #load traffic
            context.logger.info('read traffic for '+datatype)
            df_traffic = read_df_from_url(ENDPOINT_TRAFFIC.format(datatype,AGGREGATE,time_start,time_end), context)
            df_count = len(df_traffic)
            context.logger.info('num traffic '+str(df_count))            

            if(df_count > 0):
                # add to total
                df_total = df_total + df_count

                #increment specific counter for datatype
                if datatype == 'Narx':
                    COUNTER_NARX.inc(df_count)
                elif datatype == 'RDT':
                    COUNTER_RDT.inc(df_count)
                elif datatype == 'Spot':
                    COUNTER_SPOT.inc(df_count)
                else:
                    context.logger.info("No metrics for datatype "+datatype)

                #remove datatype from places with regex: anything between []
                df_traffic['place'].replace(regex=True, inplace=True, to_replace="\[(.*)\] ", value="")
                df_traffic['place'] = df_traffic['place'].str.strip()

                #merge with positions
                df = pd.merge(df_traffic, df_positions, on='place')

                #sort by timestamp/place
                df.sort_values(['time','place'], inplace=True)

                #calculate interval from timestamp used for aggregation
                df.rename(columns={'time':'time_end'}, inplace=True)
                df['time_start'] = df['time_end']-AGGREGATE_TIME

                #rename and drop columns
                df = df[['time_start','time_end','place','station','value','latitude','longitude']]

                # write to io buffer
                context.logger.info('write parquet to buffer')
                parquetio = io.BytesIO()
                df.to_parquet(parquetio, engine='pyarrow')
                # seek to start otherwise upload will be 0
                parquetio.seek(0)

                context.logger.info('upload to s3 as '+filename+'.parquet')
                s3.upload_fileobj(parquetio, S3_BUCKET, filename+'.parquet')

                # cleanup
                del df
                del parquetio
            
            del df_positions
            del df_traffic

    #end types loop

    #add total to counter
    COUNTER_TOTAL.inc(df_total)

    context.logger.info('done.')

    return context.Response(body='Done. Interval '+str(date_start)+' to '+str(date_end)+' for types '+str(datatypes),
                            headers={},
                            content_type='text/plain',
                            status_code=200)


