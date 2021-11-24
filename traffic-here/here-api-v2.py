import pandas as pd
import boto3
import botocore
import io
import json
import os
import requests
import base64
  
from pandas.io.json import json_normalize
from botocore.client import Config
from datetime import datetime

from prometheus_client import multiprocess
from prometheus_client import CollectorRegistry, Counter, Summary, REGISTRY, generate_latest,CONTENT_TYPE_LATEST

#config
S3_ENDPOINT=os.environ.get('S3_ENDPOINT')
S3_ACCESS_KEY=os.environ.get('S3_ACCESS_KEY')
S3_SECRET_KEY=os.environ.get('S3_SECRET_KEY')
S3_BUCKET=os.environ.get('S3_BUCKET')

HERE_APP_ID=os.environ.get('HERE_APP_ID')
HERE_APP_CODE=os.environ.get('HERE_APP_CODE')
HERE_ENDPOINT="https://traffic.api.here.com/traffic/6.2/flow.json"

BPOINTS_PARQUET=os.environ.get('BPOINTS_PARQUET')
BBOX=os.environ.get('BBOX')
PARTITIONED=True

#prometheus metrics
COUNTER_SEGMENTS = Counter('here_api_segments', 'Number of SEGMENTS data frames read')
COUNTER_TMC = Counter('here_api_tmc', 'Number of TMC data frames read')
COUNTER_OUT = Counter('here_api_df', 'Number of TMC data frames filtered')
REQUEST_TIME = Summary('here_api_request_processing_seconds', 'Time spent processing request')

# def init_context(context):
#     context.logger.info('init')


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


def read_json_from_url(url,params):
    response = requests.get(url,params=params)
    if(response.status_code == 200):
        return response.json()
    else:
        raise Exception('Response error, status code {}'.format(response.status_code)) 
        
        
def grouped_weighted_average(self, values, weights, *groupby_args, **groupby_kwargs):
    """
    :param values: column(s) to take the average of
    :param weights_col: column to weight on
    :param group_args: args to pass into groupby (e.g. the level you want to group on)
    :param group_kwargs: kwargs to pass into groupby
    :return: pandas.Series or pandas.DataFrame
    """

    if isinstance(values, str):
        values = [values]

    ss = []
    for value_col in values:
        df = self.copy()
        prod_name = 'prod_{v}_{w}'.format(v=value_col, w=weights)
        weights_name = 'weights_{w}'.format(w=weights)

        df[prod_name] = df[value_col] * df[weights]
        df[weights_name] = df[weights].where(~df[prod_name].isnull())
        df = df.groupby(*groupby_args, **groupby_kwargs).sum()
        s = df[prod_name] / df[weights_name]
        s.name = value_col
        ss.append(s)
    df = pd.concat(ss, axis=1) if len(ss) > 1 else ss[0]
    return df     


#extend pd
pd.DataFrame.grouped_weighted_average = grouped_weighted_average


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
    #init client
    s3 = boto3.client('s3',
                    endpoint_url=S3_ENDPOINT,
                    aws_access_key_id=S3_ACCESS_KEY,
                    aws_secret_access_key=S3_SECRET_KEY,
                    config=Config(signature_version='s3v4'),
                    region_name='us-east-1')

    params={
        'bbox':BBOX,
        'app_id':HERE_APP_ID,
        'app_code':HERE_APP_CODE,
        'ts': 'true'
        }

    #encode bbox as base64 
    bbox64 = str(base64.b64encode(BBOX.encode("utf-8")), "utf-8")
    filename='traffic-bbox-'+bbox64

    context.logger.info("call HERE api for data for "+filename)

    try:        
        res = read_json_from_url(HERE_ENDPOINT,params)
        timestamp = str(res['CREATED_TIMESTAMP'])
        date=datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f%z')
        context.logger.info('received data for '+str(timestamp))

        list = []
        #read data as json
        rws = res['RWS']
        for r in rws:
            for s in r['RW']:
                for q in s['FIS']:
                    for v in q['FI']:
                        le = v['TMC']['LE']
                        pc = v['TMC']['PC']
                        de = v['TMC']['DE']
                        qd = v['TMC']['QD']
                        for z in v['CF']:
                            if 'SU' in z:
                                su = z['SU']
                            else:
                                su = z['SP']
                                
                            entry = {
                            "timestamp": str(timestamp),
                            "tmc_id":  pc,
                            "description": de,
                            "queue_direction": str(qd),
                            "length": le,         
                            "speed": z['SP'],
                            "speed_uncapped": su,                        
                            "free_flow_speed": z['FF'],
                            "jam_factor": z['JF'],
                            "confidence_factor": z['CN'],
                            "traversability_status": z['TS']
                            }
                            list.append(entry)
                        #end z
                    #end v
                #end q
            #end s
        #end r

        if(PARTITIONED):
            path = 'year='+date.strftime('%Y')+'/month='+date.strftime('%m')+'/day='+date.strftime('%d')+'/hour='+date.strftime('%H')+'/'
        else:            
            path = date.strftime('%Y%m%d')+"/"+date.strftime('%H')+"/"
        
        filename = filename + "-"+timestamp        

        #check if raw files already exist
        exists = False
        try:
            s3.head_object(Bucket=S3_BUCKET, Key='raw/'+path+filename+'.parquet')
            exists = True
        except botocore.exceptions.ClientError:
            # Not found
            exists = False
            pass


        if exists:
            context.logger.info('files for '+filename+' already exists in bucket, skip.')
            return context.Response(body='File already exists',
                    headers={},
                    content_type='text/plain',
                    status_code=200)            

        context.logger.info('read results in dataframe')
        df = pd.DataFrame(list, columns=["timestamp",
                    "tmc_id",
                    "description",
                    "queue_direction",
                    "length",      
                    "speed",
                    "speed_uncapped",                
                    "free_flow_speed",
                    "jam_factor",
                    "confidence_factor",
                    "traversability_status"])

        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%dT%H:%M:%S.%f%z')
        # force as strings, usually not needed
        #df['description'] = df['description'].astype(str)
        #df['queue_direction'] = df['queue_direction'].astype(str)

        #derive boolean flags 
        df['direction'] = df['queue_direction'].apply(lambda x : True if str(x) == '+' else False)
        df['traversable'] = df['traversability_status'].apply(lambda x : True if str(x) == 'O' else False)

        # TO evaluate - add filters for confidence <0.5, status etc

        count = len(df)
        context.logger.info('read count: '+str(count))
        COUNTER_SEGMENTS.inc(count)

        context.logger.info('process sections and segments...')

        #process to pack all TMC segments into one, respecting DIRECTION
        #calculate weighted mean for measures wrt length
        gwdf = df.grouped_weighted_average(['speed','jam_factor','free_flow_speed'],'length',['timestamp','tmc_id','direction'])

        #group by
        gg = df.groupby(['timestamp','tmc_id','direction'])


        #sum length of segments for the same direction
        ldf = gg['length'].sum()

        #get min confidence factor for tmc_id 
        cdf = gg['confidence_factor'].min()

        #fetch additional data for grouped values
        adf = gg['description','queue_direction', 'traversability_status', 'traversable'].first()

        #merge and reset index to move timestamp, tmc_id as columns
        resdf = gwdf.merge(ldf, left_index=True, right_index=True).merge(cdf, left_index=True, right_index=True).merge(adf, left_index=True, right_index=True)
        resdf.reset_index(level=['timestamp','tmc_id','direction'], inplace=True)

        #parse timestamp as date
        if pd.api.types.is_object_dtype(resdf.timestamp):
            resdf['timestamp'] = pd.to_datetime(resdf['timestamp'], format='%Y-%m-%dT%H:%M:%S.%f%z')

        context.logger.info('res count: '+str(len(resdf)))
        COUNTER_TMC.inc(len(resdf))

        # write to io buffer
        context.logger.info('write parquet to buffer')

        parquetio = io.BytesIO()
        resdf.to_parquet(parquetio, engine='pyarrow')
        # seek to start otherwise upload will be 0
        parquetio.seek(0)

        context.logger.info('upload to s3 as '+filename+'.parquet')

        s3.upload_fileobj(parquetio, S3_BUCKET, 'raw/'+path+filename+'.parquet')

        #filter 
        context.logger.info('load bounding points mapping from s3 '+BPOINTS_PARQUET)

        obj = s3.get_object(Bucket=S3_BUCKET, Key=BPOINTS_PARQUET)
        dataio = io.BytesIO(obj['Body'].read())
        boundpoints = pd.read_parquet(dataio, engine='pyarrow', columns=['CID','LCD','LON','LAT'])

        context.logger.info('merge data with bound points for region...')
        mergeddf = pd.merge(resdf, boundpoints, how='inner', left_on='tmc_id', right_on='LCD')

        context.logger.info('res count: '+str(len(mergeddf)))
        COUNTER_OUT.inc(len(mergeddf))

        filename = filename + "-filtered"
        
        # write to io buffer
        context.logger.info('write parquet to buffer')

        filterio = io.BytesIO()
        mergeddf.to_parquet(filterio, engine='pyarrow')
        # seek to start otherwise upload will be 0
        filterio.seek(0)

        context.logger.info('upload to s3 as '+filename+'.parquet')

        s3.upload_fileobj(filterio, S3_BUCKET, 'filtered/'+path+filename+'.parquet')

        #cleanup
        del mergeddf
        del resdf
        del boundpoints
        del ldf
        del adf
        del gwdf
        del df
        del list

        return context.Response(body='Done',
                    headers={},
                    content_type='text/plain',
                    status_code=200)
    
    except Exception as e:
        context.logger.error('Error: '+str(e))        
        return context.Response(body='Error '+str(e),
                        headers={},
                        content_type='text/plain',
                        status_code=500)


