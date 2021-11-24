import pandas as pd
import sqlalchemy
import boto3
import botocore
import io
import json
import re
import os
import gc
  
from botocore.client import Config
from urllib.parse import unquote
from prometheus_client import multiprocess
from prometheus_client import CollectorRegistry, Counter, Summary, REGISTRY, generate_latest,CONTENT_TYPE_LATEST

#config
S3_ENDPOINT=os.environ.get('S3_ENDPOINT')
S3_ACCESS_KEY=os.environ.get('S3_ACCESS_KEY')
S3_SECRET_KEY=os.environ.get('S3_SECRET_KEY')
S3_BUCKET=os.environ.get('S3_BUCKET')
DB_HOST=os.environ.get('DB_HOST')
DB_PORT=os.environ.get('DB_PORT')
DB_NAME=os.environ.get('DB_NAME')
DB_USERNAME=os.environ.get('DB_USERNAME')
DB_PASSWORD=os.environ.get('DB_PASSWORD')
DB_SCHEMA=os.environ.get('DB_SCHEMA')
DB_TABLE=os.environ.get('DB_TABLE')
DB_TABLE_PREFIX=os.environ.get('DB_TABLE_PREFIX')
DB_COLUMN_MAP=os.environ.get('DB_COLUMN_MAP')
KEY_MATCH=os.environ.get('KEY_MATCH')
DB_CHUNKSIZE=5000

#prometheus metrics
COUNTER_FILES = Counter('postgres_insertdb_files', 'Number of files read')
COUNTER_DF = Counter('postgres_insertdb_df', 'Number of data frames processed')
REQUEST_TIME = Summary('postgres_insertdb_request_processing_seconds', 'Time spent processing request')

#global
engine = False

def get_matching_s3_objects(s3,bucket, prefix="", suffix=""):
    """
    Generate objects in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    paginator = s3.get_paginator("list_objects_v2")

    kwargs = {'Bucket': bucket}

    # We can pass the prefix directly to the S3 API.  If the user has passed
    # a tuple or list of prefixes, we go through them one by one.
    if isinstance(prefix, str):
        prefixes = (prefix, )
    else:
        prefixes = prefix

    for key_prefix in prefixes:
        kwargs["Prefix"] = key_prefix

        for page in paginator.paginate(**kwargs):
            try:
                contents = page["Contents"]
            except KeyError:
                return

            for obj in contents:
                key = obj["Key"]
                if key.endswith(suffix):
                    yield obj


def get_matching_s3_keys(s3, bucket, prefix="", suffix=""):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    for obj in get_matching_s3_objects(s3, bucket, prefix, suffix):
        yield obj["Key"]



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


def init_context(context):
    global engine
    # use sqlalchemy because it supports multi/insert with pagination
    engine = sqlalchemy.create_engine('postgresql://'+DB_USERNAME+':'+DB_PASSWORD+'@'+DB_HOST+':'+DB_PORT+'/'+DB_NAME)
    context.logger.info('engine initialized')

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
    global engine
    #params - expect json
    if(event.content_type == 'application/json'):
        msg = event.body
    else:
        jsstring = event.body.decode('utf-8').strip()

        if not jsstring:
            return context.Response(body='Error. Empty json',
                            headers={},
                            content_type='text/plain',
                            status_code=400)

        msg = json.loads(jsstring)

    context.logger.info(msg)

    #init client
    s3 = boto3.client('s3',
                    endpoint_url=S3_ENDPOINT,
                    aws_access_key_id=S3_ACCESS_KEY,
                    aws_secret_access_key=S3_SECRET_KEY,
                    config=Config(signature_version='s3v4'),
                    region_name='us-east-1')


    list = []

    # single file
    if 'bucket' in msg and 'key' in msg:
        if msg['key'].endswith('.parquet') or msg['key'].endswith('.csv'):
            entry = {
                'bucket' : msg['bucket'],
                'key' :  msg['key']
            }
            list.append(entry)

    # list of files
    if 'list' in msg:
        for r in msg['list']:
            if 'bucket' in r and 'key' in r:
                if r['key'].endswith('.parquet') or r['key'].endswith('.csv'):
                    entry = {
                        'bucket' : r['bucket'],
                        'key' :  r['key']
                    }
                    list.append(entry)            

    # list of events
    if 'Records' in msg:        
        for r in msg['Records']:
            if r['eventName'] == 's3:ObjectCreated:Put':
                #process only parquets
                if r['s3']['object']['key'].endswith('.parquet') or r['s3']['object']['key'].endswith('.csv'):
                    #need unquoting since key is urlencoded
                    entry = {
                        'bucket' : r['s3']['bucket']['name'],
                        'key' :  unquote(r['s3']['object']['key'])
                    }                
                    list.append(entry)

    # s3 
    if 's3' in msg:
        s3_bucket = msg['s3']['bucket']
        s3_prefix = msg['s3']['prefix']
        s3_suffix = msg['s3']['suffix']
        s3_match = msg['s3']['match']

        for obj in get_matching_s3_objects(s3, bucket=s3_bucket, prefix=s3_prefix, suffix=s3_suffix):
            okey = obj["Key"]
            if not s3_match:
                entry = {
                    'bucket' : s3_bucket,
                    'key' :  okey
                }
                list.append(entry)   
            else:
                #check if filename matches 
                ofilename = okey.rsplit('/',1)[1]
                m = re.search(s3_match, ofilename)
                if m:
                    entry = {
                        'bucket' : s3_bucket,
                        'key' :  okey
                    }
                    list.append(entry)                        

    # process
    context.logger.info('process '+str(len(list))+ ' records')


    if engine == False:
        # use sqlalchemy because it supports multi/insert with pagination
        engine = sqlalchemy.create_engine('postgresql://'+DB_USERNAME+':'+DB_PASSWORD+'@'+DB_HOST+':'+DB_PORT+'/'+DB_NAME)
    
    
    for entry in list:
        try:
            bucket = entry['bucket']
            key = entry['key']
            source = bucket +"/"+key
            source_key = source

            # db config
            schema = DB_SCHEMA
            table = DB_TABLE
            table_prefix = DB_TABLE_PREFIX

            # validate request data
            if S3_BUCKET is not None:
                if bucket != S3_BUCKET:
                    context.logger.error('bucket {} not matching {}'.format(bucket, S3_BUCKET))
                    continue

            if KEY_MATCH is not None:
                kmfilename = source.rsplit('/',1)[1]
                km = re.search(KEY_MATCH, kmfilename)
                if not km:    
                    context.logger.error('key {} not matching {}'.format(kmfilename, KEY_MATCH))            
                    continue

            
            context.logger.info('process '+source)
            if table_prefix is None:
                # set default
                table_prefix = 'insertdb'

            # checking keys if both SCHEMA and TABLE are not static
            if DB_SCHEMA is None or DB_TABLE is None or DB_COLUMN_MAP is not None:

                # derive full table name from prefix + bucket + file prefix
                key_bucket =  re.sub('[^0-9a-zA-Z]+', '_', bucket)
                key_filename = re.sub('[^0-9a-zA-Z-_]+', '_', key.rsplit('/',1)[1])
                key_table = key_filename

                key_split =  key_filename.split('-')
                if(len(key_split) > 0):
                    key_table = key_split[0]
                if(len(key_split) > 1):
                    key_table = key_split[0] +"_"+key_split[1]
                
                source_key = key_table

                if DB_SCHEMA is not None:
                    schema = DB_SCHEMA

                    if DB_TABLE is not None:
                        table = DB_TABLE
                    else:
                        table = table_prefix+'_'+key_bucket+'_'+key_table
                else:
                    schema = key_bucket
                    table = table_prefix+'_'+key_table

            # check if file already imported in dest table
            c = 0

            try:
                context.logger.info('check if table '+table+' contains data from '+source)
                q = "SELECT COUNT(*) from {}.\"{}\" where {} = '{}'"
                c = engine.execute(q.format(schema, table, table_prefix+'_source', source)).scalar()
            
            except Exception as e:
                context.logger.error('Error checking table '+table+': '+str(e))        
                #consider errors as empty table
                c = 0

            if (c > 0):
                context.logger.info('data for '+source+' already in table '+table)
            else:    
                # no results for current source, insert as new
                context.logger.info('download from s3 bucket '+bucket+' key '+key)

                obj = s3.get_object(Bucket=bucket, Key=key)
                dataio = io.BytesIO(obj['Body'].read())

                df = pd.DataFrame()

                if key.endswith('.parquet'):
                    context.logger.info('read parquet into pandas dataframe')
                    df = pd.read_parquet(dataio, engine='pyarrow')

                if key.endswith('.csv'):
                    context.logger.info('read csv into pandas dataframe')
                    df = pd.read_csv(dataio)            

                count = len(df)
                context.logger.info('read count: '+str(count))

                if count > 0:
                    COUNTER_DF.inc(count)

                    # append column with source to all entries
                    df[table_prefix+'_source'] = source

                    # append column for mapping if required
                    if DB_COLUMN_MAP is not None:
                        df[table_prefix+'_'+DB_COLUMN_MAP] = source_key

                    context.logger.debug('write dataframe into table '+table)
                    df.to_sql(table, engine, schema=schema, index=False, if_exists='append', method='multi', chunksize=DB_CHUNKSIZE)
                
                context.logger.info('done '+source)

                # try to cleanup memory / which doesn't actually works in python..
                del df
                del dataio
                gc.collect()
                df = pd.DataFrame()
                del df

            #increment counter for current file
            COUNTER_FILES.inc()

        except Exception as e:
            context.logger.error('Error reading entry '+str(entry)+': '+str(e))        

    #done, cleanup
    context.logger.info('Done.')

    return context.Response(body='Done.',
                            headers={},
                            content_type='text/plain',
                            status_code=200)
