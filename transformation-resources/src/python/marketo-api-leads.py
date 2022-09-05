import boto3
from datetime import datetime, timedelta
import os
from marketorestpython.client import MarketoClient
import json
import pandas as pd
import io
import time
import pyarrow as pa
import pyarrow.parquet as pq

s3 = boto3.client('s3')
ssm = boto3.client('ssm')

# Hard code dates used to run daily query
start_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
end_date = datetime.today().strftime('%Y-%m-%d')

def lambda_handler(event, context):
    munchkin_id_path = os.environ['MUNCHKIN_ID']
    client_id_path = os.environ['CLIENT_ID']
    client_secret_path = os.environ['CLIENT_SECRET']
    
    munchkin_id = ssm.get_parameter(Name=munchkin_id_path, WithDecryption=True)['Parameter']['Value']
    client_id = ssm.get_parameter(Name=client_id_path, WithDecryption=True)['Parameter']['Value']
    client_secret = ssm.get_parameter(Name=client_secret_path, WithDecryption=True)['Parameter']['Value']

    ## TO DO (IMPROVE) : add start_date and end_date to cron message and overwrite hard coded date to allow flexibility
    # start_date = event['start_date']
    # end_date = event['end_date_exclusive']
    
    api_limit = None
    max_retry_time = None
    
    mc = MarketoClient(munchkin_id, client_id, client_secret, api_limit, max_retry_time)

    # Fields written in and not as variables because output used to create Athena table
    # Create API query
    new_export_job_details = mc.execute(method='create_leads_export_job',
                                        fields=['id',
                                                'BU__c',
                                                'webformRequestMostrecent',
                                                'leadStatus',
                                                'SFDCType',
                                                'createdAt',
                                                'updatedAt'
                                            ],
                                            filters= {'updatedAt': {'endAt': end_date, 'startAt': start_date}})

    # Send query to queue
    enqueued_job_details = mc.execute(method='enqueue_leads_export_job', job_id=new_export_job_details[0]['exportId'])

    # Check status of query to check if it is complete
    status = "Queued"
    i = 0
    while status != "Completed" and i < 5:
        export_job_status = mc.execute(method='get_leads_export_job_status', job_id=new_export_job_details[0]['exportId'])
        status = export_job_status[0]['status']
        if status == "Completed":
            export_file_contents = mc.execute(method='get_leads_export_job_file', job_id=new_export_job_details[0]['exportId'])
            f = io.BytesIO(export_file_contents)
            df = pd.read_csv(f)
        elif i == 4:
            print("DataFrame not created; query status was never completed")
        else:
            # increment while loop to avoid indefinite processing
            i += 1
            # wait for query to be processed by Marketo
            time.sleep(180)

    output_filename = f'marketo/leads/{start_date[0:4]}/{start_date[5:7]}/leads_data_{start_date}_{end_date}.parquet'

    print(df.info())
    
    # explicitly define types to avoid errors due to missing data
    fields = [
        pa.field('id', pa.int64()),
        pa.field('BU__c', pa.string()),
        pa.field('webformRequestMostrecent', pa.string()),
        pa.field('leadStatus', pa.string()),
        pa.field('SFDCType', pa.string()),
        pa.field('createdAt', pa.string()),
        pa.field('updatedAt', pa.string())
    ]
    
    df_schema = pa.schema(fields)

    table = pa.Table.from_pandas(df, schema=df_schema, preserve_index=False)
    
    writer = pa.BufferOutputStream()
    pq.write_table(table, writer)
    body = bytes(writer.getvalue())
    

    s3.put_object(Bucket='datalake-dev-raw', Body=body, Key=output_filename)  

    return {
        'statusCode': 200,
        'body': f'Marketo Activity API queried for {start_date} until {end_date} exclusive and uploaded to {output_filename}'
    }