import boto3
from datetime import datetime, timedelta
import os
from marketorestpython.client import MarketoClient
import pandas as pd
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
    list_activity_ids = event['list_activity_ids']
    
    api_limit = None
    max_retry_time = None
    
    mc = MarketoClient(munchkin_id, client_id, client_secret, api_limit, max_retry_time)

    # Activity ids for daily => 2 : Form Filled; 12 : New Person; 22 : Change Score; 34 : Add to Opportunity;
    # 36 : Update Opportunity; 100001 : Drift Conversation URL
    act = mc.execute(
        method = 'get_lead_activities', 
        activityTypeIds = list_activity_ids,
        sinceDatetime = start_date, 
        untilDatetime = end_date) #not inclusive
    df = pd.DataFrame(act)

    output_filename = f'marketo/activity/{start_date[0:4]}/{start_date[5:7]}/activity_data_{start_date}_{end_date}.parquet'
    
    print(df.info())
    
    print(df.head())
    
    # explicitly define types to avoid errors due to missing data
    fields = [
        pa.field('id', pa.int64()),
        pa.field('marketoGUID', pa.int64()),
        pa.field('leadId', pa.int64()),
        pa.field('activityDate', pa.string()),
        pa.field('activityTypeId', pa.int64()),
        pa.field('campaignId', pa.float64()),
        pa.field('primaryAttributeValueId', pa.int64()),
        pa.field('primaryAttributeValue', pa.string()),
        pa.field('attributes', pa.string()),
    ]
    
    # ensure attributes column is read in as string and not a list
    df['marketoGUID'] = df['marketoGUID'].astype('int')
    df['attributes'] = df['attributes'].astype(str)
    
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