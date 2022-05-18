
import os
import ast
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import boto3
import json
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

s3 = boto3.client('s3')
ssm = boto3.client('ssm')

scope = ['https://www.googleapis.com/auth/analytics.readonly']
gcp_service_account_key_path = os.environ['GCP_SERVICE_ACCOUNT_KEY']
gcp_service_account_key_dict = ast.literal_eval(ssm.get_parameter(Name=gcp_service_account_key_path, WithDecryption=True)['Parameter']['Value'])

output_bucket = os.environ['OUTPUT_BUCKET']

def initialize_analyticsreporting():
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(gcp_service_account_key_dict, scope)
    analytics = build('analyticsreporting', 'v4', credentials=credentials, cache_discovery=False)
    return analytics
    
def get_report(analytics, view_id, dt):
    return analytics.reports().batchGet(
        body={'reportRequests':[
            {
                'viewId': view_id,
                'pageSize': 100000,
                'dateRanges': [
                    {
                    'startDate': dt,
                    'endDate': dt,
                    },
                ],
                'metrics': [
                    { 'expression': 'ga:uniqueEvents' },
                    { 'expression': 'ga:totalEvents' }
                ],
                'dimensions': [{ 'name': 'ga:dateHour' }],
                'filtersExpression': 'ga:productName=@apparel;ga:itemRevenue>=100',
            },
            {
                'viewId': view_id,
                'pageSize': 100000,
                'dateRanges': [
                    {
                    'startDate': dt,
                    'endDate': dt,
                    },
                ],
                'metrics': [
                    { 'expression': 'ga:uniqueEvents' },
                    { 'expression': 'ga:totalEvents' }
                ],
                'dimensions': [{ 'name': 'ga:dateHour' }],
                'filtersExpression': 'ga:productName=@apparel;ga:itemRevenue<=100',
            },
            {
                'viewId': view_id,
                'pageSize': 100000,
                'dateRanges': [
                    {
                    'startDate': dt,
                    'endDate': dt,
                    },
                ],
                'metrics': [
                    { 'expression': 'ga:uniqueEvents' },
                    { 'expression': 'ga:totalEvents' }],
                'dimensions': [{ 'name': 'ga:dateHour' }],
                'filtersExpression': 'ga:productName=@lifestyle;ga:itemRevenue>=100'
            },
            {
                'viewId': view_id,
                'pageSize': 100000,
                'dateRanges': [
                    {
                    'startDate': dt,
                    'endDate': dt,
                    },
                ],
                'metrics': [
                    { 'expression': 'ga:uniqueEvents' },
                    { 'expression': 'ga:totalEvents' }],
                'dimensions': [{ 'name': 'ga:dateHour' }],
                'filtersExpression': 'ga:dimension4==Altuglas international;ga:productCouponCode==SUMMERTIME',
            },
            ],    
        }).execute()

def sampling(report):
    try:
        samples_read = report['response']['reports'][report]['data']['samplesReadCounts'][0]
        sample_size = report['response']['reports'][report]['data']['samplingSpaceSizes'][0]
        
        return int((int(samples_read)/int(sample_size)) *100)
    except:
        return 100
        ## If samples not in file, the data was not sampled

def df_to_parquet(df, output_filename):
    table = pa.Table.from_pandas(df)
    
    writer = pa.BufferOutputStream()
    pq.write_table(table, writer)
    body = bytes(writer.getvalue())
    
    s3.put_object(Bucket=output_bucket, Body=body, Key=output_filename)

def lambda_handler(event, context):
    view_id = str(event['view_id'])

    if event['date']:
        dt = str(event['date'])
    else:
        dt = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    # Query Google Analytics data
    analytics = initialize_analyticsreporting()
    raw_object = get_report(analytics, view_id, dt)
    raw_data = json.loads(raw_object)

    
    # Transform JSON response to columnar format by looping through reports
    df = pd.DataFrame()
    report_sampling = 0
    nb_reports = len(json.loads(raw_data['request'])['reportRequests'])
    skipped = 0
        
    for report in range(nb_reports):
        filter = json.loads(raw_data['request'])['reportRequests'][report]['filtersExpression']
        
        try:
            date_hour = [r['dimensions'][1] for r in raw_data['response']['reports'][0]['data']['rows']]
            
            uniqueEvents = [int(r['metrics'][0]['values'][0]) for r in raw_data['response']['reports'][report]['data']['rows']]
            totalEvents = [int(r['metrics'][0]['values'][1]) for r in raw_data['response']['reports'][report]['data']['rows']]

            df_part = pd.DataFrame({
                'filter': filter,
                'date_hour': date_hour,
                'uniqueEvents': uniqueEvents,
                'totalEvents': totalEvents
            })

            df = df.append(df_part)

        except:
            skipped += 1
            print(f"There is no data in {filter}")

        report_sampling += sampling(report)

    if skipped != nb_reports:
            perc_sampling = report_sampling/nb_reports
            output_filename = f"google-analytics/forms/{view_id}/{dt}_samp_{perc_sampling}.parquet"
            df_to_parquet(df, output_filename)

    else:
        print("No file was written as the reports were empty")