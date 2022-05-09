import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import boto3
import json
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

s3 = boto3.client('s3')


scope = ['https://www.googleapis.com/auth/analytics.readonly']
gcp_service_account_key = 'secrets/aws-ga-lambda-cred.json'
output_bucket = "datalake-dev-raw"

def initialize_analyticsreporting():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(gcp_service_account_key, scope)
    analytics = build('analyticsreporting', 'v4', credentials=credentials, cache_discovery=False)
    return analytics
    
def get_report(analytics, view_id, dt):
    return analytics.reports().batchGet(
        body={'reportRequests':[
            {'viewId': view_id,
            'dateRanges': [{'startDate': dt, 'endDate': dt}],
            'hideTotals': True,
            'metrics': [
                { 'expression': 'ga:sessions' },
                { 'expression': 'ga:sessionDuration' },
                { 'expression': 'ga:bounces' },
                { 'expression': 'ga:goal1Completions' },
                { 'expression': 'ga:goal2Completions' },
                { 'expression': 'ga:goal3Completions' },
                { 'expression': 'ga:goal4Completions' }
            ],
            'dimensions': [
                {'name': 'ga:segment'},
                {'name': 'ga:dateHour'},
                {'name': 'ga:sourceMedium'}
            ],
            'segments':[
                {
                    'dynamicSegment': {
                        'name': "Mobile segment - no HQ",
                        'sessionSegment': {
                        'segmentFilters': [
                            {
                            'simpleSegment': {
                                'orFiltersForSegment': [
                                {
                                    'segmentFilterClauses': [
                                    {
                                        'dimensionFilter': {
                                        'dimensionName': "ga:deviceCategory",
                                        'operator': "EXACT",
                                        'expressions': ["mobile"],
                                        },
                                    },
                                    ],
                                },
                                ],
                            },
                            },
                            {
                            "not": true,
                            'simpleSegment': {
                                'orFiltersForSegment': [
                                {
                                    'segmentFilterClauses': [
                                    {
                                        'dimensionFilter': {
                                        'dimensionName': "ga:country",
                                        'operator': "EXACT",
                                        'expressions': ["Denmark"],
                                        },
                                    },
                                    ],
                                },
                                {
                                    'segmentFilterClauses': [
                                    {
                                        'dimensionFilter': {
                                        'dimensionName': "ga:screenResolution",
                                        'operator': "EXACT",
                                        'expressions': ["800x600"],
                                        },
                                    },
                                    ],
                                },
                                ],
                            },
                            },
                        ],
                        },
                    },
                },

                {
                    'dynamicSegment': {
                        'name': "Desktop segment - no HQ",
                        'sessionSegment': {
                        'segmentFilters': [
                            {
                            'simpleSegment': {
                                'orFiltersForSegment': [
                                {
                                    'segmentFilterClauses': [
                                    {
                                        'dimensionFilter': {
                                        'dimensionName': "ga:deviceCategory",
                                        'operator': "EXACT",
                                        'expressions': ["desktop"],
                                        },
                                    },
                                    ],
                                },
                                ],
                            },
                            },
                            {
                            "not": true,
                            'simpleSegment': {
                                'orFiltersForSegment': [
                                {
                                    'segmentFilterClauses': [
                                    {
                                        'dimensionFilter': {
                                        'dimensionName': "ga:country",
                                        'operator': "EXACT",
                                        'expressions': ["Denmark"],
                                        },
                                    },
                                    ],
                                },
                                {
                                    'segmentFilterClauses': [
                                    {
                                        'dimensionFilter': {
                                        'dimensionName': "ga:screenResolution",
                                        'operator': "EXACT",
                                        'expressions': ["800x600"],
                                        },
                                    },
                                    ],
                                },
                                ],
                            },
                            },
                        ],
                        },
                    },
                }
                
                ]
            }]
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

    
    # Transform JSON response to columnar format
    segment = [r['dimensions'][0] for r in raw_data['response']['reports'][0]['data']['rows']]
    date_hour = [r['dimensions'][1] for r in raw_data['response']['reports'][0]['data']['rows']]
            
    sessions = [int(r['metrics'][0]['values'][0]) for r in raw_data['response']['reports'][0]['data']['rows']]
    total_session_duration = [float(r['metrics'][0]['values'][1]) for r in raw_data['response']['reports'][0]['data']['rows']]
    bounces = [int(r['metrics'][0]['values'][2]) for r in raw_data['response']['reports'][0]['data']['rows']]
    goal1Completions = [int(r['metrics'][0]['values'][3]) for r in raw_data['response']['reports'][0]['data']['rows']]
    goal2Completions = [float(r['metrics'][0]['values'][4]) for r in raw_data['response']['reports'][0]['data']['rows']]
    goal3Completions = [int(r['metrics'][0]['values'][5]) for r in raw_data['response']['reports'][0]['data']['rows']]
    goal4Completions = [int(r['metrics'][0]['values'][6]) for r in raw_data['response']['reports'][0]['data']['rows']]

    df = pd.DataFrame({
        'date_hour': date_hour,
        # 'year': [r.year for r in date_hour],
        # 'month': [r.month for r in date_hour],
        # 'day': [r.day for r in date_hour],
        # 'hour': [r.hour for r in date_hour],
        'segment': segment,
        'sessions': sessions,
        'total_session_duration': total_session_duration,
        'bounces': bounces,
        'purchase': goal1Completions,
        'engaged_users': goal2Completions,
        'registrations': goal3Completions,
        'checkout': goal4Completions
    })
    
    # add sampling value to help with debugging
    perc_sampling = sampling(raw_data)
    output_filename = f"google-analytics/stats/{view_id}/{dt}_samp_{perc_sampling}.parquet"
    df_to_parquet(df, output_filename)