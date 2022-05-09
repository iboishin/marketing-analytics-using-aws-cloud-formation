import json
import time
import boto3
from datetime import datetime, timedelta
import re

database = 'datalake_dev_database'
output_bucket = 'datalake-dev-landing'

s3_client = boto3.client('s3')
athena_client = boto3.client('athena')

def cleanup(output_path):
    session = boto3.Session()
    s3 = session.resource('s3')
    my_bucket = s3.Bucket(output_bucket)
    for item in my_bucket.objects.filter(Prefix=output_path):
        item.delete()

def read_query(filename):
    file_contents = s3_client.get_object(Bucket="datalake-dev-lambda-resources", Key=f'sql/{filename}')
    return file_contents['Body'].read().decode('utf-8')

def adapt_query(query, event, date, leads_limit):
    if event['query_type'] == "marketo_bu":
        
        query = query.replace("LEADS_LIMIT", leads_limit)
        
        query = query.replace("QUERY_DATE", f"'{date}'")
        query = query.replace("QUERY_MONTH", f"'{date[0:7]}'")
        query = query.replace("QUERY_YTD", f"'{date[0:4]}'")
        
        query_config = event['query_config']
        query_params_all = s3_client.get_object(Bucket="datalake-dev-lambda-resources", Key='sql/bu_sql_config.json')['Body'].read().decode('utf-8')
        query_params = json.loads(query_params_all)[query_config]
        print(query_params)
        
        for param in query_params:
            query = query.replace(param, query_params[param])
    
    elif event['query_type'] == "marketo_global":
        
        query = query.replace("LEADS_LIMIT", leads_limit)
        
        query = query.replace("QUERY_DATE", f"'{date}'")
        query = query.replace("QUERY_MONTH", f"'{date[0:7]}'")
        query = query.replace("QUERY_YTD", f"'{date[0:4]}'")

    # print(query)
    print(date)
    return query

def lambda_handler(event, context):
    
    ### GET PARAMETERS
    output_path = f"dashboard/{event['output_path']}"
    query_type = event['query_type']
    truncate = event['truncate']
    
    if truncate == "True":
        cleanup(output_path)
    
    
    # historique/rattrapage ou lancement quotidien
    try:
        query_date = event['query_date']
        exec_day = (datetime.strptime(query_date, "%Y-%m-%d") + timedelta(days=1)).strftime('%d')
        leads_limit = "WHERE SUBSTR(updatedat, 1, 10) <= QUERY_DATE"
    except:
        query_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        exec_day = datetime.today().strftime('%d')
        leads_limit = ""


    # use monthly query or daily query
    if (query_type == "marketo_bu") & (exec_day == "01"):
        query_path = "bu_stats_monthly.sql"
    elif query_type == "marketo_bu":
        query_path = "bu_stats_daily.sql"
    elif (query_type == "marketo_global") & (exec_day == "01"):
        query_path = "global_stats_monthly.sql"
    elif query_type == "marketo_global":
        query_path = "global_stats_daily.sql"
    elif query_type == "pbi_combine":
        query_path = "pbi_combine.sql"

    # Load SQL    
    query = read_query(query_path)
    query = adapt_query(query, event, query_date, leads_limit)

    # Execution
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': f's3://{output_bucket}/{output_path}/{query_date}',
        }
    )
   
    return response
    
    # # Decomment if you need to check any errors and commet "return response" above
    # print(f's3://{output_bucket}/{output_path}/{query_date}')
    # print(query_date, exec_day)
    # max_execution = 5
    # state = 'RUNNING'
    
    # while (max_execution > 0 and state in ['RUNNING', 'QUEUED']):
    #     print(max_execution)
    #     max_execution = max_execution - 1
    #     result = athena_client.get_query_execution(QueryExecutionId = response['QueryExecutionId'])

    #     if 'QueryExecution' in result and \
    #             'Status' in result['QueryExecution'] and \
    #             'State' in result['QueryExecution']['Status']:
    #         state = result['QueryExecution']['Status']['State']
    #         if state == 'FAILED':
    #             # return json.loads(json.dumps(result, default=str))
    #             print(result)
    #         elif state == 'SUCCEEDED':
    #             s3_path = result['QueryExecution']['ResultConfiguration']['OutputLocation']
    #             filename = re.findall('.*\/(.*)', s3_path)[0]
    #             # return filename
    #             print(result)
    #     time.sleep(30)