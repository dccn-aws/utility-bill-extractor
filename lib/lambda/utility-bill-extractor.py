import json
import time
import urllib.parse
import boto3
import logging
from datetime import datetime

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

LOGGER.info('Loading function')
LOGGER.info('Boto3 Version: {0}'.format(boto3.__version__))

s3 = boto3.client('s3')
textract = boto3.client('textract')
ddb = boto3.resource('dynamodb') # UtilityBillDataTable

def query_S3document(bucket, key, query):
    # Event is lambda event
    # query is a QueriesConfig object for textract.start_document_analysis()
    textract_response = textract.start_document_analysis(
            DocumentLocation={
                'S3Object': {
                    'Bucket': bucket,
                    'Name': key
                }
            },
            FeatureTypes=['QUERIES'],
            QueriesConfig=query
        )
        
    job = textract_response['JobId']
    
    textract_response = textract.get_document_analysis(JobId=job)
    while(textract_response["JobStatus"] == "IN_PROGRESS"):
        time.sleep(1)
        textract_response = textract.get_document_analysis(JobId=job)

    lookup = ["QUERY", "QUERY_RESULT"]
    
    queryResults = [b for b in textract_response["Blocks"] if b['BlockType'] in lookup]
    
    return queryResults
    
def unpack_query(results):
    i = 0
    data = {}
    while i < len(results):
        data[results[i]['Query']['Alias']] = results[i+1]['Text']
        i = i+2
    return data

def lambda_handler(event, context):

    # Get the object from the event notification and send it to Textract
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    query = {
                "Queries":[
                    {
                        "Text": "What is this customer's name?",
                        "Alias": "Customer_Name"
                    },
                    {
                        "Text": "What is this customer's address?",
                        "Alias": "Customer_Address"
                    },
                    {
                        "Text": "What is the meter ID?",
                        "Alias": "ES_ID"
                    },
                    {
                        "Text": "How many kWhs were used?",
                        "Alias": "kWh_Usage"
                    },
                    {
                        "Text": "What is the statement date?",
                        "Alias": "Bill_Date"
                    },
                    {
                        "Text": "What's the name at the top of the bill?",
                        "Alias": "Utility_Provider_Name"
                    },
                    {
                        "Text": "What's the address at the top of the bill?",
                        "Alias": "Utility_Provider_Address"
                    },
                ]
                
            }
    
    queryResults = query_S3document(bucket, key, query)

    data = unpack_query(queryResults)
    data['Bill_Date'] = str(datetime.strptime(data['Bill_Date'],'%B %d,%Y'))
    
    # utilityBillTable = ddb.Table('UtilityBillDataTable')
    
    # utilityBillTable.put_item(data)
    
    LOGGER.info('Query contains {0} results, cleaned: {1}'.format(len(queryResults)/2,json.dumps(data)))
    
    return data
