import json
import time
import urllib.parse
import boto3
import logging
import re
from datetime import datetime

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

LOGGER.info('Loading function')
LOGGER.info('Boto3 Version: {0}'.format(boto3.__version__))

s3 = boto3.client('s3')
textract = boto3.client('textract')
awslambda = boto3.client('lambda')

RAW_DATA_BUCKET = 'carbonlake-bills-storage'
TRANSFORMED_DATA_BUCKET = 'carbonlake-transformed-data'

def query_Textract(bills, query):
    # Event is lambda event
    # query is a QueriesConfig object for textract.start_document_analysis()
    jobs = []
    for bill in bills:
        textract_response = textract.start_document_analysis(
                DocumentLocation={
                    'S3Object': {
                        'Bucket': RAW_DATA_BUCKET,
                        'Name': bill
                    }
                },
                FeatureTypes=['QUERIES'],
                QueriesConfig=query
            )
        
        job = textract_response['JobId']
        jobs.append(job)
        time.sleep(.1) # hardcoded throttling to avoid tripping Textract Quotas
    
    LOGGER.info('Querying {0} bills in Textract'.format(len(jobs)))
    return jobs
    
def unpack_query(results):
    i = 0
    data = {}
    while i < len(results):
        data[results[i]['Query']['Alias']] = results[i+1]['Text']
        i = i+2
    
    # Try block to catch multiple datetime formats, you may need to add more here
    for schema in ['%B %d,%Y', '%B %d, %Y']:
        try:
            data['bill_date'] = datetime.strptime(data['bill_date'],schema).strftime('%Y-%m-%d')
        except:
            pass
        
    return data
    
def retrieve_Textract_query_results(jobids):
    results = []
    for job in jobids:
        textract_response = textract.get_document_analysis(JobId=job)
        while(textract_response["JobStatus"] == "IN_PROGRESS"):
            time.sleep(1) if textract_response["JobStatus"] == "IN_PROGRESS"
            textract_response = textract.get_document_analysis(JobId=job)
    
        lookup = ["QUERY", "QUERY_RESULT"]
        
        queryResults = [b for b in textract_response["Blocks"] if b['BlockType'] in lookup]
        
        results.append(unpack_query(queryResults))
    
    LOGGER.info('Successfully retrieved {0} queries from Textract'.format(len(jobids)))
    return results
    
def get_bills(bucket):
    
    return bills
    
def transform_data(data):
    TransformedData = []
    for entry in data:
        event_id = '{0}-{1}-{2}'.format(entry['utility_provider_name'],
                                        entry['es_id'],
                                        entry['bill_date']
                                        )
        zipcode = re.search("\d{5}\-?\d{0,4}", entry['utility_provider_address']).group()
        activity = awslambda.invoke(FunctionName='GridRegionSelector',
                                    Payload={'country':'US','zipcode':zipcode},
                                    RequestType='RequestResponse'
                                    )
        TransformedData.append({
            "activity_event_id": event_id,
            "supplier": entry['utility_provider_name'],
            "scope": 2,
            "category": "grid-region-location-based",
            "activity": entry['utility_provider_name'],
            "raw_data": entry['kwh_usage'],
            "units": "kwH"
            }
        )
        
    return TransformedData
    
def write_bills_to_s3(bills):
    
    for bill in bills:
        filename = "{0}.pdf".format(bill['activity_event_id'])
        with open(filename, "w") as outfile:
            json.dump(bill, outfile)
        s3.put_ojbect(Body=filename,
                      Bucket=TRANSFORMED_DATA_BUCKET,
                      key='scope2-bill-extracted-data/{0}'.format(filename))
                      )
        
        

def lambda_handler(event, context):
    
    resp = s3.list_objects_v2(Bucket=RAW_DATA_BUCKET, Prefix="utility-bills/")
    bills = [b['Key'] for b in resp['Contents'] if '.pdf' in b['Key']]

    query = {
                "Queries":[
                    {
                        "Text": "What is this customer's name?",
                        "Alias": "customer_name"
                    },
                    {
                        "Text": "What is this customer's address?",
                        "Alias": "customer_address"
                    },
                    {
                        "Text": "What is the meter ID?",
                        "Alias": "es_id"
                    },
                    {
                        "Text": "How many kWhs were used?",
                        "Alias": "kwh_usage"
                    },
                    {
                        "Text": "What is the statement date?",
                        "Alias": "bill_date"
                    },
                    {
                        "Text": "What's the name at the top of the bill?",
                        "Alias": "utility_provider_name"
                    },
                    {
                        "Text": "What's the address at the top of the bill?",
                        "Alias": "utility_provider_address"
                    },
                ]
                
            }
    
    TextractJobs = query_Textract(bills, query)
    
    query_results = retrieve_Textract_query_results(TextractJobs)
    
    ExtractedData = unpack_query(query_results)
    
    TransformedBills = transform_data(ExtractedData)
    
    write_bills_to_s3(TransformedBills)
        return data

