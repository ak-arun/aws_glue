import json
import boto3
from botocore.exceptions import ClientError
 
def lambda_handler(event, context):
    #add code to prevent the table from looping infinitely. Right now deleteing on exist and recreate.
    client = boto3.client('glue')
    table_name=str(event["detail"]["changedTables"][0])
    database_name = str(event["detail"]["databaseName"])
    response = client.get_table(DatabaseName=database_name,Name=table_name)
    print(response)
    table_name="repl_"+table_name
   
    try:
      client.delete_table(DatabaseName= database_name, Name = table_name)
    except Exception as e:
      print("table delete failure")
   
    storage_descriptor_nested = response["Table"]["StorageDescriptor"]
    table_type = response["Table"]["TableType"]
    table_input={"Name":table_name,"StorageDescriptor":storage_descriptor_nested,"TableType":table_type}
    client.create_table(DatabaseName=database_name,TableInput=table_input)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
