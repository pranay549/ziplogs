import azure.functions as func
import logging
import json
import zipfile
import io
import os
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.monitor.ingestion import LogsIngestionClient
from azure.core.exceptions import HttpResponseError

# Create the function app
app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", 
                  path="incominglogs/{name}",
                  connection="AzureWebJobsStorage")
def ProcessZippedLogs(myblob: func.InputStream) -> None:
    logging.info(f"Python blob trigger function processed blob\n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")
    
    try:
        # Initialize managed identity credential
        credential = DefaultAzureCredential()
        
        # Storage configuration using managed identity
        storage_account_name = os.environ.get('STORAGE_ACCOUNT_NAME', 'ldblobstoragepoc')
        account_url = f"https://{storage_account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
        
        # Log Analytics configuration using managed identity
        endpoint_uri = os.environ.get('DCE_ENDPOINT', 'https://lddcepoc-6agr.eastus-1.ingest.monitor.azure.com')
        dcr_immutableid = os.environ.get('DCR_IMMUTABLE_ID', 'dcr-2333f459a2da4a9bbb6dba6d35c9cafe')
        stream_name = os.environ.get('STREAM_NAME', 'MyAppLogs_CL')
        
        # Initialize Log Analytics client with managed identity
        logs_client = LogsIngestionClient(endpoint=endpoint_uri, credential=credential)
        
        # Process only zip files
        if not myblob.name.lower().endswith('.zip'):
            logging.info(f"Skipping non-zip file: {myblob.name}")
            return
        
        # Read and validate zip file
        zip_content = myblob.read()
        zip_bytes = io.BytesIO(zip_content)
        
        if not zipfile.is_zipfile(zip_bytes):
            logging.error(f"Invalid zip file: {myblob.name}")
            return
        
        # Extract and process JSON files
        processed_logs = []
        
        with zipfile.ZipFile(zip_bytes, 'r') as zip_file:
            for file_name in zip_file.namelist():
                if file_name.lower().endswith('.json'):
                    try:
                        with zip_file.open(file_name) as json_file:
                            json_content = json_file.read().decode('utf-8')
                            
                        if json_content.strip():
                            try:
                                json_data = json.loads(json_content)
                                if isinstance(json_data, list):
                                    processed_logs.extend(json_data)
                                else:
                                    processed_logs.append(json_data)
                            except json.JSONDecodeError:
                                # Handle JSONL format (one JSON per line)
                                for line in json_content.strip().split('\n'):
                                    if line.strip():
                                        try:
                                            log_entry = json.loads(line)
                                            processed_logs.append(log_entry)
                                        except json.JSONDecodeError as e:
                                            logging.warning(f"Failed to parse JSON line: {e}")
                    
                    except Exception as e:
                        logging.error(f"Error processing file {file_name}: {str(e)}")
        
        # Send logs to Log Analytics using managed identity
        if processed_logs and not endpoint_uri.startswith('https://your-dce-endpoint'):
            try:
                formatted_logs = []
                for log in processed_logs:
                    formatted_log = {
                        "TimeGenerated": datetime.utcnow().isoformat() + "Z",
                        "SourceFile": myblob.name,
                        "RawData": json.dumps(log) if isinstance(log, dict) else str(log)
                    }
                    formatted_logs.append(formatted_log)
                
                # Upload using managed identity
                logs_client.upload(rule_id=dcr_immutableid, stream_name=stream_name, logs=formatted_logs)
                logging.info(f"Successfully sent {len(formatted_logs)} log entries to Log Analytics")
                
            except HttpResponseError as e:
                logging.error(f"Failed to send logs to Log Analytics: {e}")
            except Exception as e:
                logging.error(f"Unexpected error sending logs: {str(e)}")
        
        # Move processed file using managed identity
        try:
            source_container = blob_service_client.get_container_client("incominglogs")
            dest_container = blob_service_client.get_container_client("archivedlogs")
            
            blob_name = myblob.name.split('/')[-1]
            
            source_blob_client = source_container.get_blob_client(blob_name)
            dest_blob_client = dest_container.get_blob_client(blob_name)
            
            # Copy and delete using managed identity
            copy_source = source_blob_client.url
            dest_blob_client.start_copy_from_url(copy_source)
            source_blob_client.delete_blob()
            
            logging.info(f"Successfully moved {blob_name} to processed container")
            
        except Exception as e:
            logging.error(f"Error moving blob: {str(e)}")
        
        logging.info(f"Processing completed for {myblob.name}. Found {len(processed_logs)} log entries.")
        
    except Exception as e:
        logging.error(f"Fatal error processing blob {myblob.name}: {str(e)}")
        raise
