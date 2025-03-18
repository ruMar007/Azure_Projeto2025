import azure.functions as func
import logging
import json
import os
import uuid
from azure.data.tables import TableServiceClient
from datetime import datetime

# Initialize the Function App
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Azure Storage Configuration
STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=storageaccountt04;AccountKey=j2HYuzsHHzGMNepvs4Lddht463d1zpe+N4zNXzIxnJP5kbQptleNwTq7by5RHAE1PC8Wop2+kn8P+AStSwFt6g==;EndpointSuffix=core.windows.net"
TABLE_NAME = "ApplicationLogs"

# Create a table client
table_service_client = TableServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
table_client = table_service_client.create_table_if_not_exists(table_name=TABLE_NAME)

####### Insert log data into Azure Table #######
@app.route(route="insert_log", methods=["POST"])
def insert_log(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Receiving log data and inserting into Azure Table Storage.")

    try:
        req_body = req.get_json()
        timestamp = req_body.get("timestamp", datetime.utcnow().isoformat())
        message = req_body.get("message")

        if not message:
            return func.HttpResponse("Invalid input data. 'message' is required.", status_code=400)

        additional_info = {
            "Product": req_body.get("product"),
            "SalesTS": req_body.get("sales_ts"),
            "UserID": req_body.get("user_id")
        }

        insert_log_entry(table_client, timestamp, message, **additional_info)

        logging.info(f"Log entry inserted into table {TABLE_NAME}: {message}")

        return func.HttpResponse("Log entry successfully inserted into table.", status_code=200)

    except Exception as error:
        logging.error(f"Error inserting log entry into table: {error}")
        return func.HttpResponse("Error processing request.", status_code=500)

def insert_log_entry(table_client, timestamp, message, **kwargs):
    log_entry = {
        'PartitionKey': 'logs_function_app',
        'RowKey': str(uuid.uuid4()),
        'Timestamp': timestamp,
        'Message': message
    }
    
    for key, value in kwargs.items():
        log_entry[key] = value
    
    table_client.create_entity(entity=log_entry)
