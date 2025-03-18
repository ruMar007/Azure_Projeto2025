import azure.functions as func
import logging
import json
import pyodbc
import os
from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData
import time
import uuid
import time

# Initialize the Function App
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Configure the Event Hub
producer = EventHubProducerClient.from_connection_string(
    conn_str="Endpoint=sb://hub-04.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=TaxX8KnVxYI4uaTi5PsbbGVbc1GDg6IJ2+AEhOTGvoU=",
    eventhub_name="04sales-events_teste"
)

@app.route(route="insert_EventHub_azure", methods=["POST"])
def insert_EventHub_azure(req: func.HttpRequest) -> func.HttpResponse:
    """Receives data and inserts it into the Event Hub."""
    try:
        # Parse the incoming request
        req_body = req.get_json()
        id_sale = req_body.get("id_sale")
        product_id = req_body.get("product")
        sales_ts = req_body.get("sales_ts")
        user_id = req_body.get("user_id")

        # Validate the input data
        if not product_id or not sales_ts or not user_id or not id_sale:
            return func.HttpResponse("Invalid input data.", status_code=400)

        # Create an event batch
        events_batch = producer.create_batch()

        # Construct event data
        event_data = {
            "event_id": str(uuid.uuid4()),  # Generate a unique ID for the event
            "timestamp": time.time(),
            "id_sale": id_sale,
            "product": product_id,
            "sales_ts": sales_ts,
            "user_id": user_id
        }
    

        # Add the event data to the batch
        events_batch.add(EventData(json.dumps(event_data)))

        # Send the batch to the Event Hub
        producer.send_batch(events_batch)

        logging.info("Event sent successfully.")
        return func.HttpResponse("Sale recorded successfully.", status_code=200)

    except Exception as error:
        logging.error(f"Error processing the request: {error}")
        return func.HttpResponse("Error processing request.", status_code=500)

    finally:
        # Close the Event Hub producer
        producer.close()
