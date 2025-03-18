import azure.functions as func
import logging
import json
import os
import uuid
from azure.storage.queue import QueueClient

################ Neste Código o docker/postman só enviam uma linha e a API envia para duas QUEUES uma para BD e outra para file ################

# Inicializa a Function App
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Configuração da conta de armazenamento do Azure
STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=storageaccountt04;AccountKey=j2HYuzsHHzGMNepvs4Lddht463d1zpe+N4zNXzIxnJP5kbQptleNwTq7by5RHAE1PC8Wop2+kn8P+AStSwFt6g==;EndpointSuffix=core.windows.net"
QUEUE_NAME_1 = "bdqueuesales"
QUEUE_NAME_2 = "filequeuesales"

def create_queue_if_not_exists(queue_client):
    try:
        queue_client.create_queue()
        logging.info(f"Queue '{queue_client.queue_name}' created or already exists.")
    except Exception as error:
        logging.error(f"Error creating queue '{queue_client.queue_name}': {error}")


####### Insere os dados nas duas QUEUEs distintas do Azure #######
@app.route(route="insert_into_queues_azure", methods=["POST"])
def insert_into_queues_azure(req: func.HttpRequest) -> func.HttpResponse:
    """Recebe os dados e envia para duas filas do Azure Queue Storage."""
    logging.info("Receiving sales data and sending to Azure Queue Storage.")

    queue_client_1 = QueueClient.from_connection_string(STORAGE_CONNECTION_STRING, QUEUE_NAME_1)
    queue_client_2 = QueueClient.from_connection_string(STORAGE_CONNECTION_STRING, QUEUE_NAME_2)
    if not queue_client_1 or not queue_client_2:
        return func.HttpResponse("Failed to connect to Azure Queue Storage.", status_code=500)

    create_queue_if_not_exists(queue_client_1)
    create_queue_if_not_exists(queue_client_2)

    try:
        req_body = req.get_json()
        sales_data = {
            "id": str(uuid.uuid4()),
            "product": req_body.get("product"),
            "sales_ts": req_body.get("sales_ts"),
            "user_id": req_body.get("user_id")
        }

        if not sales_data["product"] or not sales_data["sales_ts"] or not sales_data["user_id"]:
            return func.HttpResponse("Invalid input data.", status_code=400)

        message = json.dumps(sales_data)
        
        queue_client_1.send_message(message)
        logging.info(f"Sales data sent to queue {QUEUE_NAME_1}: {message}")
        
        queue_client_2.send_message(message)
        logging.info(f"Sales data sent to queue {QUEUE_NAME_2}: {message}")

        return func.HttpResponse("Sales data successfully sent to both queues.", status_code=200)

    except Exception as error:
        logging.error(f"Error sending sales data to queues: {error}")
        return func.HttpResponse("Error processing request.", status_code=500)
