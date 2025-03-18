import azure.functions as func
import logging
import json
import os
import uuid
from azure.storage.blob import BlobServiceClient

########## QUEUE TRIGGER --> Sempre que inserir valores na QUEUE filequeuesales vai para a Blobfile  ############  -- Storage Account --> Containers --> Conteiner04


app = func.FunctionApp()

STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=storageaccountt04;AccountKey=j2HYuzsHHzGMNepvs4Lddht463d1zpe+N4zNXzIxnJP5kbQptleNwTq7by5RHAE1PC8Wop2+kn8P+AStSwFt6g==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "container04"
CONTAINER_NAME = "container04q"

@app.queue_trigger(arg_name="azqueue", queue_name="filequeuesales",
                   connection="AzureWebJobsstorageaccountt04_STORAGE")
def queue_trigger_file(azqueue: func.QueueMessage):
    logging.info('Python Queue trigger processed a message: %s', azqueue.get_body().decode('utf-8'))

    try:
        # Decodifica a mensagem da fila
        message_body = azqueue.get_body().decode('utf-8')
        sales_data = json.loads(message_body)

        # Gera um ID único para o arquivo
        file_id = uuid.uuid4()
        json_filename = f"Files/Files_{file_id}.json"

        # Escreve os dados em um arquivo JSON
        with open(json_filename, "w", encoding="utf-8") as json_file:
            json.dump(sales_data, json_file)
        
        logging.info(f"Sales data written to JSON file: {json_filename}")

        # Faz o upload do arquivo para o Azure Blob Storage
        upload_to_azure(json_filename)

    except Exception as e:
        logging.error(f"Error processing queue message: {e}")


@app.queue_trigger(arg_name="azqueue", queue_name="filequeuesales",
                   connection="AzureWebJobsstorageaccountt04_STORAGE")
def queue_trigger_file_2(azqueue: func.QueueMessage):
    logging.info('Python Queue trigger (second function) processed a message: %s', azqueue.get_body().decode('utf-8'))

    try:
        # Decodifica a mensagem da fila
        message_body = azqueue.get_body().decode('utf-8')
        sales_data = json.loads(message_body)

        # Gera um ID único para o arquivo
        file_id = uuid.uuid4()
        json_filename = f"Files/Files_{file_id}.json"

        # Escreve os dados em um arquivo JSON
        with open(json_filename, "w", encoding="utf-8") as json_file:
            json.dump(sales_data, json_file)
        
        logging.info(f"Sales data written to JSON file (second function): {json_filename}")

        # Faz o upload do arquivo para o segundo Azure Blob Storage
        upload_to_azure(json_filename, CONTAINER_NAME_2)

    except Exception as e:
        logging.error(f"Error processing queue message (second function): {e}")
        

def upload_to_azure(file_path):
    """Faz upload de um arquivo JSON para o Azure Storage."""
    try:
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=os.path.basename(file_path))

        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        logging.info(f"Uploaded {file_path} to Azure Storage.")
    except Exception as error:
        logging.error(f"Error uploading file {file_path} to Azure Storage: {error}")


