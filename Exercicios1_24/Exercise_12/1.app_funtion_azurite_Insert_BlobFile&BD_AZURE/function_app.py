import azure.functions as func
import logging
import json
import pyodbc
import os
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import uuid


# Inicializa a Function App
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Configuração da conta de armazenamento do Azure
STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=storageaccountt04;AccountKey=j2HYuzsHHzGMNepvs4Lddht463d1zpe+N4zNXzIxnJP5kbQptleNwTq7by5RHAE1PC8Wop2+kn8P+AStSwFt6g==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "container04"

# Garante que a pasta "Files" existe
if not os.path.exists("Files"):
    os.makedirs("Files")


##########Insert in DB in Azure##########
@app.route(route="insert_db_azure", methods=["POST"])
def insert_db_azure(req: func.HttpRequest) -> func.HttpResponse:
    """Recebe os dados e insere no banco de dados."""
    logging.info("Receiving sales data and inserting into the database.")

    connection = pyodbc.connect(
        DRIVER="{ODBC Driver 18 for SQL Server}",
        SERVER="dbup04.database.windows.net",
        DATABASE="db-04",
        UID="rute",
        PWD="FreireMarques*"
    )
    if not connection:
        return func.HttpResponse("Failed to connect to database.", status_code=500)

    cursor = connection.cursor()

    try:
        req_body = req.get_json()
        product_id = req_body.get("product")
        sales_ts = req_body.get("sales_ts")
        user_id = req_body.get("user_id")

        if not product_id or not sales_ts or not user_id:
            return func.HttpResponse("Invalid input data.", status_code=400)

        cursor.execute(
            """
            INSERT INTO raw.sales (product_id, sales_ts, user_id)
            VALUES (?, ?, ?)
            """,
            (product_id, sales_ts, user_id)
        )
        connection.commit()

        return func.HttpResponse("Sale recorded successfully.", status_code=200)
    except Exception as error:
        logging.error(f"Error inserting sales data: {error}")
        return func.HttpResponse("Error processing request.", status_code=500)
    finally:
        cursor.close()
        connection.close()


##############Insert in BlobFile in AZURE############ -- Storage Account --> Containers --> Conteiner04
@app.route(route="insert_blobfile_azure", methods=["POST"])
def insert_blobfile_azure(req: func.HttpRequest) -> func.HttpResponse:
    """Recebe dados de vendas e escreve em arquivos JSON antes de enviá-los para o Azure."""
    logging.info("Writing sales data to JSON files.")

    try:
        req_body = req.get_json() #json vindo do postman/docker 
        file_id = uuid.uuid4() #id unico para cada ficheiro de só 1 linha
        json_filename = f"Files_{file_id}.json"

        with open(json_filename, "w", encoding="utf-8") as json_file:
            json.dump(req_body, json_file) #dump --> Envia o json para o ficheiro

        upload_to_azure(json_filename)  # Faz o upload para o Azure Storage

        return func.HttpResponse("Sales data successfully written to JSON files and uploaded.", status_code=200)

    except Exception as error:
        logging.error(f"Error writing sales data to JSON file: {error}")
        return func.HttpResponse("Error processing request.", status_code=500)
    
    #2 Queues no Azure 
    #um endpoint que vai enviar para duas QUEUES uma para BD e outra para File um json
    #Duas novas appfuntions QUEUETRIGGER uma dessas appfuntions vai ler o json e vai escrever para a BD e a outra escreve para ficheiro.


def upload_to_azure(file_path):
    """Faz upload de um arquivo JSON para o Azure Storage."""
    try:
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING) #faz uma conecção com o azure Blob Storage 
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=os.path.basename(file_path))

        with open(file_path, "rb") as data: #abre o ficheiro para leitura
            blob_client.upload_blob(data, overwrite=True) #envia o ficheiro para o azure 

        logging.info(f"Uploaded {file_path} to Azure Storage.")

    except Exception as error:
        logging.error(f"Error uploading file {file_path} to Azure Storage: {error}")

