import azure.functions as func
import logging
import json
import pyodbc
from datetime import datetime

app = func.FunctionApp()

########## QUEUE TRIGGER --> Sempre que inserir valores na QUEUE bdqueuesales vai para a BD  ############  
@app.queue_trigger(arg_name="azqueue", queue_name="bdqueuesales",
                   connection="AzureWebJobsstorageaccountt04_STORAGE")
def queue_trigger_bd(azqueue: func.QueueMessage):
    logging.info('Python Queue trigger processed a message: %s', azqueue.get_body().decode('utf-8'))

    try:
        # Decodifica a mensagem da fila
        message_body = azqueue.get_body().decode('utf-8')
        sales_data = json.loads(message_body)

        # Conecta-se à base de dados e insere os dados
        insert_db(sales_data)

    except Exception as e:
        logging.error(f"Error processing queue message: {e}")

def insert_db(sales_data):
    """Insere os dados na base de dados da VM."""
    logging.info("Inserting sales data into the database.")

    connection = pyodbc.connect(
        DRIVER="{ODBC Driver 18 for SQL Server}",
        SERVER="dbup04.database.windows.net",
        DATABASE="db-04",
        UID="rute",
        PWD="FreireMarques*"
    )
    if not connection:
        logging.error("Failed to connect to database.")
        return

    cursor = connection.cursor()

    try:
        #print("SSSSS", type(sales_data) )
        product_id = sales_data.get("product")
        sales_ts = sales_data.get("sales_ts")
        user_id = sales_data.get("user_id")
        print(type(user_id))
        if not product_id or not sales_ts or not user_id:
            logging.error("Invalid input data.")
            return

        cursor.execute(
            """
            INSERT INTO raw.sales (product_id, sales_ts, user_id)
            VALUES (?, ?, ?)
            """,
            (int(product_id), datetime.fromisoformat(sales_ts), int(user_id))
        )
        connection.commit()
        logging.info("Sale recorded successfully.")
    except Exception as error:
        logging.error(f"Error inserting sales data: {error}")
        
    finally:
        cursor.close()
        connection.close()
