import psycopg2
import pyodbc
import logging
import random
from datetime import datetime, timedelta
import requests
import json
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def connect_to_azure_sql():
    """Connect to the Azure SQL Server database using the configuration."""
    try:
        connection = pyodbc.connect(
            DRIVER="{ODBC Driver 17 for SQL Server}",
            SERVER="dbup04.database.windows.net",
            DATABASE='db-04',
            UID='rute',
            PWD='FreireMarques*'
        )
        logging.info("Connection successful to the Azure SQL Server database.")
        return connection
    except Exception as error:
        logging.error(f"Error connecting to Azure SQL Server: {error}")
        return None

def get_products(connection):
    """Fetch product names from the database."""
    if not connection or not hasattr(connection, 'cursor'):
        logging.error("Invalid database connection in get_products.")
        return []
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT name FROM raw.products ORDER BY name ASC;")
        result = cursor.fetchall()
        cursor.close()
        products_names = [row[0] for row in result] if result else []
        logging.info(f"Products fetched: {products_names}")
        return products_names
    except Exception as error:
        logging.error(f"Error fetching products: {error}")
        return []

def get_users_id_list(connection):
    """Fetch user IDs from the database."""
    if not connection or not hasattr(connection, 'cursor'):
        logging.error("Invalid database connection in get_users_id_list.")
        return []
    try:
        user_list = []
        cursor = connection.cursor()
        cursor.execute("SELECT id FROM raw.users;")
        result = cursor.fetchall()
        user_list = [row[0] for row in result]
        cursor.close()
        logging.info(f"User IDs fetched: {user_list}")
        return user_list
    except Exception as error:
        logging.error(f"Error fetching user IDs: {error}")
        return []

def get_product_id_list(connection):
    """Fetch product IDs from the database."""
    if not connection or not hasattr(connection, 'cursor'):
        logging.error("Invalid database connection in get_product_id_list.")
        return []
    try:
        products_list = []
        cursor = connection.cursor()
        cursor.execute("SELECT id FROM raw.products;")
        result = cursor.fetchall()
        products_list = [row[0] for row in result]
        cursor.close()
        logging.info(f"Product IDs fetched: {products_list}")
        return products_list
    except Exception as error:
        logging.error(f"Error fetching product IDs: {error}")
        return []

def gerar_sales_aleatorias(connection, data_inicial_str, data_final_str):
    """Generate random sales data and send to API."""
    try:
        products = get_products(connection)
        if not products:
            logging.warning("No products found.")
            return []

        user_ids = get_users_id_list(connection)
        if not user_ids:
            logging.warning("No user IDs found.")
            return []

        data_inicial = datetime.strptime(data_inicial_str, "%Y-%m-%d")
        data_final = datetime.strptime(data_final_str, "%Y-%m-%d")
        sales_data = []

        current_date = data_inicial
        url = "https://04eventhubfunction.azurewebsites.net/api/insert_EventHub_azure?"
        #url = "http://host.docker.internal:7071/api/insert_EventHub_azure" # URL para acessar o serviço App function na máquina host que envia para o EVENT HUB, o gerador de dados é o Docker, a API só recebe os dados
        #url = "http://host.docker.internal:7071/api/insert_db_azure" # URL para acessar o serviço App function na máquina host que escreve na BD, o gerador de dados é o Docker, a API só recebe os dados
        #url = "http://host.docker.internal:7071/api/write_to_json"  # URL para acessar o serviço App function na máquina host que escreve no blob file do Azure a partir do Docker
        #url = "http://host.docker.internal:7071/api/http_trigger"  # URL para acessar o serviço App function na máquina host que escreve na BD a partir do Docker
        #url = "https://04functionapp.azurewebsites.net/api/http_trigger" #URL da API na APPFuction04 do Azureque escreve na BD      
        #url = "http://108.143.184.73:5000/data" #URL da API na VM do Azure que escreve na BD.
        headers = {'Content-Type': 'application/json'}

        while current_date <= data_final:
            num_sales = random.randint(1, 40)
            for _ in range(num_sales):
                product_id_list = get_product_id_list(connection)
                if not product_id_list:
                    logging.warning("No product IDs found.")
                    continue

                product_id = random.choice(product_id_list)
                sales_ts = f"{current_date.strftime('%Y-%m-%d')} {random.randint(0, 23):02}:{random.randint(0, 59):02}:{random.randint(0, 59):02}"
                user_id = random.choice(user_ids)

                payload = json.dumps({
                    "product": product_id,
                    "sales_ts": sales_ts,
                    "user_id": user_id
                })
                try:
                    response = requests.request("POST", url, headers=headers, data=payload)
                    if response.status_code == 200:
                        logging.info(f"Sale sent successfully: {payload}")
                    else:
                        logging.error(f"Failed to send sale: {response.status_code} - {response.text}")
                except requests.RequestException as error:
                    logging.error(f"Error sending sale to API: {error}")

                time.sleep(1)

            current_date += timedelta(days=1)

        return sales_data
    except Exception as error:
        logging.error(f"Error generating sales: {error}")
        return []

if __name__ == '__main__':
    connection = connect_to_azure_sql()
    if connection:
        data_inicial_str = '2024-12-01'
        data_final_str = '2024-12-10'
        gerar_sales_aleatorias(connection, data_inicial_str, data_final_str)
    else:
        logging.error("Failed to establish database connection.")


