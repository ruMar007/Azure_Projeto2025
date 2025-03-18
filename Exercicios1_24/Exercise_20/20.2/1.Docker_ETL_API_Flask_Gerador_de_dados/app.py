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
    """Generate random sales data and send to API in batches of 10,000, pausing every 10 events."""
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

        current_date = data_inicial
        url = "http://localhost:7071/api/insert_EventHub_azure"
        headers = {'Content-Type': 'application/json'}

        total_events = 0  # Counter for total events sent

        while current_date <= data_final:
            batch_data = []  # Batch of 10,000 events
            for _ in range(10000):  # Generate 10,000 events
                product_id_list = get_product_id_list(connection)
                if not product_id_list:
                    logging.warning("No product IDs found.")
                    continue

                product_id = random.choice(product_id_list)
                sales_ts = f"{current_date.strftime('%Y-%m-%d')} {random.randint(0, 23):02}:{random.randint(0, 59):02}:{random.randint(0, 59):02}"
                user_id = random.choice(user_ids)

                payload = {
                    "product": product_id,
                    "sales_ts": sales_ts,
                    "user_id": user_id
                }
                batch_data.append(payload)

                total_events += len(batch_data)  # Update the total events counter
                current_date += timedelta(days=1)  # Move to the next day

                # Send batch of 10,000 events to the API
                try:
                    response = requests.post(url, headers=headers, data=json.dumps(batch_data))
                    if response.status_code == 200:
                        logging.info(f"Event sent successfully.")
                    else:
                        logging.error(f"Failed to send event: {response.status_code} - {response.text}")
                except requests.RequestException as error:
                    logging.error(f"Error sending event to API: {error}")

                # Pause every 10 events
                if len(batch_data) % 10 == 0:
                    time.sleep(1)


    except Exception as error:
        logging.error(f"Error generating sales: {error}")


if __name__ == '__main__':
    connection = connect_to_azure_sql()
    if connection:
        data_inicial_str = '2024-12-01'
        data_final_str = '2024-12-10'
        gerar_sales_aleatorias(connection, data_inicial_str, data_final_str)
    else:
        logging.error("Failed to establish database connection.")


