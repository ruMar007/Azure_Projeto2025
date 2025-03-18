import psycopg2
import sqlite3
from datetime import datetime, timedelta
import random
from config import get_db_config

def connect_to_PostgreSQL():
    """Connect to the PostgreSQL database using the configuration."""
    try:
        # Get configuration
        config = get_db_config()
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(**config)
        print("Connection successful to the PostgreSQL database.")
        return connection
    except Exception as error:
        print(f"An error occurred: {error}")
        return None

#Criar uma função para gerar vendas
def get_products(connection):
    """ Connect to the PostgreSQL database server """
    if not hasattr(connection, 'cursor'):
        print("Erro ao conectar a base de dados: Funcion get_ptoducts: 'connection' object is not a valid database connection")
        return None
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT name FROM raw.products ORDER BY name ASC;")
        result = cursor.fetchall()

        cursor.close()
        products_names = [product_row[0] for product_row in result] if result else [] # Retorna uma lista de nomes de produtos
        print(products_names)
        return products_names
    
    except Exception as erro:
        print(f"Erro ao obter produtos: {erro}")
        return []

def get_users_id_list(connection): 
    if not hasattr(connection, 'cursor'):
        print("Erro ao conectar a base de dados: Funcion get_users_id_list: 'connection' object is not a valid database connection")
        return None
    user_list=[]
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT id FROM raw.users")
        result = cursor.fetchall()
        for row in result:
            user_list.append(row[0])
            #print(row[0])

        cursor.close()
        return user_list
    except Exception as erro:
        print(f"Erro ao obter user_list: {erro}")
        return None
    
def get_product_id_list(connection, products_names): 
    if not hasattr(connection, 'cursor'):
        print("Erro ao conectar a base de dados: Funcion get_product_id_list: 'connection' object is not a valid database connection")
        return None
    products_list=[]
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT id FROM raw.products")
        result = cursor.fetchall()
        for row in result:
            products_list.append(row[0])
            #print(row[0])

        cursor.close()
        return products_list
    except Exception as erro:
        print(f"Erro ao obter product_id: {erro}")
        return None
    
def gerar_sales_aleatorias(connection, data_inicial_str, data_final_str):
    try:
        products = get_products(connection)
        if not products:
            print("Nenhum produto encontrado.")
            return []

        user_ids = get_users_id_list(connection)
        if not user_ids:
            print("Nenhum user ID encontrado.")
            return []

        data_inicial = datetime.strptime(data_inicial_str, "%Y-%m-%d")
        data_final = datetime.strptime(data_final_str, "%Y-%m-%d")
        sales_data = []

        current_date = data_inicial
        while current_date <= data_final:
            num_sales = random.randint(1, 10)  # Número aleatório de vendas por dia
            for _ in range(num_sales):
                products_names = random.choice(products)
                product_id_list = get_product_id_list(connection, products_names)
                if product_id_list:
                    product_id = product_id_list[0]  # Seleciona o primeiro ID da lista
                    sales_ts = f"{current_date.strftime('%Y-%m-%d')} {random.randint(0, 23):02}:{random.randint(0, 59):02}:{random.randint(0, 59):02}"
                    user_id = random.choice(user_ids) # Seleciona um user_id válido
                    sales_data.append((product_id, sales_ts, user_id)) #A coluna id é gerada automaticamente
            current_date += timedelta(days=1)

        print(sales_data)
        return sales_data
    except Exception as erro:
        print(f"Erro ao gerar vendas: {erro}")
        return []


# Função para inserir vendas
def insert_sales_data(connection, sales_data):
    try:
        if not sales_data:
            print("Nenhuma venda a inserir.")
            return
        
        cursor = connection.cursor()
        cursor.execute("TRUNCATE TABLE raw.sales;")
        insert_query = "INSERT INTO raw.sales (product_id, sales_ts, user_id) VALUES (%s, %s, %s);"
        
        for sale in sales_data:
            cursor.execute(insert_query, sale)
        
        connection.commit()
        print(f"{len(sales_data)} vendas inseridas com sucesso.")
        cursor.close()
    except Exception as erro:
        print(f"Erro ao inserir vendas: {erro}")
        connection.rollback()

if __name__ == '__main__':
    connection = connect_to_PostgreSQL()
    data_inicial_str = '2024-12-01'
    data_final_str = '2024-12-10'
    products_names = get_products(connection)
    get_users_id_list(connection)
    products_id = get_product_id_list(connection, products_names)
    sales_data = gerar_sales_aleatorias(connection, data_inicial_str, data_final_str)
    insert_sales_data(connection, sales_data)