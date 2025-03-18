import pyodbc
from config import get_db_config
import logging
import random
from datetime import datetime, timedelta

# Configurar o logger
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='app.log',
    filemode='w'
)

logger = logging.getLogger('meu_logger')

def connect_to_azure_sql():
    """Connect to the Azure SQL Server database using the configuration."""
    try:
        # Get configuration
        config = get_db_config()
        # Connect to the Azure SQL Server database
        connection = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=' + config['host'] + ';'
            'DATABASE=' + config['database'] + ';'
            'UID=' + config['user'] + ';'
            'PWD=' + config['password']
        )
        logger.info("Connection successful to the Azure SQL Server database.")
        return connection
    except Exception as error:
        logger.error(f"An error occurred: {error}")
        return None

# Update your functions to use the new connection function
def get_raw_sales(connection):
    if not hasattr(connection, 'cursor'):
        logger.error("Erro ao conectar a base de dados: Funcion raw_sales: 'connection' object is not a valid database connection")
        return None
    try:
        cursor = connection.cursor()
        cursor.execute("""SELECT p.name AS product_name, COUNT(s.id) AS total_sales, CAST(sales_ts AS DATE) AS sales_date
                        FROM raw.sales s JOIN raw.products p ON s.product_id = p.id
                        GROUP BY CAST(sales_ts AS DATE), p.name
                        ORDER BY CAST(sales_ts AS DATE), total_sales DESC;
                    """)
        sales_data = cursor.fetchall()
        for result in sales_data:
            logger.debug(result)
        cursor.close()
        return sales_data
    except (Exception, pyodbc.DatabaseError) as error:
        logger.error(error)


def insert_sales(connection, sales_data):
    if not hasattr(connection, 'cursor'):
        logger.error("Erro ao conectar a base de dados: Funcion insert_sales: 'connection' object is not a valid database connection")
        return None
    try:
        cursor = connection.cursor()
        cursor.execute("TRUNCATE TABLE report.product_sales;")
        for row in sales_data:
            product_name, total_sales, sale_date = row
            cursor.execute("""
                        INSERT INTO report.product_sales (product_name, nr_sales, sale_date)
                        VALUES (?, ?, ?);""", 
                        (product_name, total_sales, sale_date))
        connection.commit()
        logger.info("Dados inseridos com sucesso na tabela report.product_sales.")
        cursor.close()
        return sales_data
    except (Exception, pyodbc.DatabaseError) as error:
        logger.error(error)


def get_raw_reviews(connection):
    """ Connect to the PostgreSQL database server """
    if not hasattr(connection, 'cursor'):
        logger.error("Erro ao conectar a base de dados: Funcion get_raw_reviews: 'connection' object is not a valid database connection")
        #print("Erro ao conectar a base de dados: Funcion get_raw_reviews: 'connection' object is not a valid database connection")
        return None
    try:
        crsr = connection.cursor()
        #print("Run Select query do número de reviews por produto: ")
        crsr.execute("""
                    SELECT CAST(r.review_ts AS DATE) AS review_date, p.name AS product_name, COUNT(r.id) AS total_reviews
                    FROM raw.reviews r
                    INNER JOIN raw.products p ON p.id = r.product_id
                    GROUP BY CAST(r.review_ts AS DATE), p.name
                    ORDER BY review_date;
                    """)

        #vai buscar os dados do select        
        reviews_data = crsr.fetchall()
        for result in reviews_data:
            logger.debug(result)
            #print(result)
        crsr.close()
        return reviews_data
    
    except (Exception, pyodbc.DatabaseError) as error:
        logger.error(error)
        #print(error)

#Inserir dados na tabela report.product_reviews---------------------------ACABAR ESTA PARTE -----------
def insert_reviews(connection, reviews_data):  
    """ Connect to the PostgreSQL database server """
    if not hasattr(connection, 'cursor'):
        logger.error("Erro ao conectar a base de dados: Funcion insert_reviews: 'connection' object is not a valid database connection")
        #print("Erro ao conectar a base de dados: Funcion insert_reviews: 'connection' object is not a valid database connection")
        return None
    try:
        crsr = connection.cursor()
        #Função para inserir dados de reviews na tabela 'report.product_reviews' --- FALTA O POR DIA!! # Nota: Temos de Truncar a tabela antes de inserir novos dados
        crsr.execute("TRUNCATE TABLE report.product_reviews;")
        
        for result3 in reviews_data:
            date, product_name, total_reviews = result3
            crsr.execute("""
                        INSERT INTO report.product_reviews (date, product_name, nr_reviews)
                        VALUES (?, ?, ?);""", (date, product_name, total_reviews))

        connection.commit()
        logger.info("Dados inseridos com sucesso na tabela report.product_reviews.")
        #print("Dados inseridos com sucesso na tabela report.product_reviews.")

        crsr.close()
        return reviews_data

    except (Exception, pyodbc.DatabaseError) as error:
        logger.error(error)
        #print(error)


def get_products(connection):
    """ Connect to the PostgreSQL database server """
    if not hasattr(connection, 'cursor'):
        logger.error("Erro ao conectar a base de dados: Funcion get_ptoducts: 'connection' object is not a valid database connection")
        return None
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT name FROM raw.products ORDER BY name ASC;")
        result = cursor.fetchall()

        cursor.close()
        products_names = [product_row[0] for product_row in result] if result else [] # Retorna uma lista de nomes de produtos
        logger.debug(products_names)
        return products_names
    
    except Exception as erro:
        logger.error(f"Erro ao obter produtos: {erro}")
        return []


def get_product_id_list(connection, products_names): 
    if not hasattr(connection, 'cursor'):
        logger.error("Erro ao conectar a base de dados: Funcion get_product_id_list: 'connection' object is not a valid database connection")
        return None
    products_list=[]
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT id FROM raw.products")
        result = cursor.fetchall()
        for row in result:
            products_list.append(row[0])
            logger.info(row[0])
            #print(row[0])

        cursor.close()
        return products_list
    except Exception as erro:
        logger.error(f"Erro ao obter product_id: {erro}")
        return None


#Function (dt_inicio, dt_fim) – Vai perguntar uma data inicio e uma data fim for - # Função para imprimir as datas entre duas datas
def lista_de_datas(data_inicial_str, data_final_str):
    # Converter strings de data para objetos datetime
    data_inicial = datetime.strptime(data_inicial_str, '%Y-%m-%d')
    data_final = datetime.strptime(data_final_str, '%Y-%m-%d')
    
    # Criar uma lista de datas
    lista_datas = []
    data_atual = data_inicial
    while data_atual <= data_final:
        lista_datas.append(data_atual.strftime('%Y-%m-%d'))
        #print(data_atual.strftime('%Y-%m-%d')) # Imprimir cada data
        data_atual += timedelta(days=1)
    logger.debug(lista_datas)
    return lista_datas
    

def get_users_id_list(connection): 
    if not hasattr(connection, 'cursor'):
        logger.error("Erro ao conectar a base de dados: Funcion get_users_id_list: 'connection' object is not a valid database connection")
        return None
    user_list=[]
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT id FROM raw.users")
        result = cursor.fetchall()
        for row in result:
            user_list.append(row[0])
            logger.debug(row[0])
            #print(row[0])

        cursor.close()
        return user_list
    except Exception as erro:
        logger.error(f"Erro ao obter user_list: {erro}")
        return None

        
def gerar_sales_aleatorias(connection, data_inicial_str, data_final_str):
    try:
        products = get_products(connection)
        if not products:
            logger.warning("Nenhum produto encontrado.")
            return []

        user_ids = get_users_id_list(connection)
        if not user_ids:
            logger.warning("Nenhum user ID encontrado.")
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

        logger.debug(sales_data)
        print(sales_data)
        return sales_data
    except Exception as erro:
        logger.error(f"Erro ao gerar vendas: {erro}")
        print(f"Erro ao gerar vendas: {erro}")
        return []


# Função para inserir vendas
def insert_sales_data(connection, sales_data):
    try:
        if not sales_data:
            logger.warning("Nenhuma venda a inserir.")
            return
        
        cursor = connection.cursor()
        cursor.execute("TRUNCATE TABLE raw.sales;")
        insert_query = "INSERT INTO raw.sales (product_id, sales_ts, user_id) VALUES (?, ?, ?);"
        
        for sale in sales_data:
            cursor.execute(insert_query, sale)
        
        connection.commit()
        logger.info(f"{len(sales_data)} vendas inseridas com sucesso.")
        cursor.close()
    except Exception as erro:
        logger.error(f"Erro ao inserir vendas: {erro}")
        connection.rollback()

# Possivel lista de textos para as reviews
REVIEW_TEXTS = [
    "Excelente experiência!", "Atendimento impecável.", "Muito profissional.", 
    "Não recomendo.", "Superou as expectativas!", 
    "Equipa muito atenciosa.", "Experiência não foi boa.", 
    "Experiência frustrante.", "Qualidade incrível.", "Experiência mediana!" ]


# Função para gerar reviews de forma aleatórias
def gerar_reviews_aleatorias(data_inicial_str, data_final_str, connection):
    try:
        products = get_products(connection)
        if not products:
            logger.warning("Nenhum produto encontrado.")
            return []
        
        user_ids = get_users_id_list(connection)
        if not user_ids:
            logger.warning("Nenhum user ID encontrado.")
            return []

        data_inicial = datetime.strptime(data_inicial_str, "%Y-%m-%d")
        data_final = datetime.strptime(data_final_str, "%Y-%m-%d")
        reviews_data = []

        current_date = data_inicial
        while current_date <= data_final:
            num_reviews = random.randint(1, 5)  # Número aleatório de reviews por dia
            for _ in range(num_reviews):
                products_names = random.choice(products)
                product_id_list = get_product_id_list(connection, products_names)
                if product_id_list:
                    product_id = random.choice(product_id_list)  # Escolhe um ID aleatório da lista
                    rating = random.randint(1, 5)
                    review_text = random.choice(REVIEW_TEXTS)
                    review_date = current_date + timedelta(
                        hours=random.randint(0, 23),
                        minutes=random.randint(0, 59),
                        seconds=random.randint(0, 59)
                    )
                    user_id = random.choice(user_ids) # Seleciona um user_id válido
                    #enviar para a api

                    reviews_data.append((product_id, rating, review_text, review_date, user_id))#A coluna id é gerada automaticamente
            current_date += timedelta(days=1)

        logger.debug(reviews_data)
        #print(reviews_data)  
        return reviews_data
    except Exception as erro:
        logger.error(f"ERROR: Erro encontrato ao tentar criar as reviews: {erro}")
        return []
   

# Função para inserir as reviews
def insert_reviews_data(connection, reviews_data):
    if not hasattr(connection, 'cursor'):
        logger.error("Erro ao conectar a base de dados: Funcion get_raw_reviews: 'connection' object is not a valid database connection")
        #print("Erro ao conectar a base de dados: Funcion get_raw_reviews: 'connection' object is not a valid database connection")
        return None
    try:
        cursor = connection.cursor()
        cursor.execute("TRUNCATE TABLE raw.reviews;")
        insert_query = """
        INSERT INTO raw.reviews (product_id, rating, review_text_opinion, review_ts, user_id)
        VALUES (?, ?, ?, ?, ?);
        """
        
        for review in reviews_data:
            cursor.execute(insert_query, review)
        connection.commit()
        logger.info(f"{len(reviews_data)} reviews inseridas com sucesso.")
        cursor.close()
    except Exception as erro:
        logger.error(f"ERROR, Erro ao tentar inserir as reviews: {erro}")
        #print(f"ERROR, Erro ao tentar inserir as reviews: {erro}")
        connection.rollback()
    finally:
        connection.close()


if __name__ == '__main__':
    connection = connect_to_azure_sql()
    sales_data = get_raw_sales(connection)
    insert_sales(connection, sales_data)
    #reviews_data = get_raw_reviews(connection)
    #insert_reviews(connection, reviews_data)
    #products_names = get_products(connection)
    #products_id = get_product_id_list(connection, products_names)
    data_inicial_str = '2024-12-01'
    data_final_str = '2024-12-10'
    #lista_de_datas(data_inicial_str, data_final_str)
    #get_users_id_list(connection)
    #sales_data = gerar_sales_aleatorias(connection, data_inicial_str, data_final_str)
    #insert_sales_data(connection, sales_data)
    #reviews_data = gerar_reviews_aleatorias(data_inicial_str, data_final_str, connection)
    #insert_reviews_data(connection, reviews_data)
