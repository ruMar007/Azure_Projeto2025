import psycopg2

Connection = None

try:
    # Conecta ao banco de dados local (PostgreSQL)
    Connection = psycopg2.connect(
        host="host.docker.internal",  # Permite ao contêiner aceder ao Postgres (BD) do anfitrião host
        database="09.12AZURE",
        user="postgres",
        password="postgres",
        port="5432"  # Porta do Postgres no Docker
    )

    # Query para obter reviews por produto
    query_reviews = """
        SELECT p.name AS product_name, COUNT(p.id) AS nr_reviews
        FROM raw.products p
        GROUP BY p.name
        ORDER BY p.name ASC;
    """
    cursor = Connection.cursor()
    cursor.execute(query_reviews)
    reviews_results = cursor.fetchall()

    # Mostrar resultados
    print("Resultados das reviews: ")
    for row in reviews_results:
        print(f"Produto: {row[0]}, Número de reviews: {row[1]}")

    cursor.close()

except Exception as ERROR:
    print(f"Erro: {ERROR}")

finally:
    if Connection:
        Connection.close()
        print("Conexão ao banco de dados fechada.")


#Criar uma função para gerar vendas