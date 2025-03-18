from flask import Flask, request, jsonify
import pyodbc
import json
import os

app = Flask(__name__)

# POST /data - Aceita um JSON e insere os dados na base de dados
@app.route('/data', methods=['POST'])
def post_data():
    connection = connect_to_azure_sql()
    if not connection:
        return jsonify({"error": "Erro ao conectar à base de dados"}), 500
    
    data = request.get_json()
    try:
        product_id = data.get('product')
        sales_ts = data.get('sales_ts')
        user_id = data.get('user_id')

        if not (product_id and sales_ts and user_id):
            return jsonify({"error": "Todos os campos (product, sales_ts, user_id) são obrigatórios"}), 400
        
        # Insert data into the database
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO raw.sales (product_id, sales_ts, user_id)
        VALUES (?, ?, ?);
        """
        cursor.execute(insert_query, product_id, sales_ts, user_id)
        connection.commit()
        cursor.close()
        
        # Write the data into a JSON file
        file_name = f"sales_{product_id}_{user_id}_{sales_ts.replace(':', '').replace(' ', '_')}.json"
        file_path = os.path.join("json_files", file_name)

        # Ensure the folder 'json_files' exists
        os.makedirs("json_files", exist_ok=True)

        with open(file_path, 'w', encoding='utf-8') as json_file:
            json.dump(data, json_file, indent=4, ensure_ascii=False)

        return jsonify({"message": "Dados inseridos com sucesso"}), 201
    
    except Exception as erro:
        print(f"ERROR: Erro ao tentar inserir as sales: {erro}")
        connection.rollback()
        return jsonify({"error": "Erro ao inserir os dados"}), 500
    
    finally:
        connection.close()

def connect_to_azure_sql():
    """Conecta à base de dados Azure SQL Server."""
    try:
        connection = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=dbup04.database.windows.net;'
            'DATABASE=db-04;'
            'UID=rute;'
            'PWD=FreireMarques*'
        )
        print("Conexão bem-sucedida ao Azure SQL Server.")
        return connection
    except Exception as error:
        print(f"Erro ao conectar à base de dados: {error}")
        return None

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
