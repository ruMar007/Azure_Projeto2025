# Usa uma imagem base do Python - Estou a fazer download do python 3.9
FROM python:3.9-slim

# Define o diretório de trabalho no contêiner - Aqui estou a dizer que todos os comandos que eu vou fazer a seguir vao ser executados no diretório /app
WORKDIR /app

# Copia o arquivo Python para o contêiner - Cópia o arquivo app.py para o diretório /app (destino), logo vai ficar na pasta /app/app.py
COPY app.py .

# Instala o SQLite
RUN apt-get update && apt-get install -y sqlite3
RUN pip install psycopg2-binary

# Executa o arquivo Python - Vai à linha de comandos e basicamente vai executar python app.py 
CMD ["python", "app.py"]

#Construir a imagem Docker - ELe só cria a imagem local na nossa maquina "hello-world-python-app" na nossa maquina que é uma cópia da python:3.9-slim
#docker build -t hello-world-python-app .

#Executar o contêiner Docker - Agora é que efetivamente vamos criar o contêiner e executa-lo
#docker run --name hello-world-container hello-world-python-app


