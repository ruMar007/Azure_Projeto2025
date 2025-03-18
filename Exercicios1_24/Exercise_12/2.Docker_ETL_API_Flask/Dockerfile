# Usar a imagem base do Python
FROM python:3.9-slim

# Definir o diretório de trabalho no contêiner
WORKDIR /app

# Copiar os arquivos do projeto para o contêiner
COPY . /app

# Instalar dependências do sistema necessárias
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    unixodbc \
    unixodbc-dev \
    curl \
    gnupg \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update && ACCEPT_EULA=Y apt-get install -y \
    msodbcsql17 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Instalar dependências do Python
RUN pip install --no-cache-dir -r requirements.txt

# Comando para iniciar o aplicativo
CMD ["python", "app.py"]

#Construa a imagem Docker:
#docker build -t image_function_app .

#Execute o contêiner Docker:
#docker run -d --name conteiner_function_app -p 8000:8000 image_function_app


