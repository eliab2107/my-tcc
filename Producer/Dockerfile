FROM python:3.9-slim
WORKDIR /app

# Busca o requirements na raiz (contexto .)
COPY requirements.txt . 
RUN pip install --no-cache-dir -r requirements.txt

# Copia tudo da raiz para dentro do container
COPY . .

CMD ["python", "Producer/produtor.py"]