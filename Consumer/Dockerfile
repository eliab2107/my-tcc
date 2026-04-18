FROM python:3.9-slim
WORKDIR /app

# Busca o requirements na raiz (contexto .)
COPY requirements.txt . 
RUN pip install --no-cache-dir -r requirements.txt



CMD ["python", "-m", "Consumer.manager"]