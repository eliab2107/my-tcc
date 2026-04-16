#Produtor

from multiprocessing import connection
import time
import pika
import json
from dotenv import load_dotenv  

import os

load_dotenv()
class Produtor():
    
    def __init__(self, time_sleep):
        self.time_sleep = time_sleep
        self.host = os.getenv("RABBITMQ_HOST")
        self.user = os.getenv("RABBITMQ_USER")
        self.password = os.getenv("RABBITMQ_PASS")
        
        credentials = pika.PlainCredentials(self.user, self.password)

        parametros = pika.ConnectionParameters(
            host=self.host, 
            credentials=credentials
        )
        
        self.connection = pika.BlockingConnection(parametros)
        self.channel = self.connection.channel()
        self.queue =  os.getenv("RABBITMQ_QUEUE", "fila_teste")
        self.channel.queue_declare(queue=self.queue)
        self.counter = 0


    def publicar_mensagens(self):
        print("[PUBLISHER] Publicando mensagens...")

        while True:
            message_body = {
                "id": self.counter,
                "conteudo": f"Mensagem {self.counter}",
                "cep": "01001-000",
                "date_created": time.time()
            }

            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue,
                body=json.dumps(message_body).encode("utf-8")
            )

            self.counter += 1

if __name__ == "__main__":
    time_sleep = 1  # Tempo de espera entre mensagens (em segundos)
    produtor = Produtor(time_sleep)
    produtor.publicar_mensagens()