from dotenv import load_dotenv
import pika
import json
import time
import requests
import math
from threading import Thread, Lock
import os

load_dotenv()

class Consumer:
    def __init__(self, id:int, prefetch_count=1, task_level=1):
        self.host = os.getenv("RABBITMQ_HOST")
        self.user = os.getenv("RABBITMQ_USER")
        self.password = os.getenv("RABBITMQ_PASS")
        print(f"Conectando ao RabbitMQ em {self.host} com usuário {self.user} {self.password}")
        credentials = pika.PlainCredentials(self.user, self.password)

        parametros = pika.ConnectionParameters(
            host=self.host, 
            credentials=credentials
        )
        self.connection = pika.BlockingConnection(parametros)
        self.id = id
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue="fila_teste")
        self.messsages_metrics = []
        self.lock = Lock() 
        self.prefetch_count = prefetch_count
        self.channel.basic_qos(prefetch_count=self.prefetch_count)
        self.channel.basic_consume(queue="fila_teste", on_message_callback=self.callback, auto_ack=False)
        self.task_level = task_level


    def callback(self, ch, method, properties, body):
        start = time.monotonic()
        message = json.loads(body)
        self.process_message(message)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        end = time.monotonic()
        with self.lock:
            dados = [end - start, time.time() - message.get("date_created")]
            self.messsages_metrics.append(dados)
                
    def start_consuming(self)-> None:
        print("[CONSUMER] Consumindo mensagens...")
        return self.channel.start_consuming()
    
    
    def get_data(self):
        with self.lock:
            matriz = self.messsages_metrics.copy()
            self.messsages_metrics.clear()
            return matriz    
    
    def set_new_prefetch_count(self, new_prefetch_vount:int)-> None:
        with self.lock:
            self.channel.basic_qos(prefetch_count=new_prefetch_vount)
            self.prefetch_count = new_prefetch_vount


    def process_message(self, data):
        network_url = "https://www.google.com"
        if self.task_level == 1:
            for _ in range(self.task_level * 1_000):
                _ = math.sqrt(12345.6789) * math.sin(0.5)
                
        elif self.task_level == 2:
             for _ in range(self.task_level * 1_000_000):
                _ = math.sqrt(12345.6789) * math.sin(0.5)
                
        elif self.task_level == 3:
            try:
                r = requests.get(network_url, timeout=10) 
            except Exception as e:
                print(f"<- Erro de rede: {e}") 
                

    def stop(self):
        """Chamado pela Thread do Manager"""
        print(f"[Consumer {self.id}] Solicitando parada segura...")
        if self.connection and self.connection.is_open:
            # Envia o comando para a thread correta executar
            self.connection.add_callback_threadsafe(self._realizar_parada_segura)
            
            
    def _realizar_parada_segura(self):
        """Executado pela Thread do Consumer (Dona da conexão)"""
        print(f"[Consumer {self.id}] Fechando canais e conexão...")
        if self.channel and self.channel.is_open:
            self.channel.stop_consuming()
        if self.connection and self.connection.is_open:
            self.connection.close()
            
            
if __name__ == "__main__":
    consumer = Consumer(id=1)
    consumer.start_consuming()
