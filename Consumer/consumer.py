from dotenv import load_dotenv
import pika
import json
import time
import requests
import math
import random
from threading import Thread, Lock
import os
from concurrent.futures import ThreadPoolExecutor

load_dotenv()

class Consumer:
    def __init__(self, id:int, prefetch_count=1, task_level=1):
        self.host = os.getenv("RABBITMQ_HOST")
        self.user = os.getenv("RABBITMQ_USER")
        self.password = os.getenv("RABBITMQ_PASS")  
        credentials = pika.PlainCredentials(self.user, self.password)

        self.executor = ThreadPoolExecutor(max_workers=25)
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
        self.channel.basic_consume(queue="fila_teste", on_message_callback=self.callback, auto_ack=True)
        self.task_level = task_level
        self.pending_prefetch = None


    def callback(self, ch, method, properties, body):

        with self.lock:
            if self.pending_prefetch is not None:
                print("prefetch atualizado:", self.pending_prefetch)

                self.channel.basic_qos(
                    prefetch_count=self.pending_prefetch
                )

                self.prefetch_count = self.pending_prefetch
                self.pending_prefetch = None

        start = time.monotonic()

        message = json.loads(body)

        future = self.executor.submit(
            self.process_message,
            message
        )

        def done_callback(_):

            end = time.monotonic()


            with self.lock:
                dados = [
                    end - start,
                    time.time() - message.get("date_created")
                ]

                self.messsages_metrics.append(dados)

        future.add_done_callback(done_callback)
                    
    def start_consuming(self)-> None:
        return self.channel.start_consuming()
    
    
    def get_data(self):
        with self.lock:
            matriz = self.messsages_metrics.copy()
            self.messsages_metrics.clear()
            return matriz    
    
    def set_new_prefetch_count(self, ajuste: int) -> None:
        with self.lock:
            self.pending_prefetch = max(1, self.prefetch_count + ajuste)
            


    def process_message(self, data):
        network_url = "https://www.google.com"
        if not data.get("task_level") or data.get("task_level") == 1:
            time.sleep(0.01)
            
            
        elif data.get("task_level") == 2:
            # for _ in range(self.task_level * 1_000_0):
             #   _ = math.sqrt(12345.6789) * math.sin(0.5)
            time.sleep(0.01)
                
        elif data.get("task_level") == 3: 
            #try:
             #   r = requests.get(network_url, timeout=10) 
            #except Exception as e:
             #   print(f"<- Erro de rede: {e}") 
            time.sleep(0.01)

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
