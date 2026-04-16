from dotenv import load_dotenv
import pika
import json
import time
from threading import Thread, Lock
import os
load_dotenv()
class Consumer:
    def __init__(self, id:int, prefetch_count=1):
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


    def callback(self, ch, method, properties, body):
        start = time.monotonic()
        message = json.loads(body)
        self.process_message(message)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        end = time.monotonic()
        with self.lock:
            self.messsages_metrics.append([end - start, message.get("date_created"), time.time()])
            for m in self.messsages_metrics:
                print(f"Consumer {self.id} - Tempo de processamento: {m[0]:.4f} segundos - Tempo na fila: {m[1] - message.get('date_created', time.time()):.4f} segundos")
                
    def start_consuming(self)-> None:
        self.channel.basic_consume(
            queue="fila_teste",
            on_message_callback=self.callback,
            auto_ack=False
        )
        print("[CONSUMER] Consumindo mensagens...")
        self.channel.start_consuming()
        
    
    def set_new_prefetch_count(self, new_prefetch_vount:int)-> None:
        with self.lock:
            self.channel.basic_qos(prefetch_count=new_prefetch_vount)
            self.prefetch_count = new_prefetch_vount


    def process_message(self, data):
        time.sleep(0.001)
        return 1373737/3
    
            
if __name__ == "__main__":
    consumer = Consumer(id=1)
    consumer.start_consuming()
