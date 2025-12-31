import pika
import json
import time
from threading import Thread, Lock

class Consumer:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host="localhost")
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue="fila_teste")

        self.message_count = 0
        self.total_message_count = 0
        self.total_ack_count = 0
        self.lock = Lock()
        self.channel.basic_qos(prefetch_count=100)

    def callback(self, ch, method, properties, body):
        message = json.loads(body)

        with self.lock:
            self.message_count += 1
            self.total_message_count +=1
        try:
            self.process_message(message)
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print("ERROR: ", e)
            
            
            
    def start_consuming(self):
        self.channel.basic_consume(
            queue="fila_teste",
            on_message_callback=self.callback,
            auto_ack=False
        )
        print("[CONSUMER] Consumindo mensagens...")
        self.channel.start_consuming()


    def start_monitor(self):
        def monitor():
            while True:
                time.sleep(2)
                with self.lock:
                    print(f"[MONITOR] Mensagens recebidas: {self.message_count}")
                    self.message_count = 0
                    
        Thread(target=monitor, daemon=True).start()


    def process_message(self, data):
        return 137/3
        
        
if __name__ == "__main__":
    consumer = Consumer()
    consumer.start_monitor()
    consumer.start_consuming()
