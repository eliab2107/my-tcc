import pika
import json
import time
from threading import Thread, Lock

class Consumer:
    def __init__(self, id:int, prefetch_count=1000, host="localhost"):
        self.connection = pika.BlockingConnection( pika.ConnectionParameters(host=host))
        self.id = id
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue="fila_teste")
       
        self.message_count = 0
        self.total_message_count = 0
        self.total_ack_count = 0
        self.processing_time = []
        self.lock = Lock() 
        self.prefeth_count = prefetch_count
        self.channel.basic_qos(prefetch_count=self.prefeth_count)
        

    def callback(self, ch, method, properties, body):
        start = time.monotonic()
        message = json.loads(body)

        with self.lock:
            self.message_count += 1
            self.total_message_count +=1
        try:
            self.process_message(message)
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            with self.lock:
                self.total_ack_count +=1
            end = time.monotonic()
            self.processing_time.append(end-start)
            
        except Exception as e:
            print("ERROR: ", e)
            
            
            
    def start_consuming(self)-> None:
        self.channel.basic_consume(
            queue="fila_teste",
            on_message_callback=self.callback,
            auto_ack=False
        )
        print("[CONSUMER] Consumindo mensagens...")
        self.channel.start_consuming()


    def start_monitor(self)-> None:
        def monitor():
            while True:
                time.sleep(2)
                with self.lock:
                    print(f"[MONITOR] Mensagens recebidas: {self.message_count}")
                    self.message_count = 0
                    
        Thread(target=monitor, daemon=True).start()


    def get_metrics(self)->list:
        with self.lock:
            message_count  = self.message_count
            process_times  = self.processing_time
            total_messages = self.total_message_count
            total_ack      = self.total_ack_count 
            
            self.processing_time = []
            self.message_count   = 0
            
        #da pra retornar um rawData daqui
        return [message_count, total_messages, total_ack, self.prefeth_count, process_times]
    
    
    def get_and_reset_message_count(self):
        with self.lock:
            count = self.message_count
            self.message_count = 0
        return count

    
    def set_new_prefetch_count(self, new_prefetch_vount:int)-> None:
        with self.lock:
            self.channel.basic_qos(prefetch_count=new_prefetch_vount)
            self.prefeth_count = new_prefetch_vount

    def process_message(self, data):
        time.sleep(0.1)
        return 1373737/3
        
        
if __name__ == "__main__":
    consumer = Consumer()
    consumer.start_monitor()
    consumer.start_consuming()
