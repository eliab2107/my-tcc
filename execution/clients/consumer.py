import pika
import json
import time
from threading import Thread, Lock
from DTOs.metrics_dtos import RawData

class Consumer:
    def __init__(self, id:int, prefetch_count=1, host="localhost"):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.id = id
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue="fila_teste")
        self.message_count = 0
        self.total_message_count = 0
        self.total_ack_count = 0
        self.processing_time = []
        self.lock = Lock() 
        self.prefetch_count = prefetch_count
        self.channel.basic_qos(prefetch_count=self.prefetch_count)
        self.comands = []
        self.comands_lock = Lock()
        self.apply_control_commands_thread = Thread(target=self.apply_control_commands, daemon=True)
        self.apply_control_commands_thread.start()


    def callback(self, ch, method, properties, body):
        start = time.monotonic()
        message = json.loads(body)

        try:
            with self.lock:
                self.message_count += 1
                self.total_message_count +=1
            self.process_message(message)
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            with self.lock:
                self.total_ack_count +=1
            end = time.monotonic()
            self.processing_time.append(end-start)
            if len(self.comands) != 0:
                cmd = self.comands.pop(0)
                self.set_new_prefetch_count(cmd.prefetch)
                
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


    def get_metrics(self)->list:
        with self.lock:          
            raw_data = RawData(id=self.id, timestamp=time.time(), message_count=self.message_count, total_messages=self.total_message_count, total_acks=self.total_ack_count, prefetch_count=self.prefetch_count)
            self.processing_time = []
            self.message_count   = 0
        return raw_data
    
    
    def get_and_reset_message_count(self):
        with self.lock:
            count = self.message_count
            self.message_count = 0
        return count

    
    def set_new_prefetch_count(self, new_prefetch_vount:int)-> None:
        with self.lock:
            self.channel.basic_qos(prefetch_count=new_prefetch_vount)
            self.prefetch_count = new_prefetch_vount


    def process_message(self, data):
        time.sleep(0.001)
        return 1373737/3
    
        
    def apply_control_commands(self):
        while True:
            if len(self.comands) == 0:
                time.sleep(2)
                continue
            cmd = self.comands.pop(0)  
            self.connection.add_callback_threadsafe(lambda d=cmd.prefetch: self.set_new_prefetch_count(d))
            print(f"[CONSUMER {self.id}] Applied command: {cmd}")
            
            
if __name__ == "__main__":
    consumer = Consumer()
    consumer.start_monitor()
    consumer.start_consuming()
