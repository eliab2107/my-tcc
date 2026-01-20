import time
from threading import Thread
from execution.clients.consumer import Consumer   
from typing import List, Dict
from DTOs.decision import Decision

class ConsumerManager:
    def __init__(self, num_consumers: int = 1, monitor_interval: float = 1.0):
        self.num_consumers = num_consumers
        self.monitor_interval = monitor_interval
        self.consumers: List[Consumer] = []
        self.consumer_threads:List[Thread] = []
        self.monitor_thread = None
        self.running = False


    def start_consumers(self):
        self.running = True

        for i in range(self.num_consumers):
            consumer = Consumer(id=i)
            thread = Thread(
                target=consumer.start_consuming,
                daemon=True,
                name=f"consumer-thread-{i}"
            )

            self.consumers.append(consumer)
            self.consumer_threads.append(thread)

            thread.start()
        
        
        self.monitor_thread = Thread(
            target=self.monitor_loop,
            daemon=True,
            name="consumer-monitor"
        )
        self.monitor_thread.start()


    def monitor_loop(self):
        while self.running:
            time.sleep(self.monitor_interval)
            self.snapshot_metrics()


    def snapshot_metrics(self)->dict:
        metrics = {}
        for consumer in self.consumers:
            metrics[consumer.id] = consumer.get_metrics()

        return metrics
        
        
    def stop(self):
        self.running = False
        print("Processo encerrado")
    
    
    def set_new_prefetch_counts(self, id:int, new_pc:int):
        self.consumers[id].set_new_prefetch_count(new_pc)
    
    
    def get_consumers(self):
        return self.consumers


    def update_prefetchs(self, decisions:List[Decision])-> None:
        for decision in decisions:
            consumer = self.consumers[decision.consumer_id]
            with consumer.comands_lock:
                consumer.comands.append(decision)


if __name__ == "__main__":
    manager = ConsumerManager(num_consumers=3)
    manager.start_consumers()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        manager.stop()