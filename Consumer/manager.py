import time
from threading import Thread
from Consumer import consumer
from typing import List
import psutil
import os

class ConsumerManager:
    def __init__(self, num_consumers: int = 1, monitor_interval: float = 1.0):
        self.num_consumers = num_consumers
        self.monitor_interval = monitor_interval
        self.consumers: List[consumer.Consumer] = []
        self.consumer_threads:List[Thread] = []
        self.monitor_thread = None
        self.running = False
        self.processo = psutil.Process(os.getpid())


    def start_consumers(self):
        self.running = True

        for i in range(self.num_consumers):
            consumidor = consumer.Consumer(id=i, prefetch_count=11, task_level=1)
            thread = Thread(
                target=consumidor.start_consuming,
                daemon=True,
                name=f"consumer-thread-{i}"
            ) 
        
            self.consumers.append(consumidor)
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
            dados = self.consumers[0].get_data()
            qtd_mensagens = len(dados)
            avg_processamento = sum([d[0] for d in dados])/qtd_mensagens if qtd_mensagens > 0 else 0
            avg_time_in_queue = sum([d[1] for d in dados])/qtd_mensagens if qtd_mensagens > 0 else 0
            pc_cpu , pc_ram, process_cpu, process_ram = self.obter_consumo_recursos()
            print(f"\n Quantidade de Mensagens: {qtd_mensagens} \n Average Processing Time: {avg_processamento} \n Average Time in Queue: {avg_time_in_queue} \n CPU Global: {pc_cpu}% \n RAM Global: {pc_ram}% \n CPU Processo: {process_cpu}% \n RAM Processo: {process_ram:.2f} MB\n")
            #self.snapshot_metrics()


    def snapshot_metrics(self)->dict:
        metrics = {}
        for consumer in self.consumers:
            if consumer.id not in metrics:
                metrics[consumer.id] = []
            dados = consumer.get_data()
            print(f"Snapshot Consumer {consumer.id}: {dados}")
            metrics[consumer.id].append(dados)

        return metrics
        
        
    def stop(self):
        self.running = False
        print("Processo encerrado")
    
    
    def set_new_prefetch_counts(self, id:int, new_pc:int):
        self.consumers[id].set_new_prefetch_count(new_pc)
    
    
    def get_consumers(self):
        return self.consumers

    
    def obter_consumo_recursos(self):
        # 1. Consumo GLOBAL da Máquina (CPU e RAM)
        cpu_global = psutil.cpu_percent(interval=None) # % de uso de todos os núcleos
        ram_global = psutil.virtual_memory().percent    # % de uso da RAM total

        # 2. Consumo do PROCESSO ATUAL (este script)
      
        
        # O cpu_percent do processo requer um pequeno intervalo ou chamada dupla
        # Na primeira chamada ele retorna 0.0, nas seguintes a variação desde a última
        cpu_processo = self.processo.cpu_percent(interval=None)
        
        # Memória em MB (RSS = Resident Set Size)
        ram_processo_mb = self.processo.memory_info().rss / (1024 * 1024)

        return cpu_global,  ram_global, cpu_processo,  ram_processo_mb
        

    #def update_prefetchs(self, decisions:List[Decision])-> None:
     #   for decision in decisions:
      #      consumer = self.consumers[decision.consumer_id]
       #     with consumer.comands_lock:
        #        consumer.comands.append(decision)


if __name__ == "__main__":
    manager = ConsumerManager(num_consumers=1, monitor_interval=5)
    manager.start_consumers()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        manager.stop()