import time
from threading import Thread, Lock
from Consumer import consumer
from typing import List
import psutil
import os
from dotenv import load_dotenv
import csv
from datetime import datetime
import math
from random import randint
from policies.policies import BaseMessageQuantityPolicy 

# Load environment variables from .env
load_dotenv()
# Constants / configurable via environment
INTERVALO_DE_MONITORAMENTO = int(os.getenv("MONITOR_INTERVAL", 10))
PREFETCH_INICIAL = int(os.getenv("PREFETCH_INICIAL", 4))
CSV_FLUSH_INTERVAL = int(os.getenv("CSV_FLUSH_INTERVAL", 60))
TARGET_CHANGE_INTERVAL = int(os.getenv("TARGET_CHANGE_INTERVAL", 60))
DATASET_ARQUIVO = os.getenv("DATA_FILE", "dataset.csv")
TARGET_MESSAGES_SEQUENCE = [1000, 1100, 1300, 1500,2000, 2500, 3000] # Example sequence of target message counts for dynamic policy


class ConsumerManager:
    def __init__(self, filename: str = DATASET_ARQUIVO, prefetch_count:int = PREFETCH_INICIAL, num_consumers: int = 1, monitor_interval: float = INTERVALO_DE_MONITORAMENTO):
        self.num_consumers = max(1, num_consumers)
        self.monitor_interval = monitor_interval
        self.prefetch_count = prefetch_count
        self.filename = filename
        self.consumers: List[consumer.Consumer] = []
        self.consumer_threads:List[Thread] = []
        self.monitor_thread = None
        self.flush_thread = None
        self.target_thread = None
        self.running = False
        self.processo = psutil.Process(os.getpid())
        self.data = []
        self.data_lock = Lock()
        self.target_quantity_message = TARGET_MESSAGES_SEQUENCE[6]
        self.target_index = 0
        self.policy = BaseMessageQuantityPolicy(self.target_quantity_message)
        self.count = 0
        


    def start_consumers(self):
        self.running = True

        # Start only one consumer (monitoring focuses on the first consumer)
       
        consumidor = consumer.Consumer(id=0, prefetch_count=self.prefetch_count)
        thread = Thread(
            target=consumidor.start_consuming,
            daemon=True,
            name="consumer-thread-0"
        )

        self.consumers.append(consumidor)
        self.consumer_threads.append(thread)

        thread.start()

        # Monitor loop: collects metrics periodically
        self.monitor_thread = Thread(
            target=self.monitor_loop,
            daemon=True,
            name="consumer-monitor"
        )
        self.monitor_thread.start()

        # Flush thread: persists collected metrics to CSV every CSV_FLUSH_INTERVAL seconds
        self.flush_thread = Thread(
            target=self.flush_loop,
            daemon=True,
            name="csv-flush"
        )
        self.flush_thread.start()

    
    def save_data_in_csv(self):
        # Writes any queued metrics to CSV (append). Ensures header exists.
        with self.data_lock:
            if not self.data:
                return
            file_exists = os.path.exists(self.filename)
            with open(self.filename, 'a', newline='') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow([
                       "qtd_mensagens", "avg_proc", "avg_queue",
                        "cpu_global", "ram_global", "cpu_processo", "ram_processo_mb", "prefetch_count", "decisao", "target_quantity_message"
                    ])
                for linha in self.data:
                    writer.writerow(linha)
                self.data.clear()


    def flush_loop(self):
        while self.running:
            time.sleep(CSV_FLUSH_INTERVAL)
            try:
                self.save_data_in_csv()
            except Exception as e:
                print(f"[Flush] Erro ao gravar CSV: {e}")


    def target_loop(self):
        self.target_index = randint(0, len(TARGET_MESSAGES_SEQUENCE) - 1)
        self.target_quantity_message = TARGET_MESSAGES_SEQUENCE[self.target_index]
        self.policy.set_target_quantity_message(self.target_quantity_message)
    

                
    def monitor_loop(self):
        # Keep monitor thread alive permanently; handle exceptions and continue
        while True:
            try:
                time.sleep(self.monitor_interval)
                self.count += 1
                if self.count % 12 == 0: # Change target every 3 monitor intervals (example logic)
                    self.target_loop()
                if self.count % 12 == 0: # Save to CSV every 5 monitor intervals (example logic)
                    self.save_data_in_csv()
                # Collect metrics from the first consumer only
                qtd_mensagens = 0
                dados = []
                if len(self.consumers) > 0:
                    dados = self.consumers[0].get_data()
                    qtd_mensagens = len(dados)

                if qtd_mensagens > 0:
                    tempos_procesamento = [d[0] for d in dados]
                    tempos_total = [d[1] for d in dados]

                    avg_processamento = sum(tempos_procesamento) / qtd_mensagens
                    avg_total = sum(tempos_total) / qtd_mensagens
                else:
                    avg_processamento = 0
                    avg_total = 0

                # System resources
                pc_cpu, pc_ram, process_cpu, process_ram = self.obter_consumo_recursos()


                ajuste = 0
                if self.running:
                    ajuste = self.policy.decide(dados)
                    if ajuste != 0:
                        self.set_new_prefetch_counts(id=0, ajuste=ajuste)

                linha = [ 
                    qtd_mensagens,
                    round(avg_processamento, 10),
                    round(avg_total, 10),
                    pc_cpu,
                    pc_ram,
                    process_cpu,
                    round(process_ram, 2),
                    self.prefetch_count,
                    ajuste,
                    self.target_quantity_message
                ]

                with self.data_lock:
                    self.data.append(linha)

                # Log to console for visibility
                print(f" msgs={qtd_mensagens} | prefetch={self.consumers[0].prefetch_count} | ajuste={ajuste}")
             
            except Exception as e:
                print(f"[Monitor] erro inesperado: {e}")
                # continue loop to keep thread alive
                continue
        
        
    def stop(self):
        self.running = False # Para o monitor_loop
        
        for c in self.consumers:
            c.stop() # Envia o sinal thread-safe

        print("[Manager] Aguardando encerramento das threads dos consumidores...")
        for t in self.consumer_threads:
            t.join(timeout=15) # Espera as threads morrerem sozinhas
        # Aguarda threads auxiliares
        if self.monitor_thread is not None:
            self.monitor_thread.join(timeout=5)
        if self.flush_thread is not None:
            self.flush_thread.join(timeout=5)
        if self.target_thread is not None:
            self.target_thread.join(timeout=5)

        # Limpa as referências para o próximo ciclo
        self.consumers = []
        self.consumer_threads = []
    
    
    def set_new_prefetch_counts(self, id:int, ajuste:int):
        # Apply ajuste to consumer and update manager's tracked prefetch
        self.consumers[id].set_new_prefetch_count(ajuste)
        # reflect optimistic update
        self.prefetch_count = max(1, self.prefetch_count + ajuste)

    
    def obter_consumo_recursos(self):
        cpu_global = psutil.cpu_percent(interval=None) 
        ram_global = psutil.virtual_memory().percent    
    
        cpu_processo = self.processo.cpu_percent(interval=None)
        
        ram_processo_mb = self.processo.memory_info().rss / (1024 * 1024)

        return cpu_global,  ram_global, cpu_processo,  ram_processo_mb
        
        
def iniciar_orquestracao():
    
    # 1. Instancia o Manager para o prefetch específico
    manager = ConsumerManager(
        prefetch_count=PREFETCH_INICIAL,
        filename=DATASET_ARQUIVO,
        monitor_interval=INTERVALO_DE_MONITORAMENTO
    )
    
    # 2. Inicia os consumidores e o monitoramento
    manager.start_consumers()

    # Mantém a thread principal viva enquanto o manager estiver rodando.
    try:
        while manager.running:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[Manager] Interrupção recebida, encerrando...")
        manager.stop()


if __name__ == "__main__":
    iniciar_orquestracao()