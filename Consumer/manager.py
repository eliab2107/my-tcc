import time
from threading import Thread
from Consumer import consumer
from typing import List
import psutil
import os
import csv
from datetime import datetime
import math 

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
        arquivo_csv = "dataset_tcc.csv"
        
        # Cria o cabeçalho do arquivo se ele não existir
        try:
            with open(arquivo_csv, 'x', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    "timestamp", "qtd_mensagens", "avg_proc", "p95_proc", "p99_proc",
                    "avg_queue", "p95_queue", "p99_queue", "cpu_global", "ram_global", 
                    "cpu_processo", "ram_processo_mb", "prefetch_count"
                ])
        except FileExistsError:
            pass

        while self.running:
            time.sleep(self.monitor_interval)
            
            # 1. Coleta os dados (Supondo que d[0] é processamento e d[1] é tempo de vida/fila)
            dados = self.consumers[0].get_data()
            qtd_mensagens = len(dados)
            
            if qtd_mensagens > 0:
                tempos_proc = [d[0] for d in dados]
                tempos_total = [d[1] for d in dados] # Tempo desde a criação

                # Médias
                avg_proc = sum(tempos_proc) / qtd_mensagens
                avg_total = sum(tempos_total) / qtd_mensagens

                # Percentis P95 e P99
                p95_proc = self.calcular_percentil(tempos_proc, 95)
                p99_proc = self.calcular_percentil(tempos_proc, 99)
                
                p95_total = self.calcular_percentil(tempos_total, 95)
                p99_total = self.calcular_percentil(tempos_total, 99)
            else:
                avg_proc = p95_proc = p99_proc = 0
                avg_total = p95_total = p99_total = 0

            # 2. Recursos da Máquina
            pc_cpu, pc_ram, process_cpu, process_ram = self.obter_consumo_recursos()

            # 3. Registro no Arquivo (Append mode 'a')
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            linha = [
                timestamp, qtd_mensagens, 
                round(avg_proc, 4), round(p95_proc, 4), round(p99_proc, 4),
                round(avg_total, 4), round(p95_total, 4), round(p99_total, 4),
                pc_cpu, pc_ram, process_cpu, round(process_ram, 2), self.consumers[0].prefetch_count
            ]

            with open(arquivo_csv, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(linha)

            print(f"[DATASET] Linha registrada: {qtd_mensagens} msgs processadas.")

    

    def calcular_percentil(self, lista, percentil):
        if not lista:
            return 0
        lista_ordenada = sorted(lista)
        indice = (percentil / 100) * (len(lista_ordenada) - 1)
        baixo = math.floor(indice)
        alto = math.ceil(indice)
        if baixo == alto:
            return lista_ordenada[int(indice)]
        # Interpolação linear simples
        d = indice - baixo
        return lista_ordenada[baixo] * (1 - d) + lista_ordenada[alto] * d

        
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
        


if __name__ == "__main__":
    manager = ConsumerManager(num_consumers=1, monitor_interval=5)
    manager.start_consumers()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        manager.stop()