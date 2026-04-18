import time
from threading import Thread
from Consumer import consumer
from typing import List
import psutil
import os
from dotenv import load_dotenv
import csv
from datetime import datetime
import math 

# Load environment variables from .env
load_dotenv()

class ConsumerManager:
    def __init__(self, filename: str, prefetch_count:int = 1, num_consumers: int = 1, monitor_interval: float = 5, task_level: int = 1):
        self.num_consumers = num_consumers
        self.monitor_interval = monitor_interval
        self.prefetch_count = prefetch_count
        self.task_level = task_level
        self.filename = filename
        self.consumers: List[consumer.Consumer] = []
        self.consumer_threads:List[Thread] = []
        self.monitor_thread = None
        self.running = False
        self.processo = psutil.Process(os.getpid())


    def start_consumers(self):
        self.running = True

        for i in range(self.num_consumers):
            consumidor = consumer.Consumer(id=i, prefetch_count=self.prefetch_count, task_level=self.task_level)
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

        
        # Cria o cabeçalho do arquivo se ele não existir
        try:
            with open(self.filename, 'x', newline='') as f:
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
            
            # Coleta os dados: [0] é processamento  [1] é tempo de vida/fila
            if len(self.consumers) > 0:
                dados = self.consumers[0].get_data()
                qtd_mensagens = len(dados)
            
            if qtd_mensagens > 0:
                tempos_procesamento = [d[0] for d in dados]
                tempos_total = [d[1] for d in dados] #Tempo de vida total da mensagem

                # Médias
                avg_processamento = sum(tempos_procesamento) / qtd_mensagens
                avg_total = sum(tempos_total) / qtd_mensagens

                # Percentis P95 e P99
                p95_processamento = self.calcular_percentil(tempos_procesamento, 95)
                p99_processamento = self.calcular_percentil(tempos_procesamento, 99)
                
                p95_total = self.calcular_percentil(tempos_total, 95)
                p99_total = self.calcular_percentil(tempos_total, 99)
            else:
                avg_processamento = p95_processamento = p99_processamento = 0
                avg_total = p95_total = p99_total = 0

            # 2. Recursos da Máquina
            pc_cpu, pc_ram, process_cpu, process_ram = self.obter_consumo_recursos()

            # 3. Registro no Arquivo (Append mode 'a')
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            linha = [
                timestamp, qtd_mensagens, 
                round(avg_processamento, 10), round(p95_processamento, 10), round(p99_processamento, 10),
                round(avg_total, 10), round(p95_total, 10), round(p99_total, 10),
                pc_cpu, pc_ram, process_cpu, round(process_ram, 2), self.consumers[0].prefetch_count
            ]

            with open(self.filename, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(linha)
            dados.clear() # Limpa os dados após o registro
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
        self.running = False # Para o monitor_loop
        
        for c in self.consumers:
            c.stop() # Envia o sinal thread-safe

        print("[Manager] Aguardando encerramento das threads dos consumidores...")
        for t in self.consumer_threads:
            t.join(timeout=15) # Espera as threads morrerem sozinhas
        
        # Limpa as referências para o próximo ciclo
        self.consumers = []
        self.consumer_threads = []
    
    
    def set_new_prefetch_counts(self, id:int, new_pc:int):
        self.consumers[id].set_new_prefetch_count(new_pc)
    
    
    def get_consumers(self):
        return self.consumers

    
    def obter_consumo_recursos(self):
        cpu_global = psutil.cpu_percent(interval=None) 
        ram_global = psutil.virtual_memory().percent    
    
        cpu_processo = self.processo.cpu_percent(interval=None)
        
        ram_processo_mb = self.processo.memory_info().rss / (1024 * 1024)

        return cpu_global,  ram_global, cpu_processo,  ram_processo_mb
        
        
def iniciar_orquestracao():
    # Read orchestrator configuration from environment with sensible defaults
    prefetch_inicial = int(os.getenv('PREFETCH_INICIAL', '1'))
    prefetch_final = int(os.getenv('PREFETCH_FINAL', '22'))
    tempo_experimento_segundos = int(os.getenv('TEMPO_EXPERIMENTO_SEGUNDOS', '45'))
    pausa_entre_ciclos = int(os.getenv('PAUSA_ENTRE_CICLOS', '10'))
    task_level = int(os.getenv('TASK_LEVEL', '1'))
    monitor_interval = int(os.getenv('MONITOR_INTERVAL', '10'))
    datasets_dir = os.getenv('DATASETS_DIR', 'datasets')
    os.makedirs(datasets_dir, exist_ok=True)
    print("--- INICIANDO BATERIA DE EXPERIMENTOS (1 a 22) ---")

    for prefetch_atual in range(prefetch_inicial, prefetch_final + 1):
        nome_arquivo = os.path.join(datasets_dir, f"PC_{prefetch_atual}_TASK_{task_level}.csv")
        
        print(f"\n" + "="*50)
        print(f"[EXPERIMENTO] Iniciando Ciclo {prefetch_atual}/22")
        print(f"[EXPERIMENTO] Prefetch: {prefetch_atual}")
        print(f"[EXPERIMENTO] Arquivo: {nome_arquivo}")
        print(f"[EXPERIMENTO] Tempo(s): {tempo_experimento_segundos}")
        print(f"[EXPERIMENTO] Pausa(s): {pausa_entre_ciclos}")
        print("="*50)

        # 1. Instancia o Manager para o prefetch específico
        manager = ConsumerManager(
            prefetch_count=prefetch_atual, 
            filename=nome_arquivo,
            monitor_interval=monitor_interval
        )
        
        # 2. Inicia os consumidores e o monitoramento
        manager.start_consumers()

        try:
            time.sleep(tempo_experimento_segundos)
        except KeyboardInterrupt:
            print("\n[AVISO] Orquestração interrompida pelo usuário.")
            manager.stop()
            break

        # 4. Finaliza o ciclo atual
        print(f"[INFO] Finalizando experimento de prefetch {prefetch_atual}...")
        manager.stop()
        
        # 5. Pausa de 1 minuto antes do próximo prefetch
        if prefetch_atual < prefetch_final:
            print(f"[PAUSA] Aguardando {pausa_entre_ciclos} segundos para estabilização do sistema...")
            time.sleep(pausa_entre_ciclos)

    print("\n" + "!"*50)
    print("BATERIA DE EXPERIMENTOS CONCLUÍDA COM SUCESSO!")
    print("!"*50)

if __name__ == "__main__":
    iniciar_orquestracao()

#if __name__ == "__main__":
 #   manager = ConsumerManager(num_consumers=1, monitor_interval=5)
  #  manager.start_consumers()

   # try:
    #    while True:
     #       time.sleep(1)
    #except KeyboardInterrupt:
     #   manager.stop()