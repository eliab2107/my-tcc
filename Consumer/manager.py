import time
import math
import csv
import os
import random
import requests
import psutil
from threading import Thread, Lock
from typing import List
from dotenv import load_dotenv
from Consumer.consumer import Consumer
from policies.policies import BaseMessageQuantityPolicy

load_dotenv()

INTERVALO_DE_MONITORAMENTO = int(os.getenv("MONITOR_INTERVAL", 5))
PREFETCH_INICIAL           = int(os.getenv("PREFETCH_INICIAL", 1))
CSV_FLUSH_INTERVAL         = int(os.getenv("CSV_FLUSH_INTERVAL", 240))
DATASET_ARQUIVO            = os.getenv("DATA_FILE", "dataset.csv")

# Sequência de targets que se alternam durante a coleta para
# aumentar a cobertura do espaço de estados
TARGET_MESSAGES_SEQUENCE = [100, 210, 300, 50, 150, 500, 400]
TARGET_CHANGE_INTERVAL   = int(os.getenv("TARGET_CHANGE_INTERVAL", 100000))  # segundos

# Probabilidade de perturbar o prefetch aleatoriamente em cada tick,
# para explorar estados que a política sozinha não exploraria
PERTURBATION_PROB  = float(os.getenv("PERTURBATION_PROB", 0.20))
PERTURBATION_DELTA = int(os.getenv("PERTURBATION_DELTA", 5))

# API de management do RabbitMQ para leitura do tamanho da fila
RABBITMQ_API_URL  = os.getenv("RABBITMQ_API_URL", f"http://{os.getenv('RABBITMQ_HOST', 'localhost')}:15672")
RABBITMQ_API_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_API_PASS = os.getenv("RABBITMQ_PASS", "guest")
RABBITMQ_QUEUE    = os.getenv("RABBITMQ_QUEUE", "fila_teste")


CSV_HEADER = [
    "tick",
    "timestamp",
    # --- vazão ---
    "msgs_processadas_intervalo",
    "avg_processing_time",
    "avg_queue_latency",
    # --- estado do broker ---
    "fila_broker",               # mensagens prontas na fila (via API)
    # --- controlador ---
    "prefetch_count",
    "ultima_acao",               # -1 / 0 / +1 (ação tomada no tick anterior)
    # --- contexto ---
    "target_atual",
    "distancia_target",          # msgs_processadas - target (com sinal)
    # --- recursos ---
    "cpu_global",
    "ram_global",
    "cpu_processo",
    "ram_processo_mb",
    # --- label (saída do modelo) ---
    "decisao",                   # ação tomada neste tick: -1 / 0 / +1
]


class ConsumerManager:
    def __init__(
        self,
        filename: str = DATASET_ARQUIVO,
        prefetch_count: int = PREFETCH_INICIAL,
        monitor_interval: float = INTERVALO_DE_MONITORAMENTO,
    ):
        self.monitor_interval  = monitor_interval
        self.filename          = filename
        self._initial_prefetch = prefetch_count

        self.consumers:        List[Consumer] = []
        self.consumer_threads: List[Thread]   = []
        self.monitor_thread:   Thread | None  = None
        self.flush_thread:     Thread | None  = None
        self.target_thread:    Thread | None  = None

        self.running     = False
        self.processo    = psutil.Process(os.getpid())
        self._data:      list = []
        self._data_lock  = Lock()
        self._tick_count = 0

        # Política e target
        self.target_index            = 0
        self.target_quantity_message = TARGET_MESSAGES_SEQUENCE[self.target_index]
        self.policy = BaseMessageQuantityPolicy(self.target_quantity_message)

        # Última ação registrada (para feature do dataset)
        self._last_action = 0

    # ------------------------------------------------------------------
    # Inicialização
    # ------------------------------------------------------------------
    def start_consumers(self) -> None:
        self.running = True

        c = Consumer(id=0, prefetch_count=self._initial_prefetch)
        t = Thread(target=c.start_consuming, daemon=True, name="consumer-thread-0")
        self.consumers.append(c)
        self.consumer_threads.append(t)
        t.start()

        self.monitor_thread = Thread(
            target=self._monitor_loop, daemon=True, name="consumer-monitor"
        )
        self.monitor_thread.start()

        self.flush_thread = Thread(
            target=self._flush_loop, daemon=True, name="consumer-flush"
        )
        self.flush_thread.start()

        self.target_thread = Thread(
            target=self._target_loop, daemon=True, name="target-rotation"
        )
        self.target_thread.start()

    # ------------------------------------------------------------------
    # Rotação de target
    # ------------------------------------------------------------------
    def _target_loop(self) -> None:
        """Alterna o target periodicamente para cobrir mais estados."""
        while self.running:
            time.sleep(TARGET_CHANGE_INTERVAL)
            self.target_index = (self.target_index + 1) % len(TARGET_MESSAGES_SEQUENCE)
            self.target_quantity_message = TARGET_MESSAGES_SEQUENCE[self.target_index]
            self.policy.set_target_quantity_message(self.target_quantity_message)
           # print(f"[Manager] Novo target: {self.target_quantity_message}")

    # ------------------------------------------------------------------
    # Loop de monitoramento
    # ------------------------------------------------------------------
    def _monitor_loop(self) -> None:
        while True:
            try:
                self._monitor_tick()
            except Exception as e:
                print(f"[Monitor] erro inesperado: {e}")

    def _monitor_tick(self) -> None:
        time.sleep(self.monitor_interval)
        self._tick_count += 1
        timestamp = time.time()

        # 1. Coleta métricas do consumer
        dados = self.consumers[0].get_data() if self.consumers else []
        n, avg_proc, avg_latency = self._calcular_medias(dados)

        # 2. Tamanho da fila no broker (via API de management)
        fila_broker = self._get_queue_size()

        # 3. Estado atual do controlador
        prefetch_atual = self.consumers[0].prefetch_count
        distancia      = n - self.target_quantity_message

        # 4. Decisão de ajuste
        ajuste = 0
        if self.running:
            # Perturbação aleatória para explorar o espaço de estados
            if random.random() < PERTURBATION_PROB:
                ajuste = random.choice([-PERTURBATION_DELTA, PERTURBATION_DELTA])
                #print(f"[Monitor] Perturbação aleatória: {ajuste:+d}")
            else:
                ajuste = self.policy.decide(dados)

            if ajuste != 0:
                self._ajustar_prefetch(consumer_id=0, ajuste=ajuste)

        # 5. Recursos
        cpu_g, ram_g, cpu_p, ram_p = self._obter_recursos()

        # 6. Monta linha do dataset
        linha = [
            self._tick_count,
            round(timestamp, 3),
            n,
            round(avg_proc,    8),
            round(avg_latency, 8),
            fila_broker,
            prefetch_atual,
            self._last_action,   # ação do tick ANTERIOR (feature de contexto)
            self.target_quantity_message,
            distancia,
            cpu_g,
            ram_g,
            cpu_p,
            round(ram_p, 2),
            ajuste,              # ação deste tick (label)
        ]

        self._last_action = ajuste

        with self._data_lock:
            self._data.append(linha)

        #print(          f"[Monitor] tick={self._tick_count} | msgs={n} | fila_broker={fila_broker}  prefetch={prefetch_atual} | ajuste={ajuste:+d} | target={self.target_quantity_message}")

    # ------------------------------------------------------------------
    # Ajuste de prefetch
    # ------------------------------------------------------------------
    def _ajustar_prefetch(self, consumer_id: int, ajuste: int) -> None:
        self.consumers[consumer_id].set_new_prefetch_count(ajuste)

    # ------------------------------------------------------------------
    # Coleta do tamanho da fila via API de management do RabbitMQ
    # ------------------------------------------------------------------
    def _get_queue_size(self) -> int:
        try:
            url = f"{RABBITMQ_API_URL}/api/queues/%2F/{RABBITMQ_QUEUE}"
            r = requests.get(url, auth=(RABBITMQ_API_USER, RABBITMQ_API_PASS), timeout=2)
            if r.status_code == 200:
                return r.json().get("messages_ready", 0)
        except Exception as e:
            print(f"[Monitor] Falha ao consultar API do RabbitMQ: {e}")
        return -1  # -1 indica falha na leitura

    # ------------------------------------------------------------------
    # Flush de CSV — apenas salva, nunca toca no prefetch
    # ------------------------------------------------------------------
    def _flush_loop(self) -> None:
        while self.running:
            time.sleep(CSV_FLUSH_INTERVAL)
            try:
                self._save_csv()
            except Exception as e:
                print(f"[Flush] Erro ao gravar CSV: {e}")

    def _save_csv(self) -> None:
        with self._data_lock:
            if not self._data:
                return
            rows = self._data.copy()
            self._data.clear()

        file_exists = os.path.exists(self.filename)
        with open(self.filename, "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(CSV_HEADER)
            writer.writerows(rows)
        #(f"[Flush] {len(rows)} linhas gravadas em {self.filename}")

    # ------------------------------------------------------------------
    # Encerramento
    # ------------------------------------------------------------------
    def stop(self) -> None:
        self.running = False
        for c in self.consumers:
            c.stop()

        print("[Manager] Aguardando encerramento das threads dos consumidores...")
        for t in self.consumer_threads:
            t.join(timeout=30)
        for t in [self.monitor_thread, self.flush_thread, self.target_thread]:
            if t:
                t.join(timeout=10)

        self._save_csv()  # garante que nada fica sem gravar
        self.consumers.clear()
        self.consumer_threads.clear()

    # ------------------------------------------------------------------
    # Helpers de métricas
    # ------------------------------------------------------------------
    def _calcular_medias(self, dados: list) -> tuple:
        n = len(dados)
        if n == 0:
            return 0, 0.0, 0.0
        avg_proc    = math.fsum(d["processing_time"] for d in dados) / n
        avg_latency = math.fsum(d["queue_latency"]   for d in dados) / n
        return n, avg_proc, avg_latency

    def _obter_recursos(self) -> tuple:
        cpu_g = psutil.cpu_percent(interval=None)
        ram_g = psutil.virtual_memory().percent
        cpu_p = self.processo.cpu_percent(interval=None)
        ram_p = self.processo.memory_info().rss / (1024 * 1024)
        return cpu_g, ram_g, cpu_p, ram_p


# ------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------
def iniciar_orquestracao() -> None:
    manager = ConsumerManager(
        prefetch_count=PREFETCH_INICIAL,
        filename=DATASET_ARQUIVO,
        monitor_interval=INTERVALO_DE_MONITORAMENTO,
    )
    manager.start_consumers()

    try:
        while manager.running:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[Manager] Interrupção recebida, encerrando...")
        manager.stop()


if __name__ == "__main__":
    iniciar_orquestracao()