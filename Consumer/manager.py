"""
manager.py — Dois modos de execução:

  python manager.py --modo coleta
      Gera dataset guiado por uma política (heurística ou ML).
      Inclui perturbação aleatória e rotação de targets.

  python manager.py --modo impacto
      Estuda o impacto do prefetch count ao longo do tempo.
      Incrementa o prefetch de 1 até 30 a cada 30 minutos.
      Não usa política — o prefetch é fixo em cada janela.
      Útil para entender em qual faixa de prefetch faz sentido trabalhar.
"""

import argparse
import time
import math
import csv
import os
import random
import requests
from threading import Thread, Lock
from typing import List
from dotenv import load_dotenv

from Consumer.consumer import Consumer
from policies.policies import BaseMessageQuantityPolicy

load_dotenv()

# ------------------------------------------------------------------
# Configuração geral
# ------------------------------------------------------------------
INTERVALO_DE_MONITORAMENTO = int(os.getenv("MONITOR_INTERVAL", 20))
PREFETCH_INICIAL           = int(os.getenv("PREFETCH_INICIAL", 1))
CSV_FLUSH_INTERVAL         = int(os.getenv("CSV_FLUSH_INTERVAL", 300))

RABBITMQ_API_URL  = os.getenv("RABBITMQ_API_URL", f"http://{os.getenv('RABBITMQ_HOST', 'localhost')}:15672")
RABBITMQ_API_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_API_PASS = os.getenv("RABBITMQ_PASS", "guest")
RABBITMQ_QUEUE    = os.getenv("RABBITMQ_QUEUE", "fila_teste")

# ------------------------------------------------------------------
# Configuração — modo coleta
# ------------------------------------------------------------------
TARGET_MESSAGES_SEQUENCE = [25, 190, 50, 100, 130, 160, 210, 75, 230, 250,]
TARGET_CHANGE_INTERVAL   = int(os.getenv("TARGET_CHANGE_INTERVAL", 180))
PERTURBATION_PROB        = float(os.getenv("PERTURBATION_PROB", 0.10))
PERTURBATION_DELTA       = int(os.getenv("PERTURBATION_DELTA", 3))
DATASET_ARQUIVO          = os.getenv("DATA_FILE", "dataset.csv")

# ------------------------------------------------------------------
# Configuração — modo impacto
# ------------------------------------------------------------------
IMPACTO_PREFETCH_INICIO  = int(os.getenv("IMPACTO_PREFETCH_INICIO", 1))
IMPACTO_PREFETCH_FIM     = int(os.getenv("IMPACTO_PREFETCH_FIM", 30))
IMPACTO_PREFETCH_PASSO   = int(os.getenv("IMPACTO_PREFETCH_PASSO", 1))
IMPACTO_JANELA_SEGUNDOS  = int(os.getenv("IMPACTO_JANELA_SEGUNDOS", 30))  # 30 min -> em segunfos
IMPACTO_ARQUIVO          = os.getenv("IMPACTO_FILE", "dataset_impacto_prefetch.csv")

# ------------------------------------------------------------------
# Headers dos CSVs
# ------------------------------------------------------------------
CSV_HEADER_COLETA = [
    "tick", "timestamp",
    "msgs_processadas_intervalo",
    "msgs_por_segundo",            # msgs_processadas_intervalo / monitor_interval
    "avg_processing_time",
    "avg_queue_latency",
    "fila_broker",
    "prefetch_count", "ultima_acao",
    "target_atual", "distancia_target",
    "decisao",
]

CSV_HEADER_IMPACTO = [
    "tick", "timestamp",
    "prefetch_count",              # valor fixo nesta janela
    "janela",                      # número da janela (1 = prefetch 1, 2 = prefetch 2 ...)
    "msgs_processadas_intervalo",
    "msgs_por_segundo",            # msgs_processadas_intervalo / monitor_interval
    "avg_processing_time",
    "avg_queue_latency",
    "fila_broker",
]


# ==================================================================
# MODO COLETA
# ==================================================================

class ColetaManager:
    """
    Gera dataset guiado por uma política.
    Inclui perturbação aleatória e rotação de targets para
    maximizar a cobertura do espaço de estados.

    Para trocar a política, edite a função main() abaixo.
    """

    def __init__(
        self,
        policy,
        filename: str = DATASET_ARQUIVO,
        prefetch_count: int = PREFETCH_INICIAL,
        monitor_interval: float = INTERVALO_DE_MONITORAMENTO,
    ):
        self.policy            = policy
        self.filename          = filename
        self.monitor_interval  = monitor_interval
        self._initial_prefetch = prefetch_count

        self.consumers:        List[Consumer] = []
        self.consumer_threads: List[Thread]   = []
        self.monitor_thread:   Thread | None  = None
        self.flush_thread:     Thread | None  = None
        self.target_thread:    Thread | None  = None

        self.running      = False
        self._data:       list = []
        self._data_lock   = Lock()
        self._tick_count  = 0
        self._last_action = 0

        self.target_index            = 0
        self.target_quantity_message = TARGET_MESSAGES_SEQUENCE[self.target_index]

    def start(self) -> None:
        self.running = True

        c = Consumer(id=0, prefetch_count=self._initial_prefetch)
        t = Thread(target=c.start_consuming, daemon=True, name="consumer-thread-0")
        self.consumers.append(c)
        self.consumer_threads.append(t)
        t.start()

        self.monitor_thread = Thread(
            target=self._monitor_loop, daemon=True, name="coleta-monitor"
        )
        self.monitor_thread.start()

        self.flush_thread = Thread(
            target=self._flush_loop, daemon=True, name="coleta-flush"
        )
        self.flush_thread.start()

        self.target_thread = Thread(
            target=self._target_loop, daemon=True, name="coleta-target"
        )
        self.target_thread.start()

        print(
            f"[Coleta] Iniciado | arquivo={self.filename} "
            f"| prefetch_inicial={self._initial_prefetch} "
            f"| perturbacao={PERTURBATION_PROB*100:.0f}%"
        )

    # ------------------------------------------------------------------
    # Rotação de target
    # ------------------------------------------------------------------
    def _target_loop(self) -> None:
        while self.running:
            time.sleep(TARGET_CHANGE_INTERVAL)
            self.target_index = (self.target_index + 1) % len(TARGET_MESSAGES_SEQUENCE)
            self.target_quantity_message = TARGET_MESSAGES_SEQUENCE[self.target_index]
            if hasattr(self.policy, "set_target_quantity_message"):
                self.policy.set_target_quantity_message(self.target_quantity_message)
            print(f"[Coleta] Novo target: {self.target_quantity_message}")

    # ------------------------------------------------------------------
    # Loop de monitoramento
    # ------------------------------------------------------------------
    def _monitor_loop(self) -> None:
        while True:
            try:
                self._monitor_tick()
            except Exception as e:
                print(f"[Coleta/Monitor] erro inesperado: {e}")

    def _monitor_tick(self) -> None:
        time.sleep(self.monitor_interval)
        self._tick_count += 1
        timestamp = time.time()

        dados = self.consumers[0].get_data() if self.consumers else []
        n, avg_proc, avg_latency = self._calcular_medias(dados)

        fila_broker      = self._get_queue_size()
        prefetch_atual   = self.consumers[0].prefetch_count
        distancia        = n - self.target_quantity_message
        msgs_por_segundo = round(n / self.monitor_interval, 4)

        dados_para_modelo = [
            round(timestamp, 3), n,
            round(avg_proc,    8), round(avg_latency, 8),
            fila_broker, prefetch_atual,
            self._last_action, self.target_quantity_message,
            distancia, msgs_por_segundo,
        ]

        ajuste = 0
        if self.running:
            if random.random() < PERTURBATION_PROB:
                ajuste = random.choice([-PERTURBATION_DELTA, PERTURBATION_DELTA])
            else:
                ajuste = self.policy.decide(dados_para_modelo)

            if ajuste != 0:
                self.consumers[0].set_new_prefetch_count(ajuste)

        linha = [
            self._tick_count, round(timestamp, 3),
            n, msgs_por_segundo,
            round(avg_proc, 8), round(avg_latency, 8),
            fila_broker, prefetch_atual, self._last_action,
            self.target_quantity_message, distancia,
            ajuste,
        ]

        self._last_action = ajuste

        with self._data_lock:
            self._data.append(linha)

    # ------------------------------------------------------------------
    # Flush / encerramento / helpers
    # ------------------------------------------------------------------
    def _flush_loop(self) -> None:
        while self.running:
            time.sleep(CSV_FLUSH_INTERVAL)
            try:
                self._save_csv()
            except Exception as e:
                print(f"[Coleta/Flush] erro: {e}")

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
                writer.writerow(CSV_HEADER_COLETA)
            writer.writerows(rows)
        print(f"[Coleta/Flush] {len(rows)} linhas → {self.filename}")

    def stop(self) -> None:
        self.running = False
        for c in self.consumers:
            c.stop()
        print("[Coleta] Aguardando encerramento das threads...")
        for t in self.consumer_threads:
            t.join(timeout=30)
        for t in [self.monitor_thread, self.flush_thread, self.target_thread]:
            if t:
                t.join(timeout=10)
        self._save_csv()
        self.consumers.clear()
        self.consumer_threads.clear()
        print("[Coleta] Encerrado.")

    def _calcular_medias(self, dados: list) -> tuple:
        n = len(dados)
        if n == 0:
            return 0, 0.0, 0.0
        avg_proc    = math.fsum(d["processing_time"] for d in dados) / n
        avg_latency = math.fsum(d["queue_latency"]   for d in dados) / n
        return n, avg_proc, avg_latency

    def _get_queue_size(self) -> int:
        try:
            url = f"{RABBITMQ_API_URL}/api/queues/%2F/{RABBITMQ_QUEUE}"
            r = requests.get(url, auth=(RABBITMQ_API_USER, RABBITMQ_API_PASS), timeout=2)
            if r.status_code == 200:
                return r.json().get("messages_ready", 0)
        except Exception as e:
            print(f"[Coleta] Falha API RabbitMQ: {e}")
        return -1


# ==================================================================
# MODO IMPACTO
# ==================================================================

class ImpactoManager:
    """
    Estuda o impacto do prefetch count no throughput.
    Fixa o prefetch em cada valor da sequência por janela_segundos
    e registra o comportamento do sistema sem interferência de política.

    O encerramento é automático ao completar a sequência.
    """

    def __init__(
        self,
        filename: str = IMPACTO_ARQUIVO,
        monitor_interval: float = INTERVALO_DE_MONITORAMENTO,
        prefetch_inicio: int = IMPACTO_PREFETCH_INICIO,
        prefetch_fim: int = IMPACTO_PREFETCH_FIM,
        prefetch_passo: int = IMPACTO_PREFETCH_PASSO,
        janela_segundos: int = IMPACTO_JANELA_SEGUNDOS,
    ):
        self.filename         = filename
        self.monitor_interval = monitor_interval
        self.janela_segundos  = janela_segundos

        self.sequencia_prefetch = list(range(prefetch_inicio, prefetch_fim + 1, prefetch_passo))
        self._janela_index      = 0
        self._prefetch_atual    = self.sequencia_prefetch[0]

        self.consumers:        List[Consumer] = []
        self.consumer_threads: List[Thread]   = []
        self.monitor_thread:   Thread | None  = None
        self.flush_thread:     Thread | None  = None
        self.janela_thread:    Thread | None  = None

        self.running     = False
        self._data:      list = []
        self._data_lock  = Lock()
        self._tick_count = 0

        total_min = len(self.sequencia_prefetch) * janela_segundos / 60
        print(
            f"[Impacto] Planejamento: prefetch {prefetch_inicio}→{prefetch_fim} "
            f"passo={prefetch_passo} | {len(self.sequencia_prefetch)} janelas × "
            f"{janela_segundos // 60}min = ~{total_min:.0f} min total"
        )

    def start(self) -> None:
        self.running = True

        c = Consumer(id=0, prefetch_count=self._prefetch_atual)
        t = Thread(target=c.start_consuming, daemon=True, name="consumer-thread-0")
        self.consumers.append(c)
        self.consumer_threads.append(t)
        t.start()

        self.monitor_thread = Thread(
            target=self._monitor_loop, daemon=True, name="impacto-monitor"
        )
        self.monitor_thread.start()

        self.flush_thread = Thread(
            target=self._flush_loop, daemon=True, name="impacto-flush"
        )
        self.flush_thread.start()

        self.janela_thread = Thread(
            target=self._janela_loop, daemon=True, name="impacto-janela"
        )
        self.janela_thread.start()

        print(
            f"[Impacto] Iniciado | janela 1/{len(self.sequencia_prefetch)} "
            f"| prefetch={self._prefetch_atual} | arquivo={self.filename}"
        )

    # ------------------------------------------------------------------
    # Avanço de janela
    # ------------------------------------------------------------------
    def _janela_loop(self) -> None:
        """Aguarda o tempo de cada janela e avança para o próximo prefetch."""
        while self.running:
            time.sleep(self.janela_segundos)

            self._janela_index += 1

            if self._janela_index >= len(self.sequencia_prefetch):
                print("[Impacto] Todas as janelas concluídas — encerrando.")
                self.running = False
                return

            novo = self.sequencia_prefetch[self._janela_index]
            delta = novo - self.consumers[0].prefetch_count
            if delta != 0:
                self.consumers[0].set_new_prefetch_count(delta)
            self._prefetch_atual = novo

            print(
                f"[Impacto] Janela {self._janela_index + 1}/{len(self.sequencia_prefetch)} "
                f"| prefetch={novo}"
            )

    # ------------------------------------------------------------------
    # Loop de monitoramento
    # ------------------------------------------------------------------
    def _monitor_loop(self) -> None:
        while True:
            try:
                self._monitor_tick()
            except Exception as e:
                print(f"[Impacto/Monitor] erro inesperado: {e}")

    def _monitor_tick(self) -> None:
        time.sleep(self.monitor_interval)
        self._tick_count += 1
        timestamp = time.time()

        dados = self.consumers[0].get_data() if self.consumers else []
        n, avg_proc, avg_latency = self._calcular_medias(dados)

        fila_broker      = self._get_queue_size()
        prefetch_real    = self.consumers[0].prefetch_count
        msgs_por_segundo = round(n / self.monitor_interval, 4)

        linha = [
            self._tick_count,
            round(timestamp, 3),
            prefetch_real,
            self._janela_index + 1,
            n,
            msgs_por_segundo,
            round(avg_proc,    8),
            round(avg_latency, 8),
            fila_broker,
        ]

        with self._data_lock:
            self._data.append(linha)

        print(
            f"[Impacto] tick={self._tick_count} | prefetch={prefetch_real} "
            f"| msgs/s={msgs_por_segundo} | fila={fila_broker}"
        )

    # ------------------------------------------------------------------
    # Flush / encerramento / helpers
    # ------------------------------------------------------------------
    def _flush_loop(self) -> None:
        while self.running:
            time.sleep(CSV_FLUSH_INTERVAL)
            try:
                self._save_csv()
            except Exception as e:
                print(f"[Impacto/Flush] erro: {e}")

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
                writer.writerow(CSV_HEADER_IMPACTO)
            writer.writerows(rows)
        print(f"[Impacto/Flush] {len(rows)} linhas → {self.filename}")

    def stop(self) -> None:
        self.running = False
        for c in self.consumers:
            c.stop()
        print("[Impacto] Aguardando encerramento das threads...")
        for t in self.consumer_threads:
            t.join(timeout=30)
        for t in [self.monitor_thread, self.flush_thread, self.janela_thread]:
            if t:
                t.join(timeout=10)
        self._save_csv()
        self.consumers.clear()
        self.consumer_threads.clear()
        print("[Impacto] Encerrado.")

    def _calcular_medias(self, dados: list) -> tuple:
        n = len(dados)
        if n == 0:
            return 0, 0.0, 0.0
        avg_proc    = math.fsum(d["processing_time"] for d in dados) / n
        avg_latency = math.fsum(d["queue_latency"]   for d in dados) / n
        return n, avg_proc, avg_latency

    def _get_queue_size(self) -> int:
        try:
            url = f"{RABBITMQ_API_URL}/api/queues/%2F/{RABBITMQ_QUEUE}"
            r = requests.get(url, auth=(RABBITMQ_API_USER, RABBITMQ_API_PASS), timeout=2)
            if r.status_code == 200:
                return r.json().get("messages_ready", 0)
        except Exception as e:
            print(f"[Impacto] Falha API RabbitMQ: {e}")
        return -1


# ==================================================================
# ENTRYPOINT
# ==================================================================

def _parse_args():
    parser = argparse.ArgumentParser(
        description="Manager de coleta de dataset — TCC controle de prefetch"
    )
    parser.add_argument(
        "--modo",
        choices=["coleta", "impacto"],
        required=True,
        help=(
            "coleta: dataset guiado por política com perturbação e rotação de targets. "
            "impacto: fixa prefetch por janela de tempo para estudar o impacto no throughput."
        ),
    )
    parser.add_argument("--arquivo",          type=str,   default=None, help="Caminho do CSV de saída")
    parser.add_argument("--prefetch-inicial", type=int,   default=None, help="Prefetch inicial (modo coleta)")
    parser.add_argument("--intervalo",        type=float, default=None, help="Intervalo de monitoramento em segundos")
    parser.add_argument("--janela",           type=int,   default=None, help="Duração de cada janela em segundos (modo impacto)")
    parser.add_argument("--prefetch-fim",     type=int,   default=None, help="Prefetch máximo a testar (modo impacto)")
    parser.add_argument("--prefetch-passo",   type=int,   default=None, help="Passo do incremento (modo impacto)")
    return parser.parse_args()


def main():
    args = _parse_args()
    intervalo = args.intervalo or INTERVALO_DE_MONITORAMENTO

    if args.modo == "coleta":
        # ── Troca a política aqui ──────────────────────────────────
        policy = BaseMessageQuantityPolicy(TARGET_MESSAGES_SEQUENCE[0])
        # from policies.policies import HeuristicaRica
        # policy = HeuristicaRica()
        # from policies.policies import MLPolicy
        # policy = MLPolicy("modelos/rf.joblib")
        # ──────────────────────────────────────────────────────────

        manager = ColetaManager(
            policy=policy,
            filename=args.arquivo or DATASET_ARQUIVO,
            prefetch_count=args.prefetch_inicial or PREFETCH_INICIAL,
            monitor_interval=intervalo,
        )

    else:  # impacto
        manager = ImpactoManager(
            filename=args.arquivo or IMPACTO_ARQUIVO,
            monitor_interval=intervalo,
            prefetch_inicio=IMPACTO_PREFETCH_INICIO,
            prefetch_fim=args.prefetch_fim or IMPACTO_PREFETCH_FIM,
            prefetch_passo=args.prefetch_passo or IMPACTO_PREFETCH_PASSO,
            janela_segundos=args.janela or IMPACTO_JANELA_SEGUNDOS,
        )

    manager.start()

    try:
        while manager.running:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n[Manager] Interrupção recebida — encerrando modo {args.modo}...")
    finally:
        manager.stop()


if __name__ == "__main__":
    main()