import time
import math
import csv
import os
import requests
import threading
from threading import Thread, Lock, Event
from typing import List
from dotenv import load_dotenv
from Consumer.consumer import Consumer

load_dotenv()

# ------------------------------------------------------------------
# Configuração
# ------------------------------------------------------------------
INTERVALO_DE_MONITORAMENTO = 30
PREFETCH_INICIAL           = int(os.getenv("PREFETCH_INICIAL", 1))
CSV_FLUSH_INTERVAL         = int(os.getenv("CSV_FLUSH_INTERVAL", 600))

RABBITMQ_API_URL  = os.getenv("RABBITMQ_API_URL", f"http://{os.getenv('RABBITMQ_HOST', 'localhost')}:15672")
RABBITMQ_API_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_API_PASS = os.getenv("RABBITMQ_PASS", "guest")
RABBITMQ_QUEUE    = os.getenv("RABBITMQ_QUEUE", "fila_teste")

# Tempo máximo para aguardar confirmação da aplicação do prefetch
PREFETCH_APPLY_TIMEOUT = float(os.getenv("PREFETCH_APPLY_TIMEOUT", 3.0))

# ------------------------------------------------------------------
# Cenários
# ------------------------------------------------------------------
CENARIOS = {
    "mudanca_target": {
        "targets":         [230, 30, 190, 75, 140],
        "change_interval": 2160,#8000
        "duracao_min":     180,
    },
}

# ------------------------------------------------------------------
# Header do CSV
# ------------------------------------------------------------------
AVALIACAO_HEADER = [
    "tick", "timestamp", "cenario", "controlador",
    "msgs_processadas_intervalo",
    "msgs_por_segundo",
    "target_atual",
    "erro_absoluto",
    "erro_relativo",
    "dentro_banda",
    "fila_broker",
    "prefetch_count",
    "decisao",
    "ticks_desde_mudanca_target",
    "prefetch_aplicado",   # 1 se o prefetch foi confirmado neste tick, 0 se ainda pendente
]


class EvaluationManager:
    def __init__(
        self,
        policy,
        controlador: str,
        cenario: str,
        output_dir: str = "avaliacao",
        prefetch_count: int = PREFETCH_INICIAL,
        monitor_interval: float = INTERVALO_DE_MONITORAMENTO,
    ):
        assert cenario in CENARIOS, f"Cenário inválido: {cenario}. Opções: {list(CENARIOS.keys())}"

        self.policy            = policy
        self.controlador       = controlador
        self.cenario           = cenario
        self.monitor_interval  = monitor_interval
        self._initial_prefetch = prefetch_count

        cfg = CENARIOS[cenario]
        self._targets          = cfg["targets"]
        self._change_interval  = cfg["change_interval"]
        self._duracao_segundos = cfg["duracao_min"] * 60

        os.makedirs(output_dir, exist_ok=True)
        self.filename = os.path.join(output_dir, f"{controlador}_{cenario}.csv")

        self.consumers:        List[Consumer] = []
        self.consumer_threads: List[Thread]   = []
        self.monitor_thread:   Thread | None  = None
        self.flush_thread:     Thread | None  = None
        self.target_thread:    Thread | None  = None

        self.running     = False
        self._data:      list = []
        self._data_lock  = Lock()
        self._tick_count = 0

        self._target_index               = 0
        self.target_quantity_message     = self._targets[0]
        self._ticks_desde_mudanca_target = 0
        self._last_action                = 0

        # Evento usado para aguardar confirmação da aplicação do prefetch
        self._prefetch_applied_event = Event()

    # ------------------------------------------------------------------
    # Inicialização
    # ------------------------------------------------------------------
    def start(self) -> None:
        self.running     = True
        self._start_time = time.time()

        c = Consumer(id=0, prefetch_count=self._initial_prefetch)
        t = Thread(target=c.start_consuming, daemon=True, name="consumer-thread-0")
        self.consumers.append(c)
        self.consumer_threads.append(t)
        t.start()

        self.monitor_thread = Thread(
            target=self._monitor_loop, daemon=True, name="eval-monitor"
        )
        self.monitor_thread.start()

        self.flush_thread = Thread(
            target=self._flush_loop, daemon=True, name="eval-flush"
        )
        self.flush_thread.start()

        self.target_thread = Thread(
            target=self._target_loop, daemon=True, name="eval-target"
        )
        self.target_thread.start()

        print(
            f"[Avaliação] Iniciando | controlador={self.controlador} "
            f"| cenario={self.cenario} | duração={CENARIOS[self.cenario]['duracao_min']}min"
        )

    # ------------------------------------------------------------------
    # Rotação de target
    # ------------------------------------------------------------------
    def _target_loop(self) -> None:
        while self.running:
            time.sleep(self._change_interval)
            if not self.running:
                break
            self._target_index = (self._target_index + 1) % len(self._targets)
            novo_target = self._targets[self._target_index]

            if novo_target != self.target_quantity_message:
                self.target_quantity_message     = novo_target
                self._ticks_desde_mudanca_target = 0

                if hasattr(self.policy, "set_target_quantity_message"):
                    self.policy.set_target_quantity_message(novo_target)

                print(f"[Avaliação] Novo target: {novo_target}")

    # ------------------------------------------------------------------
    # Loop de monitoramento
    # ------------------------------------------------------------------
    def _monitor_loop(self) -> None:
        while True:
            try:
                self._monitor_tick()
            except Exception as e:
                print(f"[Avaliação/Monitor] erro: {e}")

    def _monitor_tick(self) -> None:
        time.sleep(self.monitor_interval)

        if time.time() - self._start_time >= self._duracao_segundos:
            print(f"[Avaliação] Duração atingida — encerrando {self.controlador}/{self.cenario}")
            self.running = False
            return

        self._tick_count += 1
        self._ticks_desde_mudanca_target += 1
        timestamp = time.time()

        # 1. Métricas do consumer
        dados = self.consumers[0].get_data() if self.consumers else []
        n, avg_proc, avg_latency = self._calcular_medias(dados)
        msgs_por_segundo = round(n / self.monitor_interval, 4)

        # 2. Fila no broker
        fila_broker = self._get_queue_size()

        # 3. Estado atual do controlador
        prefetch_atual = self.consumers[0].prefetch_count

        # 4. Métricas de controle
        target        = self.target_quantity_message
        erro_absoluto = abs(msgs_por_segundo - target)
        erro_relativo = (msgs_por_segundo - target) / target if target > 0 else 0
        dentro_banda  = int(abs(erro_relativo) <= 0.1)

        # 5. Monta array no novo formato (sem CPU/RAM, com msgs_por_segundo)
        dados_para_policy = [
            round(timestamp, 3),   # 0  timestamp
            n,                     # 1  msgs_processadas_intervalo
            msgs_por_segundo,      # 2  msgs_por_segundo
            round(avg_proc,    8), # 3  avg_processing_time
            round(avg_latency, 8), # 4  avg_queue_latency
            fila_broker,           # 5  fila_broker
            prefetch_atual,        # 6  prefetch_count
            self._last_action,     # 7  ultima_acao
            target,                # 8  target_atual
            msgs_por_segundo - target,  # 9  distancia_target
        ]

        # 6. Decisão — sem perturbação
        ajuste           = 0
        prefetch_aplicado = 0

        if self.running:
            ajuste = self.policy.decide(dados_para_policy)

            if ajuste != 0:
                # Limpa o evento antes de agendar para garantir que
                # não está setado de uma aplicação anterior
                self._prefetch_applied_event.clear()

                # Agenda o ajuste na thread do consumer
                self._ajustar_prefetch(consumer_id=0, ajuste=ajuste)

                # Aguarda confirmação com timeout
                # Se o consumer confirmar dentro do timeout, registra como aplicado
                aplicado = self._prefetch_applied_event.wait(
                    timeout=PREFETCH_APPLY_TIMEOUT
                )
                prefetch_aplicado = 1 if aplicado else 0

                if not aplicado:
                    print(
                        f"[{self.controlador}] AVISO: prefetch não confirmado em "
                        f"{PREFETCH_APPLY_TIMEOUT}s — continuando sem bloquear"
                    )

                # Lê o prefetch após aguardar — reflete o valor real aplicado
                prefetch_atual = self.consumers[0].prefetch_count

        # 7. Monta linha
        linha = [
            self._tick_count,
            round(timestamp, 3),
            self.cenario,
            self.controlador,
            n,
            msgs_por_segundo,
            target,
            erro_absoluto,
            round(erro_relativo, 6),
            dentro_banda,
            fila_broker,
            prefetch_atual,
            ajuste,
            self._ticks_desde_mudanca_target,
            prefetch_aplicado,
        ]

        self._last_action = ajuste

        with self._data_lock:
            self._data.append(linha)

        print(
            f"[{self.controlador}] tick={self._tick_count} "
            f"| msgs={n} | msgs/s={msgs_por_segundo} | target={target} "
            f"| erro={erro_relativo:+.2f} | banda={dentro_banda} "
            f"| prefetch={prefetch_atual} | acao={ajuste:+d}"
        )

    # ------------------------------------------------------------------
    # Ajuste de prefetch com sinalização de confirmação
    # ------------------------------------------------------------------
    def _ajustar_prefetch(self, consumer_id: int, ajuste: int) -> None:
        """
        Agenda o ajuste de prefetch na thread do consumer via
        add_callback_threadsafe e registra um callback de confirmação
        que sinaliza o evento quando o prefetch foi de fato aplicado.
        """
        consumer = self.consumers[consumer_id]
        event    = self._prefetch_applied_event

        # Calcula o novo valor aqui para passar ao callback
        with consumer._lock:
            novo = max(1, consumer._prefetch_count + ajuste)

        def _aplicar_e_sinalizar():
            consumer._recreate_consumer(novo)
            event.set()  # sinaliza para o monitor_tick que o prefetch foi aplicado

        consumer.connection.add_callback_threadsafe(_aplicar_e_sinalizar)

    # ------------------------------------------------------------------
    # Fila no broker
    # ------------------------------------------------------------------
    def _get_queue_size(self) -> int:
        try:
            url = f"{RABBITMQ_API_URL}/api/queues/%2F/{RABBITMQ_QUEUE}"
            r = requests.get(url, auth=(RABBITMQ_API_USER, RABBITMQ_API_PASS), timeout=2)
            if r.status_code == 200:
                return r.json().get("messages_ready", 0)
        except Exception as e:
            print(f"[Avaliação] Falha ao consultar API RabbitMQ: {e}")
        return -1

    # ------------------------------------------------------------------
    # Flush de CSV
    # ------------------------------------------------------------------
    def _flush_loop(self) -> None:
        while self.running:
            time.sleep(CSV_FLUSH_INTERVAL)
            try:
                self._save_csv()
            except Exception as e:
                print(f"[Avaliação/Flush] Erro: {e}")

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
                writer.writerow(AVALIACAO_HEADER)
            writer.writerows(rows)
        print(f"[Avaliação/Flush] {len(rows)} linhas → {self.filename}")

    # ------------------------------------------------------------------
    # Encerramento
    # ------------------------------------------------------------------
    def stop(self) -> None:
        self.running = False
        for c in self.consumers:
            c.stop()

        print("[Avaliação] Aguardando encerramento das threads...")
        for t in self.consumer_threads:
            t.join(timeout=30)
        for t in [self.monitor_thread, self.flush_thread, self.target_thread]:
            if t:
                t.join(timeout=10)

        self._save_csv()
        self.consumers.clear()
        self.consumer_threads.clear()
        print(f"[Avaliação] Encerrado — {self.controlador}/{self.cenario} → {self.filename}")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _calcular_medias(self, dados: list) -> tuple:
        n = len(dados)
        if n == 0:
            return 0, 0.0, 0.0
        avg_proc    = math.fsum(d["processing_time"] for d in dados) / n
        avg_latency = math.fsum(d["queue_latency"]   for d in dados) / n
        return n, avg_proc, avg_latency


# ------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------
def rodar_avaliacao(policy, controlador: str, cenario: str) -> None:
    manager = EvaluationManager(
        policy=policy,
        controlador=controlador,
        cenario=cenario,
        prefetch_count=PREFETCH_INICIAL,
        monitor_interval=INTERVALO_DE_MONITORAMENTO,
    )
    manager.start()

    try:
        while manager.running:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[Avaliação] Interrompido pelo usuário")
    finally:
        manager.stop()


if __name__ == "__main__":
    from policies.policies import (
        BaseMessageQuantityPolicy,
        MLPolicy,
    )

    EXPERIMENTOS = [
#        (MLPolicy("modelos/V2/random_forest_model.joblib"),   "random_forest", "mudanca_target"),

#        (MLPolicy("modelos/V2/xgboost_model.joblib",  encoder_path="modelos/V2/label_encoder_xgboost.joblib"), "xgboost", "mudanca_target"),

#        (MLPolicy("modelos/V2/lgbm_model.joblib",     encoder_path="modelos/V2/label_encoder_lgbm.joblib"),      "lgbm", "mudanca_target"),

#         (MLPolicy("modelos/V2/knn_model.joblib",   scaler_path="modelos/V2/scaler_knn.joblib"),     "knn", "mudanca_target"),

        (MLPolicy("modelos/V2/mlp_model.joblib",        scaler_path="modelos/V2/mlp_scaler.joblib"), "mlp", "mudanca_target"),
    ]

    for policy, controlador, cenario in EXPERIMENTOS:
        print(f"\n{'='*60}")
        print(f"Iniciando: {controlador} / {cenario}")
        print(f"{'='*60}")
        rodar_avaliacao(policy, controlador, cenario)
        print("Aguardando 30s antes do próximo experimento...")
        time.sleep(30)
