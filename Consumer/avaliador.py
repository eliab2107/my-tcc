import time
import math
import csv
import os
import requests
import psutil
from threading import Thread, Lock
from typing import List
from dotenv import load_dotenv
from Consumer.consumer import Consumer

load_dotenv()

# ------------------------------------------------------------------
# Configuração
# ------------------------------------------------------------------
INTERVALO_DE_MONITORAMENTO = int(os.getenv("MONITOR_INTERVAL", 5))
PREFETCH_INICIAL           = int(os.getenv("PREFETCH_INICIAL", 1))
CSV_FLUSH_INTERVAL         = int(os.getenv("CSV_FLUSH_INTERVAL", 60))

RABBITMQ_API_URL  = os.getenv("RABBITMQ_API_URL", f"http://{os.getenv('RABBITMQ_HOST', 'localhost')}:15672")
RABBITMQ_API_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_API_PASS = os.getenv("RABBITMQ_PASS", "guest")
RABBITMQ_QUEUE    = os.getenv("RABBITMQ_QUEUE", "fila_teste")

# ------------------------------------------------------------------
# Cenários disponíveis
# Cada cenário define a sequência de targets e o intervalo de troca.
# O producer deve ser configurado externamente de acordo com o cenário.
# ------------------------------------------------------------------
CENARIOS = {   
    
    "mudanca_target": {
        "targets":         [100, 800, 200, 500, 350, 700],
        "change_interval": 20,     # segundos
        "duracao_min":     2,
    },
}

# ------------------------------------------------------------------
# Header do CSV de avaliação — diferente do CSV de coleta,
# focado nas métricas de controle
# ------------------------------------------------------------------
AVALIACAO_HEADER = [
    "tick",
    "timestamp",
    "cenario",
    "controlador",
    # Métricas de controle
    "msgs_processadas_intervalo",
    "target_atual",
    "erro_absoluto",               # abs(msgs - target)
    "erro_relativo",               # (msgs - target) / target
    "dentro_banda",                # 1 se abs(erro_relativo) <= 0.1
    "fila_broker",
    # Estado do controlador
    "prefetch_count",
    "decisao",
    # Para calcular convergência no pós-processamento
    "ticks_desde_mudanca_target",
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

        self.policy           = policy
        self.controlador      = controlador
        self.cenario          = cenario
        self.monitor_interval = monitor_interval
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
        self.processo    = psutil.Process(os.getpid())
        self._data:      list = []
        self._data_lock  = Lock()
        self._tick_count = 0

        # Controle de target
        self._target_index   = 0
        self.target_quantity_message = self._targets[0]

        # Controle de convergência
        self._ticks_desde_mudanca_target = 0

        # Última ação
        self._last_action = 0

    # ------------------------------------------------------------------
    # Inicialização
    # ------------------------------------------------------------------
    def start(self) -> None:
        self.running   = True
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
    # Rotação de target — sequência fixa, sem aleatoriedade
    # ------------------------------------------------------------------
    def _target_loop(self) -> None:
        while self.running:
            time.sleep(self._change_interval)
            if not self.running:
                break
            self._target_index = (self._target_index + 1) % len(self._targets)
            novo_target = self._targets[self._target_index]

            if novo_target != self.target_quantity_message:
                self.target_quantity_message = novo_target
                self._ticks_desde_mudanca_target = 0  # reseta contador de convergência

                # Propaga o novo target para a policy se ela suportar
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

        # Encerra automaticamente ao atingir a duração do cenário
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

        # 2. Fila no broker
        fila_broker = self._get_queue_size()

        # 3. Estado do controlador
        prefetch_atual = self.consumers[0].prefetch_count

        # 4. Métricas de controle
        target        = self.target_quantity_message
        erro_absoluto = abs(n - target)
        erro_relativo = (n - target) / target if target > 0 else 0
        dentro_banda  = int(abs(erro_relativo) <= 0.1)

        # 5. Decisão — SEM perturbação aleatória na avaliação
        dados_para_policy = [
            round(timestamp, 3),
            n,
            round(avg_proc,    8),
            round(avg_latency, 8),
            fila_broker,
            prefetch_atual,
            self._last_action,
            target,
            n - target,       # distancia
            0, 0, 0, 0,       # cpu/ram — não usados na avaliação mas mantém formato
        ]

        ajuste = 0
        if self.running:
            ajuste = self.policy.decide(dados_para_policy)
            if ajuste != 0:
                self._ajustar_prefetch(consumer_id=0, ajuste=ajuste)

        # 6. Monta linha de avaliação
        linha = [
            self._tick_count,
            round(timestamp, 3),
            self.cenario,
            self.controlador,
            n,
            target,
            erro_absoluto,
            round(erro_relativo, 6),
            dentro_banda,
            fila_broker,
            prefetch_atual,
            ajuste,
            self._ticks_desde_mudanca_target,
        ]

        self._last_action = ajuste

        with self._data_lock:
            self._data.append(linha)

        print(
            f"[{self.controlador}] tick={self._tick_count} "
            f"| msgs={n} | target={target} "
            f"| erro={erro_relativo:+.2f} | banda={dentro_banda} "
            f"| prefetch={prefetch_atual} | acao={ajuste:+d}"
        )

    # ------------------------------------------------------------------
    # Ajuste de prefetch
    # ------------------------------------------------------------------
    def _ajustar_prefetch(self, consumer_id: int, ajuste: int) -> None:
        self.consumers[consumer_id].set_new_prefetch_count(ajuste)

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
# Entrypoint — troca só a policy e o cenário, tudo mais é igual
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
        RandomForestBaseInQtdMsgPolicy,
        XGBoostPolicyBaseInQtdMsgPolicy,
        MLPBaseInQtdMsgPolicy,
        MLPolicy,
    )

    # ------------------------------------------------------------------
    # Configure aqui qual controlador e cenário rodar.
    # Para rodar todos em sequência, descomente o loop abaixo.
    # ------------------------------------------------------------------

    EXPERIMENTOS = [
        # (policy,                                                                                                              controlador,         cenario)
        (BaseMessageQuantityPolicy(target_quantity_message=300),                                                                "heuristica_simples", "mudanca_target"),
        (MLPolicy("modelos/BaseQtdMsg/mlp_model.joblib", "modelos/BaseQtdMsg/mlp_scaler.joblib"),                               "mlp",           "mudanca_target"),
        (RandomForestBaseInQtdMsgPolicy("modelos/BaseQtdMsg/random_forest_model.joblib"),                                       "random_forest", "mudanca_target"),
       # (XGBoostPolicyBaseInQtdMsgPolicy("modelos/BaseQtdMsg/xgboost_model.joblib", "modelos/BaseQtdMsg/label_encoder.joblib"), "xgboost",       "mudanca_target"),
        # Adicione os demais controladores seguindo o mesmo padrão
    ]

    for policy, controlador, cenario in EXPERIMENTOS:
        print(f"\n{'='*60}")
        print(f"Iniciando: {controlador} / {cenario}")
        print(f"{'='*60}")
        rodar_avaliacao(policy, controlador, cenario)
        print(f"Aguardando 30s antes do próximo experimento...")
        time.sleep(30)  # pausa entre experimentos para o sistema estabilizar