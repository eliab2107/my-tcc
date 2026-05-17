import time
import pika
import json
from dotenv import load_dotenv
from random import randint, choices, choice
import argparse
import os
from threading import Thread

load_dotenv()

# Sequência de perfis de carga que se repetem ciclicamente.
# Cada tupla é (sleep_entre_msgs_segundos, descrição).
# Quanto menor o sleep, maior a taxa de produção.
LOAD_PROFILES = [
    (0.000, "sobrecarga"),   # sem sleep — máxima taxa possível
    (0.001, "alta"),
    (0.003, "media-alta"),
    (0.005, "media"),
    (0.010, "media-baixa"),
    (0.020, "baixa"),
    (0.005, "media"),        # sobe gradualmente de volta
    (0.001, "alta"),
]

PROFILE_DURATION_SECONDS = 5 * 60  # 5 minutos por perfil


class Produtor():

    def __init__(self, time_sleep: float = 0.001, mode: str = "skewed",
                 constant_task: int = 1, vary_load: bool = False):
        self.time_sleep = float(time_sleep)
        self.mode = mode
        self.constant_task = int(constant_task)
        self.vary_load = vary_load
        self._stop = False

        self.host = os.getenv("RABBITMQ_HOST")
        self.user = os.getenv("RABBITMQ_USER")
        self.password = os.getenv("RABBITMQ_PASS")

        credentials = pika.PlainCredentials(self.user, self.password)
        parametros = pika.ConnectionParameters(host=self.host, credentials=credentials)

        self.connection = pika.BlockingConnection(parametros)
        self.channel = self.connection.channel()
        self.queue = os.getenv("RABBITMQ_QUEUE", "fila_teste")
        self.channel.queue_declare(queue=self.queue)
        self.counter = 0

    def _next_task(self) -> int:
        if self.mode == "constant":
            return self.constant_task
        elif self.mode == "skewed":
            return choices([1, 2, 3], weights=[70, 20, 10], k=1)[0]
        elif self.mode == "balanced":
            return choice([1, 2, 3])
        return randint(1, 3)

    def _load_variation_loop(self) -> None:
        """
        Roda em thread separada e altera self.time_sleep ciclicamente
        a cada PROFILE_DURATION_SECONDS, percorrendo LOAD_PROFILES.
        """
        profile_index = 0
        while not self._stop:
            sleep_val, label = LOAD_PROFILES[profile_index % len(LOAD_PROFILES)]
            self.time_sleep = sleep_val
            print(f"[PUBLISHER] Perfil de carga: {label} (sleep={sleep_val}s)")
            time.sleep(PROFILE_DURATION_SECONDS)
            profile_index += 1

    def publicar_mensagens(self) -> None:
        print(
            f"[PUBLISHER] Iniciando... mode={self.mode} "
            f"sleep_inicial={self.time_sleep} vary_load={self.vary_load}"
        )

        if self.vary_load:
            t = Thread(target=self._load_variation_loop, daemon=True, name="load-variation")
            t.start()

        try:
            while True:
                task = self._next_task()
                message_body = {
                    "conteudo": f"Mensagem {self.counter}",
                    "date_created": time.time(),
                    "task_level": task,
                }
                self.channel.basic_publish(
                    exchange="",
                    routing_key=self.queue,
                    body=json.dumps(message_body).encode("utf-8"),
                )
                self.counter += 1

                # Lê time_sleep a cada iteração — reflete mudanças da thread de variação
                current_sleep = self.time_sleep
                if current_sleep > 0:
                    time.sleep(current_sleep)

        except KeyboardInterrupt:
            print("[PUBLISHER] Interrompido, fechando conexão...")
        finally:
            self._stop = True
            try:
                self.connection.close()
            except Exception:
                pass


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Produtor RabbitMQ com variação de carga"
    )
    parser.add_argument(
        "--mode", choices=["constant", "skewed", "balanced"], default="skewed",
        help="Modo de tarefa: constant, skewed (70/20/10), balanced"
    )
    parser.add_argument(
        "--task", type=int, choices=[1, 2, 3], default=1,
        help="Tarefa fixa quando --mode=constant"
    )
    parser.add_argument(
        "--sleep", type=float, default=0.001,
        help="Sleep inicial entre envios (segundos)"
    )
    parser.add_argument(
        "--vary-load", action="store_true", default=False,
        help="Ativa variação automática de carga a cada 5 minutos"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    Produtor(
        time_sleep=args.sleep,
        mode=args.mode,
        constant_task=args.task,
        vary_load=args.vary_load,
    ).publicar_mensagens()