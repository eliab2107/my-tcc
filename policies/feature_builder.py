class FeatureBuilder:
    """
    Recebe o array bruto do manager e produz as features do treinamento.
    Índices do array de entrada:
        0  timestamp
        1  qtd_mensagens_processadas
        2  avg_proc
        3  avg_latency
        4  fila_broker
        5  prefetch_atual
        6  last_action
        7  target_atual
        8  distancia
        9  cpu_g
        10 ram_g
        11 cpu_p
        12 ram_p
    """

    # Índices do array bruto — evita magic numbers
    I_TIMESTAMP  = 0
    I_MSGS       = 1
    I_AVG_PROC   = 2
    I_AVG_LAT    = 3
    I_FILA       = 4
    I_PREFETCH   = 5
    I_LAST_ACT   = 6
    I_TARGET     = 7
    I_DISTANCIA  = 8

    FEATURE_COLUMNS = [
        "msgs_processadas_intervalo",
        "fila_broker",
        "avg_queue_latency",
        "avg_processing_time",
        "prefetch_count",
        "ultima_acao",
        "target_atual",
        "erro_relativo",
        "delta_msgs",
        "delta_fila",
        "delta_latencia",
        "delta_prefetch",
        "razao_fila_vazao",
    ]

    def __init__(self):
        self._prev = None

    def build(self, raw: list, mudou_target: bool = False) -> dict:
        n            = raw[self.I_MSGS]
        avg_proc     = raw[self.I_AVG_PROC]
        avg_latency  = raw[self.I_AVG_LAT]
        fila_broker  = raw[self.I_FILA]
        prefetch     = raw[self.I_PREFETCH]
        ultima_acao  = raw[self.I_LAST_ACT]
        target       = raw[self.I_TARGET]

        erro_relativo    = (n - target) / target if target > 0 else 0
        razao_fila_vazao = fila_broker / n if n > 0 else 0

        if self._prev is None or mudou_target:
            delta_msgs     = 0
            delta_fila     = 0
            delta_latencia = 0
            delta_prefetch = 0
        else:
            delta_msgs     = n           - self._prev["msgs_processadas_intervalo"]
            delta_fila     = fila_broker - self._prev["fila_broker"]
            delta_latencia = avg_latency - self._prev["avg_queue_latency"]
            delta_prefetch = prefetch    - self._prev["prefetch_count"]

        snapshot = {
            "msgs_processadas_intervalo": n,
            "fila_broker":                fila_broker,
            "avg_queue_latency":          avg_latency,
            "avg_processing_time":        avg_proc,
            "prefetch_count":             prefetch,
            "ultima_acao":                ultima_acao,
            "target_atual":               target,
            "erro_relativo":              erro_relativo,
            "delta_msgs":                 delta_msgs,
            "delta_fila":                 delta_fila,
            "delta_latencia":             delta_latencia,
            "delta_prefetch":             delta_prefetch,
            "razao_fila_vazao":           razao_fila_vazao,
        }

        self._prev = snapshot.copy()
        return snapshot

    def to_dataframe(self, snapshot: dict):
        import pandas as pd
        return pd.DataFrame([snapshot])[self.FEATURE_COLUMNS]