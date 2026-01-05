from metrics.interfaces.consumer_metrics_provider import ConsumerMetricsProvider
from threading import Thread, Lock
from time import sleep, monotonic
from dataclasses import dataclass
from collections import deque


@dataclass
class RawData:
    timestamp: float
    message_count: int
    total_messages: int
    total_acks: int


@dataclass
class DerivedData:
    timestamp: float
    msg_rate: float
    ack_rate: float
    in_flight: int


@dataclass
class HistoryById:
    id:int 
    raw_data_history:RawData
    derived_data_history:DerivedData
    last_raw:list


@dataclass
class AllHistory:
    raw_history:dict
    derived_history:dict
    
    
class Metrics:
    def __init__(self, provider:ConsumerMetricsProvider, interval: float = 1.0, window_size: int = 60):
        """
        provider: objeto que expõe snapshot_metrics()
        interval: intervalo de coleta (segundos)
        window_size: tamanho máximo do histórico por consumer. 
        """
        self.provider = provider
        self.interval = interval
        self.window_size = window_size

        self.raw_history = {}
        self.derived_history = {}
        self.last_raw = {}

        self.lock = Lock()
        self.running = False
        self.thread = None

    
    def start_collector(self):
        self.running = True
        self.thread = Thread(target=self.start_collect_loop, daemon=True, name="metrics-loop")
        self.thread.start()

    
    def stop(self):
        self.running = False

    
    def start_collect_loop(self):
        while self.running:
            timestamp = monotonic()
            snapshot = self.provider.snapshot_metrics()

            with self.lock:
                for consumer_id, data in snapshot.items():
                    self.process_consumer(consumer_id, data, timestamp)

            sleep(self.interval)


    def process_consumer(self, consumer_id, data, timestamp):
        message_count, total_messages, total_acks, total_proc_time = data

        raw = RawData(timestamp=timestamp, message_count=message_count, total_messages=total_messages, total_acks=total_acks, total_processing_time=total_proc_time )

        if consumer_id not in self.raw_history:
            self.raw_history[consumer_id] = deque(maxlen=self.window_size)
            self.derived_history[consumer_id] = deque(maxlen=self.window_size)
            self.last_raw[consumer_id] = raw
            self.raw_history[consumer_id].append(raw)
            return

        previous = self.last_raw[consumer_id]
        dt = timestamp - previous.timestamp
        if dt <= 0:
            return

        delta_msgs = raw.total_messages - previous.total_messages
        delta_acks = raw.total_acks - previous.total_acks

        msg_rate = delta_msgs / dt
        ack_rate = delta_acks / dt
        in_flight = raw.total_messages - raw.total_acks
     

        derived = DerivedData(timestamp=timestamp, msg_rate=msg_rate, ack_rate=ack_rate, in_flight=in_flight)

        self.raw_history[consumer_id].append(raw)
        self.derived_history[consumer_id].append(derived)
        self.last_raw[consumer_id] = raw


    def get_all_by_id(self, consumer_id:int):
        return HistoryById(id=consumer_id, raw_data_history=self.raw_history[consumer_id], derived_data_history=self.derived_history[consumer_id],  last_raw=self.last_raw[consumer_id])
    
    def get_all_from_system(self):
        return AllHistory(raw_history=self.raw_history, derived_history=self.derived_history)
  
                                                                                                                                                                                                                                                                                                                                                                                                                                                                    