from dataclasses import dataclass

from typing import Dict

@dataclass
class RawData:
    id: int
    timestamp: float
    message_count: int
    total_messages: int
    total_acks: int
    prefetch_count: int


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
    raw_history:Dict[int, RawData]
    derived_history:Dict[int, DerivedData]
    