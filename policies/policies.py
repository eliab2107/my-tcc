from policies.base_policy import BaseDecisionPolicy
from typing import Any, Dict, List
from execution.clients.consumer import Consumer
from DTOs.decision import Decision
from DTOs.metrics_dtos import AllHistory
from policies.feature_builder import FeatureBuilder
import joblib

class FirstPolicy(BaseDecisionPolicy):
    def decide(self, all_history: AllHistory) -> List[Decision]:
        decisions = []
       
        new_prefetch = all_history.raw_history[0][len(all_history.raw_history[0]) - 1].prefetch_count + 1
        decisions.append(Decision(consumer_id=0, prefetch=new_prefetch))
        
        return decisions
    
    
class SecondPolicy(BaseDecisionPolicy):
    def decide(self, all_history: AllHistory) -> Any:
        decisions = []
        for _id, metrics in all_history.items():
            decisions.append(Decision(consumer_id=_id, prefetch=10))
        return decisions
    
class BaseMessageQuantityPolicy():
    def __init__(self, target_quantity_message:int = 400):
        self.target_quantity_message = target_quantity_message

    def set_target_quantity_message(self, target_quantity_message: int) -> None:
        self.target_quantity_message = target_quantity_message
        print(f"[Policy] Novo target de mensagens definido: {self.target_quantity_message}")

    def decide(self, data) -> Any:
        quantity_message = data[-1] # Índice -1: msg por segundo
        if quantity_message > self.target_quantity_message * 1.08:
            return -1
        elif quantity_message < self.target_quantity_message * 0.92:
            return 1
        else:   
            return 0
        
        
class BaseSystemStatusPolicy():
    def __init__(self, target: int = 400):
        self.target = target

    def decide(self, dados, estado) -> int:
        """
        dados:  lista de métricas do consumer (retorno de get_data())
        estado: dict com fila_broker, delta_fila, prefetch_count, ultima_acao
        """
        n = len(dados)
        if n == 0:
            return 0

        erro = (n - self.target) / self.target  # erro_relativo

        fila_crescendo  = estado.get("delta_fila", 0) > 0
        vazao_caindo    = estado.get("delta_msgs", 0) < 0
        acabou_de_agir  = abs(estado.get("ultima_acao", 0)) > 0
        fila_critica    = estado.get("fila_broker", 0) > self.target * 3

        # Urgente: fora da banda E situação piorando
        if erro < -0.1 and (fila_crescendo or vazao_caindo):
            return 1
        if erro > 0.1:
            return -1

        # Preventivo: dentro da banda mas tendência ruim
        if erro < -0.05 and vazao_caindo and not acabou_de_agir:
            return 1

        # Pressão de fila crítica mesmo com vazão ok
        if fila_critica and not acabou_de_agir:
            return 1

        return 0
    
    
class RandomForestBaseInQtdMsgPolicy():
    def __init__(self, model_path: str):
        self.model       = joblib.load(model_path)
        self._builder    = FeatureBuilder()
        self._prev_target = None

    def decide(self, raw: list) -> int:
        mudou_target = (
            self._prev_target is not None and
            raw[FeatureBuilder.I_TARGET] != self._prev_target
        )
        self._prev_target = raw[FeatureBuilder.I_TARGET]

        snapshot = self._builder.build(raw, mudou_target=mudou_target)
        X        = self._builder.to_dataframe(snapshot)
        resp     = self.model.predict(X)[0]
        return int(resp)
    
class XGBoostPolicyBaseInQtdMsgPolicy():
    def __init__(self, model_path: str, encoder_path: str):
        self.model        = joblib.load(model_path)
        self.label_encoder = joblib.load(encoder_path)
        self._builder     = FeatureBuilder()
        self._prev_target = None

    def decide(self, raw: list) -> int:
        mudou_target = (
            self._prev_target is not None and
            raw[FeatureBuilder.I_TARGET] != self._prev_target
        )
        self._prev_target = raw[FeatureBuilder.I_TARGET]

        snapshot = self._builder.build(raw, mudou_target=mudou_target)
        X = self._builder.to_dataframe(snapshot)

        # XGBoost não precisa de scaler — envia direto
        pred_encoded = self.model.predict(X)[0]

        # Decodifica 0,1,2 → -1,0,1
        return int(self.label_encoder.inverse_transform([int(pred_encoded)])[0])
    
class MLPBaseInQtdMsgPolicy():
    def __init__(self, model_path: str, scaler_path: str):
        self.model       = joblib.load(model_path)
        self.scaler      = joblib.load(scaler_path)
        self._builder    = FeatureBuilder()
        self._prev_target = None 
        
    def decide(self, raw: list) -> int:
        mudou_target = (
            self._prev_target is not None and
            raw[FeatureBuilder.I_TARGET] != self._prev_target
        )
        self._prev_target = raw[FeatureBuilder.I_TARGET]

        snapshot = self._builder.build(raw, mudou_target=mudou_target)
        data_formated_to_model = self._builder.to_dataframe(snapshot)
        data_scaled_to_model = self.scaler.transform(data_formated_to_model)

        return int(self.model.predict(data_scaled_to_model)[0])

class MLPolicy():
    def __init__(self, model_path: str, scaler_path: str = None, encoder_path: str = None):
        self.model        = joblib.load(model_path)
        self._builder     = FeatureBuilder()
        self._prev_target = None

        self.scaler  = joblib.load(scaler_path)  if scaler_path  else None
        self.encoder = joblib.load(encoder_path) if encoder_path else None

    def decide(self, raw: list) -> int:
        mudou_target = (
            self._prev_target is not None and
            raw[FeatureBuilder.I_TARGET] != self._prev_target
        )
        self._prev_target = raw[FeatureBuilder.I_TARGET]

        snapshot = self._builder.build(raw, mudou_target=mudou_target)
        x_line = self._builder.to_dataframe(snapshot)

        x_input = self.scaler.transform(x_line) if self.scaler else x_line

        pred = self.model.predict(x_input)[0]

        # Decodifica 0,1,2 → -1,0,1 se houver encoder (XGBoost e LightGBM)
        if self.encoder:
            return int(self.encoder.inverse_transform([int(pred)])[0])

        return int(pred)
    

class HPAInspiredPolicy:

    def decide(self, raw: list) -> int:
        n        = raw[FeatureBuilder.I_MSGS_S]
        target   = raw[FeatureBuilder.I_TARGET]
        prefetch = raw[FeatureBuilder.I_PREFETCH]

        if target == 0 or n == 0:
            return 0

        new_pc = round(prefetch * (n / target))

        # Teto absoluto — fundamentado no experimento de impacto do prefetch
        new_pc = max(1, min(new_pc, 30))

        delta = new_pc - prefetch

        # Limita a velocidade de mudança por tick
        delta = max(-30, min(delta, 30))

        return delta