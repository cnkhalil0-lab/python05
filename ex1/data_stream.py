from abc import ABC, abstractmethod
from typing import List, Any, Dict, Union, Optional

# --- 1. La Base Abstraite (Le Contrat) ---


class DataStream(ABC):
    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        self.processed_count = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """Méthode abstraite obligatoire."""
        pass

    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        """Implémentation par défaut (filtrage textuel)."""
        if not criteria:
            return data_batch
        return [item for item in data_batch if criteria in str(item)]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Statistiques de base."""
        return {"id": self.stream_id, "processed": self.processed_count}

# --- 2. Les Classes Spécialisées ---

class SensorStream(DataStream):
    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            self.processed_count += len(data_batch)
            readings = [r for r in data_batch if isinstance(r, (int, float))]
            avg = sum(readings) / len(readings) if readings else 0
            # Ajout : Alertes pour valeurs extrêmes (ex: > 30°C)
            alerts = [r for r in readings if r > 30]
            alert_msg = f", {len(alerts)} alerts detected" if alerts else ""
            return f"Sensor analysis: {len(data_batch)} readings processed, avg temp: {avg}°C{alert_msg}"
        except Exception as e:
            return f"Sensor failure: {e}"


class TransactionStream(DataStream):
    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            self.processed_count += len(data_batch)
            net_flow = 0
            for op in data_batch:
                val = int(str(op).split(':')[-1])
                net_flow += val if "sell" in str(op) else -val
            
            flow_str = f"+{net_flow}" if net_flow > 0 else str(net_flow)
            return f"Transaction analysis: {len(data_batch)} operations, net flow: {flow_str} units"
        except Exception as e:
            return f"Transaction failure: {e}"

    # Overriding pour filtrage spécifique (ex: filtrer par type d'opération)
    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        if criteria in ["buy", "sell"]:
            return [op for op in data_batch if str(op).startswith(criteria)]
        return super().filter_data(data_batch, criteria)


class EventStream(DataStream):
    def process_batch(self, data_batch: List[Any]) -> str:
        self.processed_count += len(data_batch)
        # Ajout : Catégorisation (Erreurs vs Warnings)
        errors = [e for e in data_batch if "error" in str(e).lower()]
        warnings = [e for e in data_batch if "warning" in str(e).lower()]
        return f"Event analysis: {len(data_batch)} events ({len(errors)} errors, {len(warnings)} warnings)"

# --- 3. Le Manager Polymorphique ---

class StreamProcessor:
    def __init__(self):
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream):
        if isinstance(stream, DataStream):
            self.streams.append(stream)

    def process_all(self, global_batches: List[List[Any]]):
        for i, stream in enumerate(self.streams):
            try:
                # Polymorphisme en action
                print(f"- {stream.process_batch(global_batches[i])}")
            except Exception as e:
                print(f"Critical error on {stream.stream_id}: {e}")

# --- 4. Démo Finale ---

if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")
    
    # Init
    s = SensorStream("SENSOR_001")
    t = TransactionStream("TRANS_001")
    e = EventStream("EVENT_001")
    
    proc = StreamProcessor()
    for stream in [s, t, e]: proc.add_stream(stream)

    # Simulation de données
    data = [
        [22.5, 35.2, 21.0],           # Sensor (inclut une valeur extrême > 30)
        ["buy:100", "sell:150"],      # Transaction
        ["login", "error_404", "warning_timeout"] # Event (catégorisé)
    ]
    
    print("\n=== Polymorphic Stream Processing ===")
    proc.process_all(data)

    print("\nStream filtering active: High-priority data only")
    # Démo filtrage spécifique Transaction
    large_tx = t.filter_data(["buy:10", "sell:500", "buy:20"], "sell")
    print(f"Filtered results: {len(large_tx)} large transaction(s) found.")

    print("\nAll streams processed successfully. Nexus throughput optimal.")