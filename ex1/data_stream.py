from abc import ABC, abstractmethod
from typing import List, Any, Dict, Union, Optional


class DataStream(ABC):
    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        self.processed_count = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """Méthode abstraite obligatoire."""
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria is None:
            return data_batch

        liste_filtree = []

        for item in data_batch:

            texte_de_l_element = str(item)

            if criteria in texte_de_l_element:

                liste_filtree.append(item)

        return liste_filtree

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Statistiques de base."""
        return {"id": self.stream_id, "processed": self.processed_count}


class SensorStream(DataStream):
    def process_batch(self, data_batch: List[Any]) -> str:
        try:

            self.processed_count += len(data_batch)

            readings = []
            alerts_count = 0

            for r in data_batch:
                if isinstance(r, (int, float)):
                    readings.append(r)
                    if r > 30:
                        alerts_count += 1

            if len(readings) > 0:
                avg = sum(readings) / len(readings)
            else:
                avg = 0

            alert_msg = ""
            if alerts_count > 0:
                alert_msg = f", {alerts_count} alerts detected"

            return (f"Sensor analysis: {len(data_batch)} readings processed, "
                    f"avg temp: {avg:.1f}°C{alert_msg}")
        except Exception as e:

            return f"Sensor failure: {e}"


class TransactionStream(DataStream):

    def process_batch(self, data_batch: List[Any]) -> str:
        try:

            self.processed_count += len(data_batch)
            net_flow = 0

            for op in data_batch:
                texte_op = str(op)  

                morceaux = texte_op.split(':')

                valeur = int(morceaux[-1])

                if "sell" in texte_op:
                    net_flow += valeur

                else:
                    net_flow -= valeur

            if net_flow > 0:
                flow_str = f"+{net_flow}"
            else:
                flow_str = str(net_flow)

            return (f"Transaction analysis: {len(data_batch)} operations, "
                    f"net flow: {flow_str} units")

        except Exception as e:
            return f"Transaction failure: {e}"

    def filter_data(self, data_batch: List[Any], 
                    criteria: Optional[str] = None) -> List[Any]:

        if criteria == "buy" or criteria == "sell":
            resultat = []
            for op in data_batch:
                if str(op).startswith(criteria):
                    resultat.append(op)
            return resultat
        return super().filter_data(data_batch, criteria)


class EventStream(DataStream):
    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            self.processed_count += len(data_batch)

            errors_count = 0
            warnings_count = 0

            for e in data_batch:

                texte_evenement = str(e).lower()

                if "error" in texte_evenement:
                    errors_count += 1
                if "warning" in texte_evenement:
                    warnings_count += 1

            return (f"Event analysis: {len(data_batch)} events "
                    f"({errors_count} errors, {warnings_count} warnings)")

        except Exception as e:
            return f"Event failure: {e}"


class StreamProcessor:
    def __init__(self) -> None:
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:
        if isinstance(stream, DataStream):
            self.streams.append(stream)

    def process_all(self, global_batches: List[List[Any]]) -> None:
        for i, stream in enumerate(self.streams):
            try:
                print(f"- {stream.process_batch(global_batches[i])}")
            except Exception as e:
                print(f"Critical error on {stream.stream_id}: {e}")


if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")

    s = SensorStream("SENSOR_001")
    t = TransactionStream("TRANS_001")
    e = EventStream("EVENT_001")

    proc = StreamProcessor()
    for stream in [s, t, e]:
        proc.add_stream(stream)

    data = [
        [22.5, 35.2, 21.0],
        ["buy:100", "sell:150"],
        ["login", "error_404", "warning_timeout"]
    ]

    print("\n=== Polymorphic Stream Processing ===")
    proc.process_all(data)

    print("\n=== Filtering & Stats Demo ===")
    print("Stream filtering active: High-priority data only")
    large_tx = t.filter_data(["buy:10", "sell:500", "buy:20"], "sell")
    print(f"Filtered Transaction results: {len(large_tx)} 'sell' "
          f"transaction(s) found. -> {large_tx}")
    critical_events = e.filter_data(data[2], "error")
    print(f"Filtered Event results: {len(critical_events)} 'error' "
          f"event(s) found. -> {critical_events}")
    print("\nStream Statistics:")
    print(f"Sensor: {s.get_stats()}")
    print(f"Transaction: {t.get_stats()}")
    print(f"Event: {e.get_stats()}")
    print("\nAll streams processed successfully. Nexus throughput optimal.")
