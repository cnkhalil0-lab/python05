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

    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        
        # 1. Si on ne cherche rien (criteria est vide), on rend la liste telle quelle
        if criteria == None:
            return data_batch
            
        # 2. On prépare une nouvelle liste vide pour stocker ce qu'on va trouver
        liste_filtree = []
        
        # 3. On regarde chaque élément un par un
        for item in data_batch:
            
            # 4. On le convertit en texte pour pouvoir chercher dedans
            texte_de_l_element = str(item)
            
            # 5. Si le mot qu'on cherche est dans ce texte
            if criteria in texte_de_l_element:
                # 6. On l'ajoute à notre nouvelle liste
                liste_filtree.append(item)
            
        # 7. On renvoie le résultat final
        return liste_filtree

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Statistiques de base."""
        return {"id": self.stream_id, "processed": self.processed_count}


# --- 2. Les Classes Spécialisées ---

class SensorStream(DataStream):
    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            # On met à jour le compteur global
            self.processed_count += len(data_batch)
            
            # 1. On trie les vraies températures et on compte les alertes
            readings = []
            alerts_count = 0
            
            for r in data_batch:
                if isinstance(r, (int, float)):  # Si c'est bien un nombre
                    readings.append(r)
                    if r > 30:                   # Si la température est critique
                        alerts_count += 1
            
            # 2. On calcule la moyenne de façon sécurisée
            if len(readings) > 0:
                avg = sum(readings) / len(readings)
            else:
                avg = 0
                
            # 3. On prépare le bout de texte pour les alertes (seulement s'il y en a)
            alert_msg = ""
            if alerts_count > 0:
                alert_msg = f", {alerts_count} alerts detected"
                
            # 4. On renvoie la phrase finale assemblée
            return f"Sensor analysis: {len(data_batch)} readings processed, avg temp: {avg:.1f}°C{alert_msg}"
            
        except Exception as e:
            # En cas de problème inattendu, on attrape l'erreur
            return f"Sensor failure: {e}"


class TransactionStream(DataStream):
    
    # --- PARTIE 1 : LE CALCUL DES PROFITS ---
    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            # 1. On met à jour le compteur global d'opérations
            self.processed_count += len(data_batch)
            
            # 2. La caisse enregistreuse (le flux net) commence à 0
            net_flow = 0
            
            # 3. On analyse chaque transaction une par une
            for op in data_batch:
                texte_op = str(op)  # On sécurise en texte (ex: "buy:100")
                
                # On coupe le texte en deux au niveau du ":"
                # "buy:100" devient une liste ["buy", "100"]
                morceaux = texte_op.split(':')
                
                # On prend la dernière case ([-1]) qui est "100" et on la transforme en vrai nombre (int)
                valeur = int(morceaux[-1])
                
                # Si c'est une vente (sell), l'argent rentre dans la caisse (+)
                if "sell" in texte_op:
                    net_flow += valeur
                # Sinon (c'est un buy), l'argent sort de la caisse (-)
                else:
                    net_flow -= valeur
            
            # 4. On met en forme le résultat pour l'affichage (ajouter un "+" si c'est positif)
            if net_flow > 0:
                flow_str = f"+{net_flow}"
            else:
                flow_str = str(net_flow)
                
            return f"Transaction analysis: {len(data_batch)} operations, net flow: {flow_str} units"
            
        except Exception as e:
            # Si une transaction est corrompue (ex: "buy:pomme"), l'erreur est attrapée ici
            return f"Transaction failure: {e}"

    # --- PARTIE 2 : LE FILTRE SPÉCIFIQUE (METHOD OVERRIDING) ---
    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        
        # Si on nous demande de filtrer spécifiquement les achats ou les ventes :
        if criteria == "buy" or criteria == "sell":
            resultat = []
            for op in data_batch:
                # On vérifie si la transaction COMMENCE par ce mot
                if str(op).startswith(criteria): 
                    resultat.append(op)
            return resultat
        
        # Si on cherche un autre mot (ex: "error"), on laisse la classe mère s'en occuper !
        return super().filter_data(data_batch, criteria)


class EventStream(DataStream):
    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            self.processed_count += len(data_batch)
            # Ajout : Catégorisation (Erreurs vs Warnings)
            errors = [e for e in data_batch if "error" in str(e).lower()]
            warnings = [e for e in data_batch if "warning" in str(e).lower()]
            return f"Event analysis: {len(data_batch)} events ({len(errors)} errors, {len(warnings)} warnings)"
        except Exception as e:
            return f"Event failure: {e}"


# --- 3. Le Manager Polymorphique ---

class StreamProcessor:
    def __init__(self) -> None:
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:
        if isinstance(stream, DataStream):
            self.streams.append(stream)

    def process_all(self, global_batches: List[List[Any]]) -> None:
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
    for stream in [s, t, e]: 
        proc.add_stream(stream)

    # Simulation de données
    data = [
        [22.5, 35.2, 21.0],                             # Sensor (inclut une valeur extrême > 30)
        ["buy:100", "sell:150"],                        # Transaction
        ["login", "error_404", "warning_timeout"]       # Event (catégorisé)
    ]

    print("\n=== Polymorphic Stream Processing ===")
    proc.process_all(data)

    print("\n=== Filtering & Stats Demo ===")
    print("Stream filtering active: High-priority data only")

    # Démo filtrage spécifique Transaction
    large_tx = t.filter_data(["buy:10", "sell:500", "buy:20"], "sell")
    print(f"Filtered Transaction results: {len(large_tx)} 'sell' transaction(s) found. -> {large_tx}")

    # Démo filtrage par défaut (hérité de DataStream) sur EventStream
    critical_events = e.filter_data(data[2], "error")
    print(f"Filtered Event results: {len(critical_events)} 'error' event(s) found. -> {critical_events}")

    # Démo des statistiques
    print("\nStream Statistics:")
    print(f"Sensor: {s.get_stats()}")
    print(f"Transaction: {t.get_stats()}")
    print(f"Event: {e.get_stats()}")

    print("\nAll streams processed successfully. Nexus throughput optimal.")
