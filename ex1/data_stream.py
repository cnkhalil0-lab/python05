from abc import ABC, abstractmethod
import typing

# ==============================================================================
# PARTIE 1 : LES CLASSES DE L'EXERCICE 0 (Copiées-collées ici comme demandé)
# ==============================================================================

class DataProcessor(ABC):
    def __init__(self) -> None:
        self.storage: list[tuple[int, str]] = []
        self.rank: int = 0

    @abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if len(self.storage) == 0:
            raise ValueError("Aucune donnee a extraire.")
        # On recupere et supprime le premier element (le plus ancien)
        return self.storage.pop(0)


class NumericProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        # Si c'est un simple nombre
        if isinstance(data, int) or isinstance(data, float):
            return True
            
        # Si c'est une liste, on verifie chaque case une par une
        if isinstance(data, list):
            for item in data:
                if not (isinstance(item, int) or isinstance(item, float)):
                    return False # On a trouve un intrus
            return True # Tout est bon
            
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if self.validate(data) == False:
            raise ValueError("Improper numeric data")

        if isinstance(data, list):
            for item in data:
                self.storage.append((self.rank, str(item)))
                self.rank += 1
        else:
            self.storage.append((self.rank, str(data)))
            self.rank += 1


class TextProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, str):
            return True
            
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, str):
                    return False
            return True
            
        return False

    def ingest(self, data: str | list[str]) -> None:
        if self.validate(data) == False:
            raise ValueError("Improper text data")

        if isinstance(data, list):
            for item in data:
                self.storage.append((self.rank, item))
                self.rank += 1
        else:
            self.storage.append((self.rank, data))
            self.rank += 1


class LogProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        # On cree une petite fonction interne juste pour verifier UN dictionnaire
        def is_valid_dict(d: typing.Any) -> bool:
            if not isinstance(d, dict):
                return False
            # On parcourt le dictionnaire
            for key, value in d.items():
                # Si la cle ou la valeur n'est pas du texte, on refuse
                if not isinstance(key, str) or not isinstance(value, str):
                    return False
            return True

        if is_valid_dict(data) == True:
            return True
            
        if isinstance(data, list):
            for item in data:
                if is_valid_dict(item) == False:
                    return False
            return True
            
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if self.validate(data) == False:
            raise ValueError("Improper log data")

        # Petite fonction interne pour formater joliment le log
        def format_log(log: dict[str, str]) -> str:
            if 'log_level' in log and 'log_message' in log:
                return f"{log['log_level']}: {log['log_message']}"
            return str(log)

        if isinstance(data, list):
            for item in data:
                self.storage.append((self.rank, format_log(item)))
                self.rank += 1
        else:
            self.storage.append((self.rank, format_log(data))) # type: ignore
            self.rank += 1


# ==============================================================================
# PARTIE 2 : LE NOUVEAU CODE DE L'EXERCICE 1
# ==============================================================================

class DataStream:
    def __init__(self) -> None:
        # Notre registre : une liste vide pour stocker les processeurs
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        # On ajoute le processeur a notre equipe
        self.processors.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        # On prend chaque colis (item) sur le tapis roulant (stream)
        for item in stream:
            
            # On met un drapeau a Faux pour savoir si quelqu'un a pris le colis
            element_processed = False

            # On interroge chaque processeur de notre equipe
            for processor in self.processors:
                
                # Si le processeur dit "Oui, je peux traiter ca"
                if processor.validate(item) == True:
                    processor.ingest(item)
                    # Le colis est pris ! On met le drapeau a Vrai
                    element_processed = True
                    # On s'arrete de demander aux autres processeurs pour CE colis
                    break 

            # Si on a demande a tout le monde et que le drapeau est toujours Faux
            if element_processed == False:
                print(f"DataStream error - Can't process element in stream: {item}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        
        # Si notre equipe est vide
        if len(self.processors) == 0:
            print("No processor found, no data")
            return

        # On fait le bilan pour chaque processeur de l'equipe
        for processor in self.processors:
            
            # On recupere le nom de sa classe
            class_name = processor.__class__.__name__
            
            # On modifie le nom pour qu'il s'affiche exactement comme dans le sujet
            display_name = ""
            if class_name == "NumericProcessor":
                display_name = "Numeric Processor"
            elif class_name == "TextProcessor":
                display_name = "Text Processor"
            elif class_name == "LogProcessor":
                display_name = "Log Processor"
            else:
                display_name = class_name

            # On recupere le nombre total (le rang) et le nombre restant (taille de la liste)
            total_processed = processor.rank
            remaining = len(processor.storage)

            print(f"{display_name}: total {total_processed} items processed, remaining {remaining} on processor")


# ==============================================================================
# PARTIE 3 : TESTS DU SUJET (Pour vérifier que tout marche)
# ==============================================================================

if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")
    
    print("Initialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()
    
    print("Registering Numeric Processor")
    num_proc = NumericProcessor()
    stream.register_processor(num_proc)
    
    batch = [
        'Hello world', 
        [3.14, -1, 2.71], 
        [{'log_level': 'WARNING', 'log_message': 'Telnet access! Use ssh instead'}, 
         {'log_level': 'INFO', 'log_message': 'User wil is connected'}], 
        42, 
        ['Hi', 'five']
    ]
    
    print(f"Send first batch of data on stream: {batch}")
    stream.process_stream(batch)
    stream.print_processors_stats()
    
    print("Registering other data processors")
    text_proc = TextProcessor()
    log_proc = LogProcessor()
    stream.register_processor(text_proc)
    stream.register_processor(log_proc)
    
    print("Send the same batch again")
    stream.process_stream(batch)
    stream.print_processors_stats()
    
    print("Consume some elements from the data processors: Numeric 3, Text 2, Log 1")
    for i in range(3):
        num_proc.output()
    for i in range(2):
        text_proc.output()
    for i in range(1):
        log_proc.output()
        
    stream.print_processors_stats()
