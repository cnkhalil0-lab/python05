from abc import ABC, abstractmethod
from typing import Any

# ==========================================
# 1. CLASSE ABSTRAITE (Le Moule)
# ==========================================
class DataProcessor(ABC):
    def __init__(self) -> None:
        self.storage: list[tuple[int, str]] = []
        self.rank: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self.storage:
            raise ValueError("Aucune donnée à extraire.")
        return self.storage.pop(0)


# ==========================================
# 2. NUMERIC PROCESSOR
# ==========================================
class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, (int, float)):
                    return False
            return True
        return False

    # Attention ici : on restreint les types (ce qui va fâcher mypy, comme voulu par le sujet)
    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")

        if isinstance(data, list):
            for item in data:
                self.storage.append((self.rank, str(item)))
                self.rank += 1
        else:
            self.storage.append((self.rank, str(data)))
            self.rank += 1


# ==========================================
# 3. TEXT PROCESSOR
# ==========================================
class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, str):
                    return False
            return True
        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")

        if isinstance(data, list):
            for item in data:
                self.storage.append((self.rank, item))
                self.rank += 1
        else:
            self.storage.append((self.rank, data))
            self.rank += 1


# ==========================================
# 4. LOG PROCESSOR
# ==========================================
class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        # Fonction utilitaire pour vérifier un seul dictionnaire
        def is_valid_dict(d: Any) -> bool:
            if not isinstance(d, dict):
                return False
            for key, value in d.items():
                if not isinstance(key, str) or not isinstance(value, str):
                    return False
            return True

        if is_valid_dict(data):
            return True
        if isinstance(data, list):
            for item in data:
                if not is_valid_dict(item):
                    return False
            return True
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")

        def format_log(log: dict[str, str]) -> str:
            # On formate proprement si on trouve les clés spécifiques du sujet
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


# ==========================================
# TESTS (Exécution principale)
# ==========================================
if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===")
    
    # --- Tests Numeric Processor ---
    print("Testing Numeric Processor...")
    num_proc = NumericProcessor()
    print(f"Trying to validate input '42': {num_proc.validate('42')}")
    print(f"Trying to validate input 'Hello': {num_proc.validate('Hello')}")
    
    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        # On force une erreur pour valider le comportement demandé
        num_proc.ingest("foo") # type: ignore
    except Exception as e:
        print(f"Got exception: {e}")
        
    print("Processing data: [1, 2, 3, 4, 5]")
    num_proc.ingest([1, 2, 3, 4, 5])
    print("Extracting 3 values...")
    for _ in range(3):
        rank, val = num_proc.output()
        print(f"Numeric value {rank}: {val}")

    # --- Tests Text Processor ---
    print("\nTesting Text Processor...")
    text_proc = TextProcessor()
    print(f"Trying to validate input '42': {text_proc.validate(42)}")
    print("Processing data: ['Hello', 'Nexus', 'World']")
    text_proc.ingest(['Hello', 'Nexus', 'World'])
    print("Extracting 1 value...")
    rank, val = text_proc.output()
    print(f"Text value {rank}: {val}")

    # --- Tests Log Processor ---
    print("\nTesting Log Processor...")
    log_proc = LogProcessor()
    print(f"Trying to validate input 'Hello': {log_proc.validate('Hello')}")
    
    log_data = [
        {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
        {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}
    ]
    print(f"Processing data: {log_data}")
    log_proc.ingest(log_data)
    print("Extracting 2 values...")
    for _ in range(2):
        rank, val = log_proc.output()
        print(f"Log entry {rank}: {val}")