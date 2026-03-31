from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional
del Optional, List, Union, Dict


class DataProcessor(ABC):
    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, list) and len(data) > 0:
            return all(isinstance(x, (int, float)) for x in data)
        return False

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                return "Error: Invalid numeric data"

            total = sum(data)
            count = len(data)

            if count > 0:
                average = total / count
            else:
                average = 0.0

            res = f"Processed {count} numeric values, sum={total}, avg={average}"
            return super().format_output(res)
        except Exception as e:
            return f"Error: {e}"


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        return isinstance(data, str)

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                return "Error: Invalid text data"

            char_count = len(data)
            word_count = len(data.split())

            res = f"Processed text: {char_count} characters, {word_count} words"
            return super().format_output(res)
        except Exception as e:
            return f"Error: {e}"


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        return isinstance(data, str)

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                return "Error: Invalid log data"

            if ":" in data:
                p = data.split(":", 1)
                header = p[0]
                content = p[1]
            else:
                header = "INFO"
                content = data
            if header == "ERROR":
                tag = "[ALERT]"
            else:
                tag = "[INFO]"

            res = f"{tag} {header} level detected: {content}"
            return super().format_output(res)
        except Exception as e:
            return f"Error: {e}"


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    print("Initializing Numeric Processor...")
    n = NumericProcessor()
    print("Processing data: [1, 2, 3, 4, 5]")
    print("Validation: Numeric data verified")
    print(n.process([1, 2, 3, 4, 5]))
    print()

    print("Initializing Text Processor...")
    print("Processing data: \"Hello Nexus World\"")
    print("Validation: Text data verified")
    t = TextProcessor()
    print(t.process("Hello Nexus World"))
    print()

    print("Initializing Log Processor...")
    lo = LogProcessor()
    print("Processing data: \"ERROR: Connection timeout\"")
    print("Validation: Log entry verified")
    print(lo.process("ERROR: Connection timeout"))
    print()

    print("=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")
    processors = [n, t, lo]
    data_to_process = [[1, 2, 3], "Hello World", "INFO: System ready"]
    for i in range(len(processors)):
        result = processors[i].process(data_to_process[i])
        clean_res = result[8:]
        print(f"Result {i+1}: {clean_res}")

    print("\nFoundation systems online. Nexus ready for advanced streams.")
