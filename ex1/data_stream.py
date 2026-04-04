from abc import ABC, abstractmethod
import typing


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
        return self.storage.pop(0)


class NumericProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, int) or isinstance(data, float):
            return True

        if isinstance(data, list):
            for item in data:
                if not (isinstance(item, int) or isinstance(item, float)):
                    return False
            return True
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if self.validate(data) is False:
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
        if self.validate(data) is False:
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
        def is_valid_dict(d: typing.Any) -> bool:
            if not isinstance(d, dict):
                return False
            for key, value in d.items():
                if not isinstance(key, str) or not isinstance(value, str):
                    return False
            return True

        if is_valid_dict(data) is True:
            return True

        if isinstance(data, list):
            for item in data:
                if is_valid_dict(item) is False:
                    return False
            return True
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if self.validate(data) is False:
            raise ValueError("Improper log data")

        def format_log(log: dict[str, str]) -> str:
            if 'log_level' in log and 'log_message' in log:
                return f"{log['log_level']}: {log['log_message']}"
            return str(log)

        if isinstance(data, list):
            for item in data:
                self.storage.append((self.rank, format_log(item)))
                self.rank += 1
        else:
            self.storage.append((self.rank, format_log(data)))
            self.rank += 1


class DataStream:
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        for item in stream:

            element_processed = False

            for processor in self.processors:

                if processor.validate(item) is True:
                    processor.ingest(item)
                    element_processed = True
                    break

            if element_processed is False:
                print(
                    f"DataStream error - Can't process element in stream: "
                    f"{item}"
                )

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")

        if len(self.processors) == 0:
            print("No processor found, no data")
            return

        for processor in self.processors:

            class_name = processor.__class__.__name__

            total_processed = processor.rank
            remaining = len(processor.storage)

            print(
                f"{class_name}: total {total_processed} items processed, "
                f"remaining {remaining} on processor"
                )


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
        [
            {
                'log_level': 'WARNING',
                'log_message': 'Telnet access! Use ssh instead'
            },
            {
                'log_level': 'INFO',
                'log_message': 'User wil is connected'
            }
        ],
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

    print(
        "Consume some elements from the data processors: "
        "Numeric 3, Text 2, Log 1"
    )
    for i in range(3):
        num_proc.output()
    for i in range(2):
        text_proc.output()
    for i in range(1):
        log_proc.output()

    stream.print_processors_stats()
