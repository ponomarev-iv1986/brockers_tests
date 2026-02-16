import queue
from abc import ABC, abstractmethod
from typing import Any

from kafka.consumer.fetcher import ConsumerRecord


class Subscriber(ABC):

    def __init__(self):
        self._messages: queue.Queue = queue.Queue()

    @property
    @abstractmethod
    def topic(self) -> str: ...

    def handle_messages(self, record: ConsumerRecord) -> None:
        self._messages.put(record)

    def get_message(self, timeout: int = 10) -> Any:
        try:
            return self._messages.get(timeout=timeout)
        except queue.Empty:
            raise AssertionError(f"No messages from topic: {self.topic}, within timeout: {timeout}")

    def find_message(self, **kwargs) -> None:
        for _ in range(10):
            message = self.get_message()
            for key, value in kwargs.items():
                if message.value[key] != value:
                    break
            else:
                return
        else:
            raise AssertionError(f"Message with {kwargs} not found")
