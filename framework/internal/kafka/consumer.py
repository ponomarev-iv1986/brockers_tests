import json
import threading
import time
from collections import defaultdict
from types import TracebackType

from kafka import KafkaConsumer

from framework.internal.kafka.subscriber import Subscriber
from framework.internal.singleton import Singleton


class Consumer(Singleton):
    _started: bool = False

    def __init__(
        self,
        subscribers: list[Subscriber],
        bootstrap_servers: list[str] = ["185.185.143.231:9092"],
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._subscribers = subscribers
        self._consumer: KafkaConsumer | None = None
        self._running: threading.Event = threading.Event()
        self._ready: threading.Event = threading.Event()
        self._thread: threading.Thread | None = None
        self._watchers: dict[str, list[Subscriber]] = defaultdict(list)

    def _consume(self) -> None:
        self._ready.set()
        print("Consumer started")
        try:
            while self._running.is_set():
                messages = self._consumer.poll(timeout_ms=1000, max_records=10)
                for topic_partition, records in messages.items():
                    topic = topic_partition.topic
                    for record in records:
                        for watcher in self._watchers[topic]:
                            print(f"{topic}: {record}")
                            watcher.handle_messages(record)
                    time.sleep(0.1)

                if not messages:
                    time.sleep(0.1)

        except Exception as err:
            print(f"Error: {err}")

    def register(self):
        if self._subscribers is None:
            raise RuntimeError("Subscribers not initialized")

        if self._started:
            raise RuntimeError("Consumer is already started")

        for subscriber in self._subscribers:
            print(f"Registering subscriber {subscriber.topic}")
            self._watchers[subscriber.topic].append(subscriber)

    def start(self) -> None:
        self._consumer = KafkaConsumer(
            *self._watchers.keys(),
            bootstrap_servers=self._bootstrap_servers,
            auto_offset_reset="latest",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        self._running.set()
        self._ready.clear()
        self._thread = threading.Thread(target=self._consume, daemon=True)
        self._thread.start()

        if not self._ready.wait(timeout=10):
            raise RuntimeError("Consumer is not ready")

        self._started = True

    def stop(self) -> None:
        self._running.clear()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
            if self._thread.is_alive():
                print("Thread is still alive")
        if self._consumer:
            try:
                self._consumer.close(timeout_ms=2000)
                print("Stop consuming")
            except Exception as err:
                print(f"Error while closing consumer: {err}")

        del self._consumer
        self._watchers.clear()
        self._subscribers.clear()
        self._started = False

        print("Consumer stopped")

    def __enter__(self) -> "Consumer":
        self.register()
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.stop()
