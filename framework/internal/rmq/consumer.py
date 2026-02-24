import json
import queue
import threading
import time
from json import JSONDecodeError
from types import TracebackType
from typing import Any

import pika
from pika.adapters.blocking_connection import BlockingChannel

from framework.internal.singleton import Singleton


class RmqConsumer(Singleton):
    _started: bool = False

    def __init__(
        self,
        url: str = "amqp://guest:guest@185.185.143.231:5672",
    ) -> None:
        self._url = url
        self._connection: pika.BlockingConnection = pika.BlockingConnection(pika.URLParameters(self._url))
        self._channel: BlockingChannel = self._connection.channel()
        self._running: threading.Event = threading.Event()
        self._ready: threading.Event = threading.Event()
        self._thread: threading.Thread | None = None
        self._messages: queue.Queue = queue.Queue()
        self._queue_name: str = ""

    @property
    def exchange(self):
        raise NotImplementedError("Set exchange")

    @property
    def routing_key(self):
        raise NotImplementedError("Set routing_key")

    def _consume(self) -> None:
        self._ready.set()
        print("Consumer rmq started")

        def on_message_callback(ch, method, properties, body):
            try:
                body_str = body.decode("utf-8")
                try:
                    data = json.loads(body_str)
                except JSONDecodeError:
                    data = body_str
                self._messages.put(data)
                print("Received message", data)
            except Exception as err:
                print(f"Error while processing message rmq: {err}")

        self._channel.basic_consume(self._queue_name, on_message_callback, auto_ack=True)

        try:
            while self._running.is_set():
                self._connection.process_data_events(time_limit=0.1)
                time.sleep(0.1)
        except Exception as err:
            print(f"Error: {err}")

    def _start(self) -> None:
        result = self._channel.queue_declare(
            queue="",
            exclusive=True,
            auto_delete=True,
            durable=False,
        )
        self._queue_name = result.method.queue
        print(f"Declare queue with name {self._queue_name}")

        self._channel.queue_bind(
            queue=self._queue_name,
            exchange=self.exchange,
            routing_key=self.routing_key,
        )

        self._running.set()
        self._ready.clear()
        self._thread = threading.Thread(target=self._consume, daemon=True)
        self._thread.start()

        if not self._ready.wait(timeout=10):
            raise RuntimeError("Consumer is not ready")

        self._started = True

    def _stop(self) -> None:
        self._running.clear()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
            if self._thread.is_alive():
                print("Thread is still alive")

        if self._channel:
            try:
                self._channel.close()
                print("Close channel")
            except Exception as err:
                print(f"Error while closing consumer: {err}")

        if self._connection:
            try:
                self._connection.close()
                print("Close connection")
            except Exception as err:
                print(f"Error while closing consumer: {err}")

        self._started = False

        print("Consumer stopped")

    def get_message(self, timeout: int = 10) -> Any:
        try:
            return self._messages.get(timeout=timeout)
        except queue.Empty:
            raise AssertionError(f"No messages from topic: {self._queue_name}, within timeout: {timeout}")

    def __enter__(self) -> "RmqConsumer":
        self._start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._stop()
