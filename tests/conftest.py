from typing import Generator

import pytest

from framework.helpers.kafka.consumers.register_events import RegisterEventsSubscriber
from framework.helpers.kafka.consumers.register_events_errors import (
    RegisterEventsErrorsSubscriber,
)
from framework.helpers.rmq.consumers.dm_mail_sending import DmMailSending
from framework.internal.http.account import AccountAPI
from framework.internal.http.mail import MailAPI
from framework.internal.kafka.consumer import Consumer
from framework.internal.kafka.producer import Producer
from framework.internal.rmq.publisher import RmqPublisher


@pytest.fixture(scope="session")
def account() -> AccountAPI:
    return AccountAPI()


@pytest.fixture(scope="session")
def mail() -> MailAPI:
    return MailAPI()


@pytest.fixture(scope="session")
def kafka_producer() -> Generator[Producer, None, None]:
    with Producer() as producer:
        yield producer


@pytest.fixture(scope="session")
def register_events_subscriber() -> RegisterEventsSubscriber:
    return RegisterEventsSubscriber()


@pytest.fixture(scope="session")
def register_events_errors_subscriber() -> RegisterEventsErrorsSubscriber:
    return RegisterEventsErrorsSubscriber()


@pytest.fixture(scope="session", autouse=True)
def kafka_consumer(
    register_events_subscriber: RegisterEventsSubscriber,
    register_events_errors_subscriber: RegisterEventsErrorsSubscriber,
) -> Generator[Consumer, None, None]:
    with Consumer(subscribers=[register_events_subscriber, register_events_errors_subscriber]) as consumer:
        yield consumer


@pytest.fixture(scope="session")
def rmq_publisher() -> Generator[RmqPublisher, None, None]:
    with RmqPublisher() as publisher:
        yield publisher


@pytest.fixture(scope="session", autouse=True)
def rmq_dm_mail_sending_consumer() -> Generator[DmMailSending, None, None]:
    with DmMailSending() as consumer:
        yield consumer
