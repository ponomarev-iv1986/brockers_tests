import pytest

from framework.internal.http.account import AccountAPI
from framework.internal.http.mail import MailAPI
from framework.internal.kafka.producer import Producer


@pytest.fixture(scope="session")
def account() -> AccountAPI:
    return AccountAPI()


@pytest.fixture(scope="session")
def mail() -> MailAPI:
    return MailAPI()


@pytest.fixture(scope="session")
def kafka_producer() -> Producer:
    with Producer() as producer:
        yield producer
