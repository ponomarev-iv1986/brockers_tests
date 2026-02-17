import datetime
import json
import time

import pytest

from framework.helpers.kafka.consumers.register_events import RegisterEventsSubscriber
from framework.helpers.kafka.consumers.register_events_errors import (
    RegisterEventsErrorsSubscriber,
)
from framework.internal.http.account import AccountAPI
from framework.internal.http.mail import MailAPI
from framework.internal.kafka.producer import Producer
from framework.internal.rmq.publisher import RmqPublisher


@pytest.fixture
def register_message() -> dict[str, str]:
    login = f"iponomarev_{str(datetime.datetime.now().timestamp())[:-4]}"
    email = f"{login}@mail.ru"
    return {
        "login": login,
        "email": email,
        "password": "qwerty123",
    }


@pytest.fixture
def rmq_message() -> dict[str, str]:
    timestamp = str(datetime.datetime.now().timestamp())[:-4]
    return {
        "address": f"iponomarev_{timestamp}@mail.ru",
        "subject": f"message_{timestamp}",
        "body": f"message_{timestamp}",
    }


def test_failed_registration(account: AccountAPI, mail: MailAPI) -> None:
    expected_mail = "iponomarev_0001@mail.ru"
    account.register_user(
        login="iponomarev_001",
        email=expected_mail,
        password="qwerty123",
    )
    for _ in range(3):
        response = mail.find_message(query=expected_mail)
        if response.json()["total"] > 0:
            raise AssertionError("Email not found")
        time.sleep(1)


def test_failed_registration_with_kafka_producer_consumer(
    register_events_subscriber: RegisterEventsSubscriber,
    register_events_errors_subscriber: RegisterEventsErrorsSubscriber,
    account: AccountAPI,
) -> None:
    login = "iponomarev_001"
    email = f"{login}@mail.ru"
    message = {
        "login": login,
        "email": email,
        "password": "qwerty123",
    }
    account.register_user(**message)
    register_events_subscriber.find_message(login=login, email=email)
    register_events_errors_subscriber.find_message(input_data=message, error_type="validation")


def test_success_registration(
    register_events_subscriber: RegisterEventsSubscriber,
    account: AccountAPI,
    mail: MailAPI,
    register_message: dict[str, str],
) -> None:
    account.register_user(**register_message)
    register_events_subscriber.find_message(login=register_message["login"])
    for _ in range(10):
        response = mail.find_message(query=register_message["login"])
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")


def test_success_registration_with_kafka_producer(
    mail: MailAPI,
    kafka_producer: Producer,
    register_message: dict[str, str],
) -> None:
    kafka_producer.send("register-events", register_message)
    for _ in range(10):
        response = mail.find_message(query=register_message["login"])
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")


def test_register_events_error_consumer(
    mail: MailAPI,
    account: AccountAPI,
    kafka_producer: Producer,
    register_message: dict[str, str],
) -> None:
    message = {
        "input_data": register_message,
        "error_message": {
            "type": "https://tools.ietf.org/html/rfc7231#section-6.5.1",
            "title": "Validation failed",
            "status": 400,
            "traceId": "00-7bfb048a473fd86e323076367015ad7b-e92e2bd7d5f2f442-01",
            "errors": {
                "Login": ["Taken"],
            },
        },
        "error_type": "unknown",
    }
    kafka_producer.send("register-events-errors", message)
    for _ in range(10):
        response = mail.find_message(query=register_message["login"])
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")
    token = json.loads(response.json()["items"][0]["Content"]["Body"])["ConfirmationLinkUrl"].split("/")[-1]
    response = account.activate_user(token)
    assert response.status_code == 200


def test_register_events_errors_consumer_producer(
    register_events_errors_subscriber: RegisterEventsErrorsSubscriber,
    kafka_producer: Producer,
) -> None:
    user_data = {
        "login": "iponomarev_001",
        "email": "iponomarev_001@mail.ru",
        "password": "qwerty123",
    }
    message = {
        "input_data": user_data,
        "error_message": {
            "type": "https://tools.ietf.org/html/rfc7231#section-6.5.1",
            "title": "Validation failed",
            "status": 400,
            "traceId": "00-4f751ad7cbffa1749fbb7f5cecd4a401-4804124409b8975c-01",
            "errors": {"Email": ["Taken"], "Login": ["Taken"]},
        },
        "error_type": "unknown",
    }
    kafka_producer.send("register-events-errors", message)
    register_events_errors_subscriber.find_message(input_data=user_data, error_type="validation")


def test_success_registration_with_kafka_producer_consumer(
    register_events_subscriber: RegisterEventsSubscriber,
    kafka_producer: Producer,
    register_message: dict[str, str],
) -> None:
    kafka_producer.send("register-events", register_message)
    register_events_subscriber.find_message(login=register_message["login"], email=register_message["email"])


def test_rmq(
    rmq_publisher: RmqPublisher,
    mail: MailAPI,
    rmq_message: dict[str, str],
) -> None:
    rmq_publisher.publish("dm.mail.sending", rmq_message)
    for _ in range(10):
        response = mail.find_message(query=rmq_message["body"])
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")
