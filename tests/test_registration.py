import datetime
import json
import time

from framework.internal.http.account import AccountAPI
from framework.internal.http.mail import MailAPI
from framework.internal.kafka.producer import Producer


def test_failed_registration(account: AccountAPI, mail: MailAPI) -> None:
    expected_mail = "iponomarev_0001@mail.ru"
    response = account.register_user(
        login="iponomarev_001",
        email=expected_mail,
        password="qwerty123",
    )
    for _ in range(10):
        response = mail.find_message(query=expected_mail)
        if response.json()["total"] > 0:
            raise AssertionError("Email not found")
        time.sleep(1)


def test_success_registration(account: AccountAPI, mail: MailAPI) -> None:
    login = f"iponomarev_{str(datetime.datetime.now().timestamp())[:-4]}"
    email = f"{login}@mail.ru"
    response = account.register_user(
        login=login,
        email=email,
        password="qwerty123",
    )
    for _ in range(10):
        response = mail.find_message(query=login)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")


def test_success_registration_with_kafka_producer(mail: MailAPI, kafka_producer: Producer) -> None:
    login = f"iponomarev_{str(datetime.datetime.now().timestamp())[:-4]}"
    email = f"{login}@mail.ru"
    message = {
        "login": login,
        "email": email,
        "password": "qwerty123",
    }
    kafka_producer.send("register-events", message)
    for _ in range(10):
        response = mail.find_message(query=login)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")


def test_register_events_error_consumer(mail: MailAPI, account: AccountAPI, kafka_producer: Producer) -> None:
    login = f"iponomarev_{str(datetime.datetime.now().timestamp())[:-4]}"
    email = f"{login}@mail.ru"
    message = {
        "input_data": {
            "login": login,
            "email": email,
            "password": "qwerty123",
        },
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
        response = mail.find_message(query=login)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")
    token = json.loads(response.json()["items"][0]["Content"]["Body"])["ConfirmationLinkUrl"].split("/")[-1]
    response = account.activate_user(token)
    assert response.status_code == 200
