import datetime
import json
import time

from framework.internal.http.account import AccountAPI
from framework.internal.http.mail import MailAPI

from kafka import KafkaProducer

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
