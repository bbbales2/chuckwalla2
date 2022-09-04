from dataclasses import dataclass

import boto3
import configparser
import json
import os

CONFIG_NAME = ".chuckwalla2.ini"


@dataclass
class Secrets:
    username: str
    password: str
    host: str
    port: int


def get_secrets() -> Secrets:
    config = configparser.ConfigParser()

    directories = (os.getcwd(), os.path.expanduser("~"))
    filenames = (os.path.join(directory, CONFIG_NAME) for directory in directories)

    config.read(filenames)

    if "mysql" not in config:
        raise Exception("A configuration must be provided for mysql")

    username = config.get("mysql", "username")
    password = config.get("mysql", "password")
    host = config.get("mysql", "host")
    port = config.get("mysql", "port")

    return Secrets(username, password, host, port)


def download_secrets():
    secret_name = "chuckwalla2-database"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    get_secret_value_response = client.get_secret_value(SecretId=secret_name)

    config = configparser.ConfigParser()

    if 'SecretString' in get_secret_value_response:
        secrets = json.loads(get_secret_value_response['SecretString'])
    else:
        raise Exception("Secrets should be encoded as string")

    config["mysql"] = {
        "host": secrets["host"],
        "port": secrets["port"],
        "username": secrets["username"],
        "password": secrets["password"]
    }

    directories = (os.getcwd(), os.path.expanduser("~"))
    for directory in directories:
        path = os.path.join(directory, CONFIG_NAME)
        if os.path.exists(path):
            break

    with open(path, "w") as configfile:
        config.write(configfile)


if __name__ == "__main__":
    download_secrets()
