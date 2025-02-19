from dataclasses import dataclass
from enum import Enum
import os
import dotenv
from dotenv import load_dotenv


class ConnectionType(Enum):
    NewSkies = "NEWSKIES"
    Infare = "INFARE"


class SqlServerConnection:
    user: str
    pwd: str
    uri: str
    port: str
    database: str

    def __init__(self, connection_type: ConnectionType):
        load_dotenv(".env")
        self.user = os.environ.get(f"{connection_type.value}_USER")
        self.pwd = os.environ.get(f"{connection_type.value}_PWD")
        self.uri = os.environ.get(f"{connection_type.value}_URI")
        self.port = os.environ.get(f"{connection_type.value}_PORT")
        self.database = os.environ.get(f"{connection_type.value}_DATABASE")
