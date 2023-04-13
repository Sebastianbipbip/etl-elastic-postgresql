from dataclasses import dataclass


@dataclass
class API:
    index = "1c-graylog"
    uid = "6351401251da434ee875fc7a"
    table = "tbapi"

    query = {
        "query": {
            "bool": {
                "filter": [{"term": {"streams": uid}}]
            }
        },
        "size": 200,
        "sort": {"timestamp": {"order": "asc"}}
    }

    mapping = """CREATE TABLE IF NOT EXISTS {table}(
                    id SERIAL PRIMARY KEY NOT NULL,
                    uid UUID NOT NULL UNIQUE,
                    timestamp TIMESTAMP NOT NULL,
                    login CHAR(11),
                    client UUID,
                    operation VARCHAR(80),
                    success BOOLEAN NOT NULL,
                    exception TEXT,
                    request_id UUID,
                    level INTEGER,
                    azp VARCHAR(30)
                )"""
    fields = [field.strip().split(" ")[0] for field in mapping.split("\n")[2:-1]]


@dataclass
class GW:
    index = "graylog"
    uid = "547b29b6d4c6c10b4f1b934d"
    table = "gw"

    query = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"streams": uid}},
                    {"term": {"host": "gw.centrofinans.ru"}}
                ]
            }
        },
        "size": 200,
        "sort": {"timestamp": {"order": "asc"}}
    }

    mapping = """CREATE TABLE IF NOT EXISTS {table}(
                    id SERIAL PRIMARY KEY NOT NULL,
                    uid UUID NOT NULL UNIQUE,
                    timestamp TIMESTAMP NOT NULL,
                    request_id UUID,
                    operation VARCHAR(100) NOT NULL,
                    request TEXT,
                    body_bytes_sent INTEGER,
                    remote_addr_city_name VARCHAR(50),
                    request_time REAL,
                    remote_addr_geolocation VARCHAR(50),
                    remote_addr VARCHAR(25),
                    gl2_accounted_message_size INTEGER,
                    response_status INTEGER,
                    request_uri TEXT,
                    uri VARCHAR(100)
                )"""

    fields = [field.strip().split(" ")[0] for field in mapping.split("\n")[2:-1]]


@dataclass
class Microservices:
    index = "kuber-all"
    uid = "5b06c2307bb9fd00018e4dae"
    table = "microservices"

    query = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"streams": uid}},
                    {"bool":
                        {"should": [
                            {"regexp": {"payment_id": ".+"}},
                            {"regexp": {"service": "phone-verification-.*"}}
                        ]
                        }
                    }  # noqa
                ]
            }
        },
        "size": 200,
        "sort": {"timestamp": {"order": "asc"}}
    }

    mapping = """CREATE TABLE IF NOT EXISTS {table}(
                     id SERIAL PRIMARY KEY NOT NULL,
                     uid UUID NOT NULL UNIQUE,
                     timestamp TIMESTAMP NOT NULL,
                     message TEXT,
                     service VARCHAR(50),
                     phone BIGINT,
                     time_process DOUBLE PRECISION,
                     customer_id UUID,
                     payment_id UUID,
                     card_number VARCHAR(50),
                     commis_amount INTEGER,
                     ecom_msg TEXT,
                     handler_name CHAR(10),
                     payment_amount INTEGER,
                     payment_status VARCHAR(50),
                     status VARCHAR(20),
                     status_redirect TEXT,
                     url TEXT,
                     verification_count INTEGER
                 )"""

    fields = [field.strip().split(" ")[0] for field in mapping.split("\n")[2:-1]]


@dataclass
class TestTable:
    index = GW.index
    uid = GW.uid
    table = "test_gw"

    query = GW.query

    mapping = GW.mapping
    fields = [field.strip().split(" ")[0] for field in mapping.split("\n")[2:-1]]
