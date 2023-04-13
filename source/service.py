#!/usr/bin/env python3

import os
import time
from argparse import ArgumentParser
import logging
import sys
import traceback
from datetime import datetime, timedelta

import requests.exceptions
from pythonjsonlogger import jsonlogger

from lib.elastic import Elasticsearch
from lib.postgres import Postgres
from lib.mapping import GW, API, Microservices, TestTable

service_name = "pars-graylog"

streams = {
    "1c-graylog": API,
    "graylog": GW,
    "microservices": Microservices,
    "test": TestTable
}


class Service:
    def __init__(self, args, logger):
        self.args = args

        self.logger = logger

        self.stream = streams[args.stream]

        self.elastic = Elasticsearch(
            url=args.elastic_url,
            logger=logger,
            stream=self.stream.uid,
            index=self.stream.index
        )

        self.postgres = Postgres(
            url=self.args.postgres_url,
            logger=logger,
            table=self.stream.table,
            mapping=self.stream.mapping
        )

        self.id_scroll = None

        self.interval_commit = 0

    def start(self):
        self.postgres.create_table()

        while True:
            if self.id_scroll is None:
                response = self._set_id_scroll()

            else:
                response = self.elastic.scroll(id_scroll=self.id_scroll)

            try:
                if response.status_code != 200:
                    self.logger.info(
                        "Некорректный код ответа от ElasticSearch",
                        extra={
                            "status_code": response.status_code
                        }
                    )

                    self.id_scroll = None

                    continue

                hits = response.json()["hits"]["hits"]

                if len(hits) == 0:
                    self.elastic.delete_scroll_id(self.id_scroll)
                    self.id_scroll = None

                    self.logger.info("Sleep one minute")
                    time.sleep(60)

                    continue

                list_of_values = list()

                last_timestamp = ""

                for hit in hits:
                    source = {key.lower(): value for key, value in hit["_source"].items()}

                    source.update(uid=hit['_id'])

                    if self.stream.index == "1c-graylog":
                        if source.get("operation") is None:
                            continue

                    if self.stream.index == "graylog":
                        if "uri" not in source:
                            source.update(uri=source["request_path"].split(" ")[1])

                        source.update(
                            operation=source["uri"].split("/")[-1]
                        )

                    if self.stream.index == "kuber-all":
                        payment_id = source.get("payment_id")

                        if payment_id and len(payment_id) != 36:
                            payment_id = payment_id.split("/")[0]

                            source.update(payment_id=payment_id)

                    timestamp = (
                        datetime.strptime(source.get("timestamp"), "%Y-%m-%d %H:%M:%S.%f") + timedelta(hours=3)
                    )

                    source.update(
                        timestamp=timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")
                    )

                    list_of_values.append(tuple(source.get(field) for field in self.stream.fields))

                    last_timestamp = source.get("timestamp")

                self.postgres.insert_data(keys=self.stream.fields, values=list_of_values)

                self.logger.info("Значения записаны в бд", extra={
                    "size": len(list_of_values),
                    "last_date": last_timestamp,
                    "table": self.stream.table
                })

            except KeyError:
                self.logger.error(
                    "Ошибка получения значений из Elastic",
                    extra={
                        "response": response.json()
                    }
                )
                continue

            except requests.exceptions.JSONDecodeError as er:
                self.logger.error("Некорректный ответ от ElasticSearch",
                                  extra={
                                      "response": response.text,
                                      "status_code": response.status_code,
                                      "headers": response.headers,
                                      "error": er
                                  })
                break

            self.interval_commit += 1

            if self.interval_commit == 10:
                self.postgres.save()

                self.interval_commit = 0

    def stop(self):
        self.postgres.close()
        self.logger.info("Service stopped")

    def _set_id_scroll(self):
        from_date = self.args.date

        if from_date is None:
            from_date = self.postgres.get_last_date()

            self.logger.info(f"Последняя полученная дата из бд - {from_date}")

        while self.id_scroll is None:
            response = self.elastic.get_scroll_id(
                search=self.stream.query,
                from_date=from_date,
                size=1000
            )

            if response.status_code == 200:
                self.id_scroll = response.json()["_scroll_id"]

                return response


def _get_logger(level):
    log = logging.getLogger(service_name)
    log.setLevel(level * 10)

    stream_handler = logging.StreamHandler(sys.stdout)
    log_format = "%(levelname)-8s %(asctime)s %(message)s"
    formatter = jsonlogger.JsonFormatter(
        fmt=log_format, json_ensure_ascii=False
    )

    stream_handler.setFormatter(formatter)
    log.addHandler(stream_handler)

    return log


def _args_parser():
    parser = ArgumentParser()

    parser.add_argument(
        "-s",
        "--stream",
        choices=streams.keys(),
        type=str,
        default=os.environ.get("ELASTIC_STREAM")
    )

    parser.add_argument(
        "--loglevel",
        default=2,
        type=int
    )

    parser.add_argument(
        "--date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d %H:%M:%S"),
        default=None
    )

    parser.add_argument(
        "--elastic_url",
        default=os.environ.get("ELASTIC_URL")
    )

    parser.add_argument(
        "--postgres_url",
        default=os.environ.get("POSTGRES_URL")
    )

    return parser


if __name__ == '__main__':
    args = _args_parser().parse_args()

    logger = _get_logger(args.loglevel)

    service = Service(args, logger)

    try:
        logger.info("Service starting")
        service.start()

    except KeyboardInterrupt:
        logger.warning("Принудительная остановка")

    except Exception:
        logger.critical("Критическая ошибка", extra={
            "error": traceback.format_exc()
        })

    finally:
        service.stop()
