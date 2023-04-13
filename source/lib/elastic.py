from datetime import datetime, timedelta

import requests


class Elasticsearch:
    def __init__(self, url, stream, index, logger):
        self.logger = logger

        self._elastic_url = url
        self.stream = stream
        self.index = index

    def _request_get(self, method: str, base_url: str = None, **kwargs):
        try:
            if base_url is None:
                base_url = self._elastic_url

            return requests.get(base_url + method, params=kwargs)

        except requests.exceptions.JSONDecodeError:
            self.logger.critical("Ошибка GET запроса", extra=dict(
                params=str(kwargs)
            ))

    def _request_post(self, method: str, base_url: str = None, body: dict = None):
        try:
            if base_url is None:
                base_url = self._elastic_url

            return requests.post(base_url + method, json=body)

        except requests.exceptions.JSONDecodeError:
            self.logger.critical("Ошибка POST запроса", extra=dict(
                params=str(body)
            ))

    def _get_index_by_alias(self, name: str, date: list[datetime]):
        response = self._request_get("/_cat/aliases?v")

        aliases = response.text.split("\n")[1:]

        by_alias = (f"{name}_{i.strftime('%Y.%m.%d')}" for i in date)

        if name == "microservices":
            by_alias = (f"microservice__{i.strftime('%Y.%m.%d')}" for i in date)

        result = []
        for row in aliases:
            text = [i for i in row.split(" ") if i != ""]

            if len(text) != 0:
                if text[0] in by_alias:
                    result.append(text[1])

        return ",".join(result)

    def get_scroll_id(
            self,
            search: dict,
            from_date: datetime,
            size: int = 200,
            scroll: int = 5,
            sort_by: str = "timestamp",
            sort: str = "asc"
    ):
        from_date -= timedelta(hours=3)

        index = self._get_index_by_alias(
            self.index,
            date=[
                from_date,
                from_date + timedelta(days=1)
            ]
        )

        timerange = {
            "range": {
                "timestamp": {
                    "gte": from_date.strftime(
                        "%Y-%m-%d %H:%M:%S.%f"
                    )[:-3]
                }
            }
        }

        search["query"]["bool"]["filter"].append(timerange)
        search["size"] = size
        search["sort"] = {sort_by: sort}

        return self._request_post(f"/{index}/_search?scroll={scroll}m", body=search)

    def scroll(self, id_scroll, scroll=1):
        body = {
            "scroll": f"{scroll}m",
            "scroll_id": str(id_scroll)
        }

        return self._request_post("/_search/scroll", body=body)

    def delete_scroll_id(self, scroll_id):
        body = {
            "scroll_id": scroll_id
        }

        requests.delete(f"{self._elastic_url}/_search/scroll", json=body)
