import logging
import time
from typing import Dict, List

import requests
from retrying import retry


class RateLimitedException(Exception):
    """Raised when status code is 429"""

    def __init__(self, message="Rate limited exception. Try again later."):
        self.message = message
        super().__init__(self.message)


def retry_on_exceptions(exception):
    return isinstance(exception, RateLimitedException)


class Zumper:
    SLEEP = 10

    def __init__(self, city: str):
        self.city = city
        self.session = requests.Session()

        headers = self._get_headers()
        self.headers = {
            "X-Csrftoken": headers["csrf"],
            "X-Zumper-Xz-Token": headers["xz_token"],
        }

    def _get_headers(self):
        resp = self.session.get("https://www.zumper.com/api/t/1/bundle")
        if resp.status_code == 200:
            return resp.json()

    @retry(
        retry_on_exception=retry_on_exceptions,
        wait_random_min=100000,
        wait_random_max=200000,
        wrap_exception=False,
    )
    def get_listables(self) -> List:
        listables = []
        data = {
            "url": self.city,
            "longTerm": True,
            "limit": 100,
            "offset": 0,
        }
        resp = self.session.post(
            "https://www.zumper.com/api/t/1/pages/listables",
            headers=self.headers,
            data=data,
        )
        if resp.status_code == 429:
            raise RateLimitedException

        if resp.status_code not in [200, 429]:
            raise Exception(
                f"Unexpected status code {resp.status_code}, implement retry."
            )

        resp = resp.json()
        matching = resp["matching"]
        listables.extend(resp["listables"])

        # Determine number of loops to make
        pages = matching // 100 + 1
        for i in range(1, pages):
            logging.info(f"Getting listables from page {i}...")
            data["offset"] = i * 100
            resp = self.session.post(
                "https://www.zumper.com/api/t/1/pages/listables",
                headers=self.headers,
                data=data,
            )
            if resp.status_code == 429:
                raise RateLimitedException

            if resp.status_code not in [200, 429]:
                raise Exception(
                    f"Unexpected status code {resp.status_code}, implement retry."
                )

            resp = resp.json()
            listables.extend(resp["listables"])

        return listables

    @retry(
        retry_on_exception=retry_on_exceptions,
        wait_random_min=100000,
        wait_random_max=200000,
        wrap_exception=False,
    )
    def get_location_scores(self, group_id: int, lat: float, lng: float) -> Dict:
        resp = self.session.post(
            "https://www.zumper.com/api/x/1/location_scores",
            headers=self.headers,
            data={
                "group_id": group_id,
                "lat": lat,
                "lng": lng,
            },
        )
        if resp.status_code == 429:
            logging.info(f"RateLimitedException, trying again in {self.SLEEP}s...")
            time.sleep(self.SLEEP)
            raise RateLimitedException

        if resp.status_code not in [200, 429]:
            raise Exception(
                f"Unexpected status code {resp.status_code}, implement retry."
            )

        return resp.json()
