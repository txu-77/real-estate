import logging
import time
from typing import Dict, List

import requests
from retrying import retry

from .HonestDoor_graphql_config import (
    CREALISTINGFULL_QUERY,
    FILTERMAPDRAWER_QUERY,
    FILTERMAPDRAWER_VARIABLES,
    PROPERTYFULL_QUERY,
)


class RateLimitedException(Exception):
    """Raised when status code is 429"""

    def __init__(self, message="Rate limited exception. Try again later."):
        self.message = message
        super().__init__(self.message)


def retry_on_exceptions(exception):
    return isinstance(exception, RateLimitedException)


class HonestDoor:
    BASE_URL = "https://api.honestdoor.com/api/v1"
    FILTERMAPDRAWER_QUERY = FILTERMAPDRAWER_QUERY
    FILTERMAPDRAWER_VARIABLES = FILTERMAPDRAWER_VARIABLES
    FILTERMAPDRAWER_LIMIT = 250

    CREALISTINGFULL_QUERY = CREALISTINGFULL_QUERY

    PROPERTYFULL_QUERY = PROPERTYFULL_QUERY

    def __init__(self, city: str, province: str):
        self.city = city
        self.province = province
        self.FILTERMAPDRAWER_VARIABLES["creaListingsInput"]["city"] = self.city
        self.FILTERMAPDRAWER_VARIABLES["creaListingsInput"]["province"] = self.province

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        }
        self.session = requests.Session()

    @retry(
        retry_on_exception=retry_on_exceptions,
        wait_random_min=100000,
        wait_random_max=200000,
        wrap_exception=False,
    )
    def get_all_properties(self) -> List[Dict]:
        """
        Scrapes city for all available properties
        """
        properties = []
        self.FILTERMAPDRAWER_VARIABLES["creaListingsInput"]["page"] = 0
        self.FILTERMAPDRAWER_VARIABLES["creaListingsInput"][
            "size"
        ] = self.FILTERMAPDRAWER_LIMIT
        resp = self.session.post(
            self.BASE_URL,
            headers=self.headers,
            json={
                "query": self.FILTERMAPDRAWER_QUERY,
                "variables": self.FILTERMAPDRAWER_VARIABLES,
            },
        )
        if resp.status_code == 429:
            time.sleep(10)
            raise RateLimitedException

        resp = resp.json()
        properties.extend(resp["data"]["filterMap"]["creaListings"]["results"])

        total_results = resp["data"]["filterMap"]["creaListings"]["count"]
        pages = total_results // self.FILTERMAPDRAWER_LIMIT
        for page in range(1, pages + 1):
            logging.info(f"Getting data from page {page}/{pages}...")
            self.FILTERMAPDRAWER_VARIABLES["creaListingsInput"]["page"] = page
            resp = self.session.post(
                self.BASE_URL,
                headers=self.headers,
                json={
                    "query": self.FILTERMAPDRAWER_QUERY,
                    "variables": self.FILTERMAPDRAWER_VARIABLES,
                },
            )
            if resp.status_code == 429:
                time.sleep(10)
                raise RateLimitedException
            resp = resp.json()
            properties.extend(resp["data"]["filterMap"]["creaListings"]["results"])

        return properties

    @retry(
        retry_on_exception=retry_on_exceptions,
        wait_random_min=100000,
        wait_random_max=200000,
        wrap_exception=False,
    )
    def get_property_id(self, id_: str) -> Dict:
        """
        Gets property_id for each scraped property
        """
        resp = self.session.post(
            self.BASE_URL,
            headers=self.headers,
            json={"query": CREALISTINGFULL_QUERY, "variables": {"id": id_}},
        )
        if resp.status_code == 429:
            time.sleep(10)
            raise RateLimitedException

        try:
            return resp.json()["data"]["creaListing"]["property"]
        except requests.JSONDecodeError:
            raise RateLimitedException
        except (TypeError, KeyError):
            return None

    @retry(
        retry_on_exception=retry_on_exceptions,
        wait_random_min=100000,
        wait_random_max=200000,
        wrap_exception=False,
    )
    def get_property(self, property_dict: Dict) -> Dict:
        """
        Gets in-depth property information including valuation

        """
        resp = self.session.post(
            self.BASE_URL,
            headers=self.headers,
            json={
                "query": PROPERTYFULL_QUERY,
                "variables": {"id": property_dict["id"]},
            },
        )
        if resp.status_code == 429:
            time.sleep(10)
            raise RateLimitedException
        return resp.json()["data"]["property"]
