import logging
import re
from typing import Dict, List, Optional, Union

import requests
from bs4 import BeautifulSoup
from retrying import retry


class RateLimitedException(Exception):
    """Raised when status code is 429"""

    def __init__(self, message="Rate limited exception. Try again later."):
        self.message = message
        super().__init__(self.message)


def retry_on_exceptions(exception):
    return isinstance(exception, RateLimitedException)


class RoyalLePage:
    INIT_URL = "https://www.royallepage.ca/en/search/homes/"
    API_URL = "https://www.royallepage.ca/en/search/get-map-data-esri/"
    DETAILS_URL = "https://www.royallepage.ca/en"

    def __init__(self, search_str: str):
        # Set data for requests
        self.city, self.province, _ = search_str.split(", ")
        self.data = {
            "search_str": search_str,
            "city_name": self.city,
            "address": self.city,
            "prov_code": self.province,
            "address_type": "city",
        }

        self.session = requests.Session()

        page_data = self._get_page_data()
        self.session.cookies.set("csrfmiddlewaretoken", page_data["csrf_token"])

        self.property_types = page_data["property_types"]
        price_ranges = page_data["price_ranges"]
        self.price_ranges = [
            {"min": price_ranges[i], "max": price_ranges[i + 1]}
            for i in range(len(price_ranges) - 1)
        ]

        # Set headers
        self.headers = {
            "Referer": self.INIT_URL,
            "X-Csrftoken": page_data["csrf_token"],
        }

    def _get_page_data(self):
        """
        Scrapes page to get csrf_token, price_ranges, and property_types
        """
        resp = self.session.get(self.INIT_URL)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html")

            csrf_token = soup.find("input", {"name": "csrfmiddlewaretoken"})
            csrf_token = csrf_token["value"]

            price_ranges = soup.find("select", {"name": "max_price"})
            price_ranges = price_ranges.text.split("\n$")[1:]
            price_ranges = [p.replace(",", "").strip() for p in price_ranges]

            property_types = {}
            types = soup.find("ul", {"id": "property-type"})
            for p in types.find_all("li"):
                property_types[p.text.strip()] = int(p["id"])

            return {
                "csrf_token": csrf_token,
                "price_ranges": price_ranges,
                "property_types": property_types,
            }

    @retry(
        retry_on_exception=retry_on_exceptions,
        wait_random_min=100000,
        wait_random_max=200000,
        wrap_exception=False,
    )
    def get_listings(
        self,
        property_type: int,
        min_price: int,
        max_price: Union[int, str],
        transactionType: Optional[str] = "SALE",
    ) -> List[Dict]:
        """
        Gets all listings in the specified search_str meeting property_type, min_price, max_price
        criteria
        """
        # Append additional requests to data. Can only limit by the property_type,
        # value of property, and transactionType
        logging.info(
            f"Getting listings for {property_type}, {min_price} to {max_price}..."
        )
        self.data.update(
            {
                "property_type": property_type,
                "min_price": min_price,
                "max_price": max_price,
                "transactionType": transactionType,
            }
        )
        resp = self.session.post(self.API_URL, headers=self.headers, data=self.data)
        if resp.status_code != 200:
            raise RateLimitedException()

        resp = resp.json()
        if resp["count"] != len(resp["list"]):
            logging.warning(
                f"{self.city}, {property_type} has {resp['count']} properties "
                f"between {min_price} and {max_price}. Not all results returned."
            )
        logging.info(f"Found {len(resp['list'])} listings.")
        return resp["list"]

    @retry(
        retry_on_exception=retry_on_exceptions,
        wait_random_min=100000,
        wait_random_max=200000,
        wrap_exception=False,
    )
    def get_listing_details(self, detailsPath: str) -> Dict[str, str]:
        """
        Returns listing details not found in get_listings() for a single listing
        """
        details = {}
        resp = self.session.get(f"{self.DETAILS_URL}/{detailsPath}")
        if resp.status_code != 200:
            logging.info(resp.status_code)
            raise RateLimitedException

        soup = BeautifulSoup(resp.text, "html")
        ul = soup.find_all("ul", {"class": "property-features-list"})
        for entry in ul:
            for e in entry.find_all("li"):
                label = (
                    e.find("span", {"class": "label"})
                    .text.lower()
                    .replace(
                        " ",
                        "_",
                    )
                    .replace(":", "")
                    .replace(".", "")
                    .replace("-", "_")
                    .replace("(", "")
                    .replace(")", "")
                )
                if label.startswith("\n"):
                    continue
                value = e.find("span", {"class": "value"}).text
                details[label] = value

        return details

    @retry(
        retry_on_exception=retry_on_exceptions,
        wait_random_min=100000,
        wait_random_max=200000,
        wrap_exception=False,
    )
    def get_listing_lifestyle_scores(
        self, detailsPath: str, lat: float, lng: float
    ) -> Dict:
        """
        Gets lifestyle scores for a single listing
        """
        # Get Lifestyle scores using API
        resp = self.session.get(f"{self.DETAILS_URL}/{detailsPath}")
        if resp.status_code != 200:
            raise RateLimitedException

        try:
            token = re.findall("token=(.*)\&callback=lifestyleResolve", resp.text)[0]
        except IndexError:
            logging.info(f"Listing @ {detailsPath} does not have any lifestyle scores.")
            return None

        lifestyle_api_endpoint = "https://api.locallogic.co/v1/scores"
        lifestyle_api_params = {
            "token": token,
            "lat": lat,
            "lng": lng,
            "locale": "en",
            "radius": 1000,
        }
        lifestyle_resp = self.session.get(
            lifestyle_api_endpoint, params=lifestyle_api_params
        )
        if lifestyle_resp.status_code == 404:
            logging.info(f"No score available for location {detailsPath}")
            return None

        if lifestyle_resp.status_code not in [200, 404]:
            logging.info(f"Got {lifestyle_resp.status_code} for {detailsPath}")
            raise Exception

        lifestyle_resp = lifestyle_resp.json()
        return lifestyle_resp

    @retry(
        retry_on_exception=retry_on_exceptions,
        wait_random_min=100000,
        wait_random_max=200000,
        wrap_exception=False,
    )
    def get_listing_demographic_information(self, detailsPath: str) -> Dict:
        """
        Gets demographic information for a single listing
        """
        resp = self.session.get(f"{self.DETAILS_URL}/{detailsPath}")
        if resp.status_code != 200:
            logging.info(resp.status_code)
            raise RateLimitedException
        try:
            soup = BeautifulSoup(resp.text, "html")
            demographics = soup.find(
                "section", {"class": "property-demographics-box expandable-box"}
            )
            demographic_details = demographics.find_all(
                "div",
                {
                    "class": re.compile(
                        "card card--stacked card--no-border demostats-item.*"
                    )
                },
            )
        except AttributeError:
            logging.info(
                f"Listing @ {detailsPath} does not have any demographic information"
            )
            return None

        demographic_information = {}
        for d in demographic_details:
            try:
                category = (
                    d.find("p")
                    .text.strip()
                    .lower()
                    .replace(" ", "_")
                    .replace("/", "_")
                    .replace("-", "_")
                    .replace("(", "")
                    .replace(")", "")
                )
                value = d.find("span").text.strip()
                demographic_information[category] = value
            except AttributeError:
                continue

        return demographic_information
