import requests
from retrying import retry


class RateLimitedException(Exception):
    """Raised when status code is 429"""

    def __init__(self, message="Rate limited exception. Try again later."):
        self.message = message
        super().__init__(self.message)


def retry_on_exceptions(exception):
    return isinstance(exception, RateLimitedException)


class RentFaster:
    BASE_URL = "https://www.rentfaster.ca/api"

    def __init__(self, city_id: str):
        self.city_id = city_id

    @retry(
        retry_on_exception=retry_on_exceptions,
        wait_random_min=1000,
        wait_random_max=10000,
        wrap_exception=False,
    )
    def get_city_features(self):
        resp = requests.get(
            f"{self.BASE_URL}/fields.json", params={"city_id": self.city_id}
        )
        if resp.status_code == 200:
            return resp.json()

    @retry(
        retry_on_exception=retry_on_exceptions,
        wait_random_min=1000,
        wait_random_max=10000,
        wrap_exception=False,
    )
    def get_city_rentals(self, unit_type: str, neighborhood: str):
        """
        Some cities have empty array for neighborhood
        """
        if neighborhood == "":
            neighborhood = None
        try:
            resp = requests.post(
                f"{self.BASE_URL}/map.json",
                params={
                    "city_id": self.city_id,
                    "neighborhood": [neighborhood],
                    "type": unit_type,
                },
            )
            if resp.status_code == 403:
                raise RateLimitedException

        except requests.exceptions.ConnectionError:
            raise RateLimitedException

        resp = resp.json()
        if len(resp["listings"]) > 500:
            raise Exception(
                f"The neighborhood {neighborhood} in city {self.city_id} has more "
                f"than 500 rental properties, please refine the filter."
            )
        return resp["listings"]

    @staticmethod
    @retry(
        retry_on_exception=retry_on_exceptions,
        wait_random_min=1000,
        wait_random_max=10000,
        wrap_exception=False,
    )
    def get_scores(latitude: float, longitude: float):
        """
        Gets scores / listing

        Args:
                ref_id (str): ref_id of listing
                lat (float): latitude of listing
                long (float): longitude of listing
        """
        url = "https://widget-api.proximitii.com//wapi/"
        params = {
            "site": "www.rentfaster.ca",
            "public_key": "e2e7a44ef5caa36b53878335bb02288c",  # May change?
            "lat": latitude,
            "long": longitude,
        }
        resp = requests.get(url, params=params)
        if resp.status_code != 200:
            raise RateLimitedException()
        return resp.json()
