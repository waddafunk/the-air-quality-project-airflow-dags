import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum
from centralized_rate_limiter import RateLimitedSession
import os
from enum import Enum

class TimeAggregation(Enum):
    HOURLY = "hourly"
    DAILY = "daily"
    MONTHLY = "monthly"
    YEARLY = "yearly"


@dataclass
class SensorData:
    sensor_id: str
    location_id: str
    parameter: str
    measurements: pd.DataFrame


class OpenAQv3Collector:
    def __init__(self):
        self.base_url = "https://api.openaq.org/v3"
        self.logger = self.setup_logging()
        self.session = RateLimitedSession(
            requests_per_second=1,
            additional_limits=[
                (2000, 3600),  # 2000 requests per hour
            ],
        )

    def setup_logging(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def _make_request(
        self, endpoint: str, params: Optional[Dict] = None, limit: int = 100, reset_index=True
    ) -> Dict:
        """
        Make HTTP request to OpenAQ API with error handling
        """
        data = []
        page = 1
        if params is None:
            params = {}

        try:
            url = f"{self.base_url}/{endpoint}"
            while page < limit:
                params["page"] = page
                response = self.session.get(
                    url, params=params, headers={"X-API-Key": os.environ["OPENAQ_API_KEY"]}
                )
                response.raise_for_status()
                page_data = response.json()

                if not page_data["results"]:
                    break

                page += 1
                data.append(pd.json_normalize(page_data["results"]))

            result = pd.concat(data)
            if reset_index:
                result = result.reset_index(drop=True)
                
            return result
        
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed: {str(e)}")
            raise

    def get_all_countries(self) -> pd.DataFrame:
        """
        Get all available countries
        """
        return self._make_request("countries")

    def get_locations(
        self, countries_id: Optional[List[int]] = None, limit: int = 100
    ) -> pd.DataFrame:
        """
        Get all available locations, optionally filtered by country
        """
        params = {"countries_id": countries_id} if countries_id else {}
        return self._make_request("locations", params, limit)

    def get_sensors_by_location(
        self, location_id: str, limit: int = 100
    ) -> pd.DataFrame:
        """
        Get all sensors for a specific location
        """
        return self._make_request(f"locations/{location_id}/sensors", {}, limit)

    def get_measurements(
        self,
        sensor_id: str,
        aggregation: TimeAggregation = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Get measurements for a specific sensor with optional time aggregation
        """
        endpoint = f"sensors/{sensor_id}/measurements"

        if aggregation:
            endpoint = f"sensors/{sensor_id}/{aggregation.value}"

        params = {
            "date_from": date_from,
            "date_to": date_to,
            "limit": 1000,  # adjust based on needs
        }

        return self._make_request(endpoint, params)

    def get_latest_measurements(self, location_id: str) -> pd.DataFrame:
        """
        Get latest measurements for a location
        """
        return self._make_request(f"locations/{location_id}/latest")

    def collect_location_data(
        self,
        location_id: str,
        aggregation: TimeAggregation = None,
        date_from=(datetime.now() - timedelta(days=2)).isoformat(),
        date_to=(datetime.now() - timedelta(days=1)),
    ) -> List[SensorData]:
        """
        Collect data for all sensors in a location
        """
        sensor_data_list = []

        # Get all sensors for the location
        sensors = self.get_sensors_by_location(location_id)

        for _, sensor in sensors.iterrows():
            try:
                measurements = self.get_measurements(
                    sensor_id=sensor["id"],
                    aggregation=aggregation,
                    date_from=date_from,
                    date_to=date_to,
                )

                sensor_data = SensorData(
                    sensor_id=sensor["id"],
                    location_id=location_id,
                    parameter=sensor["parameter"],
                    measurements=measurements,
                )

                sensor_data_list.append(sensor_data)

            except Exception as e:
                self.logger.error(
                    f"Error collecting data for sensor {sensor['id']}: {str(e)}"
                )
                continue

        return sensor_data_list

    def validate_measurements(self, df: pd.DataFrame) -> bool:
        """
        Validate collected measurements data
        """
        try:
            # Check for required columns
            required_columns = ["value", "parameter", "timestamp"]
            missing_columns = [col for col in required_columns if col not in df.columns]

            if missing_columns:
                self.logger.warning(f"Missing columns: {missing_columns}")
                return False

            # Check for data validity
            if df["value"].isnull().all():
                self.logger.warning("No valid measurements found")
                return False

            return True

        except Exception as e:
            self.logger.error(f"Validation error: {str(e)}")
            return False