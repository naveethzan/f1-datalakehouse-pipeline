"""
OpenF1Hook - Production-ready API client for OpenF1 data extraction.

This module implements a robust API client for the OpenF1 API with proper
error handling, rate limiting, and retry strategies.
"""

import time
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from airflow.hooks.base import BaseHook


class OpenF1Hook(BaseHook):
    """
    A simplified, production-ready API client for the OpenF1 API.

    This hook provides a clean, reusable interface for fetching data from OpenF1,
    with built-in error handling, rate limiting, and retry strategies.
    It is designed to be a "dumb" client, with orchestration logic handled by Airflow tasks.
    """
    
    conn_id = "openf1_api"
    
    def __init__(
        self,
        rate_limit_seconds: float = 0.2,
        max_retries: int = 3,
        timeout_seconds: int = 30,
        base_url: str = "https://api.openf1.org/v1"
    ):
        """Initializes the OpenF1Hook."""
        super().__init__()
        self.rate_limit_seconds = rate_limit_seconds
        self.max_retries = max_retries
        self.timeout_seconds = timeout_seconds
        self.base_url = base_url.rstrip('/')
        self.last_request_time = 0
        self.logger = logging.getLogger(__name__)
        self.session = self._create_session()
        self.logger.info(
            f"OpenF1Hook initialized - Rate limit: {rate_limit_seconds}s, "
            f"Max retries: {max_retries}, Timeout: {timeout_seconds}s"
        )
    
    def _create_session(self) -> requests.Session:
        """Creates an HTTP session with a robust retry strategy."""
        session = requests.Session()
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update({
            'User-Agent': 'F1-Data-Pipeline/1.0',
            'Accept': 'application/json'
        })
        return session
    
    def _enforce_rate_limit(self) -> None:
        """Ensures that requests do not exceed the defined rate limit."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < self.rate_limit_seconds:
            sleep_time = self.rate_limit_seconds - time_since_last_request
            self.logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        self.last_request_time = time.time()
    
    def _make_api_request(self, endpoint: str, params: Optional[Dict] = None) -> List[Dict]:
        """
        Internal method to make a generic, rate-limited, and retriable API request.

        Args:
            endpoint: The API endpoint to call.
            params: A dictionary of query parameters.

        Returns:
            A list of dictionaries from the JSON response.
        """
        self._enforce_rate_limit()
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        self.logger.debug(f"Making API request to {url} with params: {params or {}}")
        
        try:
            response = self.session.get(url, params=params, timeout=self.timeout_seconds)
            response.raise_for_status()
            json_data = response.json()
            if not isinstance(json_data, list):
                self.logger.warning(f"API endpoint {url} did not return a list. Response: {json_data}")
                return []
            self.logger.debug(f"API request successful - received {len(json_data)} records")
            return json_data
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed for {url}: {e}")
            raise
        except ValueError as e: # Catches JSON decoding errors
            self.logger.error(f"Invalid JSON response from {url}: {e}")
            raise requests.exceptions.RequestException(f"Invalid JSON response from {url}")

    def get_sessions(self, year: int, session_type: Optional[str] = None) -> List[Dict]:
        """
        Public method to discover sessions for a given year.

        Args:
            year: The year to discover sessions for.
            session_type: Optional. The type of session (e.g., 'Race', 'Qualifying').

        Returns:
            A list of session dictionaries.
        """
        self.logger.info(f"Discovering sessions for year {year} (type: {session_type or 'all'})")
        params = {"year": year}
        if session_type:
            params["session_name"] = session_type
        
        return self._make_api_request("sessions", params=params)

    def get_data_for_session(self, session_key: int, endpoint: str) -> List[Dict]:
        """
        Public method to get data for a specific session and endpoint.

        Args:
            session_key: The unique key for the session.
            endpoint: The data endpoint to query (e.g., 'drivers', 'laps', 'pit').

        Returns:
            A list of data records for that session and endpoint.
        """
        self.logger.info(f"Fetching '{endpoint}' data for session_key={session_key}")
        params = {"session_key": session_key}
        return self._make_api_request(endpoint, params=params)
    
    def test_connection(self) -> bool:
        """
        Tests the API connection by making a simple request.
        
        Returns:
            True if the connection is successful, False otherwise.
        """
        try:
            self.logger.info("Testing OpenF1 API connection...")
            sessions = self.get_sessions(year=datetime.now().year - 1) # Get sessions from last year
            if sessions:
                self.logger.info(f"OpenF1 API connection test successful - received {len(sessions)} sessions.")
                return True
            self.logger.warning("OpenF1 API connection test returned no sessions, but was successful.")
            return True
        except Exception as e:
            self.logger.error(f"OpenF1 API connection test failed: {e}")
            return False
    
    def close(self) -> None:
        """Closes the underlying HTTP session."""
        if self.session:
            self.session.close()
            self.logger.debug("HTTP session closed")
