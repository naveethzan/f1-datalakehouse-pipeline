"""
OpenF1 API Client for F1 Data Extraction

Simplified API client for the OpenF1 API, designed for AWS MWAA compatibility.
No Airflow dependencies - pure Python with requests and robust error handling.
"""

import time
import logging
from typing import Dict, List, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.dags import DAGConfig


class OpenF1Client:
    """
    Simplified OpenF1 API client for AWS MWAA.
    
    Provides clean interface for fetching F1 data with built-in rate limiting,
    retry strategies, and error handling. Uses DAG configuration for settings.
    """
    
    def __init__(self):
        """
        Initialize the OpenF1 API client using DAG configuration.
        """
        # Use values from DAG config
        self.base_url = DAGConfig.API_BASE_URL
        self.rate_limit_seconds = DAGConfig.API_RATE_LIMIT_SECONDS
        self.max_retries = DAGConfig.API_MAX_RETRIES
        self.timeout_seconds = DAGConfig.API_TIMEOUT_SECONDS
        self.last_request_time = 0
        
        # Set up logging
        self.logger = logging.getLogger(__name__)
        
        # Create session with retry strategy
        self.session = self._create_session()
        
        self.logger.info(
            f"OpenF1Client initialized - Rate limit: {self.rate_limit_seconds}s, "
            f"Max retries: {self.max_retries}, Timeout: {self.timeout_seconds}s"
        )
    
    def _create_session(self) -> requests.Session:
        """
        Create HTTP session with robust retry strategy.
        
        Returns:
            Configured requests Session
        """
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=1,  # 1s, 2s, 4s delays
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        # Add retry adapter
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        # Set headers
        session.headers.update({
            'User-Agent': 'F1-MWAA-Pipeline/1.0',
            'Accept': 'application/json'
        })
        
        return session
    
    def _enforce_rate_limit(self) -> None:
        """
        Enforce rate limiting between API requests.
        """
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.rate_limit_seconds:
            sleep_time = self.rate_limit_seconds - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> List[Dict]:
        """
        Make a rate-limited API request with error handling.
        
        Args:
            endpoint: API endpoint (e.g., 'sessions', 'drivers')
            params: Query parameters
            
        Returns:
            List of records from API response
            
        Raises:
            requests.RequestException: On API errors
            ValueError: On invalid JSON response
        """
        # Enforce rate limiting
        self._enforce_rate_limit()
        
        # Build URL
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        
        try:
            # Make request
            response = self.session.get(
                url, 
                params=params or {}, 
                timeout=self.timeout_seconds
            )
            
            # Check for HTTP errors
            response.raise_for_status()
            
            # Parse JSON
            data = response.json()
            
            # Validate response format
            if not isinstance(data, list):
                self.logger.warning(f"API returned non-list response: {type(data)}")
                return []
            
            return data
            
        except requests.exceptions.Timeout:
            self.logger.error(f"Request timeout after {self.timeout_seconds}s: {url}")
            raise
            
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP error for {url}: {e}")
            raise
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed for {url}: {e}")
            raise
            
        except ValueError as e:
            self.logger.error(f"Invalid JSON response from {url}: {e}")
            raise requests.exceptions.RequestException(f"Invalid JSON response: {e}")
    
    def get_sessions(self, year: int, session_type: Optional[str] = None) -> List[Dict]:
        """
        Get sessions for a given year and optional session type.
        
        Args:
            year: Year to get sessions for (e.g., 2025)
            session_type: Optional session type filter ('Qualifying', 'Race')
            
        Returns:
            List of session dictionaries
        """
        self.logger.info(f"Getting sessions for year={year}, type={session_type or 'all'}")
        
        # Build parameters
        params = {"year": year}
        if session_type:
            params["session_name"] = session_type
        
        try:
            sessions = self._make_request("sessions", params)
            self.logger.info(f"Found {len(sessions)} sessions")
            return sessions
            
        except Exception as e:
            self.logger.error(f"Failed to get sessions: {e}")
            raise
    
    def get_data_for_session(self, session_key: int, endpoint: str) -> List[Dict]:
        """
        Get data for a specific session and endpoint.
        
        Args:
            session_key: Session key from sessions API
            endpoint: Data endpoint ('drivers', 'laps', 'pit', 'session_result')
            
        Returns:
            List of data records for the session/endpoint
        """
        self.logger.info(f"Getting {endpoint} data for session_key={session_key}")
        
        # Build parameters
        params = {"session_key": session_key}
        
        try:
            data = self._make_request(endpoint, params)
            self.logger.info(f"Retrieved {len(data)} {endpoint} records")
            return data
            
        except Exception as e:
            self.logger.error(f"Failed to get {endpoint} data for session {session_key}: {e}")
            raise
    
    def test_connection(self) -> bool:
        """
        Test the API connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.logger.info("Testing OpenF1 API connection...")
            
            # Validate connection with configured year
            sessions = self.get_sessions(DAGConfig.YEAR)
            
            if sessions:
                self.logger.info(f"Connection test successful - found {len(sessions)} sessions")
                return True
            else:
                self.logger.warning("Connection test returned no sessions")
                return True  # API works, just no data
                
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
    
    def close(self) -> None:
        """
        Close the HTTP session.
        """
        if self.session:
            self.session.close()
