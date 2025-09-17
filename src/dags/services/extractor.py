"""
Service Data Extractor

Simple helper that combines OpenF1Client and S3Writer for DAG use.
"""

import logging
from typing import List, Dict, Any

from src.dags import DAGConfig
from src.dags.services import OpenF1Client, S3Writer


class ServiceExtractor:
    """
    Simple helper for service data extraction.
    """
    
    def __init__(self):
        """Initialize extractor."""
        self.client = OpenF1Client()
        self.writer = S3Writer()
        self.logger = logging.getLogger(__name__)
    
    def extract_session_data(self, session_info: Dict) -> Dict[str, Any]:
        """
        Extract data for a session.
        
        Args:
            session_info: Session metadata from API
            
        Returns:
            Extraction results
        """
        session_key = session_info.get('session_key')
        session_type = session_info.get('session_type', '')
        
        self.logger.info(f"Extracting {session_type} data for session {session_key}")
        
        # Get endpoints for this session type
        endpoints = DAGConfig.get_endpoints_for_session(session_type)
        
        results = {'session_key': session_key, 'total_records': 0}
        
        # Extract each endpoint
        for endpoint in endpoints:
            data = self.client.get_data_for_session(session_key, endpoint)
            
            if data:
                write_result = self.writer.write(session_info, endpoint, data)
                if write_result:
                    results['total_records'] += write_result['record_count']
                    self.logger.info(f"✅ {endpoint}: {write_result['record_count']} records")
        
        return results
    
    def close(self):
        """Close client."""
        self.client.close()
