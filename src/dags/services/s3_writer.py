"""
S3 Writer Service for F1 Data

Simple S3 writer service for Bronze layer data using temp files.
Maintains the same partitioning strategy as the original implementation.
"""

import os
import re
import logging
import unicodedata
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import pandas as pd
import boto3

from src.dags import DAGConfig


class S3Writer:
    """
    Simple S3 writer service for Bronze layer F1 data.
    Uses temp files for reliable Parquet writing in MWAA.
    """
    
    def __init__(self):
        """Initialize S3 writer using DAG configuration."""
        self.bucket = DAGConfig.BRONZE_BUCKET
        self.base_prefix = "bronze/"
        
        # Create S3 client
        self.s3_client = boto3.client('s3', region_name=DAGConfig.AWS_REGION)
        
        # Set up logging
        self.logger = logging.getLogger(__name__)
        
        self.logger.info(f"S3Writer initialized for bucket '{self.bucket}'")
    
    def _normalize_grand_prix_name(self, location: str) -> str:
        """
        Normalize Grand Prix location for partitioning.
        
        Args:
            location: Location from session data
            
        Returns:
            Normalized location name
        """
        if not location:
            return "unknown"
        
        # Handle international characters
        normalized = unicodedata.normalize('NFD', location)
        normalized = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')
        
        # Convert to lowercase with underscores
        normalized = re.sub(r'[^\w\s]', '', normalized.lower())
        normalized = re.sub(r'\s+', '_', normalized.strip())
        
        return normalized or "unknown"
    
    def _generate_partition_path(self, session_info: Dict, endpoint: str) -> str:
        """
        Generate partition path: bronze/{endpoint}/year={year}/grand_prix={gp}/session_type={type}/
        
        Args:
            session_info: Session metadata from API
            endpoint: Data endpoint name
            
        Returns:
            S3 partition path
        """
        year = session_info.get('year', DAGConfig.YEAR)
        location = session_info.get('location', 'Unknown')
        session_type = session_info.get('session_type', 'unknown').lower()
        
        grand_prix = self._normalize_grand_prix_name(location)
        
        return (
            f"{self.base_prefix.rstrip('/')}/{endpoint}/"
            f"year={year}/grand_prix={grand_prix}/session_type={session_type}/"
        )
    
    def write(self, session_info: Dict, endpoint: str, data: List[Dict]) -> Optional[Dict[str, Any]]:
        """
        Write data to S3 Bronze layer.
        
        Args:
            session_info: Session metadata from API
            endpoint: Data endpoint name
            data: List of data records
            
        Returns:
            Write metadata or None if no data
        """
        if not data:
            self.logger.warning(f"No {endpoint} data for session {session_info.get('session_key')}")
            return None
        
        try:
            # Create DataFrame
            df = pd.DataFrame(data)
            record_count = len(df)
            
            # Generate paths
            partition_path = self._generate_partition_path(session_info, endpoint)
            session_key = session_info.get('session_key', 'unknown')
            timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
            filename = f"{endpoint}_{session_key}_{timestamp}.parquet"
            s3_key = f"{partition_path}{filename}"
            full_s3_path = f"s3://{self.bucket}/{s3_key}"
            
            # Create temporary file for upload
            temp_file = f"/tmp/{filename}"
            df.to_parquet(
                temp_file,
                compression='snappy',
                engine='pyarrow',
                index=False
            )
            
            self.logger.info(f"Writing {record_count} {endpoint} records to {full_s3_path}")
            
            # Upload to S3
            self.s3_client.upload_file(temp_file, self.bucket, s3_key)
            
            # Remove temporary file
            os.remove(temp_file)
            
            self.logger.info(f"Successfully uploaded {record_count} {endpoint} records")
            
            return {
                'file_path': full_s3_path,
                'endpoint': endpoint,
                'record_count': record_count,
                'session_key': session_key,
                'created_at': datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            # Remove temporary file if it exists
            temp_file = f"/tmp/{endpoint}_{session_info.get('session_key', 'unknown')}_{timestamp}.parquet"
            if os.path.exists(temp_file):
                os.remove(temp_file)
            
            self.logger.error(f"Error writing {endpoint} data: {e}")
            raise
