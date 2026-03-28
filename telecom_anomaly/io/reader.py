"""
Data reader for telecom CSV files
"""

import csv
import os
import logging
import pandas as pd
from typing import List, Dict, Any, Optional, Union
from datetime import datetime

from telecom_anomaly.validation.validator import DataValidator

logger = logging.getLogger(__name__)


class TelecomDataReader:
    """Read and validate telecom data from CSV files"""
    
    def __init__(self, config, batch_size: int = 10000):
        self.config = config
        self.batch_size = batch_size
        self.headers: List[str] = []
        self.validator = DataValidator()
    
    def read_files(self, file_paths: Union[str, List[str]], 
                   encoding: str = 'utf-8-sig') -> List[Dict[str, Any]]:
        """
        Read and validate data from CSV files
        
        Returns list of cleaned records with original indices
        """
        if isinstance(file_paths, str):
            file_paths = [file_paths]
        
        all_records = []
        original_data = []
        
        for file_path in file_paths:
            if not os.path.exists(file_path):
                logger.warning(f"File not found: {file_path}")
                continue
            
            logger.info(f"Reading data from {file_path}")
            
            try:
                file_records, file_original = self._read_single_file(
                    file_path, encoding, len(original_data)
                )
                all_records.extend(file_records)
                original_data.extend(file_original)
                
            except Exception as e:
                logger.error(f"Failed to read {file_path}: {e}")
        
        logger.info(f"Read {len(all_records)} valid records from {len(original_data)} total rows")
        return all_records, original_data

    def read_dataframe(self, df: 'pd.DataFrame') -> List[Dict[str, Any]]:
        """
        Read and validate data from a pandas DataFrame
        
        Returns list of cleaned records with original indices
        """
        records = []
        original_data = []
        
        # Set headers from dataframe columns if not already set
        if not self.headers:
            self.headers = [str(c).strip() for c in df.columns]
        
        # Convert DataFrame to records (list of dicts)
        # We need to handle it row by row to match the original index-based cleaning
        for idx, row in df.iterrows():
            # Create row dictionary
            row_dict = row.to_dict()
            # Ensure all values are strings or expected types for cleaning
            row_dict = {k: str(v) if pd.notna(v) else "" for k, v in row_dict.items()}
            
            # Store original
            original_copy = row_dict.copy()
            original_copy['_file_source'] = "dataframe_input"
            original_copy['_row_number'] = idx + 1
            original_data.append(original_copy)
            
            # Clean record
            cleaned = self._clean_record(row_dict, len(original_data) - 1)
            if cleaned:
                records.append(cleaned)
        
        logger.info(f"Processed {len(records)} valid records from {len(original_data)} DataFrame rows")
        return records, original_data
    
    def _read_single_file(self, file_path: str, encoding: str, 
                          base_idx: int) -> tuple:
        """Read and validate a single CSV file"""
        records = []
        original_data = []
        
        with open(file_path, 'r', encoding=encoding) as f:
            reader = csv.reader(f)
            
            # Read headers
            if not self.headers:
                self.headers = next(reader)
                self.headers = [h.strip() for h in self.headers]
            else:
                next(reader)  # Skip header if already read
            
            for row_num, row in enumerate(reader, start=1):
                if not row or all(not cell for cell in row):
                    continue
                
                # Ensure row length matches headers
                if len(row) < len(self.headers):
                    row.extend([''] * (len(self.headers) - len(row)))
                elif len(row) > len(self.headers):
                    row = row[:len(self.headers)]
                
                # Create row dictionary
                row_dict = dict(zip(self.headers, row))
                
                # Store original
                original_copy = row_dict.copy()
                original_copy['_file_source'] = os.path.basename(file_path)
                original_copy['_row_number'] = row_num
                original_data.append(original_copy)
                
                # Clean record
                cleaned = self._clean_record(row_dict, base_idx + len(original_data) - 1)
                if cleaned:
                    records.append(cleaned)
        
        return records, original_data
    
    def _clean_record(self, row: Dict[str, Any], original_idx: int) -> Optional[Dict[str, Any]]:
        """Clean and validate a single record"""
        cleaned = {}
        
        # Strip string values
        for key, value in row.items():
            cleaned[key] = value.strip() if isinstance(value, str) else value
        
        # Add metadata
        cleaned['_original_idx'] = original_idx
        
        # Validate MSISDN
        msisdn = cleaned.get('msisdn', '')
        if not self.validator.validate_msisdn(msisdn):
            return None
        
        # Validate duration
        cleaned['duration'] = self.validator.validate_duration(cleaned.get('duration'))
        
        # Validate coordinates
        lat, lon = self.validator.validate_coordinates(
            cleaned.get('latitude'), cleaned.get('longitude')
        )
        cleaned['latitude'] = lat
        cleaned['longitude'] = lon
        
        # Parse datetime
        cleaned['datetime'] = self.validator.parse_datetime(
            cleaned.get('event_date', ''),
            cleaned.get('event_time', '')
        )
        
        # Check night call
        cleaned['is_night'] = self.validator.is_night_call(
            cleaned.get('event_time', ''),
            self.config.night_hours_start,
            self.config.night_hours_end
        )
        
        # Check service number
        calling = cleaned.get('calling_no', '')
        cleaned['is_service'] = self.validator.is_service_number(
            calling, self.config.service_numbers
        )
        
        return cleaned