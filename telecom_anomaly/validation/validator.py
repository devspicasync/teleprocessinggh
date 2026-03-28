"""
Data validation utilities for telecom data
"""

from datetime import datetime
from typing import Optional, Tuple, Any
import logging

logger = logging.getLogger(__name__)


class DataValidator:
    """Validate and clean telecom data"""
    
    @staticmethod
    def validate_msisdn(msisdn: str) -> bool:
        """Validate MSISDN format"""
        if not msisdn:
            return False
        msisdn_str = str(msisdn).strip()
        # Allow service names
        if msisdn_str in ['MTN', 'MOBILEMONEY', 'MTN-DATA', 'MTNXTRATIME', 'EVD']:
            return True
        # Ghana format: starts with 233 and 12 digits
        return msisdn_str.startswith('233') and len(msisdn_str) == 12
    
    @staticmethod
    def validate_duration(duration: Any) -> float:
        """Validate and clean duration"""
        if duration is None or duration == '':
            return 0.0
        try:
            duration_float = float(duration)
            return max(0.0, duration_float)
        except (ValueError, TypeError):
            return 0.0
    
    @staticmethod
    def validate_coordinates(lat: Any, lon: Any) -> Tuple[Optional[float], Optional[float]]:
        """Validate latitude and longitude"""
        try:
            lat_float = float(lat) if lat not in (None, '', '0.0') else None
            lon_float = float(lon) if lon not in (None, '', '0.0') else None
            
            if lat_float is not None and not (-90 <= lat_float <= 90):
                lat_float = None
            if lon_float is not None and not (-180 <= lon_float <= 180):
                lon_float = None
                
            return lat_float, lon_float
        except (ValueError, TypeError):
            return None, None
    
    @staticmethod
    def parse_datetime(date_str: str, time_str: str) -> Optional[datetime]:
        """Parse datetime from date and time strings"""
        try:
            if date_str and time_str and date_str != 'NULL' and time_str != 'NULL':
                return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError) as e:
            logger.debug(f"Failed to parse datetime: {date_str} {time_str} - {e}")
        return None
    
    @staticmethod
    def is_night_call(time_str: str, night_start: int = 23, night_end: int = 5) -> bool:
        """Check if call occurred at night"""
        try:
            if not time_str or time_str == 'NULL':
                return False
            hour = int(time_str.split(':')[0])
            return hour >= night_start or hour < night_end
        except (ValueError, IndexError):
            return False
    
    @staticmethod
    def is_service_number(calling_no: str, service_numbers: list) -> bool:
        """Check if number is a service number"""
        return calling_no in service_numbers