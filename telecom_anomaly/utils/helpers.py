"""
Helper utilities
"""

import hashlib
from typing import Any, Dict, List, Set
from collections import defaultdict


def generate_row_hash(row: Dict[str, Any]) -> str:
    """Generate a unique hash for a row"""
    row_str = ''.join(str(v) for v in row.values() if v is not None)
    return hashlib.md5(row_str.encode()).hexdigest()


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safe division with default value"""
    try:
        if denominator == 0:
            return default
        return numerator / denominator
    except (ZeroDivisionError, TypeError, ValueError):
        return default


def chunk_list(lst: List, chunk_size: int):
    """Split list into chunks"""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


class SubscriberTracker:
    """Track subscriber data efficiently"""
    
    def __init__(self):
        self.subscriber_data: Dict[str, List[Dict]] = defaultdict(list)
        self.subscriber_indices: Dict[str, Set[int]] = defaultdict(set)
    
    def add_record(self, msisdn: str, record: Dict, index: int):
        """Add a record for a subscriber"""
        self.subscriber_data[msisdn].append(record)
        self.subscriber_indices[msisdn].add(index)
    
    def get_records(self, msisdn: str) -> List[Dict]:
        """Get all records for a subscriber"""
        return self.subscriber_data.get(msisdn, [])
    
    def get_indices(self, msisdn: str) -> Set[int]:
        """Get all indices for a subscriber"""
        return self.subscriber_indices.get(msisdn, set())
    
    def get_all_subscribers(self) -> List[str]:
        """Get all subscriber MSISDNs"""
        return list(self.subscriber_data.keys())
    
    def get_subscriber_count(self) -> int:
        """Get number of unique subscribers"""
        return len(self.subscriber_data)
    
    def clear(self):
        """Clear all data"""
        self.subscriber_data.clear()
        self.subscriber_indices.clear()