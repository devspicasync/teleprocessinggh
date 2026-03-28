"""
Data models for the anomaly detection system
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Dict, Any, List, Optional


@dataclass
class TelecomConfig:
    """Configuration for telecom anomaly detection"""
    
    # Call duration thresholds (seconds)
    short_call_threshold: float = 3.0
    long_call_threshold: float = 5400.0  # 90 minutes
    extreme_call_threshold: float = 10800.0  # 3 hours
    
    # Call frequency thresholds
    max_calls_per_hour: int = 60
    max_calls_per_day: int = 300
    max_calls_per_minute: int = 10
    
    # SMS thresholds
    max_sms_per_hour: int = 100
    max_sms_per_minute: int = 20
    ignore_service_sms: bool = True
    
    # Geographic thresholds
    max_location_change_speed: float = 1000.0  # km/h
    location_cluster_radius: float = 10.0  # km
    
    # Behavioral thresholds
    call_ratio_outgoing_incoming: float = 20.0
    unique_contacts_per_day: int = 100
    
    # Service numbers to ignore
    service_numbers: List[str] = field(default_factory=list)
    
    # Statistical thresholds
    z_score_threshold: float = 4.0
    iqr_multiplier: float = 2.0
    
    # Time-based patterns
    night_hours_start: int = 23
    night_hours_end: int = 5
    
    # Minimum samples needed
    min_samples_for_pattern: int = 20
    
    # Performance
    batch_size: int = 10000
    
    def validate(self):
        """Validate configuration"""
        assert self.short_call_threshold >= 0, "Short call threshold must be non-negative"
        assert self.long_call_threshold > self.short_call_threshold, \
            "Long call threshold must be greater than short call threshold"
        assert self.extreme_call_threshold > self.long_call_threshold, \
            "Extreme call threshold must be greater than long call threshold"
        return self
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'TelecomConfig':
        """Create config from dictionary"""
        return cls(**{k: v for k, v in config_dict.items() if k in cls.__dataclass_fields__})


@dataclass
class AnomalyResult:
    """Anomaly detection result"""
    row_index: int
    column_name: str
    value: Any
    detection_method: str
    score: float
    threshold: float
    confidence: float
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    msisdn: str = ""
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        result = asdict(self)
        result['timestamp'] = self.timestamp.isoformat()
        return result


@dataclass
class DetectionStatistics:
    """Statistics from anomaly detection"""
    total_rows: int
    rows_with_anomalies: int
    total_anomalies_detected: int
    anomaly_rate: float
    unique_subscribers: int
    anomaly_types: Dict[str, int]
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return asdict(self)