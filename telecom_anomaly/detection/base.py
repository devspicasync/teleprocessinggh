"""
Base classes for anomaly detectors
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from collections import defaultdict

from telecom_anomaly.core.models import AnomalyResult, TelecomConfig


class BaseDetector(ABC):
    """Base class for all anomaly detectors"""
    
    def __init__(self, config: TelecomConfig):
        self.config = config
        self.name = self.__class__.__name__
    
    @abstractmethod
    def detect(self, data: List[Dict], **kwargs) -> List[AnomalyResult]:
        """Detect anomalies in the data"""
        pass
    
    def _create_anomaly(self, 
                        row_index: int,
                        column_name: str,
                        value: Any,
                        method: str,
                        score: float,
                        threshold: float,
                        confidence: float,
                        msisdn: str = "",
                        metadata: Optional[Dict] = None) -> AnomalyResult:
        """Helper method to create an anomaly result"""
        return AnomalyResult(
            row_index=row_index,
            column_name=column_name,
            value=value,
            detection_method=method,
            score=score,
            threshold=threshold,
            confidence=confidence,
            msisdn=msisdn,
            metadata=metadata or {}
        )


class SubscriberBasedDetector(BaseDetector):
    """Base class for detectors that operate on subscriber data"""
    
    def __init__(self, config: TelecomConfig):
        super().__init__(config)
        self.subscriber_data: Dict[str, List[Dict]] = defaultdict(list)
    
    def prepare_data(self, data: List[Dict]):
        """Prepare data by grouping by subscriber"""
        self.subscriber_data.clear()
        for row in data:
            msisdn = row.get('msisdn')
            if msisdn:
                self.subscriber_data[msisdn].append(row)
    
    @abstractmethod
    def detect(self, data: List[Dict], **kwargs) -> List[AnomalyResult]:
        pass