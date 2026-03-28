"""
Time-based pattern anomaly detector
"""

from typing import List, Dict, Any

from telecom_anomaly.detection.base import BaseDetector
from telecom_anomaly.core.models import AnomalyResult, TelecomConfig


class TimePatternDetector(BaseDetector):
    """Detect unusual time patterns"""
    
    def __init__(self, config: TelecomConfig):
        super().__init__(config)
    
    def detect(self, data: List[Dict], **kwargs) -> List[AnomalyResult]:
        """Detect time-based anomalies"""
        anomalies = []
        
        for idx, row in enumerate(data):
            # Check for night calls (excluding service)
            if row.get('is_night') and not row.get('is_service'):
                # Simple heuristic: flag all night calls
                # This can be enhanced with statistical models
                anomalies.append(self._create_anomaly(
                    row_index=idx,
                    column_name='time',
                    value=row.get('event_time', ''),
                    method='night_call',
                    score=1.0,
                    threshold=0.5,
                    confidence=0.60,
                    msisdn=row.get('msisdn', '')
                ))
        
        return anomalies