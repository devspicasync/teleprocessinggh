"""
Call duration anomaly detector
"""

from typing import List, Dict, Any

from telecom_anomaly.detection.base import BaseDetector
from telecom_anomaly.core.models import AnomalyResult, TelecomConfig


class DurationDetector(BaseDetector):
    """Detect anomalous call durations"""
    
    def __init__(self, config: TelecomConfig):
        super().__init__(config)
    
    def detect(self, data: List[Dict], **kwargs) -> List[AnomalyResult]:
        """Detect duration anomalies"""
        anomalies = []
        
        for idx, row in enumerate(data):
            # Skip non-voice, service calls, or zero duration
            if row.get('usage_type') != 'VOICE' or \
               row.get('is_service') or \
               row.get('duration', 0) == 0:
                continue
            
            duration = row['duration']
            msisdn = row.get('msisdn', '')
            
            # Extreme duration
            if duration > self.config.extreme_call_threshold:
                anomalies.append(self._create_anomaly(
                    row_index=idx,
                    column_name='duration',
                    value=duration,
                    method='extreme_duration',
                    score=min(duration / self.config.extreme_call_threshold, 10.0),
                    threshold=self.config.extreme_call_threshold,
                    confidence=0.95,
                    msisdn=msisdn
                ))
            
            # Long duration
            elif duration > self.config.long_call_threshold:
                anomalies.append(self._create_anomaly(
                    row_index=idx,
                    column_name='duration',
                    value=duration,
                    method='long_duration',
                    score=min(duration / self.config.long_call_threshold, 5.0),
                    threshold=self.config.long_call_threshold,
                    confidence=0.80,
                    msisdn=msisdn
                ))
            
            # Short duration
            elif 0 < duration < self.config.short_call_threshold:
                anomalies.append(self._create_anomaly(
                    row_index=idx,
                    column_name='duration',
                    value=duration,
                    method='short_duration',
                    score=self.config.short_call_threshold / max(duration, 0.1),
                    threshold=self.config.short_call_threshold,
                    confidence=0.75,
                    msisdn=msisdn
                ))
        
        return anomalies