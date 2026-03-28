"""
Call frequency anomaly detector
"""

from typing import List, Dict, Any
from collections import defaultdict

from telecom_anomaly.detection.base import SubscriberBasedDetector
from telecom_anomaly.core.models import AnomalyResult, TelecomConfig


class FrequencyDetector(SubscriberBasedDetector):
    """Detect abnormal call frequency"""
    
    def __init__(self, config: TelecomConfig):
        super().__init__(config)
    
    def detect(self, data: List[Dict], **kwargs) -> List[AnomalyResult]:
        """Detect frequency anomalies"""
        self.prepare_data(data)
        anomalies = []
        
        for msisdn, calls in self.subscriber_data.items():
            # Filter for voice calls with valid datetime
            voice_calls = [
                (idx, call) for idx, call in enumerate(data)
                if call.get('msisdn') == msisdn and
                call.get('usage_type') == 'VOICE' and
                not call.get('is_service') and
                call.get('datetime')
            ]
            
            if len(voice_calls) < 10:
                continue
            
            # Sort by datetime
            voice_calls.sort(key=lambda x: x[1]['datetime'])
            
            # Check for call bursts (calls per minute)
            for i in range(len(voice_calls) - self.config.max_calls_per_minute):
                time_window = (
                    voice_calls[i + self.config.max_calls_per_minute - 1][1]['datetime'] - 
                    voice_calls[i][1]['datetime']
                )
                
                if time_window and time_window.total_seconds() < 60:
                    anomalies.append(self._create_anomaly(
                        row_index=voice_calls[i][0],
                        column_name='frequency',
                        value=self.config.max_calls_per_minute,
                        method='call_burst',
                        score=1.0,
                        threshold=self.config.max_calls_per_minute,
                        confidence=0.85,
                        msisdn=msisdn
                    ))
                    break  # One burst per subscriber is enough
        
        return anomalies