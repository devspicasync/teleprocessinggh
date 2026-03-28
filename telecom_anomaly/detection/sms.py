"""
SMS flooding detector
"""

from typing import List, Dict, Any

from telecom_anomaly.detection.base import SubscriberBasedDetector
from telecom_anomaly.core.models import AnomalyResult, TelecomConfig


class SMSDetector(SubscriberBasedDetector):
    """Detect SMS flooding"""
    
    def __init__(self, config: TelecomConfig):
        super().__init__(config)
    
    def detect(self, data: List[Dict], **kwargs) -> List[AnomalyResult]:
        """Detect SMS anomalies"""
        self.prepare_data(data)
        anomalies = []
        
        for msisdn, sms_list in self.subscriber_data.items():
            # Filter for SMS with valid datetime
            sms_events = [
                (idx, sms) for idx, sms in enumerate(data)
                if sms.get('msisdn') == msisdn and
                sms.get('usage_type') == 'SMS' and
                not sms.get('is_service') and
                sms.get('datetime')
            ]
            
            if len(sms_events) < 10:
                continue
            
            # Sort by datetime
            sms_events.sort(key=lambda x: x[1]['datetime'])
            
            # Check for SMS bursts (5 in 30 seconds)
            for i in range(len(sms_events) - 5):
                time_window = sms_events[i + 4][1]['datetime'] - sms_events[i][1]['datetime']
                
                if time_window and time_window.total_seconds() < 30:
                    anomalies.append(self._create_anomaly(
                        row_index=sms_events[i][0],
                        column_name='sms',
                        value=5,
                        method='sms_burst',
                        score=1.0,
                        threshold=5,
                        confidence=0.90,
                        msisdn=msisdn
                    ))
                    break  # One burst per subscriber is enough
        
        return anomalies