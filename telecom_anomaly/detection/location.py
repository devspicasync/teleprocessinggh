"""
Location jump anomaly detector
"""

from typing import List, Dict, Any, Optional
from datetime import datetime

from telecom_anomaly.detection.base import SubscriberBasedDetector
from telecom_anomaly.core.models import AnomalyResult, TelecomConfig
from telecom_anomaly.utils.geo import haversine_distance, calculate_speed


class LocationDetector(SubscriberBasedDetector):
    """Detect impossible location jumps"""
    
    def __init__(self, config: TelecomConfig):
        super().__init__(config)
    
    def detect(self, data: List[Dict], **kwargs) -> List[AnomalyResult]:
        """Detect location anomalies"""
        self.prepare_data(data)
        anomalies = []
        
        for msisdn, calls in self.subscriber_data.items():
            # Get calls with valid location and datetime
            valid_calls = []
            for call in calls:
                idx = data.index(call) if call in data else -1
                if (call.get('latitude') and call.get('longitude') and 
                    not call.get('is_service') and 
                    call.get('datetime') and idx >= 0):
                    valid_calls.append((idx, call))
            
            if len(valid_calls) < 3:
                continue
            
            # Sort by datetime
            valid_calls.sort(key=lambda x: x[1]['datetime'])
            
            # Check consecutive calls
            for i in range(len(valid_calls) - 1):
                idx1, call1 = valid_calls[i]
                idx2, call2 = valid_calls[i + 1]
                
                dt1: Optional[datetime] = call1.get('datetime')
                dt2: Optional[datetime] = call2.get('datetime')
                
                if dt1 and dt2:
                    time_diff = (dt2 - dt1).total_seconds() / 3600  # hours
                    
                    # Check calls within 24 hours
                    if 0 < time_diff < 24:
                        distance = haversine_distance(
                            call1['latitude'], call1['longitude'],
                            call2['latitude'], call2['longitude']
                        )
                        
                        if distance > 100:  # More than 100km
                            speed = calculate_speed(distance, time_diff)
                            
                            if speed > self.config.max_location_change_speed:
                                anomalies.append(self._create_anomaly(
                                    row_index=idx2,
                                    column_name='location',
                                    value=distance,
                                    method='impossible_speed',
                                    score=min(speed / self.config.max_location_change_speed, 10.0),
                                    threshold=self.config.max_location_change_speed,
                                    confidence=0.90,
                                    msisdn=msisdn,
                                    metadata={
                                        'speed': speed,
                                        'distance': distance,
                                        'time_diff_hours': time_diff
                                    }
                                ))
        
        return anomalies