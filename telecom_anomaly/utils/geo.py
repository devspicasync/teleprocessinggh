"""
Geographic utilities for location-based anomaly detection
"""

import math
from typing import Optional, Tuple


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great circle distance between two points
    on the Earth (specified in decimal degrees)
    
    Returns distance in kilometers
    """
    if None in (lat1, lon1, lat2, lon2):
        return 0.0
    
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(min(math.sqrt(a), 1.0))
    
    # Radius of Earth in kilometers
    r = 6371
    return c * r


def calculate_speed(distance_km: float, time_hours: float) -> float:
    """Calculate speed in km/h"""
    if time_hours <= 0:
        return 0.0
    return distance_km / time_hours


def is_plausible_location_change(
    lat1: float, lon1: float, 
    lat2: float, lon2: float,
    time_diff_hours: float,
    max_speed_kmh: float = 1000.0
) -> Tuple[bool, float, float]:
    """
    Check if a location change is plausible
    
    Returns: (is_plausible, distance_km, speed_kmh)
    """
    distance = haversine_distance(lat1, lon1, lat2, lon2)
    speed = calculate_speed(distance, time_diff_hours)
    
    is_plausible = speed <= max_speed_kmh
    
    return is_plausible, distance, speed