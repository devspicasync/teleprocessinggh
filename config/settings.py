"""
Configuration settings for the telecom anomaly detection system
"""

import os
from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'
INPUT_DIR = DATA_DIR / 'input'
OUTPUT_DIR = DATA_DIR / 'output'
SAVE_PDF_DIR = DATA_DIR / 'save_pdf'
LOG_DIR = BASE_DIR / 'logs'

# Create directories if they don't exist
# On environments like Vercel, this may fail due to read-only filesystem
for dir_path in [DATA_DIR, INPUT_DIR, OUTPUT_DIR, SAVE_PDF_DIR, LOG_DIR]:
    try:
        dir_path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"Warning: Could not create directory {dir_path}: {e}")

# Logging configuration
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'log_file': LOG_DIR / 'telecom_anomaly_detection.log'
}

# Default detection thresholds (can be overridden by config file)
DEFAULT_THRESHOLDS = {
    'short_call_threshold': 3.0,
    'long_call_threshold': 5400.0,
    'extreme_call_threshold': 10800.0,
    'max_calls_per_hour': 60,
    'max_calls_per_day': 300,
    'max_calls_per_minute': 10,
    'max_sms_per_hour': 100,
    'max_sms_per_minute': 20,
    'max_location_change_speed': 1000.0,
    'location_cluster_radius': 10.0,
    'call_ratio_outgoing_incoming': 20.0,
    'unique_contacts_per_day': 100,
    'z_score_threshold': 4.0,
    'iqr_multiplier': 2.0,
    'night_hours_start': 23,
    'night_hours_end': 5,
    'min_samples_for_pattern': 20,
    'batch_size': 10000
}

# Service numbers to ignore
SERVICE_NUMBERS = [
    'MTN', 'MOBILEMONEY', 'MTN-DATA', 'MTNXTRATIME', 'MTN-JUST4U',
    'EVD', '5040', '1506', '431', '1326', '1349', '438840',
    '54211', '435942', '5381060', '783273271', '141', '402',
    'TWO SURE LP', 'DRMBAWUMIA', 'MYMTN 2.0', 'EOCO', 'DREAMTRAVEL'
]