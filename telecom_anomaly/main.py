"""
Main entry point for telecom anomaly detection system
"""

import sys
import logging
from pathlib import Path

from telecom_anomaly import TelecomAnomalyDetector, TelecomConfig
from config.settings import DEFAULT_THRESHOLDS, SERVICE_NUMBERS, INPUT_DIR, OUTPUT_DIR, SAVE_PDF_DIR

# Configure logging
from config.settings import LOGGING_CONFIG
logging.basicConfig(
    level=LOGGING_CONFIG['level'],
    format=LOGGING_CONFIG['format'],
    handlers=[
        logging.FileHandler(LOGGING_CONFIG['log_file']),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def main():
    """Main function"""
    print("\n" + "=" * 60)
    print("📱 TELECOM ANOMALY DETECTION SYSTEM")
    print("=" * 60)
    
    # Create config with defaults
    config = TelecomConfig()
    config.service_numbers = SERVICE_NUMBERS
    config.output_dir = str(OUTPUT_DIR)
    config.save_pdf_dir = str(SAVE_PDF_DIR)
    
    # Get files
    if len(sys.argv) > 1:
        file_paths = sys.argv[1:]
    else:
        print(f"\nEnter CSV file paths (or place files in {INPUT_DIR}):")
        print("Press Enter to scan input directory, or type paths:")
        
        user_input = input().strip()
        
        if not user_input:
            # Scan input directory
            file_paths = [str(p) for p in INPUT_DIR.glob('*.csv')]
            if file_paths:
                print(f"Found {len(file_paths)} CSV files in input directory")
            else:
                print("No CSV files found in input directory")
                file_paths = []
        else:
            file_paths = [user_input]
            
            # Get additional files
            while True:
                path = input().strip()
                if not path:
                    break
                file_paths.append(path)
    
    # Validate files
    valid_files = []
    for path in file_paths:
        if Path(path).exists():
            valid_files.append(path)
        else:
            print(f"❌ File not found: {path}")
    
    if not valid_files:
        print("\n❌ No valid input files. Exiting.")
        return
    
    print(f"\n📊 Processing {len(valid_files)} file(s)...")
    
    # Run detection
    try:
        detector = TelecomAnomalyDetector(config)
        detector.load_data(valid_files)
        anomalies = detector.detect_anomalies()
        
        # Export results
        outputs = detector.export_results()
        
        # Prepare results for PDF (enriched records)
        pdf_results = []
        requested_fields = [
            "event_date", "event_time", "call_direction", "usage_sub_type", 
            "calling_no", "called_no", "usage_type", "duration", "imei", 
            "location_id", "region", "district", "city", "msisdn", 
            "longitude", "latitude", "azimuth"
        ]
        for idx, row in enumerate(detector.original_data):
            methods = detector.row_anomaly_map.get(idx, [])
            if methods:
                record = {field: row.get(field, "") for field in requested_fields}
                record.update({
                    "has_anomaly": "YES",
                    "anomaly_count": len(methods),
                    "anomaly_methods": "; ".join(methods)
                })
                pdf_results.append(record)
        
        # Generate PDF report
        pdf_source_name = Path(valid_files[0]).name if valid_files else "Batch_Analysis"
        pdf_path = detector.export_pdf(pdf_results, pdf_source_name)
        outputs['pdf_report'] = pdf_path
        
        # Show results
        stats = detector.get_statistics()
        
        print("\n" + "=" * 60)
        print("✅ ANALYSIS COMPLETE")
        print("=" * 60)
        print(f"📁 Total records: {stats.total_rows:,}")
        print(f"👤 Subscribers with anomalies: {stats.unique_subscribers:,}")
        print(f"🔍 Rows with anomalies: {stats.rows_with_anomalies:,}")
        print(f"⚠️  Total anomalies: {stats.total_anomalies_detected:,}")
        print(f"📊 Anomaly rate: {stats.anomaly_rate:.2f}%")
        print("\n📄 Files created:")
        for name, path in outputs.items():
            print(f"  - {name}: {Path(path).name}")
        print("\n" + "=" * 60)
        
    except Exception as e:
        logger.error(f"Detection failed: {e}", exc_info=True)
        print(f"\n❌ Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
