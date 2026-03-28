import os
import shutil
import tempfile
import logging
import pandas as pd
import io
from pathlib import Path
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, UploadFile, File, HTTPException

from telecom_anomaly.core.detector import TelecomAnomalyDetector
from telecom_anomaly.core.models import TelecomConfig
from config.settings import SERVICE_NUMBERS, OUTPUT_DIR, SAVE_PDF_DIR
from telecom_anomaly.utils.pdf_processor import (
    extract_pdf_to_lines, 
    process_telecom_data_df, 
    strict_clean_df
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Telecom Anomaly Detection API",
    description="API for detecting anomalies in telecom Call Data Records (CDR)",
    version="1.0.0"
)

@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "version": "1.0.0"}

@app.post("/analyze")
async def analyze_cdr(
    file: UploadFile = File(...), 
    save_results: bool = False, 
    save_pdf: bool = False, 
    filter_movements: bool = False,
    run_anomaly_detection: bool = True,
    cluster_km: float = 1.0
):
    """
    Analyze a PDF or CSV file for telecom anomalies.
    Settings are matched to the original CSV analysis endpoint.
    """
    filename = file.filename.lower()
    is_pdf = filename.endswith('.pdf')
    is_csv = filename.endswith('.csv')
    
    if not (is_pdf or is_csv):
        raise HTTPException(status_code=400, detail="Only PDF and CSV files are supported")
    
    # Create a temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix=Path(file.filename).suffix) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = tmp.name
    
    try:
        # Step 1: Initialize detector with original settings
        config = TelecomConfig()
        config.service_numbers = SERVICE_NUMBERS
        config.output_dir = str(OUTPUT_DIR) if save_results else None
        config.save_pdf_dir = str(SAVE_PDF_DIR) if save_pdf else None
        config.location_cluster_km = cluster_km
        
        detector = TelecomAnomalyDetector(config)
        
        # Step 2: Load Data
        if is_pdf:
            logger.info(f"Processing PDF: {file.filename}")
            base_name = os.path.splitext(file.filename)[0]
            
            # Use original logic via lines -> df -> strict_clean
            raw_lines = extract_pdf_to_lines(tmp_path)
            if not raw_lines:
                raise HTTPException(status_code=500, detail="Failed to extract tables from PDF")
                
            df_processed = process_telecom_data_df(raw_lines, base_name)
            if df_processed is None:
                raise HTTPException(status_code=500, detail="Failed to process extracted telecom data")
                
            df_final = strict_clean_df(df_processed)
            if df_final is None or df_final.empty:
                raise HTTPException(status_code=500, detail="No valid records found after cleaning")
            
            # Load into detector
            detector.load_dataframe(df_final, filter_movements=filter_movements)
        else:
            logger.info(f"Processing CSV: {file.filename}")
            # Use original CSV loading logic
            detector.load_data([tmp_path], filter_movements=filter_movements)
        
        # Step 3: Run Analysis
        anomalies = []
        if run_anomaly_detection:
            anomalies = detector.detect_anomalies()
        
        # Get statistics
        stats = detector.get_statistics()
        
        # Step 4: Prepare Results (Exactly like original)
        requested_fields = [
            "event_date", "event_time", "call_direction", "usage_sub_type", 
            "calling_no", "called_no", "usage_type", "duration", "imei", 
            "location_id", "region", "district", "city", "msisdn", 
            "longitude", "latitude", "azimuth"
        ]
        
        final_records_to_return = []
        kept_original_indices = set(detector.data_indices.values())
        
        for idx, row in enumerate(detector.original_data):
            if idx not in kept_original_indices:
                continue
                
            methods = detector.row_anomaly_map.get(idx, [])
            
            if run_anomaly_detection and not methods:
                continue
                
            record = {field: row.get(field, "") for field in requested_fields}
            
            # Format dates/times for JSON parity
            if hasattr(record['event_date'], 'isoformat'):
                record['event_date'] = record['event_date'].isoformat()
            if hasattr(record['event_time'], 'isoformat'):
                record['event_time'] = record['event_time'].isoformat()

            record.update({
                "has_anomaly": "YES" if methods else "NO",
                "anomaly_count": len(methods),
                "anomaly_methods": "; ".join(methods)
            })
            final_records_to_return.append(record)
        
        saved_files = []
        if save_results and detector.exporter:
            outputs = detector.export_results(only_anomalies=run_anomaly_detection)
            saved_files.extend([Path(p).name for p in outputs.values()])
            
        if save_pdf and detector.pdf_exporter:
            pdf_path = detector.export_pdf(final_records_to_return, file.filename)
            saved_files.append(Path(pdf_path).name)
        
        return {
            "filename": file.filename,
            "statistics": stats.to_dict() if run_anomaly_detection else {"total_rows": len(detector.data)},
            "results": final_records_to_return,
            "saved_files": saved_files
        }
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except:
                pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
