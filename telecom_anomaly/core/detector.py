"""
Main anomaly detector orchestrator
"""

import logging
import numpy as np
from typing import List, Dict, Any, Optional, Set
from collections import defaultdict
from datetime import datetime
from sklearn.cluster import DBSCAN

from telecom_anomaly.core.models import TelecomConfig, AnomalyResult, DetectionStatistics
from telecom_anomaly.io.reader import TelecomDataReader
from telecom_anomaly.io.exporter import ResultExporter
from telecom_anomaly.io.pdf_exporter import PDFExporter
from telecom_anomaly.validation.validator import DataValidator

# Import detectors
from telecom_anomaly.detection.duration import DurationDetector
from telecom_anomaly.detection.frequency import FrequencyDetector
from telecom_anomaly.detection.sms import SMSDetector
from telecom_anomaly.detection.location import LocationDetector
# from telecom_anomaly.detection.imei import IMEIDetector
from telecom_anomaly.detection.time_patterns import TimePatternDetector

logger = logging.getLogger(__name__)


class TelecomAnomalyDetector:
    """
    Main orchestrator for telecom anomaly detection
    """
    
    def __init__(self, config: Optional[TelecomConfig] = None):
        self.config = config or TelecomConfig()
        self.config.validate()
        
        # Initialize components
        self.reader = TelecomDataReader(self.config, self.config.batch_size)
        self.exporter = None
        self.pdf_exporter = None
        
        if self.config.output_dir:
            self.exporter = ResultExporter(self.config.output_dir)
            
        if self.config.save_pdf_dir:
            self.pdf_exporter = PDFExporter(self.config.save_pdf_dir)
        elif self.config.output_dir:
            pdf_dir = str(Path(self.config.output_dir).parent / "save_pdf")
            self.pdf_exporter = PDFExporter(pdf_dir)
            
        self.validator = DataValidator()
        
        # Initialize detectors
        self.detectors = [
            DurationDetector(self.config),
            FrequencyDetector(self.config),
            SMSDetector(self.config),
            LocationDetector(self.config),
            # IMEIDetector(self.config),
            TimePatternDetector(self.config)
        ]
        
        # Data storage
        self.data: List[Dict[str, Any]] = []
        self.headers: List[str] = []
        self.original_data: List[Dict[str, Any]] = []
        self.data_indices: Dict[int, int] = {}  # processed_idx -> original_idx
        self.anomalies: List[AnomalyResult] = []
        
        # Anomaly tracking
        self.rows_with_anomalies: Set[int] = set()
        self.row_anomaly_map: Dict[int, List[str]] = defaultdict(list)
        self.subscriber_anomalies: Dict[str, Dict] = defaultdict(
            lambda: {'total': 0, 'methods': set()}
        )
        
        logger.info(f"Initialized TelecomAnomalyDetector with {len(self.detectors)} detectors")
    
    def load_data(self, file_paths: List[str], filter_movements: bool = False) -> 'TelecomAnomalyDetector':
        """Load data from files"""
        self.data, self.original_data = self.reader.read_files(file_paths)
        self.headers = self.reader.headers
        
        if filter_movements and self.data:
            self.filter_distinct_movements()
            
        # Build index mapping
        self.data_indices = {}
        for processed_idx, row in enumerate(self.data):
            if '_original_idx' in row:
                self.data_indices[processed_idx] = row['_original_idx']
        
        logger.info(f"Loaded {len(self.data)} valid records")
        return self

    def load_dataframe(self, df: 'pd.DataFrame', filter_movements: bool = False) -> 'TelecomAnomalyDetector':
        """Load data from a pandas DataFrame"""
        self.data, self.original_data = self.reader.read_dataframe(df)
        self.headers = self.reader.headers
        
        if filter_movements and self.data:
            self.filter_distinct_movements()
            
        # Build index mapping
        self.data_indices = {}
        for processed_idx, row in enumerate(self.data):
            if '_original_idx' in row:
                self.data_indices[processed_idx] = row['_original_idx']
        
        logger.info(f"Loaded {len(self.data)} valid records from DataFrame")
        return self

    def filter_distinct_movements(self):
        """
        Filter data to keep only representative records for distinct geographic locations.
        Uses DBSCAN with Haversine distance.
        """
        if not self.data:
            return
            
        logger.info("Filtering for distinct geographic movements...")
        
        # Extract coordinates for valid location rows
        valid_indices = []
        coords = []
        for i, row in enumerate(self.data):
            lat = row.get('latitude')
            lon = row.get('longitude')
            if lat is not None and lon is not None:
                valid_indices.append(i)
                coords.append([lat, lon])
        
        if not coords:
            return
            
        coords_np = np.array(coords)
        
        # Parameters for DBSCAN
        distance_km = self.config.location_cluster_km
        kms_per_radian = 6371.0088
        epsilon = distance_km / kms_per_radian
        
        # Run DBSCAN
        db = DBSCAN(
            eps=epsilon, 
            min_samples=1, 
            algorithm='ball_tree', 
            metric='haversine'
        ).fit(np.radians(coords_np))
        
        # Identify representative row for each cluster
        cluster_labels = db.labels_
        unique_clusters = set(cluster_labels)
        
        kept_indices = set()
        cluster_to_idx = {}
        
        for i, label in enumerate(cluster_labels):
            if label not in cluster_to_idx:
                cluster_to_idx[label] = valid_indices[i]
                kept_indices.add(valid_indices[i])
        
        # Also keep rows without coordinates
        all_indices = set(range(len(self.data)))
        coord_indices = set(valid_indices)
        no_coord_indices = all_indices - coord_indices
        
        final_indices = sorted(list(kept_indices | no_coord_indices))
        
        original_count = len(self.data)
        self.data = [self.data[i] for i in final_indices]
        
        logger.info(f"Geographic filtering complete: {original_count} -> {len(self.data)} records")
    
    def detect_anomalies(self) -> List[AnomalyResult]:
        """Run all anomaly detectors"""
        start_time = datetime.now()
        logger.info("Starting anomaly detection")
        
        all_anomalies = []
        
        # Run each detector
        for detector in self.detectors:
            try:
                detector_anomalies = detector.detect(self.data)
                all_anomalies.extend(detector_anomalies)
                logger.info(f"{detector.name}: {len(detector_anomalies)} anomalies")
            except Exception as e:
                logger.error(f"Error in {detector.name}: {e}")
        
        # Combine anomalies
        self.anomalies = self._combine_anomalies(all_anomalies)
        
        # Update tracking
        self._update_anomaly_tracking()
        
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"Detection completed in {elapsed:.2f}s")
        logger.info(f"Rows with anomalies: {len(self.rows_with_anomalies)}/{len(self.data)}")
        
        return self.anomalies
    
    def _combine_anomalies(self, anomalies: List[AnomalyResult]) -> List[AnomalyResult]:
        """Combine anomalies, removing duplicates"""
        if not anomalies:
            return []
        
        # Separate row and subscriber anomalies
        row_anomalies = defaultdict(list)
        subscriber_anomalies = defaultdict(list)
        
        for a in anomalies:
            if a.row_index >= 0:
                row_anomalies[a.row_index].append(a)
            else:
                subscriber_anomalies[a.msisdn].append(a)
        
        combined = []
        
        # Keep unique methods per row, highest confidence
        for row_idx, row_anoms in row_anomalies.items():
            best_per_method = {}
            for anom in row_anoms:
                if anom.detection_method not in best_per_method or \
                   anom.confidence > best_per_method[anom.detection_method].confidence:
                    best_per_method[anom.detection_method] = anom
            combined.extend(best_per_method.values())
        
        # Keep unique methods per subscriber
        for msisdn, sub_anoms in subscriber_anomalies.items():
            best_per_method = {}
            for anom in sub_anoms:
                if anom.detection_method not in best_per_method:
                    best_per_method[anom.detection_method] = anom
            combined.extend(best_per_method.values())
        
        return combined
    
    def _update_anomaly_tracking(self):
        """Update anomaly tracking maps"""
        for anomaly in self.anomalies:
            # Track rows with anomalies
            if anomaly.row_index >= 0:
                self.rows_with_anomalies.add(anomaly.row_index)
                original_idx = self.data_indices.get(anomaly.row_index)
                if original_idx is not None:
                    self.row_anomaly_map[original_idx].append(anomaly.detection_method)
            
            # Track subscriber anomalies
            if anomaly.msisdn:
                self.subscriber_anomalies[anomaly.msisdn]['total'] += 1
                self.subscriber_anomalies[anomaly.msisdn]['methods'].add(anomaly.detection_method)
    
    def get_statistics(self) -> DetectionStatistics:
        """Get detection statistics"""
        rows_with_anomalies = len(self.rows_with_anomalies)
        
        # Calculate anomaly rate correctly
        anomaly_rate = (rows_with_anomalies / len(self.data)) * 100 if self.data else 0
        
        # Count anomaly types
        anomaly_types = defaultdict(int)
        for anomaly in self.anomalies:
            anomaly_types[anomaly.detection_method] += 1
        
        return DetectionStatistics(
            total_rows=len(self.data),
            rows_with_anomalies=rows_with_anomalies,
            total_anomalies_detected=len(self.anomalies),
            anomaly_rate=anomaly_rate,
            unique_subscribers=len(set(a.msisdn for a in self.anomalies if a.msisdn)),
            anomaly_types=dict(anomaly_types)
        )
    
    def export_results(self, format: str = 'json', only_anomalies: bool = True) -> Dict[str, str]:
        """Export all results"""
        stats = self.get_statistics()
        anomaly_dicts = [a.to_dict() for a in self.anomalies]
        
        outputs = {}
        
        # Export trackable data
        outputs['trackable'] = self.exporter.export_trackable_data(
            self.original_data, self.headers, self.row_anomaly_map,
            only_anomalies=only_anomalies
        )
        
        # Export JSON results
        outputs['json'] = self.exporter.export_results(
            anomaly_dicts, stats.to_dict(), 'json'
        )
        
        # Export CSV results
        outputs['csv'] = self.exporter.export_results(
            anomaly_dicts, stats.to_dict(), 'csv'
        )
        
        # Export report
        outputs['report'] = self.exporter.export_report(stats.to_dict())
        
        # Export suspicious subscribers
        outputs['subscribers'] = self.exporter.export_suspicious_subscribers(
            dict(self.subscriber_anomalies)
        )
        
        return outputs

    def export_pdf(self, results: List[Dict[str, Any]], filename: str) -> str:
        """Export results as PDF report"""
        stats = self.get_statistics().to_dict()
        return self.pdf_exporter.generate_report(stats, results, filename)