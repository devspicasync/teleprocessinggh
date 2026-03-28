"""
Export utilities for anomaly detection results
"""

import csv
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class ResultExporter:
    """Export anomaly detection results in various formats"""
    
    def __init__(self, output_dir: Optional[str] = None):
        self.output_dir = Path(output_dir) if output_dir else Path.cwd()
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def export_trackable_data(self, 
                              original_data: List[Dict[str, Any]],
                              headers: List[str],
                              row_anomaly_map: Dict[int, List[str]],
                              output_path: Optional[str] = None,
                              only_anomalies: bool = True) -> str:
        """Export original data with anomaly flags"""
        
        if not output_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = self.output_dir / f"telecom_data_results_{timestamp}.csv"
        
        logger.info(f"Generating trackable data: {output_path}")
        
        # Prepare headers
        trackable_headers = headers.copy()
        trackable_headers.extend(['has_anomaly', 'anomaly_count', 'anomaly_methods'])
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(trackable_headers)
            
            exported_rows = 0
            
            for original_idx, original_row in enumerate(original_data):
                anomaly_methods = row_anomaly_map.get(original_idx, [])
                
                # If only_anomalies is True, only export rows that HAVE anomalies
                if only_anomalies and not anomaly_methods:
                    continue
                
                exported_rows += 1
                has_anomaly = 'YES' if anomaly_methods else 'NO'
                
                # Get original data in header order
                row_data = [original_row.get(h, '') for h in headers]
                
                # Add anomaly columns
                row_data.extend([
                    has_anomaly,
                    len(anomaly_methods),
                    '; '.join(anomaly_methods[:3])
                ])
                
                writer.writerow(row_data)
            
            logger.info(f"Exported {exported_rows} records to CSV")
        
        return str(output_path)
        
        return str(output_path)
    
    def export_results(self,
                       anomalies: List[Dict],
                       statistics: Dict[str, Any],
                       format: str = 'json',
                       output_path: Optional[str] = None) -> str:
        """Export results in specified format"""
        
        if not output_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = self.output_dir / f"telecom_anomaly_results_{timestamp}.{format}"
        
        if format == 'json':
            self._export_json(anomalies, statistics, output_path)
        elif format == 'csv':
            self._export_csv(anomalies, output_path)
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        logger.info(f"Results exported to {output_path}")
        return str(output_path)
    
    def _export_json(self, anomalies: List[Dict], statistics: Dict[str, Any], output_path: Path):
        """Export as JSON"""
        data = {
            'metadata': {
                'timestamp': datetime.now().isoformat(),
                'statistics': statistics
            },
            'anomalies': anomalies
        }
        
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)
    
    def _export_csv(self, anomalies: List[Dict], output_path: Path):
        """Export as CSV"""
        with open(output_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['row_index', 'msisdn', 'method', 'value', 'confidence', 'column'])
            
            for a in anomalies:
                if a.get('row_index', -1) >= 0:
                    writer.writerow([
                        a.get('row_index', ''),
                        a.get('msisdn', ''),
                        a.get('detection_method', ''),
                        f"{a.get('value', 0):.2f}" if isinstance(a.get('value'), (int, float)) else a.get('value', ''),
                        f"{a.get('confidence', 0):.2f}",
                        a.get('column_name', '')
                    ])
    
    def export_report(self,
                      statistics: Dict[str, Any],
                      output_path: Optional[str] = None) -> str:
        """Generate human-readable report"""
        
        if not output_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = self.output_dir / f"telecom_anomaly_report_{timestamp}.txt"
        
        with open(output_path, 'w') as f:
            f.write("=" * 60 + "\n")
            f.write("TELECOM ANOMALY DETECTION REPORT\n")
            f.write("=" * 60 + "\n\n")
            
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("SUMMARY\n")
            f.write("-" * 40 + "\n")
            f.write(f"Total Records: {statistics['total_rows']:,}\n")
            f.write(f"Rows with Anomalies: {statistics['rows_with_anomalies']:,}\n")
            f.write(f"Total Anomalies: {statistics['total_anomalies_detected']:,}\n")
            f.write(f"Anomaly Rate: {statistics['anomaly_rate']:.2f}%\n")
            f.write(f"Unique Subscribers: {statistics['unique_subscribers']:,}\n\n")
            
            f.write("ANOMALIES BY TYPE\n")
            f.write("-" * 40 + "\n")
            for method, count in sorted(statistics['anomaly_types'].items(), 
                                       key=lambda x: x[1], reverse=True):
                f.write(f"  {method}: {count}\n")
            
            f.write("\n" + "=" * 60 + "\n")
        
        logger.info(f"Report generated: {output_path}")
        return str(output_path)
    
    def export_suspicious_subscribers(self,
                                     subscriber_anomalies: Dict[str, Dict],
                                     output_path: Optional[str] = None) -> str:
        """Export suspicious subscribers"""
        
        if not output_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = self.output_dir / f"suspicious_subscribers_{timestamp}.csv"
        
        with open(output_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['msisdn', 'anomaly_count', 'unique_methods', 'methods'])
            
            for msisdn, data in sorted(subscriber_anomalies.items(), 
                                      key=lambda x: x[1]['total'], reverse=True):
                writer.writerow([
                    msisdn,
                    data['total'],
                    len(data['methods']),
                    ', '.join(data['methods'])
                ])
        
        logger.info(f"Suspicious subscribers exported to {output_path}")
        return str(output_path)