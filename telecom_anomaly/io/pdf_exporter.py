from fpdf import FPDF
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

class PDFExporter:
    """Generates professional Landscape PDF reports for telecom anomaly detection"""
    
    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate_report(self, stats: Dict[str, Any], results: List[Dict[str, Any]], filename: str) -> str:
        # Use Landscape orientation to fit 20 columns
        pdf = FPDF(orientation="L", unit="mm", format="A4")
        pdf.add_page()
        
        # Title
        pdf.set_font("helvetica", "B", 18)
        pdf.set_text_color(0, 51, 102)
        pdf.cell(0, 12, "Telecom Anomaly Detection - Detailed Report", ln=True, align="C")
        
        # Metadata
        pdf.set_font("helvetica", "I", 9)
        pdf.set_text_color(100, 100, 100)
        pdf.cell(0, 8, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Source: {filename}", ln=True, align="C")
        pdf.ln(5)
        
        # Summary Section
        pdf.set_font("helvetica", "B", 12)
        pdf.set_text_color(0, 0, 0)
        pdf.set_fill_color(240, 240, 240)
        pdf.cell(0, 8, " 1. Detection Summary", ln=True, fill=True)
        pdf.ln(3)
        
        pdf.set_font("helvetica", "", 10)
        summary_text = (f"Total Records: {stats['total_rows']:,} | "
                       f"Anomalies: {stats['rows_with_anomalies']:,} | "
                       f"Subscribers: {stats['unique_subscribers']:,} | "
                       f"Rate: {stats['anomaly_rate']:.2f}%")
        pdf.cell(0, 8, summary_text, ln=True)
        pdf.ln(5)
        
        # Detailed Table
        pdf.set_font("helvetica", "B", 12)
        pdf.cell(0, 8, " 2. Detailed Anomalous Records (All 20 Fields)", ln=True, fill=True)
        pdf.ln(3)
        
        # Define 20 columns and their widths (Landscape A4 width is ~277mm inside margins)
        fields = [
            ("Date", "event_date", 16),
            ("Time", "event_time", 14),
            ("Dir", "call_direction", 8),
            ("Sub-Type", "usage_sub_type", 16),
            ("Calling No", "calling_no", 22),
            ("Called No", "called_no", 22),
            ("Type", "usage_type", 11),
            ("Dur", "duration", 8),
            ("IMEI", "imei", 24),
            ("Loc-ID", "location_id", 14),
            ("Region", "region", 15),
            ("District", "district", 15),
            ("City", "city", 15),
            ("MSISDN", "msisdn", 22),
            ("Long", "longitude", 14),
            ("Lat", "latitude", 14),
            ("Azi", "azimuth", 8),
            ("Anom", "has_anomaly", 8),
            ("Cnt", "anomaly_count", 6),
            ("Methods", "anomaly_methods", 0) # 0 means fill remaining width
        ]
        
        # Calculate width of last column
        used_width = sum(f[2] for f in fields[:-1])
        fields[-1] = (fields[-1][0], fields[-1][1], 277 - used_width)

        # Header
        pdf.set_font("helvetica", "B", 5.5) # Small font for many columns
        pdf.set_fill_color(200, 220, 255)
        for label, _, width in fields:
            pdf.cell(width, 7, label, border=1, fill=True, align="C")
        pdf.ln()
        
        # Data Rows
        pdf.set_font("helvetica", "", 5.5)
        for record in results[:100]: # Limit to top 100 in PDF for performance
            # Draw row
            for _, key, width in fields:
                val = str(record.get(key, ""))
                # Truncate if too long for cell
                if pdf.get_string_width(val) > width - 1:
                    while pdf.get_string_width(val + "..") > width - 1 and len(val) > 0:
                        val = val[:-1]
                    val += ".."
                
                pdf.cell(width, 6, val, border=1)
            pdf.ln()

        if len(results) > 100:
            pdf.set_font("helvetica", "I", 7)
            pdf.cell(0, 10, f"Showing top 100 of {len(results)} anomalies. Please refer to CSV/JSON for full dataset.", ln=True, align="R")

        # Save PDF
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = self.output_dir / f"Anomaly_Report_Detailed_{timestamp}.pdf"
        pdf.output(str(output_path))
        return str(output_path)
