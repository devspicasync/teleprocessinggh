import tabula
import pandas as pd
import io
import re
import os
import tempfile
from pathlib import Path

# --- CONFIGURATION (Restored) ---
DATA_BASE = Path("data")
RAW_CSV_DIR = DATA_BASE / "raw_csv_temp"
PROCESS_OUTPUT_DIR = DATA_BASE / "output_process_data"
STRICT_CLEAN_DIR = DATA_BASE / "strictly_cleaned_data"

def setup_folders():
    """Ensure all necessary output directories exist."""
    for folder in [RAW_CSV_DIR, PROCESS_OUTPUT_DIR, STRICT_CLEAN_DIR]:
        try:
            if not os.path.exists(folder):
                os.makedirs(folder)
        except Exception as e:
            print(f"Warning: Could not create folder {folder}: {e}")

# --- ORIGINAL FUNCTIONS (Restored) ---

def extract_pdf_to_csv(pdf_path, base_name):
    """Stage 1: Convert PDF tables to a raw CSV file."""
    setup_folders()
    csv_path = os.path.join(RAW_CSV_DIR, f"{base_name}_raw.csv")
    try:
        tabula.convert_into(pdf_path, csv_path, output_format="csv", pages="all", lattice=True)
        return csv_path
    except Exception as e:
        print(f"Error extracting {pdf_path}: {e}")
        return None

def process_telecom_data_complete(input_path, base_name):
    """Stage 2: Parse raw CSV and repair fragmented rows."""
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        return _process_lines_logic(lines, base_name, save_files=True)
    except Exception as e:
        print(f"Error reading {input_path}: {e}")
        return None

def strict_clean_data(file_path, base_name):
    """Stage 3: Final strict cleaning."""
    df = pd.read_csv(file_path)
    df_cleaned = _strict_clean_logic(df)
    
    setup_folders()
    final_path = os.path.join(STRICT_CLEAN_DIR, f'{base_name}_strictly_cleaned.csv')
    df_cleaned.to_csv(final_path, index=False)
    return final_path

# --- IN-MEMORY VERSIONS (Exact same logic) ---

def extract_pdf_to_lines(pdf_file_path):
    """Stage 1 (In-Memory): Returns raw CSV lines."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp_path = tmp.name
    try:
        tabula.convert_into(pdf_file_path, tmp_path, output_format="csv", pages="all", lattice=True)
        with open(tmp_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        return lines
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

def process_telecom_data_df(lines, base_name):
    """Stage 2 (In-Memory): Returns DataFrame."""
    return _process_lines_logic(lines, base_name, save_files=False)

def strict_clean_df(df):
    """Stage 3 (In-Memory): Returns Cleaned DataFrame."""
    return _strict_clean_logic(df)

# --- SHARED LOGIC (To ensure 100% parity) ---

def _process_lines_logic(lines, base_name, save_files=False):
    header_idx = -1
    for i, line in enumerate(lines):
        if "event_date_time" in line:
            header_idx = i
            break
    
    if header_idx == -1:
        return None

    if save_files:
        setup_folders()
        customer_text = "".join(lines[:header_idx])
        df_customer = pd.read_csv(io.StringIO(customer_text))
        df_customer.columns = [re.sub(r'[\r\n\s]+', ' ', c).strip() for c in df_customer.columns]
        cust_path = os.path.join(PROCESS_OUTPUT_DIR, f'{base_name}_customer_info.csv')
        df_customer.to_csv(cust_path, index=False)

    call_text = "".join(lines[header_idx:])
    df_raw = pd.read_csv(io.StringIO(call_text))
    df_raw.columns = [c.strip() for c in df_raw.columns]
    main_col = df_raw.columns[0] 
    df_raw = df_raw[df_raw[main_col].astype(str).str.contains("event_date_time") == False]

    fixed_rows = []
    current_row = None
    for _, row in df_raw.iterrows():
        val = str(row[main_col]).strip()
        if re.match(r'^\d{4}-\d{2}-\d{2}', val):
            if current_row is not None:
                fixed_rows.append(current_row)
            current_row = row.copy()
        elif current_row is not None:
            current_row[main_col] = (str(current_row[main_col]) + " " + val).replace('\r', '').replace('\n', '')
            for col in df_raw.columns:
                if col != main_col and pd.api.types.is_string_dtype(df_raw[col]):
                    fragment = str(row[col]).strip()
                    if fragment.lower() != 'nan':
                        current_row[col] = (str(current_row[col]) + " " + fragment).replace('\r', '').replace('\n', ' ')

    if current_row is not None:
        fixed_rows.append(current_row)
        
    df_calls = pd.DataFrame(fixed_rows)
    df_calls[main_col] = df_calls[main_col].str.replace(r'[\r\n]+', '', regex=True).str.replace(r'\s+', ' ', regex=True).str.strip()
    dt_series = pd.to_datetime(df_calls[main_col], errors='coerce')
    df_calls['event_date'] = dt_series.dt.date
    df_calls['event_time'] = dt_series.dt.time
    df_calls.drop(columns=[main_col], inplace=True)

    uppercase_cols = ['district', 'city', 'usage_type', 'usage_sub_type', 'calling_no', 'called_no', 'imei', 'location_id', 'msisdn']
    for col in df_calls.columns:
        c_low = col.lower()
        if df_calls[col].dtype == 'object':
            df_calls[col] = df_calls[col].astype(str).str.replace(r'[\r\n\s]+', ' ', regex=True).str.strip()
            if c_low in uppercase_cols:
                df_calls[col] = df_calls[col].str.upper()
            elif c_low == 'region':
                df_calls[col] = df_calls[col].str.title()
            elif c_low == 'call_direction':
                df_calls[col] = df_calls[col].str.title().replace({'Incomingmt': 'Incoming (MT)', '\(\(': '(', '\)\)': ')'}, regex=True)

    ordered_cols = ['event_date', 'event_time'] + [c for c in df_calls.columns if c not in ['event_date', 'event_time']]
    df_calls = df_calls[ordered_cols]
    
    if save_files:
        interim_path = os.path.join(PROCESS_OUTPUT_DIR, f'{base_name}_call_records_final.csv')
        df_calls.to_csv(interim_path, index=False)
        return interim_path
    return df_calls

def _strict_clean_logic(df):
    if 'duration' in df.columns and df['duration'].dtype == 'object':
        df['duration'] = df['duration'].str.replace(',', '.').astype(float)
    df['event_date_copy'] = pd.to_datetime(df['event_date'], errors='coerce')
    df['event_time_check'] = pd.to_datetime(df['event_time'].astype(str), format='%H:%M:%S', errors='coerce')
    df_cleaned = df.dropna().copy()
    return df_cleaned.drop(columns=['event_date_copy', 'event_time_check'])
