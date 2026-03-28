# Use official Python slim image
FROM python:3.9-slim

# Install Java Runtime Environment (REQUIRED for tabula-py to process PDFs)
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Set PYTHONPATH to ensure imports work correctly
ENV PYTHONPATH=/app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Create necessary directories (though the app is configured not to save, logic uses them)
RUN mkdir -p data/raw_csv_temp data/output_process_data data/strictly_cleaned_data data/output data/save_pdf logs

# Expose port 8000
EXPOSE 8000

# Command to run the API
CMD ["uvicorn", "telecom_anomaly.api:app", "--host", "0.0.0.0", "--port", "8000"]
