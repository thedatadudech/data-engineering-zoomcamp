import os
import requests
import pandas as pd
from datetime import datetime
import streamlit as st
from google.cloud import storage
from google.oauth2 import service_account

def get_gcp_credentials():
    """Get GCP credentials from Streamlit secrets"""
    try:
        if 'gcp_service_account' not in st.secrets:
            st.error("GCP credentials not found in .streamlit/secrets.toml")
            st.stop()

        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        return credentials
    except Exception as e:
        st.error(f"Failed to get GCP credentials: {str(e)}")
        return None

def get_service_folder(dataset_type):
    """Get the appropriate folder name for the dataset type"""
    if dataset_type == "Yellow Taxi":
        return "yellow_taxi"
    elif dataset_type == "Green Taxi":
        return "green_taxi"
    elif dataset_type == "FHV":
        return "fhv"
    else:  # FHVHV
        return "fhvhv"

def clean_gcs_bucket(bucket_name, dataset_type):
    """Delete existing data in GCS bucket for the dataset type"""
    try:
        credentials = get_gcp_credentials()
        if not credentials:
            return False

        project_id = st.secrets["gcp_service_account"]["project_id"]
        storage_client = storage.Client(credentials=credentials, project=project_id)
        bucket = storage_client.bucket(bucket_name)

        # Get the service-specific folder
        service_folder = get_service_folder(dataset_type)
        prefix = f"raw/{service_folder}/"

        st.write(f"Cleaning up existing data in {prefix}...")
        blobs = bucket.list_blobs(prefix=prefix)
        for blob in blobs:
            blob.delete()

        st.success("✅ Existing data cleaned up successfully")
        return True
    except Exception as e:
        st.error(f"Error cleaning GCS bucket: {str(e)}")
        return False

def get_existing_files(bucket_name, dataset_type):
    """Get list of existing files in GCS bucket"""
    try:
        credentials = get_gcp_credentials()
        if not credentials:
            return set()

        project_id = st.secrets["gcp_service_account"]["project_id"]
        storage_client = storage.Client(credentials=credentials, project=project_id)
        bucket = storage_client.bucket(bucket_name)

        service_folder = get_service_folder(dataset_type)
        prefix = f"raw/{service_folder}/"

        existing_files = set()
        blobs = bucket.list_blobs(prefix=prefix)
        for blob in blobs:
            filename = blob.name.split('/')[-1]
            # Extract year-month from filename
            if filename.endswith('.parquet'):
                date_part = filename.split('_')[-1].replace('.parquet', '')
                existing_files.add(date_part)

        return existing_files
    except Exception as e:
        st.error(f"Error checking existing files: {str(e)}")
        return set()

def download_nyc_data(dataset_type, year, month):
    """Download NYC TLC data for specified type and period"""
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    # Format month and dataset name
    month_str = f"{month:02d}"
    if dataset_type == "Yellow Taxi":
        prefix = "yellow"
    elif dataset_type == "Green Taxi":
        prefix = "green"
    elif dataset_type == "FHV":
        prefix = "fhv"
    else:  # FHVHV
        prefix = "fhvhv"

    # Construct filename and URL
    filename = f"{prefix}_tripdata_{year}-{month_str}.parquet"
    url = f"{base_url}/{filename}"

    # Update progress tracking
    progress_key = f"{year}-{month_str}"
    st.session_state.download_progress[progress_key] = False

    try:
        # Download file with progress bar
        st.write(f"Downloading {filename}...")
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            total_size = int(response.headers.get('content-length', 0))
            block_size = 1024
            progress_bar = st.progress(0)

            with open(filename, 'wb') as f:
                downloaded = 0
                for data in response.iter_content(block_size):
                    f.write(data)
                    downloaded += len(data)
                    if total_size:
                        progress = min(downloaded / total_size, 1.0)
                        progress_bar.progress(progress)

            st.success(f"✅ Download completed: {filename}")
            st.session_state.download_progress[progress_key] = True
            return filename
        else:
            st.error(f"❌ Failed to download {filename}")
            return None
    except Exception as e:
        st.error(f"Error downloading {filename}: {str(e)}")
        return None

def upload_to_gcs(bucket_name, source_file, dataset_type):
    """Upload file to Google Cloud Storage with proper folder structure"""
    try:
        credentials = get_gcp_credentials()
        if not credentials:
            return False

        project_id = st.secrets["gcp_service_account"]["project_id"]
        storage_client = storage.Client(credentials=credentials, project=project_id)

        # Create bucket if it doesn't exist
        try:
            bucket = storage_client.get_bucket(bucket_name)
        except Exception:
            bucket = storage_client.create_bucket(bucket_name)
            st.info(f"Created new bucket: {bucket_name}")

        # Get the service-specific folder
        service_folder = get_service_folder(dataset_type)

        # Create the destination blob path with proper structure
        destination_blob = f"raw/{service_folder}/{source_file}"
        blob = bucket.blob(destination_blob)

        # Update upload progress
        progress_key = source_file.split('_')[-1].replace('.parquet', '')
        st.session_state.upload_progress[progress_key] = False

        st.write(f"Uploading {source_file} to GCS...")
        blob.upload_from_filename(source_file)
        os.remove(source_file)  # Clean up local file
        st.success(f"✅ Upload completed: {source_file}")
        st.session_state.upload_progress[progress_key] = True
        return True
    except Exception as e:
        st.error(f"Error uploading to GCS: {str(e)}")
        return False

def process_selected_data():
    """Process the selected dataset"""
    if 'dataset' not in st.session_state:
        st.error("Please select a dataset first")
        return

    dataset = st.session_state['dataset']
    start_year = st.session_state['start_year']
    end_year = st.session_state['end_year']
    start_month = st.session_state['start_month']
    end_month = st.session_state['end_month']
    overwrite = st.session_state.get('overwrite', False)

    # Reset progress tracking
    st.session_state.download_progress = {}
    st.session_state.upload_progress = {}

    # Create bucket name from project ID
    project_id = st.secrets["gcp_service_account"]["project_id"]
    bucket_name = f"{project_id}-taxi-data"

    # Check existing files first
    existing_files = get_existing_files(bucket_name, dataset)

    if existing_files and not overwrite:
        st.info(f"Found {len(existing_files)} existing files for {dataset}")

        # Calculate which files need to be downloaded
        needed_files = set()
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                if year == start_year and month < start_month:
                    continue
                if year == end_year and month > end_month:
                    continue
                date_key = f"{year}-{month:02d}"
                if date_key not in existing_files:
                    needed_files.add(date_key)

        if not needed_files:
            st.success("✅ All required data files already exist!")
            # Mark all as complete to allow pipeline to continue
            st.session_state.upload_progress = {key: True for key in existing_files}
            return True
        else:
            st.write(f"Will download {len(needed_files)} missing files")
    else:
        if overwrite:
            if not clean_gcs_bucket(bucket_name, dataset):
                return False
        needed_files = None  # Will download all files in range

    processing_container = st.container()
    processed_files = []

    with processing_container:
        st.write("📋 Processing Details:")
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                # Skip months outside the selected range
                if year == start_year and month < start_month:
                    continue
                if year == end_year and month > end_month:
                    continue

                # Skip if file exists and we're not overwriting
                date_key = f"{year}-{month:02d}"
                if not overwrite and needed_files and date_key not in needed_files:
                    st.info(f"Skipping {date_key} - already exists")
                    st.session_state.upload_progress[date_key] = True
                    continue

                filename = download_nyc_data(dataset, year, month)
                if filename:
                    if upload_to_gcs(bucket_name, filename, dataset):
                        processed_files.append(filename)

    if processed_files:
        st.success(f"✅ Successfully processed {len(processed_files)} files")
        return True
    elif not overwrite and existing_files:
        st.success("✅ Using existing data files")
        return True
    else:
        st.error("No files were processed successfully")
        return False

if __name__ == "__main__":
    if st.button("Start Data Processing"):
        process_selected_data()