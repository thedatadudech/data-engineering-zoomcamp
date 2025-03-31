import streamlit as st
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
from google.api_core import retry

def init_bigquery_client():
    """Initialize BigQuery client with credentials"""
    try:
        if 'gcp_service_account' not in st.secrets:
            st.error("GCP credentials not found in .streamlit/secrets.toml")
            st.stop()

        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        project_id = st.secrets["gcp_service_account"]["project_id"]
        client = bigquery.Client(credentials=credentials, project=project_id)
        return client
    except Exception as e:
        st.error(f"Failed to initialize BigQuery client: {str(e)}")
        return None


def check_data_availability(client, dataset_id, table_id):
    """Check if data exists in the external table"""
    try:
        query = f"""
        SELECT COUNT(*) as count
        FROM `{client.project}.{dataset_id}.{table_id}`
        LIMIT 1
        """

        st.info("Checking data availability...")
        query_job = client.query(query)
        result = query_job.result()
        row = list(result)[0]

        if row.count > 0:
            st.success(f"✅ Found data in table {table_id}")
            return True
        else:
            st.warning(f"⚠️ No data found in table {table_id}")
            return False
    except Exception as e:
        st.error(f"Error checking data availability: {str(e)}")
        return False


def create_dataset(client, dataset_id):
    """Create BigQuery dataset if it doesn't exist"""
    try:
        dataset_ref = client.dataset(dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        dataset = client.create_dataset(dataset, exists_ok=True)
        return dataset
    except Exception as e:
        st.error(f"Error creating dataset: {str(e)}")
        return None


def create_external_table(client, dataset_id, table_id, dataset_type):
    """Create external table pointing to GCS data"""
    try:
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        # Get the service-specific prefix and folder
        if dataset_type == "Yellow Taxi":
            prefix = "yellow"
            folder = "yellow_taxi"
            pickup_datetime_field = "tpep_pickup_datetime"
            dropoff_datetime_field = "tpep_dropoff_datetime"
        elif dataset_type == "Green Taxi":
            prefix = "green"
            folder = "green_taxi"
            pickup_datetime_field = "lpep_pickup_datetime"
            dropoff_datetime_field = "lpep_dropoff_datetime"
        elif dataset_type == "FHV":
            prefix = "fhv"
            folder = "fhv"
            pickup_datetime_field = "pickup_datetime"
            dropoff_datetime_field = "dropoff_datetime"
        else:  # FHVHV
            prefix = "fhvhv"
            folder = "fhvhv"
            pickup_datetime_field = "pickup_datetime"
            dropoff_datetime_field = "dropoff_datetime"

        # Store the datetime field names in session state for later use
        st.session_state.pickup_datetime_field = pickup_datetime_field
        st.session_state.dropoff_datetime_field = dropoff_datetime_field

        # Construct GCS path with proper folder structure
        project_id = st.secrets["gcp_service_account"]["project_id"]
        bucket_name = f"{project_id}-taxi-data"
        gcs_path = f"gs://{bucket_name}/raw/{folder}/*.parquet"

        st.write(f"Creating external table with GCS path: {gcs_path}")

        external_config = bigquery.ExternalConfig("PARQUET")
        external_config.source_uris = [gcs_path]
        external_config.autodetect = True

        table = bigquery.Table(table_ref)
        table.external_data_configuration = external_config

        # Create or replace the table
        table = client.create_table(table, exists_ok=True)
        st.success(f"✅ Created external table: {table_id}")

        return table
    except Exception as e:
        st.error(f"Error creating external table: {str(e)}")
        st.exception(e)
        return None


def create_partitioned_table(client, dataset_id, source_table_id, dest_table_id):
    """Create a partitioned and clustered table from external table"""
    try:
        # Check if data exists
        if not check_data_availability(client, dataset_id, source_table_id):
            st.warning("Skipping partition creation - no data available")
            return False

        # Get the datetime field names from session state
        pickup_datetime_field = st.session_state.get('pickup_datetime_field', 'pickup_datetime')
        dropoff_datetime_field = st.session_state.get('dropoff_datetime_field', 'dropoff_datetime')

        # Query to copy and optimize data with date partitioning
        query = f"""
        CREATE OR REPLACE TABLE `{client.project}.{dataset_id}.{dest_table_id}`
        PARTITION BY DATE({pickup_datetime_field})
        CLUSTER BY VendorID
        AS
        SELECT 
            {pickup_datetime_field},
            {dropoff_datetime_field},
            VendorID,
            PULocationID,
            DOLocationID,
            CAST(trip_distance AS FLOAT64) as trip_distance,
            CAST(fare_amount AS FLOAT64) as fare_amount
        FROM `{client.project}.{dataset_id}.{source_table_id}`
        WHERE {pickup_datetime_field} IS NOT NULL
          AND trip_distance > 0
          AND fare_amount > 0
        """

        st.info(f"Starting creation of partitioned table {dest_table_id}...")
        st.code(query, language="sql")

        # Execute the query
        query_job = client.query(query)
        query_job.result()

        # Verify the table was created with data - show only latest 5 partitions
        verify_query = f"""
        SELECT 
            DATE({pickup_datetime_field}) as partition_date,
            COUNT(*) as record_count,
            COUNT(DISTINCT PULocationID) as pickup_zones,
            COUNT(DISTINCT DOLocationID) as dropoff_zones,
            AVG(fare_amount) as avg_fare
        FROM `{client.project}.{dataset_id}.{dest_table_id}`
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 5
        """
        results = client.query(verify_query).result()

        # Display partitioning results
        st.write("Recent data distribution (latest 5 partitions):")
        for row in results:
            st.write(
                f"- Partition {row.partition_date}: "
                f"{row.record_count:,} trips, "
                f"{row.pickup_zones} pickup zones, "
                f"{row.dropoff_zones} dropoff zones, "
                f"avg fare ${row.avg_fare:.2f}"
            )

        st.success(f"Created partitioned table {dest_table_id} with date partitioning and VendorID clustering")
        return True
    except Exception as e:
        st.error(f"Error creating partitioned table: {str(e)}")
        st.exception(e)
        return False

def get_gcp_credentials():
    """Helper function to get GCP credentials."""
    if 'gcp_service_account' not in st.secrets:
        st.error("GCP credentials not found in .streamlit/secrets.toml")
        st.stop()
    return service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])

def get_service_folder(dataset_type):
    """Helper function to get the correct service folder name."""
    if dataset_type == "Yellow Taxi":
        return "yellow_taxi"
    elif dataset_type == "Green Taxi":
        return "green_taxi"
    elif dataset_type == "FHV":
        return "fhv"
    else:  # FHVHV
        return "fhvhv"


def verify_gcs_data(client, bucket_name, dataset_type):
    """Verify data exists in GCS bucket"""
    try:
        storage_client = storage.Client(
            credentials=get_gcp_credentials(),
            project=st.secrets["gcp_service_account"]["project_id"]
        )

        # Get the service folder
        folder = get_service_folder(dataset_type)

        # List files in the bucket/folder
        bucket = storage_client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=f"raw/{folder}/"))

        if blobs:
            st.success(f"✅ Found {len(blobs)} files in GCS path: raw/{folder}/")
            for blob in blobs[:5]:  # Show first 5 files
                st.write(f"- {blob.name}")
            return True
        else:
            st.warning(f"⚠️ No files found in GCS path: raw/{folder}/")
            return False
    except Exception as e:
        st.error(f"Error checking GCS data: {str(e)}")
        return False

def setup_bigquery_tables():
    """Main function to set up BigQuery tables"""
    client = init_bigquery_client()
    if not client:
        return False

    dataset_id = "nyc_taxi_data"

    with st.spinner("Setting up BigQuery environment..."):
        # Create dataset
        dataset = create_dataset(client, dataset_id)
        if not dataset:
            return False

        # Get the currently selected dataset type
        selected_dataset = st.session_state.get('dataset')
        if selected_dataset not in ["Yellow Taxi", "Green Taxi", "FHV", "FHVHV"]:
            st.error("Please select a valid dataset type")
            return False

        # Verify GCS data exists
        project_id = st.secrets["gcp_service_account"]["project_id"]
        bucket_name = f"{project_id}-taxi-data"

        if not verify_gcs_data(client, bucket_name, selected_dataset):
            st.error("No data found in GCS. Please ensure data is uploaded first.")
            return False

        # Create external table with data verification
        ext_table = create_external_table(
            client, 
            dataset_id, 
            f"raw_taxi_data_external_{selected_dataset.lower().replace(' ', '_')}", 
            selected_dataset
        )

        if ext_table:
            # Create optimized table
            success = create_partitioned_table(
                client,
                dataset_id,
                f"raw_taxi_data_external_{selected_dataset.lower().replace(' ', '_')}", 
                f"taxi_trips_optimized_{selected_dataset.lower().replace(' ', '_')}"
            )

            if success:
                st.success(f"""
                ✅ Successfully created optimized BigQuery table for {selected_dataset} with:
                - Date partitioning on pickup_datetime
                - Clustering on VendorID
                """)
                return True
            else:
                st.error(f"Failed to create optimized table for {selected_dataset}")
                return False
        else:
            st.error(f"Failed to create external table for {selected_dataset}")
            return False

if __name__ == "__main__":
    setup_bigquery_tables()