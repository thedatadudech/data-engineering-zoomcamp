import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime

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

def get_datetime_fields(dataset_type):
    """Get the appropriate datetime field names based on dataset type"""
    if dataset_type == "yellow_taxi":
        return "tpep_pickup_datetime", "tpep_dropoff_datetime"
    elif dataset_type == "green_taxi":
        return "lpep_pickup_datetime", "lpep_dropoff_datetime"
    else:  # FHV and FHVHV
        return "pickup_datetime", "dropoff_datetime"

def transform_taxi_data():
    """Transform taxi data using BigQuery scheduled queries"""
    try:
        # Initialize BigQuery client
        client = init_bigquery_client()
        if not client:
            return False

        # Get dataset type from session state
        dataset_type = st.session_state.get('dataset', '').lower().replace(' ', '_')
        table_name = f"taxi_trips_optimized_{dataset_type}"

        # Get appropriate datetime field names
        pickup_field, dropoff_field = get_datetime_fields(dataset_type)

        # Get date range from session state
        start_year = st.session_state.get('start_year')
        start_month = st.session_state.get('start_month')
        end_year = st.session_state.get('end_year')
        end_month = st.session_state.get('end_month')

        if not all([start_year, start_month, end_year, end_month]):
            st.error("Missing date range parameters. Please confirm your selection first.")
            return False

        # Create date range strings for SQL
        start_date = f"{start_year}-{start_month:02d}-01"
        if end_month == 12:
            end_date = f"{end_year + 1}-01-01"
        else:
            end_date = f"{end_year}-{(end_month + 1):02d}-01"

        st.write(f"Processing table: {table_name}")
        st.write(f"Using pickup field: {pickup_field}")
        st.write(f"Date range: {start_date} to {end_date}")

        # Create monthly metrics table with dynamic field handling
               # Modify the monthly metrics query to CREATE or REPLACE the table
        monthly_metrics_query = f"""
        CREATE OR REPLACE TABLE `{st.secrets['gcp_service_account']['project_id']}.nyc_taxi_data.monthly_trip_metrics` AS
        WITH fare_percentiles AS (
            SELECT
                APPROX_QUANTILES(fare_amount, 100) as percentiles
            FROM `{st.secrets['gcp_service_account']['project_id']}.nyc_taxi_data.{table_name}`
            WHERE {pickup_field} IS NOT NULL
              AND {pickup_field} >= TIMESTAMP("{start_date}")
              AND {pickup_field} < TIMESTAMP("{end_date}")
              AND fare_amount > 0
        ),
        filtered_data AS (
            SELECT 
                DATE_TRUNC({pickup_field}, MONTH) as year_month,
                fare_amount,
                trip_distance,
                PULocationID,
                DOLocationID
            FROM `{st.secrets['gcp_service_account']['project_id']}.nyc_taxi_data.{table_name}`
            WHERE {pickup_field} IS NOT NULL
              AND {pickup_field} >= TIMESTAMP("{start_date}")
              AND {pickup_field} < TIMESTAMP("{end_date}")
              AND trip_distance > 0
              AND fare_amount > (SELECT percentiles[OFFSET(5)] FROM fare_percentiles)  -- 5th percentile
              AND fare_amount < (SELECT percentiles[OFFSET(95)] FROM fare_percentiles)  -- 95th percentile
        )
        SELECT
            year_month,
            COUNT(*) as total_trips,
            SUM(fare_amount) as total_revenue,
            AVG(fare_amount) as avg_fare,
            MIN(fare_amount) as min_fare,
            MAX(fare_amount) as max_fare,
            APPROX_QUANTILES(fare_amount, 4)[OFFSET(1)] as fare_25th,
            APPROX_QUANTILES(fare_amount, 4)[OFFSET(2)] as fare_median,
            APPROX_QUANTILES(fare_amount, 4)[OFFSET(3)] as fare_75th,
            AVG(trip_distance) as avg_distance,
            COUNT(DISTINCT PULocationID) as unique_pickup_zones,
            COUNT(DISTINCT DOLocationID) as unique_dropoff_zones,
            '{dataset_type}' as source_type,
            CURRENT_TIMESTAMP() as last_updated
        FROM filtered_data
        GROUP BY year_month
        ORDER BY year_month
        """

        # Execute the transformation
        st.write("Creating monthly metrics table...")
        st.code(monthly_metrics_query, language='sql')

        query_job = client.query(monthly_metrics_query)
        try:
            query_job.result(timeout=60)  # Wait up to 60 seconds for the query to complete
        except Exception as e:
            st.error(f"Query execution failed: {str(e)}")
            return False

        # Verify the transformation results
        verify_query = """
        SELECT 
            COUNT(*) as row_count,
            MIN(year_month) as first_month,
            MAX(year_month) as last_month,
            AVG(avg_fare) as mean_fare,
            source_type,
            last_updated
        FROM `nyc_taxi_data.monthly_trip_metrics`
        GROUP BY source_type, last_updated
        """
        verify_job = client.query(verify_query)
        result = verify_job.result()
        row = list(result)[0]

        st.write("Transformation results:")
        st.write(f"- Row count: {row.row_count}")
        st.write(f"- Date range: {row.first_month} to {row.last_month}")
        st.write(f"- Mean fare: ${row.mean_fare:.2f}")
        st.write(f"- Source: {row.source_type}")
        st.write(f"- Updated: {row.last_updated}")

        if row.row_count > 0:
            st.success(f"""
            ✅ Transformation completed successfully!
            - Generated {row.row_count} monthly records
            - Date range: {row.first_month.strftime('%Y-%m')} to {row.last_month.strftime('%Y-%m')}
            - Source type: {row.source_type}
            - Last updated: {row.last_updated}
            """)
            return True
        else:
            st.error("❌ No data found in the transformed table")
            return False

    except Exception as e:
        st.error(f"Error transforming data: {str(e)}")
        st.exception(e)
        return False

if __name__ == "__main__":
    transform_taxi_data()