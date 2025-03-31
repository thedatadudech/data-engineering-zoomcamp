import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
import sys
import os

# Debug information
print("Starting Streamlit app with BigQuery integration...")

# Page config
st.set_page_config(
    page_title="NYC Taxi Analysis",
    layout="wide"
)

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

def check_data_availability():
    """Check if transformed data is available"""
    try:
        client = init_bigquery_client()
        check_query = """
        SELECT COUNT(*) as count
        FROM `nyc_taxi_data.monthly_trip_metrics`
        LIMIT 1
        """
        result = client.query(check_query).result()
        row = list(result)[0]
        return row.count > 0
    except Exception as e:
        st.error(f"Error checking data availability: {str(e)}")
        return False

def load_monthly_metrics():
    """Load monthly trip metrics from transformed table"""
    try:
        client = init_bigquery_client()
        metrics_query = """
        SELECT *
        FROM `nyc_taxi_data.monthly_trip_metrics`
        ORDER BY year_month
        """
        return client.query(metrics_query).to_dataframe()
    except Exception as e:
        st.error(f"Error loading monthly metrics: {str(e)}")
        return None

# Main dashboard
st.title("NYC Taxi Analysis Dashboard")

# Initialize BigQuery client
client = init_bigquery_client()

if client:
    st.sidebar.success("✅ Connected to BigQuery")

    # Check data availability
    if not check_data_availability():
        st.warning("""
        ⚠️ No transformed data available. Please follow these steps:
        1. Go to the Dataset Selection page
        2. Select and download taxi data
        3. Run the data transformations
        """)
        st.stop()

    # Load and display metrics
    metrics_data = load_monthly_metrics()

    if metrics_data is not None and not metrics_data.empty:
        # Create time series plot with dual y-axis
        fig_metrics = go.Figure()

        # Add trip count line
        fig_metrics.add_trace(go.Scatter(
            x=metrics_data['year_month'],
            y=metrics_data['total_trips'],
            name='Number of Trips',
            line=dict(color='blue')
        ))

        # Add revenue line on secondary y-axis
        fig_metrics.add_trace(go.Scatter(
            x=metrics_data['year_month'],
            y=metrics_data['total_revenue'],
            name='Total Revenue ($)',
            line=dict(color='green'),
            yaxis='y2'
        ))

        fig_metrics.update_layout(
            title='Monthly Trips and Revenue',
            xaxis=dict(title='Month'),
            yaxis=dict(title='Number of Trips', showgrid=False),
            yaxis2=dict(title='Total Revenue ($)', overlaying='y', side='right'),
            height=600,
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )

        st.plotly_chart(fig_metrics, use_container_width=True)

        # Display summary metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Trips", f"{metrics_data['total_trips'].sum():,.0f}")
        with col2:
            st.metric("Average Fare", f"${metrics_data['avg_fare'].mean():.2f}")
        with col3:
            st.metric("Total Revenue", f"${metrics_data['total_revenue'].sum():,.2f}")

        # Additional metrics
        st.subheader("Geographical Distribution")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Unique Pickup Zones", metrics_data['unique_pickup_zones'].max())
        with col2:
            st.metric("Unique Dropoff Zones", metrics_data['unique_dropoff_zones'].max())

        # Show source information
        st.info(f"""
        Data Source: {metrics_data['source_type'].iloc[0]}
        Last Updated: {metrics_data['last_updated'].iloc[0]}
        """)
    else:
        st.warning("No monthly metrics available")
else:
    st.error("❌ Not connected to BigQuery")