import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date
from data_ingestion import process_selected_data
from bigquery_setup import setup_bigquery_tables
from data_transformer import transform_taxi_data
from google.cloud import bigquery
from google.oauth2 import service_account

def load_monthly_metrics():
    """Load monthly trip metrics from transformed table"""
    try:
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        client = bigquery.Client(credentials=credentials, 
                                project=st.secrets["gcp_service_account"]["project_id"])

        metrics_query = """
        SELECT *
        FROM `nyc_taxi_data.monthly_trip_metrics`
        ORDER BY year_month
        """
        return client.query(metrics_query).to_dataframe()
    except Exception as e:
        st.error(f"Error loading monthly metrics: {str(e)}")
        return None

def load_zone_flows():
    """Load pickup to dropoff zone flows"""
    try:
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        client = bigquery.Client(credentials=credentials, 
                                project=st.secrets["gcp_service_account"]["project_id"])

        dataset_type = st.session_state.get('dataset', '').lower().replace(' ', '_')
        table_name = f"taxi_trips_optimized_{dataset_type}"

        # Get the pickup datetime field based on dataset type
        if dataset_type == "yellow_taxi":
            pickup_field = "tpep_pickup_datetime"
        elif dataset_type == "green_taxi":
            pickup_field = "lpep_pickup_datetime"
        else:
            pickup_field = "pickup_datetime"

        # Get date range from session state
        start_year = st.session_state.get('start_year')
        start_month = st.session_state.get('start_month')
        end_year = st.session_state.get('end_year')
        end_month = st.session_state.get('end_month')

        if not all([start_year, start_month, end_year, end_month]):
            st.error("Missing date range parameters")
            return None

        # Create date range strings
        start_date = f"{start_year}-{start_month:02d}-01"
        if end_month == 12:
            end_date = f"{end_year + 1}-01-01"
        else:
            end_date = f"{end_year}-{(end_month + 1):02d}-01"

        # First get total trips after filtering
        total_query = f"""
        WITH fare_percentiles AS (
            SELECT
                APPROX_QUANTILES(fare_amount, 100) as percentiles
            FROM `nyc_taxi_data.{table_name}`
            WHERE {pickup_field} >= TIMESTAMP("{start_date}")
              AND {pickup_field} < TIMESTAMP("{end_date}")
              AND fare_amount > 0
        )
        SELECT 
            COUNT(*) as total_filtered_trips
        FROM `nyc_taxi_data.{table_name}`
        WHERE {pickup_field} >= TIMESTAMP("{start_date}")
          AND {pickup_field} < TIMESTAMP("{end_date}")
          AND fare_amount > (SELECT percentiles[OFFSET(5)] FROM fare_percentiles)
          AND fare_amount < (SELECT percentiles[OFFSET(95)] FROM fare_percentiles)
        """

        total_result = client.query(total_query).result()
        total_filtered_trips = list(total_result)[0].total_filtered_trips

        # Then get zone flows with the same filtering
        flow_query = f"""
        WITH fare_percentiles AS (
            SELECT
                APPROX_QUANTILES(fare_amount, 100) as percentiles
            FROM `nyc_taxi_data.{table_name}`
            WHERE {pickup_field} >= TIMESTAMP("{start_date}")
              AND {pickup_field} < TIMESTAMP("{end_date}")
              AND fare_amount > 0
        ),
        filtered_trips AS (
            SELECT *
            FROM `nyc_taxi_data.{table_name}`
            WHERE {pickup_field} >= TIMESTAMP("{start_date}")
              AND {pickup_field} < TIMESTAMP("{end_date}")
              AND fare_amount > (SELECT percentiles[OFFSET(5)] FROM fare_percentiles)
              AND fare_amount < (SELECT percentiles[OFFSET(95)] FROM fare_percentiles)
        )
        SELECT 
            PULocationID as pickup_zone,
            DOLocationID as dropoff_zone,
            COUNT(*) as trip_count,
            AVG(fare_amount) as avg_fare,
            SUM(fare_amount) as total_fare
        FROM filtered_trips
        GROUP BY pickup_zone, dropoff_zone
        HAVING trip_count > 100
        ORDER BY trip_count DESC
        LIMIT 20  -- Top 20 flows for visualization clarity
        """

        flow_data = client.query(flow_query).to_dataframe()
        if not flow_data.empty:
            # Add total filtered trips for reference
            flow_data.attrs['total_filtered_trips'] = total_filtered_trips
            # Calculate sum of displayed flows
            flow_data.attrs['displayed_trips'] = flow_data['trip_count'].sum()

        return flow_data
    except Exception as e:
        st.error(f"Error loading zone flows: {str(e)}")
        return None

st.set_page_config(
    page_title="NYC Taxi Data Selection",
    layout="wide"
)

# Initialize session state for tracking progress
if 'download_progress' not in st.session_state:
    st.session_state.download_progress = {}
if 'upload_progress' not in st.session_state:
    st.session_state.upload_progress = {}
if 'bigquery_status' not in st.session_state:
    st.session_state.bigquery_status = "not_started"
if 'transformation_status' not in st.session_state:
    st.session_state.transformation_status = "not_started"

st.title("NYC Taxi Dataset Selection")

# Dataset selection
selected_dataset = st.selectbox(
    "Select a dataset to analyze",
    ["Yellow Taxi", "Green Taxi"],
    index=0
)

# Time period selection with extended range
st.subheader("Select Time Period")

# Get current date for range validation
current_date = datetime.now()
max_year = current_date.year + 1  # Allow scheduling one year into the future

# Create columns for year and month selection
col1, col2, col3, col4 = st.columns(4)

with col1:
    start_year = st.selectbox("Start Year", range(2019, max_year + 1), index=0)
with col2:
    start_month = st.selectbox("Start Month", range(1, 13), index=0, format_func=lambda x: f"{x:02d}")
with col3:
    end_year = st.selectbox("End Year", range(2019, max_year + 1), index=current_date.year - 2019)
with col4:
    end_month = st.selectbox("End Month", range(1, 13), index=current_date.month - 1, format_func=lambda x: f"{x:02d}")

# Add overwrite option before confirm button
overwrite = st.checkbox("Overwrite existing data", value=False, 
                      help="If checked, all existing data will be deleted before uploading new data")

if st.button("Confirm Selection"):
    # Store selections in session state
    st.session_state.update({
        'dataset': selected_dataset,
        'start_year': start_year,
        'start_month': start_month,
        'end_year': end_year,
        'end_month': end_month,
        'overwrite': overwrite,
        'download_progress': {},
        'upload_progress': {},
        'bigquery_status': "not_started",
        'transformation_status': "not_started"
    })

    # Format the time period for display
    start_date = f"{start_year}-{start_month:02d}"
    end_date = f"{end_year}-{end_month:02d}"

    st.success(f"""
    ✅ Dataset configuration saved:
    - Dataset: {selected_dataset}
    - Time period: {start_date} to {end_date}
    - Data handling: {"Overwrite existing" if overwrite else "Keep existing"}
    """)

# Pipeline Progress Tracking
st.header("Pipeline Progress")

# Create tabs for each stage
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📥 Data Download", 
    "☁️ GCS Upload", 
    "🔄 BigQuery Load",
    "📊 Transformation",
    "📈 Dashboard"
])

with tab1:
    total_files = len(st.session_state.get('download_progress', {}))
    if total_files > 0:
        completed = sum(1 for status in st.session_state.download_progress.values() if status)
        progress = completed / total_files
        st.progress(progress, text=f"Downloaded {completed}/{total_files} files")

    if st.button("Start Download", key="download_btn"):
        st.session_state.bigquery_status = "not_started"
        st.session_state.transformation_status = "not_started"
        process_selected_data()

with tab2:
    total_uploads = len(st.session_state.get('upload_progress', {}))
    if total_uploads > 0:
        completed_uploads = sum(1 for status in st.session_state.upload_progress.values() if status)
        upload_progress = completed_uploads / total_uploads
        st.progress(upload_progress, text=f"Uploaded {completed_uploads}/{total_uploads} files")
        if completed_uploads == total_uploads:
            st.success("✅ All files uploaded successfully")

with tab3:
    if st.session_state.bigquery_status == "completed":
        st.success("✅ BigQuery tables created successfully")
    elif st.session_state.bigquery_status == "in_progress":
        st.info("⏳ Creating BigQuery tables...")
    elif st.session_state.bigquery_status == "failed":
        st.error("❌ BigQuery table creation failed")

    # Enable BigQuery setup if files are uploaded or skipped
    can_setup_bigquery = (
        any(st.session_state.upload_progress.values()) or 
        (not st.session_state.get('overwrite', False) and 
         st.session_state.get('dataset') is not None)
    )

    if st.button("Setup BigQuery", key="bigquery_btn", disabled=not can_setup_bigquery):
        st.session_state.bigquery_status = "in_progress"
        st.session_state.transformation_status = "not_started"
        if setup_bigquery_tables():
            st.session_state.bigquery_status = "completed"
        else:
            st.session_state.bigquery_status = "failed"

with tab4:
    if st.session_state.transformation_status == "completed":
        st.success("✅ Data transformations completed")
        st.info("Click the Dashboard tab to view the visualizations!")
    elif st.session_state.transformation_status == "in_progress":
        st.info("⏳ Running data transformations...")
    elif st.session_state.bigquery_status != "completed":
        st.warning("⚠️ Please complete BigQuery setup first")
    else:
        if st.button("Run Data Transformations"):
            st.session_state.transformation_status = "in_progress"
            if transform_taxi_data():
                st.session_state.transformation_status = "completed"
                st.success("✅ Data transformations completed successfully!")
            else:
                st.session_state.transformation_status = "failed"
                st.error("❌ Data transformation failed")

with tab5:
    if st.session_state.transformation_status != "completed":
        st.warning("⚠️ Please complete the data transformations first to view the dashboard")
    else:
        # Load data
        metrics_data = load_monthly_metrics()
        flow_data = load_zone_flows()

        if metrics_data is not None and not metrics_data.empty:
            # Create two columns for key metrics
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Trips", f"{metrics_data['total_trips'].sum():,.0f}")
                st.metric("Average Fare", f"${metrics_data['avg_fare'].mean():.2f}")
            with col2:
                st.metric("Total Revenue", f"${metrics_data['total_revenue'].sum():,.2f}")
                st.metric("Time Period", 
                         f"{metrics_data['year_month'].min().strftime('%Y-%m')} to "
                         f"{metrics_data['year_month'].max().strftime('%Y-%m')}")

            # Monthly averages table with better formatting
            st.subheader("Monthly Averages")

            # Calculate metrics only for the selected date range
            period_metrics = metrics_data.copy()
            period_trips = int(period_metrics['total_trips'].sum())
            period_revenue = float(period_metrics['total_revenue'].sum())
            period_avg_distance = float(period_metrics['avg_distance'].mean())
            period_avg_fare = float(period_metrics['avg_fare'].mean())
            period_pickup_zones = int(period_metrics['unique_pickup_zones'].max())
            period_dropoff_zones = int(period_metrics['unique_dropoff_zones'].max())

            # Display period metrics in columns
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric(
                    "Total Trips",
                    f"{period_trips:,.0f}",
                    help="Total number of trips in selected period"
                )
                st.metric(
                    "Avg Trip Distance",
                    f"{period_avg_distance:.1f} miles",
                    help="Average distance per trip"
                )
            with col2:
                st.metric(
                    "Total Revenue",
                    f"${period_revenue:,.2f}",
                    help="Total revenue in selected period"
                )
                st.metric(
                    "Active Pickup Zones",
                    f"{period_pickup_zones:,.0f}",
                    help="Number of unique pickup zones"
                )
            with col3:
                st.metric(
                    "Avg Fare",
                    f"${period_avg_fare:.2f}",
                    help="Average fare per trip"
                )
                st.metric(
                    "Active Dropoff Zones",
                    f"{period_dropoff_zones:,.0f}",
                    help="Number of unique dropoff zones"
                )

            # Monthly Performance
            st.subheader("Monthly Performance")

            # Line charts
            col1, col2 = st.columns(2)

            with col1:
                # Trips line chart
                fig_trips = go.Figure()
                fig_trips.add_trace(go.Scatter(
                    x=metrics_data['year_month'].dt.strftime('%Y-%m'),
                    y=metrics_data['total_trips'],
                    name='Total Trips',
                    mode='lines+markers',
                    line=dict(color='blue', width=2),
                    marker=dict(size=8)
                ))
                fig_trips.update_layout(
                    title='Monthly Trips',
                    xaxis=dict(
                        title='Month',
                        type='category',
                        tickmode='array',
                        ticktext=metrics_data['year_month'].dt.strftime('%Y-%m'),
                        tickvals=metrics_data['year_month'].dt.strftime('%Y-%m')
                    ),
                    yaxis=dict(title='Number of Trips'),
                    height=300,
                    margin=dict(l=20, r=20, t=40, b=20)
                )
                st.plotly_chart(fig_trips, use_container_width=True)

            with col2:
                # Revenue line chart
                fig_revenue = go.Figure()
                fig_revenue.add_trace(go.Scatter(
                    x=metrics_data['year_month'].dt.strftime('%Y-%m'),
                    y=metrics_data['total_revenue'],
                    name='Total Revenue',
                    mode='lines+markers',
                    line=dict(color='green', width=2),
                    marker=dict(size=8)
                ))
                fig_revenue.update_layout(
                    title='Monthly Revenue',
                    xaxis=dict(
                        title='Month',
                        type='category',
                        tickmode='array',
                        ticktext=metrics_data['year_month'].dt.strftime('%Y-%m'),
                        tickvals=metrics_data['year_month'].dt.strftime('%Y-%m')
                    ),
                    yaxis=dict(title='Revenue ($)'),
                    height=300,
                    margin=dict(l=20, r=20, t=40, b=20)
                )
                st.plotly_chart(fig_revenue, use_container_width=True)

            # Fare Percentiles Chart
            st.subheader("Monthly Fare Distribution")

            fig_percentiles = go.Figure()

            # Add median line
            fig_percentiles.add_trace(go.Scatter(
                x=metrics_data['year_month'].dt.strftime('%Y-%m'),
                y=metrics_data['fare_median'],
                name='Median Fare',
                mode='lines+markers',
                line=dict(color='blue', width=2),
                marker=dict(size=8)
            ))

            # Add 25th percentile line
            fig_percentiles.add_trace(go.Scatter(
                x=metrics_data['year_month'].dt.strftime('%Y-%m'),
                y=metrics_data['fare_25th'],
                name='25th Percentile',
                mode='lines+markers',
                line=dict(color='lightblue', width=2),
                marker=dict(size=8)
            ))

            # Add 75th percentile line
            fig_percentiles.add_trace(go.Scatter(
                x=metrics_data['year_month'].dt.strftime('%Y-%m'),
                y=metrics_data['fare_75th'],
                name='75th Percentile',
                mode='lines+markers',
                line=dict(color='darkblue', width=2),
                marker=dict(size=8)
            ))

            fig_percentiles.update_layout(
                title='Monthly Fare Distribution',
                xaxis=dict(
                    title='Month',
                    type='category',
                    tickmode='array',
                    ticktext=metrics_data['year_month'].dt.strftime('%Y-%m'),
                    tickvals=metrics_data['year_month'].dt.strftime('%Y-%m')
                ),
                yaxis=dict(title='Fare Amount ($)'),
                height=400,
                margin=dict(l=20, r=20, t=40, b=20),
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )

            st.plotly_chart(fig_percentiles, use_container_width=True)

            st.info("""
            **Fare Distribution Details:**
            - Blue line shows median fare (50th percentile)
            - Light blue line shows 25th percentile (lower quartile)
            - Dark blue line shows 75th percentile (upper quartile)
            - The space between 25th and 75th percentiles represents where 50% of fares fall
            - Data excludes extreme outliers (below 5th and above 95th percentiles)
            """)


            # Zone Flow Sankey Diagram
            st.subheader("Top Pickup to Dropoff Zone Flows")
            if flow_data is not None and not flow_data.empty:
                # Show trip count information
                total_filtered = flow_data.attrs.get('total_filtered_trips', 0)
                displayed_trips = flow_data.attrs.get('displayed_trips', 0)

                st.info(f"""
                **Trip Count Details:**
                - Total trips after removing fare outliers: {total_filtered:,}
                - Trips shown in top 20 flows: {displayed_trips:,} ({(displayed_trips/total_filtered*100):.1f}% of filtered trips)
                - Note: Only showing flows with >100 trips between zones
                """)

                # Convert zone IDs to strings consistently
                flow_data['pickup_zone'] = flow_data['pickup_zone'].astype(str)
                flow_data['dropoff_zone'] = flow_data['dropoff_zone'].astype(str)

                # Create distinct colors for zones
                zones = sorted(set(flow_data['pickup_zone'].unique()) | set(flow_data['dropoff_zone'].unique()))
                colors = px.colors.qualitative.Set3 + px.colors.qualitative.Pastel + px.colors.qualitative.Safe
                zone_colors = dict(zip(zones, colors[:len(zones)]))

                try:
                    fig_sankey = go.Figure(data=[go.Sankey(
                        node=dict(
                            pad=15,
                            thickness=20,
                            line=dict(color="black", width=0.5),
                            label=zones,
                            color=[zone_colors[zone] for zone in zones]
                        ),
                        link=dict(
                            source=[zones.index(str(x)) for x in flow_data['pickup_zone']],
                            target=[zones.index(str(x)) for x in flow_data['dropoff_zone']],
                            value=flow_data['trip_count'],
                            color=flow_data['avg_fare'].apply(
                                lambda x: f'rgba(0,100,255,{min(x/100, 0.8)})'
                            )
                        )
                    )])

                    fig_sankey.update_layout(
                        title="Top Zone Flows",
                        font_size=10,
                        height=400
                    )

                    st.plotly_chart(fig_sankey, use_container_width=True)

                    # Show flow details in an expander
                    with st.expander("View Zone Flow Details"):
                        st.dataframe(
                            flow_data.style.format({
                                'avg_fare': '${:.2f}',
                                'total_fare': '${:,.2f}',
                                'trip_count': '{:,}'
                            })
                        )
                except Exception as e:
                    st.error(f"Error creating Sankey diagram: {str(e)}")
                    st.write("Flow data for debugging:", flow_data.head())

                # Source information
                st.info(f"""
                Data Source: {metrics_data['source_type'].iloc[0]}
                Last Updated: {metrics_data['last_updated'].iloc[0]}
                Period: {metrics_data['year_month'].min().strftime('%Y-%m')} to {metrics_data['year_month'].max().strftime('%Y-%m')}
                """)
        else:
            st.error("No data available. Please ensure the data transformation completed successfully.")

# Schedule Information
st.header("📅 Schedule Information")
st.info("""
This pipeline can be scheduled to run automatically to fetch new data as it becomes available.
Current schedule:
- Checks for new data daily
- Processes data up to the selected end date
- Automatically updates visualizations
""")

if st.checkbox("Show progress details"):
    st.write("Download Progress:", st.session_state.get('download_progress', {}))
    st.write("Upload Progress:", st.session_state.get('upload_progress', {}))
    st.write("BigQuery Status:", st.session_state.bigquery_status)
    st.write("Transformation Status:", st.session_state.transformation_status)