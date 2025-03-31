# NYC Taxi Data Pipeline & Analytics Dashboard

![NYC Taxi Data Pipeline](attached_assets/NYC_project.png)

## Problem Statement

The NYC Taxi & Limousine Commission regularly releases large datasets of taxi trip records, but analyzing this data efficiently requires a robust data pipeline and visualization system. This project solves the challenge of:

- Automated data ingestion from NYC TLC data sources
- Processing and transforming large-scale taxi trip data
- Efficient storage and querying capabilities
- Interactive visualization and analysis of taxi trip patterns

## Project Overview

This project implements an end-to-end data pipeline that:

1. **Data Selection & Ingestion**:

   - Allows users to choose between different NYC taxi datasets (Yellow/Green)
   - Supports custom date range selection
   - Automatically downloads and processes TLC trip data

2. **Data Processing & Storage**:

   - Transforms raw taxi data into analytics-ready format
   - Loads processed data into Google BigQuery for efficient querying
   - Maintains data quality and consistency

3. **Analytics Dashboard**:
   - Visualizes key metrics including:
     - Number of trips over time
     - Total fare amounts and revenue analysis
     - Geographic distribution of trips
   - Provides interactive filters and drill-down capabilities

## Prerequisites

- Python 3.11 or higher
- Google Cloud Platform account
- GCP service account credentials (JSON key file)
- Git

## Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd TaxiDataPipeline
```

2. Create and activate a Python virtual environment:

```bash
python -m venv .venv

# On Windows
.venv\Scripts\activate

# On macOS/Linux
source .venv/bin/activate
```

3. Install `uv` package manager:

```bash
pip install uv
```

4. Install project dependencies:

```bash
uv pip sync
# or
uv sync
```

## Configuration

1. Create a `.streamlit/secrets.toml` file in the project root:

```toml
[gcp_service_account]
type = "service_account"
project_id = "your-project-id"
private_key_id = "your-private-key-id"
private_key = "your-private-key"
client_email = "your-client-email"
client_id = "your-client-id"
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
client_x509_cert_url = "your-cert-url"
```

Replace all placeholder values with your actual GCP service account credentials.

## Running the Application

To run the dataset selection interface:

```bash
streamlit run dataset_selection.py --server.port 5001 --server.headless true
```

## Project Structure

- `dataset_selection.py`: Main Streamlit application for data selection and visualization
- `data_ingestion.py`: Handles automated data ingestion from NYC TLC sources
- `data_transformer.py`: Processes and transforms taxi trip data
- `bigquery_setup.py`: Manages BigQuery operations and data warehouse setup
- `dashboard/`: Contains dashboard components and visualizations
- `terraform/`: Infrastructure as Code for GCP resource provisioning
- `.streamlit/`: Contains Streamlit configuration and secrets
- `attached_assets/`: Contains project assets and images

## Features

- Interactive dataset selection (Yellow/Green taxi data)
- Custom date range filtering
- Automated data ingestion and processing
- Real-time analytics dashboard
- Geographic visualization of trip data
- Revenue and trip count analysis
- BigQuery integration for efficient data warehousing

## Dependencies

Main project dependencies include:

- Streamlit
- Google Cloud BigQuery
- Pandas
- Folium
- Plotly
- And other packages as specified in `pyproject.toml`

## Architecture

Data Source (NYC TLC) → Data Ingestion → Data Transformation → BigQuery → Dashboard
↑
User Interface

## Notes

- Ensure your GCP service account has the necessary permissions for BigQuery and other GCP services
- Keep your credentials secure and never commit them to version control
- The application runs on port 5001 by default
- Data is processed in chunks to handle large datasets efficiently
- The dashboard updates in real-time as new data is processed

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- NYC Taxi & Limousine Commission for providing the public dataset
- DataTalks.Club for project inspiration and guidance
