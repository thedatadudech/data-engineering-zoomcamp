from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, StreamTableEnvironment

def create_taxi_events_sink_postgres(t_env):
    """Creates a PostgreSQL sink table for taxi events."""
    table_name = "taxi_events_sink"

    # Drop the table if it already exists

    sink_ddl = f"""
        CREATE TABLE {table_name} (
            VendorID INTEGER,
            lpep_pickup_datetime TIMESTAMP(3),
            lpep_dropoff_datetime TIMESTAMP(3),
            store_and_fwd_flag STRING,
            RatecodeID INTEGER,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            fare_amount DOUBLE,
            extra DOUBLE,
            mta_tax DOUBLE,
            tip_amount DOUBLE,
            tolls_amount DOUBLE,
            ehail_fee DOUBLE,
            improvement_surcharge DOUBLE,
            total_amount DOUBLE,
            payment_type INTEGER,
            trip_type INTEGER,
            congestion_surcharge DOUBLE
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_events_source_kafka(t_env):
    """Creates a Kafka source table for taxi events."""
    table_name = "taxi_events_source"
    pattern = "yyyy-MM-dd HH:mm:ss"

    # Drop the existing table if it exists
    t_env.execute_sql(f"DROP TABLE IF EXISTS {table_name}")

    source_ddl = f"""
        CREATE TABLE {table_name} (
            VendorID INTEGER,
            lpep_pickup_datetime STRING,
            lpep_dropoff_datetime STRING,
            store_and_fwd_flag STRING,
            RatecodeID INTEGER,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            fare_amount DOUBLE,
            extra DOUBLE,
            mta_tax DOUBLE,
            tip_amount DOUBLE,
            tolls_amount DOUBLE,
            ehail_fee DOUBLE,
            improvement_surcharge DOUBLE,
            total_amount DOUBLE,
            payment_type INTEGER,
            trip_type INTEGER,
            congestion_surcharge DOUBLE,
            pickup_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, '{pattern}'),
            WATERMARK FOR pickup_timestamp AS pickup_timestamp - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name


def log_processing():
    """Sets up the Flink job to process taxi events."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka source table and PostgreSQL sink table
        source_table = create_events_source_kafka(t_env)
        print("Creating PostgreSQL Sink Table...")
        postgres_sink = create_taxi_events_sink_postgres(t_env)
        print("✅ PostgreSQL Sink Table Created!")

        # Insert specific fields from Kafka to PostgreSQL
        t_env.execute_sql(
            f"""
            INSERT INTO {postgres_sink}
            SELECT
                VendorID,
                TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
                TO_TIMESTAMP(lpep_dropoff_datetime, 'yyyy-MM-dd HH:mm:ss'),
                store_and_fwd_flag,
                RatecodeID,
                PULocationID,
                DOLocationID,
                COALESCE(CAST(passenger_count AS INT), 0) AS passenger_count,
                trip_distance,
                fare_amount,
                extra,
                mta_tax,
                tip_amount,
                tolls_amount,
                ehail_fee,
                improvement_surcharge,
                total_amount,
                payment_type,
                trip_type,
                congestion_surcharge
            FROM {source_table}
            """
        ).wait()

        print("✅ Flink job successfully inserted data from Kafka to PostgreSQL!")

    except Exception as e:
        print("❌ Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_processing()
