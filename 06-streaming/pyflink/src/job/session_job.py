from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, DataTypes
from pyflink.common.time import Duration

def create_events_source_kafka(t_env):
    """Creates a Kafka source table for taxi events."""
    table_name = "green_trips"
    pattern = "yyyy-MM-dd HH:mm:ss"

    # Drop the existing table if it exists
    t_env.execute_sql(f"DROP TABLE IF EXISTS {table_name}")

    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime STRING,
            lpep_dropoff_datetime STRING,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            event_time AS TO_TIMESTAMP(lpep_dropoff_datetime, '{pattern}'),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
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


def create_trip_streaks_sink(t_env):
    """Creates a PostgreSQL sink table for longest trip streaks."""
    table_name = "trip_streaks"

    sink_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            longest_streak BIGINT,
            PRIMARY KEY (PULocationID, DOLocationID) NOT ENFORCED
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


def process_taxi_trips():
    """Sets up the Flink job to process taxi trips using session windows."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka source and PostgreSQL sink
        source_table = create_events_source_kafka(t_env)
        sink_table = create_trip_streaks_sink(t_env)

        print("✅ PostgreSQL Sink Table Created!")

        # Corrected SQL Query for Session Windows
        query = f"""
            INSERT INTO {sink_table}
            SELECT 
                PULocationID, 
                DOLocationID, 
                COUNT(*) AS longest_streak
            FROM {source_table}
            GROUP BY PULocationID, DOLocationID, 
                     SESSION(event_time, INTERVAL '5' MINUTES);
        """

        t_env.execute_sql(query).wait()
        print("✅ Flink job successfully processed session windowed data!")

    except Exception as e:
        print("❌ Flink job failed:", str(e))


if __name__ == '__main__':
    process_taxi_trips()
