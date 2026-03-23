from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
import json
import traceback
import time


LOG_PATH = "/home/vytautas.peace/data-engineering-zoomcamp/.cursor/debug-d0d984.log"


def _debug_log(hypothesisId: str, location: str, message: str, data: dict | None = None) -> None:
    """Append a single NDJSON debug record to the shared log file."""
    payload = {
        "sessionId": "d0d984",
        "runId": "pre_fix",
        "hypothesisId": hypothesisId,
        "location": location,
        "message": message,
        "data": data or {},
        "timestamp": int(time.time() * 1000),
    }
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        # Logging must never break the Flink job.
        pass


def create_events_source_kafka(t_env):
    table_name = "events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            pickup_datetime VARCHAR,
            dropoff_datetime VARCHAR,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            event_timestamp AS TO_TIMESTAMP(pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK for event_timestamp as event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def create_events_aggregated_sink(t_env):
    table_name = 'processed_events_aggregated'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT,
            total_revenue DOUBLE,
            PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
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


def log_aggregation():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    # #region agent log
    _debug_log(
        hypothesisId="H_job_start",
        location="hw_tumbling_window.py:log_aggregation_start",
        message="Starting Flink tumbling aggregation job",
        data={"sink_table": "processed_events_aggregated", "kafka_topic": "green-trips"},
    )
    # #endregion

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # #region agent log
        _debug_log(
            hypothesisId="H_job_start",
            location="hw_tumbling_window.py:create_tables_before_insert",
            message="Creating source + sink tables before INSERT",
        )
        # #endregion

        source_table = create_events_source_kafka(t_env)
        aggregated_table = create_events_aggregated_sink(t_env)

        # #region agent log
        _debug_log(
            hypothesisId="H_job_start",
            location="hw_tumbling_window.py:before_insert",
            message="About to execute INSERT INTO JDBC sink",
        )
        # #endregion

        t_env.execute_sql(f"""
        INSERT INTO {aggregated_table}
        SELECT
            window_start,
            PULocationID,
            COUNT(*) AS num_trips,
            SUM(total_amount) AS total_revenue
        FROM TABLE(
            TUMBLE(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
        )
        GROUP BY window_start, PULocationID;

        """).wait()

    except Exception as e:
        # #region agent log
        _debug_log(
            hypothesisId="H_job_failure",
            location="hw_tumbling_window.py:exception_handler",
            message="Flink job failed while writing to JDBC",
            data={"error": str(e), "traceback": traceback.format_exc(limit=2000)},
        )
        # #endregion
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_aggregation()