from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable

# Default args
default_args = {
    'owner': 'mta-team',
    'depends_on_past': False,
    'retries': 3,
}

with DAG(
    dag_id='mta_hourly_aggregations',
    default_args=default_args,
    description='Hourly metrics for MTA GTFS-RT: unique routes, trips, delays, headway, travel-time deltas, and vehicle counts',
    schedule_interval='0 * * * *',  # top of every hour
    start_date=days_ago(1),
    max_active_runs=1,
    catchup=False,
    tags=['gtfs_rt', 'bigquery', 'hourly'],
) as dag:
    # Fetch project & dataset names from Airflow Variables
    project = Variable.get('project')
    raw_ds = Variable.get('raw_dataset')
    staging_ds = Variable.get('staging_dataset')
    analytics_ds = Variable.get('analytics_dataset')
    tz = 'America/New_York'

    # 0) Stage raw updates for the last hour
    stage_hourly = BigQueryInsertJobOperator(
        task_id='stage_hourly_trip_info',
        location='US',
        configuration={'query': {
            'query': f"""
    CREATE OR REPLACE TABLE `{project}.{staging_ds}.staging_hourly_trip_info`
    PARTITION BY date
    CLUSTER BY trainline, route AS
    SELECT
      trip_id,
      stop_id,
      partition_timestamp,
      EXTRACT(DATE  FROM partition_timestamp AT TIME ZONE '{tz}') AS date,
      EXTRACT(HOUR  FROM partition_timestamp AT TIME ZONE '{tz}') AS hour,
      REGEXP_EXTRACT(trip_id, r"^([0-9]+)_")             AS dispatch_time,
      REGEXP_EXTRACT(trip_id, r"_([A-Za-z0-9]+)\.{{1,2}}")  AS trainline,
      REGEXP_EXTRACT(trip_id, r"\.{{1,2}}([A-Za-z0-9]+)$")  AS route,
      SUBSTR(REGEXP_EXTRACT(trip_id, r"\.{{1,2}}([A-Za-z0-9]+)$"),1,1) AS direction,
      arrival_time,
      departure_time,
      scheduled_track,
      actual_track,
      direction_id
    FROM `{project}.{raw_ds}.trip_updates`
    WHERE partition_timestamp >= TIMESTAMP('{{{{ execution_date }}}}')
      AND partition_timestamp < TIMESTAMP_ADD(TIMESTAMP('{{{{ execution_date }}}}'), INTERVAL 1 HOUR)
    """,
            'useLegacySql': False,
            # no writeDisposition here, since DDL ignores it
        }},
    )


    # 1) Unique routes per line
    unique_routes = BigQueryInsertJobOperator(
        task_id='hourly_unique_routes_per_line',
        location='US',
        configuration={'query': {
            'query': f"""
                INSERT INTO `{project}.{analytics_ds}.hourly_unique_routes_per_line` (date, hour, trainline, unique_routes)
                SELECT date, hour, trainline, COUNT(DISTINCT route) AS unique_routes
                FROM `{project}.{staging_ds}.staging_hourly_trip_info`
                GROUP BY date, hour, trainline
                """,
            'useLegacySql': False,

        }},
    )

    # 2) Trips per direction
    trips_per_route = BigQueryInsertJobOperator(
        task_id='hourly_trips_per_direction',
        location='US',
        configuration={'query': {
            'query': f"""
                INSERT INTO `{project}.{analytics_ds}.hourly_trips_per_route` (date, hour, trainline, route, num_trips)
                SELECT date, hour, trainline, direction, COUNT(DISTINCT trip_id) AS num_trips
                FROM `{project}.{staging_ds}.staging_hourly_trip_info`
                GROUP BY date, hour, trainline, direction
                """,
            'useLegacySql': False,
        }},
    )

    # 3) Routes affected by alerts per line
    routes_delayed = BigQueryInsertJobOperator(
        task_id='hourly_routes_delayed_per_line',
        location='US',
        configuration={'query': {
            'query': f"""
                INSERT INTO `{project}.{analytics_ds}.hourly_routes_delayed_per_line` (date, hour, trainline, delayed_routes)
                WITH exploded AS (
                SELECT
                    REGEXP_EXTRACT(trip, r"_([A-Za-z0-9]+)\.{{1,2}}") AS trainline,
                    trip,
                    partition_timestamp
                FROM `{project}.{raw_ds}.alerts` AS a,
                    UNNEST(a.informed_trip_ids) AS trip
                WHERE partition_timestamp >= TIMESTAMP('{{{{ execution_date }}}}')
                    AND partition_timestamp <  TIMESTAMP_ADD(TIMESTAMP('{{{{ execution_date }}}}'), INTERVAL 1 HOUR)
                )
                SELECT
                EXTRACT(DATE  FROM partition_timestamp AT TIME ZONE '{tz}') AS date,
                EXTRACT(HOUR  FROM partition_timestamp AT TIME ZONE '{tz}') AS hour,
                trainline,
                COUNT(DISTINCT trip) AS delayed_routes
                FROM exploded
                GROUP BY date, hour, trainline
                """,
            'useLegacySql': False,
        }},
    )

    # 4) Average headway at station per line
    avg_headway = BigQueryInsertJobOperator(
        task_id='hourly_avg_headway',
        location='US',
        configuration={'query': {
            'query': f"""
                INSERT INTO `{project}.{analytics_ds}.hourly_avg_headway` (date, hour, station_id, trainline, avg_headway_seconds)
                WITH arrivals AS (
                SELECT
                    EXTRACT(DATE FROM partition_timestamp AT TIME ZONE '{tz}') AS date,
                    EXTRACT(HOUR FROM partition_timestamp AT TIME ZONE '{tz}') AS hour,
                    stop_id AS station_id,
                    REGEXP_EXTRACT(trip_id, r"_([A-Za-z0-9]+)\.{{1,2}}") AS trainline,
                    TIMESTAMP_SECONDS(arrival_time) AS ts
                FROM `{project}.{raw_ds}.trip_updates`
                WHERE partition_timestamp >= TIMESTAMP('{{{{ execution_date }}}}')
                    AND partition_timestamp < TIMESTAMP_ADD(TIMESTAMP('{{{{ execution_date }}}}'), INTERVAL 1 HOUR)
                ), diffs AS (
                SELECT
                    date, hour, station_id, trainline,
                    TIMESTAMP_DIFF(ts, LAG(ts) OVER(PARTITION BY station_id, trainline ORDER BY ts), SECOND) AS headway
                FROM arrivals
                )
                SELECT date, hour, station_id, trainline, AVG(headway) AS avg_headway_seconds
                FROM diffs
                WHERE headway IS NOT NULL
                GROUP BY date, hour, station_id, trainline
                """,
            'useLegacySql': False,

        }},
    )

    # 5) Average delta scheduled->final arrival by arrival time
    avg_delta = BigQueryInsertJobOperator(
        task_id='hourly_avg_delta_scheduled_actual',
        location='US',
        configuration={'query': {
            'query': f"""
                INSERT INTO `{project}.{analytics_ds}.hourly_avg_delta_scheduled_actual`
                    (date, hour, trainline, avg_delta_seconds)
                WITH updates_hour AS (
                SELECT
                    trip_id,
                    stop_id,
                    REGEXP_EXTRACT(trip_id, r"_([A-Za-z0-9]+)\.{{1,2}}") AS trainline,
                    partition_timestamp,
                    TIMESTAMP_SECONDS(arrival_time) AS arrival_ts
                FROM `{project}.{raw_ds}.trip_updates`
                WHERE TIMESTAMP_SECONDS(arrival_time) >= TIMESTAMP('{{{{ execution_date }}}}')
                    AND TIMESTAMP_SECONDS(arrival_time) < TIMESTAMP_ADD(TIMESTAMP('{{{{ execution_date }}}}'), INTERVAL 1 HOUR)
                    AND arrival_time IS NOT NULL
                    AND arrival_time > 0
                ),
                ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER(PARTITION BY trip_id, stop_id, trainline ORDER BY partition_timestamp)    AS rn_first,
                    ROW_NUMBER() OVER(PARTITION BY trip_id, stop_id, trainline ORDER BY partition_timestamp DESC) AS rn_last
                FROM updates_hour
                ),
                first_updates AS (
                SELECT trip_id, stop_id, trainline, arrival_ts AS scheduled_ts
                FROM ranked WHERE rn_first = 1
                ),
                last_updates AS (
                SELECT trip_id, stop_id, trainline, arrival_ts AS final_ts
                FROM ranked WHERE rn_last = 1
                ),
                deltas AS (
                SELECT
                    f.trainline,
                    TIMESTAMP_TRUNC(l.final_ts, HOUR, "America/New_York") AS bucket_hour,
                    UNIX_SECONDS(l.final_ts) - UNIX_SECONDS(f.scheduled_ts) AS delta_seconds
                FROM first_updates f
                JOIN last_updates l USING(trip_id, stop_id, trainline)
                )
                SELECT
                DATE(bucket_hour)              AS date,
                EXTRACT(HOUR FROM bucket_hour) AS hour,
                trainline,
                AVG(delta_seconds)             AS avg_delta_seconds
                FROM deltas
                GROUP BY date, hour, trainline
                ORDER BY trainline
                """,
            'useLegacySql': False,
        }},
    )

    # 6) Average in-service vehicles per line
    avg_vehicles = BigQueryInsertJobOperator(
        task_id='hourly_avg_vehicles_in_service',
        location='US',
        configuration={'query': {
            'query': f"""
                INSERT INTO `{project}.{analytics_ds}.hourly_avg_vehicles_in_service` (date, hour, trainline, avg_vehicles)
                WITH active AS (
                SELECT
                    EXTRACT(DATE FROM TIMESTAMP_SECONDS(timestamp) AT TIME ZONE '{tz}') AS date,
                    EXTRACT(HOUR FROM TIMESTAMP_SECONDS(timestamp) AT TIME ZONE '{tz}') AS hour,
                    REGEXP_EXTRACT(trip_id, r"_([A-Za-z0-9]+)\.{{1,2}}") AS trainline,
                    trip_id
                FROM `{project}.{raw_ds}.vehicle_positions`
                WHERE timestamp >= UNIX_SECONDS(TIMESTAMP('{{{{ execution_date }}}}'))
                    AND timestamp < UNIX_SECONDS(TIMESTAMP_ADD(TIMESTAMP('{{{{ execution_date }}}}'), INTERVAL 1 HOUR))
                    AND current_status IN ('IN_TRANSIT_TO','INCOMING_AT')
                )
                SELECT date, hour, trainline, COUNT(DISTINCT trip_id) AS avg_vehicles
                FROM active
                GROUP BY date, hour, trainline
                """,
            'useLegacySql': False,
        }},
    )

    # Define task dependencies
    stage_hourly >> [
        unique_routes,
        trips_per_route,
        routes_delayed,
        avg_headway,
        avg_delta,
        avg_vehicles
    ]
