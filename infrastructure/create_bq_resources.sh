#!/usr/bin/env bash
set -euo pipefail

# ─── Configuration ──────────────────────────────────────────────────────────────
PROJECT="mta-realtime-pipeline"          # e.g. mta-realtime-pipeline
RAW_DATASET="mta_realtime_data"          # raw feed landing zone
STAGING_DATASET="gtfs_rt_staging"  # staging for parsed hourly slices
ANALYTICS_DATASET="gtfs_rt_analytics"  # final hourly summary tables
LOCATION="US"                      # BigQuery location
# ────────────────────────────────────────────────────────────────────────────────

echo "Creating datasets (if not exists)..."
bq --project_id="$PROJECT" mk --location=$LOCATION --dataset $STAGING_DATASET || true
bq --project_id="$PROJECT" mk --location=$LOCATION --dataset $ANALYTICS_DATASET || true


echo "Creating staging table: $STAGING_DATASET.staging_hourly_trip_info"
bq --project_id="$PROJECT" mk --location=$LOCATION \
  --table \
  --time_partitioning_field date \
  --clustering_fields trainline,route \
  $STAGING_DATASET.staging_hourly_trip_info \
  trip_id:STRING,stop_id:STRING,partition_timestamp:TIMESTAMP,date:DATE,hour:INT64,dispatch_time:STRING,trainline:STRING,route:STRING,direction:STRING,arrival_time:INT64,departure_time:INT64,scheduled_track:STRING,actual_track:STRING,direction_id:INT64

echo "Creating analytics tables..."

bq --project_id="$PROJECT" mk --location=$LOCATION \
  --table \
  --time_partitioning_field date \
  --clustering_fields hour,trainline \
  $ANALYTICS_DATASET.hourly_unique_routes_per_line \
  date:DATE,hour:INT64,trainline:STRING,unique_routes:INT64

bq --project_id="$PROJECT" mk --location=$LOCATION \
  --table \
  --time_partitioning_field date \
  --clustering_fields hour,trainline \
  $ANALYTICS_DATASET.hourly_trips_per_route \
  date:DATE,hour:INT64,trainline:STRING,route:STRING,num_trips:INT64

bq --project_id="$PROJECT" mk --location=$LOCATION \
  --table \
  --time_partitioning_field date \
  --clustering_fields hour,trainline \
  $ANALYTICS_DATASET.hourly_routes_delayed_per_line \
  date:DATE,hour:INT64,trainline:STRING,delayed_routes:INT64

bq --project_id="$PROJECT" mk --location=$LOCATION \
  --table \
  --time_partitioning_field date \
  --clustering_fields hour,trainline \
  $ANALYTICS_DATASET.hourly_avg_headway \
  date:DATE,hour:INT64,station_id:STRING,trainline:STRING,avg_headway_seconds:FLOAT64

bq --project_id="$PROJECT" mk --location=$LOCATION \
  --table \
  --time_partitioning_field date \
  --clustering_fields hour,trainline \
  $ANALYTICS_DATASET.hourly_avg_delta_scheduled_actual \
  date:DATE,hour:INT64,trainline:STRING,avg_delta_seconds:FLOAT64

bq --project_id="$PROJECT" mk --location=$LOCATION \
  --table \
  --time_partitioning_field date \
  --clustering_fields hour,trainline \
  $ANALYTICS_DATASET.hourly_avg_vehicles_in_service \
  date:DATE,hour:INT64,trainline:STRING,avg_vehicles:INT64

echo "All datasets and tables created. Update PROJECT and dataset names, then run:"
echo "  chmod +x create_bq_resources.sh && ./create_bq_resources.sh"

