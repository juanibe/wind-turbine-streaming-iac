#!/usr/bin/env bash
# Simulates 3 wind turbines sending one reading each per second (~2 minutes).
# Run with:  AWS_PROFILE=poc ./produce.sh
set -euo pipefail

STREAM="turbine-readings"
REGION="eu-west-1"
SECONDS_TO_RUN=120

for ((i = 1; i <= SECONDS_TO_RUN; i++)); do
  now=$(date -u +%Y-%m-%dT%H:%M:%SZ)   # timestamp in UTC, same format as the Lambda expects
  records=""

  for t in T1 T2 T3; do
    wind="$((RANDOM % 24 + 5)).$((RANDOM % 10))"   # 5.0 to 28.9 m/s; above 25 is an alert
    temp="$((RANDOM % 25)).$((RANDOM % 10))"       # 0.0 to 24.9 degrees

    # One JSON reading, ending with \n so every record lands on its own line in S3
    data="{\\\"turbine_id\\\":\\\"$t\\\",\\\"wind_speed\\\":$wind,\\\"temperature\\\":$temp,\\\"timestamp\\\":\\\"$now\\\"}\\n"

    # PartitionKey = turbine id: same turbine -> same shard -> readings stay in order
    records="$records{\"Data\":\"$data\",\"PartitionKey\":\"$t\"},"
  done

  records="[${records%,}]"   # drop the last comma and wrap the three records in a list

  # put-records sends all 3 readings in ONE call; --query prints how many failed
  failed=$(aws kinesis put-records \
    --stream-name "$STREAM" \
    --records "$records" \
    --cli-binary-format raw-in-base64-out \
    --region "$REGION" \
    --query FailedRecordCount --output text)

  echo "second $i/$SECONDS_TO_RUN: sent 3 readings, failed: $failed"
  sleep 1
done