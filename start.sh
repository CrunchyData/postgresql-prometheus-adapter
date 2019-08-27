#!/bin/bash

if [[ "${DATABASE_URL}" == "" ]]; then
  echo 'Missing DATABASE_URL'
  echo 'example -e DATABASE_URL="user=<db user> password=<db user password> host=<db host> port=<db port> database=<db name>"'
  exit 1
fi

trap shutdown INT

function shutdown() {
  pkill -SIGINT prometheus-adapter-postgresql
}

log_level="${log_level:-error}"
pg_commit_secs=${pg_commit_secs:-30}
pg_commit_rows=${pg_commit_rows:-10000}
pg_partition="${pg_partition:-hourly}"

/prometheus-adapter-postgresql \
  -log-level=${log_level} \
  -pg-commit-secs=${pg_commit_secs} \
  -pg-commit-rows=${pg_commit_rows} \
  -pg-partition=${pg_partition}

