# PostgreSQL Prometheus Adapter

Remote storage adapter enabling Prometheus to use PostgreSQL as a long-term store for time-series metrics. Code is based on [Prometheus - Remote storage adapter](https://github.com/prometheus/prometheus/tree/master/documentation/examples/remote_storage/remote_storage_adapter).

The PostgreSQL Prometheus Adapter is designed to utilize native partitioning enhancements available in recent versions of core PostgreSQL to efficiently store Prometheus time series data in a PostgreSQL database, and is not dependent on external PostgreSQL extensions.

The design is based on partitioning and threads. Incoming data is processed by one or more threads and one or more writer threads will store data in PostgreSQL daily or hourly partitions. Partitions will be auto-created by the adapter based on the timestamp of incoming data.

The PostgreSQL Prometheus Adapter accepts Prometheus remote read/write requests, and sends them to PostgreSQL.

Additional information regarding the adapter and getting started is provided below and available in this [blog post introducing the PostgreSQL Prometheus Adapter](https://info.crunchydata.com/blog/using-postgres-to-back-prometheus-for-your-postgresql-monitoring-1).

## PostgreSQL Version Support

PostgreSQL Prometheus Adapter supports:

* PostgreSQL 14
* PostgreSQL 13
* PostgreSQL 12
* PostgreSQL 11

## Building

### Compile

```shell
make
```

### Make a container (optional)

```shell
make container
```

## Running

### Running adapter

```shell
export DATABASE_URL=...
./postgresql-prometheus-adapter
```

#### Database Connection

DATABASE_URL environment variable defines PostgreSQL connection string.

```shell
export DATABASE_URL=...

```

```shell
Default: None
Description: Database connection parameters
Ex: “user=<> password=<> host=<> port=<> database=<>”
```

#### Adapter parameters

Following parameters can be used to tweak adapter behavior.

```shell
./postgresql-prometheus-adapter --help
usage: postgresql-prometheus-adapter [<flags>]

Remote storage adapter [ PostgreSQL ]

Flags:
  -h, --help                           Show context-sensitive help (also try --help-long and --help-man).
      --adapter-send-timeout=30s       The timeout to use when sending samples to the remote storage.
      --web-listen-address=":9201"     Address to listen on for web endpoints.
      --web-telemetry-path="/metrics"  Address to listen on for web endpoints.
      --log.level=info                 Only log messages with the given severity or above. One of: [debug, info, warn, error]
      --log.format=logfmt              Output format of log messages. One of: [logfmt, json]
      --pg-partition="hourly"          daily or hourly partitions, default: hourly
      --pg-commit-secs=15              Write data to database every N seconds
      --pg-commit-rows=20000           Write data to database every N Rows
      --pg-threads=1                   Writer DB threads to run 1-10
      --parser-threads=5               parser threads to run per DB writer 1-10
```
:point_right: Note: pg_commit_secs and pg_commit_rows controls when data rows will be flushed to database. First one to reach threshold will trigger the flush.

### Container

#### Run container

```shell
podman run --rm \
  --name postgresql-prometheus-adapter \
  -p 9201:9201 \
  -e DATABASE_URL="user=testuser password=test123 host=192.168.12.36 port=5432 database=testdb" \
  --detach \
  crunchydata/postgresql-prometheus-adapterl:latest
  ```

#### Stop container

```shell
podman stop postgresql-prometheus-adapter
```

#### Adapter ENV

Following `-e NAME:VALUE` can be used to tweak adapter behavior.

```shell
adapter_send_timeout=30s       The timeout to use when sending samples to the remote storage.
web_listen_address=":9201"     Address to listen on for web endpoints.
web_telemetry_path="/metrics"  Address to listen on for web endpoints.
log_level=info                 Only log messages with the given severity or above. One of: [debug, info, warn, error]
log_format=logfmt              Output format of log messages. One of: [logfmt, json]
pg_partition="hourly"          daily or hourly partitions, default: hourly
pg_commit_secs=15              Write data to database every N seconds
pg_commit_rows=20000           Write data to database every N Rows
pg_threads=1                   Writer DB threads to run 1-10
parser_threads=5               parser threads to run per DB writer 1-10
```
:point_right: Note: pg_commit_secs and pg_commit_rows controls when data rows will be flushed to database. First one to reach threshold will trigger the flush.

## Prometheus Configuration

Add the following to your prometheus.yml:

```yaml
remote_write:
    - url: "http://<ip address>:9201/write"
remote_read:
    - url: "http://<ip address>:9201/read"
 ```

## Maintainers

The PostgreSQL Prometheus Adapter is maintained by the team at [Crunchy Data](https://www.crunchydata.com/).

## Contributing to the Project

Want to contribute to the PostgreSQL Prometheus Adapter? Great! Please use GitHub to submit an issue for the PostgreSQL Prometheus Adapter project.  If you would like to work the issue, please add that information in the issue so that we can confirm we are not already working no need to duplicate efforts.

## License

The PostgreSQL Prometheus Adapter is available under the Apache 2.0 license. See [LICENSE](https://github.com/CrunchyData/postgresql-prometheus-adapter/blob/master/LICENSE) for details.
