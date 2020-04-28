# PostgreSQL Prometheus Adapter

Remote storage adapter enabling Prometheus to use PostgreSQL as a long-term store for time-series metrics. Code is based on [Prometheus - Remote storage adapter](https://github.com/prometheus/prometheus/tree/master/documentation/examples/remote_storage/remote_storage_adapter).

The PostgreSQL Prometheus Adapter accepts Prometheus remote read/write requests, and sends them to PostgreSQL. 

## PostgreSQL Version Support

PostgreSQL Prometheus Adapter supports:

* PostgreSQL 11.x
* PostgreSQL 12.x

## Building

### Compile

```shell
make
```

### Make a cotnainer (optinal)

```shell
make container
podman load -i prometheus-adapter-postgresql-1.0.tar
```

## Running

### Running adapter

```shell
export DATABASE_UL=...
./postgresql-prometheus-adapter
```

#### Database Connection

DATABASE_URL environment variable defines PostgreSQL connection string.

```shell
export DATABASE_UL=...

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
  -h, --help                   Show context-sensitive help (also try --help-long and --help-man).
      --adapter-send-timeout=30s  
                               The timeout to use when sending samples to the remote storage.
      --web-listen-address=":9201"  
                               Address to listen on for web endpoints.
      --web-telemetry-path="/metrics"  
                               Address to listen on for web endpoints.
      --log.level=info         Only log messages with the given severity or above. One of: [debug, info, warn, error]
      --log.format=logfmt      Output format of log messages. One of: [logfmt, json]
      --pg-partition="hourly"  daily or hourly partitions, default: hourly
      --pg-commit-secs=15      Write data to database every N seconds or N Rows
      --pg-commit-rows=20000   Write data to database every N Rows or N Seconds
      --pg-threads=1           Writer DB threads to run 1-10
      --parser-threads=5       parser threads to run per DB writer 1-10

```

### Container

#### Run container

```shell
podman run --rm \
  --name prometheus-adapter-postgresql \
  -p 9201:9201 \
  -e DATABASE_URL="user=testuser password=test123 host=192.168.12.36 port=5432 database=testdb" \
  --detach \
  crunchydata/prometheus-adapter-postgresql:1.0
  ```

#### Stop container

```shell
podman stop prometheus-adapter-postgresql
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
pg_commit_secs=15              Write data to database every N seconds or N Rows
pg_commit_rows=20000           Write data to database every N Rows or N Seconds
pg_threads=1                   Writer DB threads to run 1-10
parser_threads=5               parser threads to run per DB writer 1-10
```

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

Want to contribute to the PostgreSQL Operator project? Great! Please use GitHub to submit an issue for the PostgreSQL Prometheus Adapter project.  If you would like to work the issue, please add that information in the issue so that we can confirm we are not already working no need to duplicate efforts.

## License

The PostgreSQL Prometheus Adapter is available under the Apache 2.0 license. See [LICENSE] (https://github.com/CrunchyData/postgresql-prometheus-adapter/blob/master/LICENSE) for details. 

