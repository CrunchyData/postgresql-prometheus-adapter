# PostgreSQL Prometheus Adapter

Remote storage adapter enabling Prometheus to use PostgreSQL as a long-term store for time-series metrics.

The PostgreSQL Prometheus Adapter accepts Prometheus remote read/write requests, and sends them on to PostgreSQL. 

## PostgreSQL Version Support

PostgreSQL Prometheus Adapter supports:

* PostgreSQL 11.x
* PostgreSQL 12.x (Beta)

## Installation

```
podman load -i prometheus-adapter-postgresql-1.0.tar
```

## Usage

Start container

```
podman run --rm \
  --name prometheus-adapter-postgresql \
  -p 9201:9201 \
  -e DATABASE_URL="user=testuser password=test123 host=192.168.12.36 port=5432 database=testdb" \
  --detach \
  crunchydata/prometheus-adapter-postgresql:1.0
  ```

Stop container

```
podman stop prometheus-adapter-postgresql
```

## Adapter Endpoint Configuration

```
DATABASE_URL
Default: None
Description: Database connection parameters
Ex: “user=<> password=<> host=<> port=<> database=<>”

log_level
Default:error
Description: logging level, "error", "warn", "info", "debug"

pg_commit_secs
Default:30
Description: how frequently to push data in database based on time

pg_commit_rows
Default:10000
Description:how frequently to push data in database based on row count

pg_partition
Default:hourly
Description:partitioning scheme to use. 
```

## Prometheus Configuration

Add the following to your prometheus.yml:

```
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

