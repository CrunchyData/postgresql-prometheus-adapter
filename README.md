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
    
## Sponsors

[Crunchy Data](https://www.crunchydata.com/) is pleased to support the development of a number [open-source projects](https://github.com/CrunchyData/) to help promote support the PostgreSQL community and software ecosystem.

## Legal Notices

Copyright © 2019 Crunchy Data Solutions, Inc.

CRUNCHY DATA SOLUTIONS, INC. PROVIDES THIS GUIDE "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF NON INFRINGEMENT, MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.

Crunchy, Crunchy Data Solutions, Inc. and the Crunchy Hippo Logo are trademarks of Crunchy Data Solutions, Inc.

