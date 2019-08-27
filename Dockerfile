FROM centos:7

MAINTAINER Yogesh Sharma <Yogesh.Sharma@CrunchyData.com>

COPY prometheus-adapter-postgresql start.sh /

# DATABASE_URL="user=testuser password=test123 host=192.168.122.36 port=5432 database=testdb" ./prometheus-adapter-postgresql -log-level=debug -pg-commit-secs=30  -pg-commit-rows=10000 -pg-partition=daily
ENTRYPOINT ["/start.sh"]

