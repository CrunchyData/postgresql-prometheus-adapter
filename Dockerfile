FROM centos:7

MAINTAINER Yogesh Sharma <Yogesh.Sharma@CrunchyData.com>

COPY postgresql-prometheus-adapter start.sh /

ENTRYPOINT ["/start.sh"]

