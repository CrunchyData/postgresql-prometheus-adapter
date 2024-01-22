FROM golang:alpine

WORKDIR /source
COPY . /source
RUN apk --update-cache add --virtual \
  build-dependencies \
  build-base \
  && go mod download \
  && make all

ENTRYPOINT ["/bin/sh", "/source/start.sh"]

