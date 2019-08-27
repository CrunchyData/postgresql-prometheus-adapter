VERSION=1.0
OS:=$(shell uname -s | awk '{ print tolower($$1) }')
ORGANIZATION=crunchydata
GOPATH=$(shell pwd)
ARCH=$(shell go env GOARCH)
ifeq ($(ARCH),)
  ARCH=amd64
endif
ifeq ($(shell uname -m), i386)
	ARCH=386
endif

# Packages for running tests
PKGS:= $(shell go list ./... | grep -v /vendor)

SOURCES:=$(shell find . -name '*.go'  | grep -v './vendor')

TARGET:=prometheus-adapter-postgresql

.PHONY: all clean build docker-image docker-push test prepare-for-docker-build

all: $(TARGET) 

build: $(TARGET)

$(TARGET): main.go $(SOURCES)
	# GOOS=$(OS) GOARCH=${ARCH} CGO_ENABLED=0 go build -a --ldflags '-w' -o $(TARGET) main.go 
	GOOS=$(OS) GOARCH=${ARCH} CGO_ENABLED=0 go build -o $(TARGET) main.go 

container: $(TARGET) Dockerfile
	#podman rmi $(ORGANIZATION)/$(TARGET):latest $(ORGANIZATION)/$(TARGET):$(VERSION)
	podman build -t $(ORGANIZATION)/$(TARGET):latest .
	podman tag $(ORGANIZATION)/$(TARGET):latest $(ORGANIZATION)/$(TARGET):$(VERSION)

container-save: container
	rm -f $(TARGET).tar
	podman save --output=$(TARGET)-$(TARGET).tar $(ORGANIZATION)/$(TARGET):$(VERSION)

clean:
	# go clean $(PKGS)
	rm -f *~ $(TARGET)

setup:
	git clone https://github.com/jackc/pgx src/github.com/jackc/pgx/v4
	git -C src/github.com/jackc/pgx/v4 checkout v4
	go get -v ./...
