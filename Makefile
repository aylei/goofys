export CGO_ENABLED=0

VERSION := aylei_$(shell git rev-parse HEAD)
LDFLAGS := -X main.Version=$(VERSION)
BINARY := goofys

run-test: s3proxy.jar
	./test/run-tests.sh

s3proxy.jar:
	wget https://github.com/gaul/s3proxy/releases/download/s3proxy-1.8.0/s3proxy -O s3proxy.jar

get-deps: s3proxy.jar
	go get -t ./...

build:
	go build -ldflags "$(LDFLAGS)"

build-amd64:
	GOARCH=amd64 GOOS=linux go build -ldflags "$(LDFLAGS)" -o $(BINARY)-linux-amd64

build-arm64:
	GOARCH=arm64 GOOS=linux go build -ldflags "$(LDFLAGS)" -o $(BINARY)-linux-arm64

build-all: build-amd64 build-arm64

install:
	go install -ldflags "$(LDFLAGS)"
