BINARY=logical
OS := $(shell uname -s)
VERSION=0.1.0

ifeq (${OS}, Linux)
	OS=linux
endif

ifeq (${OS}, Darwin)
	OS=darwin
endif

clean:
	rm -f ${BINARY}_*_*

install:
	CGO_ENABLED=0 GOOS=${OS} GOARCH=amd64 go build -ldflags="-s -w"  -o ${BINARY}_${OS}_${VERSION}

linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w"  -o ${BINARY}_linux_${VERSION}

darwin:
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -ldflags="-s -w"  -o ${BINARY}_darwin_${VERSION}
