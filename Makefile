.PHONY: build clean test help default

BIN_NAME=s3mini

VERSION := $(shell grep "const Version " cmd/version.go | sed -E 's/.*"(.+)"$$/\1/')
GIT_COMMIT=$(shell git rev-parse HEAD)

default: test

help:
	@echo 'Management commands for s3mini:'
	@echo
	@echo 'Usage:'
	@echo '    make build           Compile the project.'
	@echo '    make test            Run tests on a compiled project.'
	@echo '    make clean           Clean the directory tree.'
	@echo

build: clean
	@echo "building ${BIN_NAME} ${VERSION}"
	@echo "GOPATH=${GOPATH}"
	go build -ldflags "-X main.gitcommit=${GIT_COMMIT}" -o bin/${BIN_NAME}

package:
	@echo "building image ${BIN_NAME} ${VERSION} $(GIT_COMMIT)"

clean:
	@test ! -e bin/${BIN_NAME} || rm bin/${BIN_NAME}

test:
	go test ./...

.PHONY: fmt
fmt: ## Verifies all files have men `gofmt`ed
	@echo "+ $@"
	@gofmt -s -l . | grep -v '.pb.go:' | grep -v vendor | tee /dev/stderr

.PHONY: lint
lint: ## Verifies `golint` passes
	@echo "+ $@"
	@golint ./... | grep -v '.pb.go:' | grep -v vendor | tee /dev/stderr

.PHONY: install
install: ## Installs the executable or package
	@echo "+ $@"
	go install -a .
