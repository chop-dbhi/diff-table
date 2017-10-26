PROG_NAME := "diff-table"
IMAGE_NAME := "dbhi/diff-table"
CMD_PATH := "./cmd/diff-table"

GIT_SHA := $(shell git log -1 --pretty=format:"%h" .)
GIT_TAG := $(shell git describe --tags --exact-match . 2>/dev/null)
GIT_BRANCH := $(shell git symbolic-ref -q --short HEAD)
GIT_VERSION := $(shell git log -1 --pretty=format:"%h (%ci)" .)

build:
	go build -ldflags "-X \"main.buildVersion=$(GIT_VERSION)\"" \
		-o $(GOPATH)/bin/$(PROG_NAME) $(CMD_PATH)

dist-build-linux:
	mkdir -p dist

	go build -ldflags "-extldflags \"-static\" -X \"main.buildVersion=$(GIT_VERSION)\"" \
		-o ./dist/linux-amd64/$(PROG_NAME) $(CMD_PATH)

dist-linux:
	docker build -f Dockerfile.build -t dbhi/diff-table-builder .

	docker run --rm -it \
		-v ${PWD}:/go/src/github.com/chop-dbhi/diff-table \
		dbhi/diff-table-builder

dist-build: dist-linux
	mkdir -p dist

	go build -ldflags "-X \"main.buildVersion=$(GIT_VERSION)\"" \
		-o ./dist/darwin-amd64/$(PROG_NAME) $(CMD_PATH)

dist-zip:
	cd dist && zip $(PROG_NAME)-darwin-amd64.zip darwin-amd64/*
	cd dist && zip $(PROG_NAME)-linux-amd64.zip linux-amd64/*

dist: dist-build dist-zip

docker:
	docker build -v .:/go/src/gitub.com/chop-dbhi/diff-table -t ${IMAGE_NAME}:${GIT_SHA} .
	docker tag ${IMAGE_NAME}:${GIT_SHA} ${IMAGE_NAME}:${GIT_BRANCH}
	if [ -n "${GIT_TAG}" ] ; then \
		docker tag ${IMAGE_NAME}:${GIT_SHA} ${IMAGE_NAME}:${GIT_TAG} ; \
	fi;
	if [ "${GIT_BRANCH}" == "master" ]; then \
		docker tag ${IMAGE_NAME}:${GIT_SHA} ${IMAGE_NAME}:latest ; \
	fi;

docker-push:
	docker push ${IMAGE_NAME}:${GIT_SHA}
	docker push ${IMAGE_NAME}:${GIT_BRANCH}
	if [ -n "${GIT_TAG}" ]; then \
		docker push ${IMAGE_NAME}:${GIT_TAG} ; \
	fi;
	if [ "${GIT_BRANCH}" == "master" ]; then \
		docker push ${IMAGE_NAME}:latest ; \
	fi;

.PHONY: build dist-build dist
