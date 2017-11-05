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

dist-build:
	mkdir -p dist

	GOOS=darwin go build -ldflags "-extldflags \"-static\" -X \"main.buildVersion=$(GIT_VERSION)\"" \
		-o ./dist/darwin-amd64/$(PROG_NAME) $(CMD_PATH)

	GOOS=linux go build -ldflags "-extldflags \"-static\" -X \"main.buildVersion=$(GIT_VERSION)\"" \
		-o ./dist/linux-amd64/$(PROG_NAME) $(CMD_PATH)

	GOOS=windows go build -ldflags "-extldflags \"-static\" -X \"main.buildVersion=$(GIT_VERSION)\"" \
		-o ./dist/windows-amd64/$(PROG_NAME) $(CMD_PATH)

dist-zip:
	cd dist && zip $(PROG_NAME)-darwin-amd64.zip darwin-amd64/*
	cd dist && zip $(PROG_NAME)-linux-amd64.zip linux-amd64/*
	cd dist && zip $(PROG_NAME)-windows-amd64.zip windows-amd64/*

dist: dist-build dist-zip

docker:
	docker build -t ${IMAGE_NAME}:${GIT_SHA} .
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
