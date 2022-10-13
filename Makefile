#!/usr/bin/make
# Makefile readme (ru): <http://linux.yaroslavl.ru/docs/prog/gnu_make_3-79_russian_manual.html>
# Makefile readme (en): <https://www.gnu.org/software/make/manual/html_node/index.html#SEC_Contents>

SHELL = /bin/sh

test_coverage:
	rm -rf coverage-ci
	mkdir ./coverage-ci
	go test -v -race -cover -tags=debug -coverpkg=./... -coverprofile=./coverage-ci/pipe.out -covermode=atomic ./ipc/pipe
	go test -v -race -cover -tags=debug -coverpkg=./... -coverprofile=./coverage-ci/socket.out -covermode=atomic ./ipc/socket
	go test -v -race -cover -tags=debug -coverpkg=./... -coverprofile=./coverage-ci/pool_static.out -covermode=atomic ./pool/static_pool
	go test -v -race -cover -tags=debug -coverpkg=./... -coverprofile=./coverage-ci/worker.out -covermode=atomic ./worker
	go test -v -race -cover -tags=debug -coverpkg=./... -coverprofile=./coverage-ci/bst.out -covermode=atomic ./bst
	go test -v -race -cover -tags=debug -coverpkg=./... -coverprofile=./coverage-ci/pq.out -covermode=atomic ./priority_queue
	go test -v -race -cover -tags=debug -coverpkg=./... -coverprofile=./coverage-ci/worker_stack.out -covermode=atomic ./worker_watcher
	go test -v -race -cover -tags=debug -coverpkg=./... -coverprofile=./coverage-ci/events.out -covermode=atomic ./events
	echo 'mode: atomic' > ./coverage-ci/summary.txt
	tail -q -n +2 ./coverage-ci/*.out >> ./coverage-ci/summary.txt

test: ## Run application tests
	go test -v -race ./ipc/pipe
	go test -v -race ./ipc/socket
	go test -v -race -fuzz=FuzzStaticPoolEcho -fuzztime=30s -tags=debug ./pool/static_pool
	go test -v -race ./pool/static_pool
	go test -v -race ./worker
	go test -v -race ./bst
	go test -v -race ./priority_queue
	go test -v -race ./worker_watcher
	go test -v -race ./events
