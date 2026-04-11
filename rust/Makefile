# Simple targets

.PHONY: all check run test

all: check test integration

check:
	cargo clippy

test:
	cargo test

run:
	cargo run 

build:
	cargo build  --target-dir=/tmp/codecrafters-build-redis-rust --manifest-path Cargo.toml

# Integration tests - run server and test with bats scripts
# Code borrowed from: https://www.funwithlinux.net/blog/running-two-processes-in-parallel-from-makefile/

SERVER_CMD := /tmp/codecrafters-build-redis-rust/debug/codecrafters-redis
CLIENT_CMD := bats ../integration/lists.bats
SERVER_PORT := 6379   
SERVER_HOST := localhost
MAX_RETRIES := 20       
RETRY_DELAY := 0.5      

.PHONY: integration

integration: build
	@trap 'kill $$SERVER_PID $$CLIENT_PID 2>/dev/null; exit 1' SIGINT SIGTERM; \
	echo "=== Starting Development Environment ==="; \
	\
	echo "Starting server..."; \
	( $(SERVER_CMD) 2>&1 | sed 's/^/[server] /' ) & \
	SERVER_PID=$$!; \
	echo "Server running with PID: $$SERVER_PID"; \
	\
	echo "Waiting for server on $(SERVER_HOST):$(SERVER_PORT)..."; \
	retry_count=0; \
	until nc -z $(SERVER_HOST) $(SERVER_PORT) || [ $$retry_count -ge $(MAX_RETRIES) ]; do \
	    sleep $(RETRY_DELAY); \
	    retry_count=$$((retry_count + 1)); \
	done; \
	\
	if [ $$retry_count -ge $(MAX_RETRIES) ]; then \
	    echo "ERROR: Server failed to start on $(SERVER_HOST):$(SERVER_PORT)"; \
	    kill $$SERVER_PID 2>/dev/null; \
	    exit 1; \
	fi; \
	\
	echo "Server ready! Starting client..."; \
	( $(CLIENT_CMD) 2>&1 | sed 's/^/[client] /' ) & \
	CLIENT_PID=$$!; \
	echo "Client running with PID: $$CLIENT_PID"; \
	\
	echo "Waiting for $$SERVER_PID $$CLIENT_PID"; \
	wait $$SERVER_PID $$CLIENT_PID; \
	echo "=== One process exited. Cleaning up ==="; \
	kill $$SERVER_PID $$CLIENT_PID 2>/dev/null; \
	exit 0

kill-server:
	kill $$(lsof -i tcp:6379 | tail +2 | cut -w -f 2)
#	kill $$(ps ax | grep /tmp/codecrafters-build-redis-rust/release/codecrafters-redis | grep -v grep | cut -w -f 1)

integration-base:
	bats ../integration/base.bats

integration-lists:
	bats ../integration/lists.bats

integration-rdb:
	bats ../integration/rdb.bats

integration-ms:
	/tmp/codecrafters-build-redis-rust/release/codecrafters-redis --dir ../integration --dbfilename read_sample.rdb & ./your_program.sh --port 6380 --replicaof "localhost 6379" & bats ../integration/ms.bats; kill \%1; kill \%2

