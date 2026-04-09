
.PHONY: all check run test

all: check test integration

check:
	cargo clippy

test:
	cargo test

.PHONY: integration integration-base # integration-rdb integration-ms

integration: build integration-base integration-lists

build:
	cargo build --release --target-dir=/tmp/codecrafters-build-redis-rust --manifest-path Cargo.toml

integration-base:
	/tmp/codecrafters-build-redis-rust/release/codecrafters-redis & bats ../integration/base.bats; kill \%1

integration-lists:
	/tmp/codecrafters-build-redis-rust/release/codecrafters-redis & bats ../integration/lists.bats; kill \%1

integration-rdb:
	/tmp/codecrafters-build-redis-rust/release/codecrafters-redis --dir ../integration --dbfilename read_sample.rdb & bats ../integration/rdb.bats; kill \%1

integration-ms:
	/tmp/codecrafters-build-redis-rust/release/codecrafters-redis --dir ../integration --dbfilename read_sample.rdb & ./your_program.sh --port 6380 --replicaof "localhost 6379" & bats ../integration/ms.bats; kill \%1; kill \%2

