
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

run:
	cargo run --release

integration-base:
	bats ../integration/base.bats

integration-lists:
	bats ../integration/lists.bats

integration-rdb:
	bats ../integration/rdb.bats

integration-ms:
	/tmp/codecrafters-build-redis-rust/release/codecrafters-redis --dir ../integration --dbfilename read_sample.rdb & ./your_program.sh --port 6380 --replicaof "localhost 6379" & bats ../integration/ms.bats; kill \%1; kill \%2

