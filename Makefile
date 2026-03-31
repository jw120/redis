
.PHONY: all check run run_2022 run_2023

PYTHON_SRC := app/ redis/

all: check python-test integration-test

check:
	ruff format $(PYTHON_SRC)
	ruff check $(PYTHON_SRC)
	pyright $(PYTHON_SRC)

python-test:
	pytest $(PYTHON_SRC)

integration-test: integration-base integration-rdb integration-ms

integration-base:
	./your_program.sh & bats integration/base.bats; kill \%1

integration-rdb:
	./your_program.sh --dir integration --dbfilename read_sample.rdb & bats integration/rdb.bats; kill \%1

integration-ms:
	./your_program.sh --dir integration --dbfilename read_sample.rdb & ./your_program.sh --port 6380 --replicaof "localhost 6379" & bats integration/ms.bats; kill \%1; kill \%2

code-test:
	codecrafters test

