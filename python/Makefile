
.PHONY: all check run test fix unsafe-fix 

PYTHON_SRC := app/ redis/

all: check test integration

check:
	ruff format $(PYTHON_SRC)
	ruff check $(PYTHON_SRC)
	ty check $(PYTHON_SRC)

test:
	uv run pytest $(PYTHON_SRC)

fix:
	ruff check --fix $(PYTHON_SRC)

unsafe-fix:
	ruff check --fix --unsafe-fixes $(PYTHON_SRC)

.PHONY: integration integration-base integration-rdb integration-ms

integration: integration-base integration-rdb integration-ms

integration-base:
	./your_program.sh & bats ../integration/base.bats; kill \%1

integration-rdb:
	./your_program.sh --dir ../integration --dbfilename read_sample.rdb & bats ../integration/rdb.bats; kill \%1

integration-ms:
	./your_program.sh --dir ../integration --dbfilename read_sample.rdb & ./your_program.sh --port 6380 --replicaof "localhost 6379" & bats ../integration/ms.bats; kill \%1; kill \%2

