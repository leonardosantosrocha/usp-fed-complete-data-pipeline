DBT_COMMAND = dbt

.PHONY: help install build run seed clean

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

install:
	$(DBT_COMMAND) deps

seed:
	$(DBT_COMMAND) seed

run:
	$(DBT_COMMAND) run

pipeline:
	$(DBT_COMMAND) deps
	$(DBT_COMMAND) seed
	$(DBT_COMMAND) run
	$(DBT_COMMAND) test

dbt-build:
	$(DBT_COMMAND) build