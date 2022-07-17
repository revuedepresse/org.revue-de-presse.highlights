SHELL:=/bin/bash

.PHONY: doc help

.PHONY: build clean deps install

.PHONY: restart start stop test

COMPOSE_PROJECT_NAME = ?= 'org_example_highlights'
WORKER ?= 'highlights.example.org'
TMP_DIR ?= '/tmp/tmp_${WORKER}'

doc:
	@cat doc/commands.md && echo ''

help: doc
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

build: ## Build worker image
	@/bin/bash -c 'source fun.sh && build'

clean: ## Remove worker container
	@/bin/bash -c 'source fun.sh && clean "${TMP_DIR}"'

deps: ## Install dependencies
	@lein deps

install: build ## Install requirements
	@/bin/bash -c 'source fun.sh && install'

start: ## Run worker e.g. COMMAND=''
	@/bin/bash -c 'source fun.sh && start'

test: deps ## Run tests
	@/bin/bash -c 'source fun.sh && test'

run-clojure-container: ## Run Clojure container
	@/bin/bash -c "source ./bin/console.sh && run_clojure_container"

refresh-highlights: ## Refresh highlights
	@/bin/bash -c "source ./bin/console.sh && refresh_highlights"

save-highlights-for-all-aggregates: ## Save highlights for all aggregates
	@/bin/bash -c "source ./bin/console.sh && save_highlights_for_all_aggregates"
