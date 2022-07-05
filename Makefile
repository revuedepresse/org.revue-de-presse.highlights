SHELL:=/bin/bash

.PHONY: doc build clean help install restart start stop test

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

install: build ## Install requirements
	@/bin/bash -c 'source fun.sh && install'

start: install ## Run worker e.g. COMMAND=''
	@/bin/bash -c 'source fun.sh && start'

run-clojure-container: ## Run Clojure container
	@/bin/bash -c "source ./bin/console.sh && run_clojure_container"

refresh-highlights: ## Refresh highlights
	@/bin/bash -c "source ./bin/console.sh && refresh_highlights"

save-highlights-for-all-aggregates: ## Save highlights for all aggregates
	@/bin/bash -c "source ./bin/console.sh && save_highlights_for_all_aggregates"
