SHELL:=/bin/bash

## See also https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html

.PHONY: help

help:
		@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

remove-clojure-image: ## Remove Clojure container
		@/bin/bash -c 'source ./bin/commands.sh && remove_clojure_container'

build-clojure-container: ## Build Clojure container
		@/bin/bash -c 'source ./bin/commands.sh && build_clojure_container'

run-clojure-container: ## Build Clojure container
		@/bin/bash -c "source ./bin/commands.sh && run_clojure_container"
