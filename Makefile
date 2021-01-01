SHELL:=/bin/bash

## See also https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html

.PHONY: help

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

create-network: ## Create Docker network
	@/bin/bash -c 'source ./bin/console.sh && create_network'

remove-clojure-image: ## Remove Clojure container
	@/bin/bash -c 'source ./bin/console.sh && remove_clojure_container'

build-clojure-container: ## Build Clojure container
	@/bin/bash -c 'source ./bin/console.sh && build_clojure_container'

run-clojure-container: ## Run Clojure container
	@/bin/bash -c "source ./bin/console.sh && run_clojure_container"

refresh-highlights: ## Refresh highlights
	@/bin/bash -c "source ./bin/console.sh && refresh_highlights"

save-highlights-for-all-aggregates: ## Save highlights for all aggregates
	@/bin/bash -c "source ./bin/console.sh && save_highlights_for_all_aggregates"
