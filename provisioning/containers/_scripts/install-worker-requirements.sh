#!/usr/bin/env bash
set -Eeuo pipefail

source '/scripts/requirements.sh'

function install_worker_requirements() {
    add_system_user_group
    install_system_packages
    create_log_files_when_non_existing "${WORKER}"
    install_tracing
    set_permissions
    clear_package_management_system_cache
}
install_worker_requirements

set -Eeuo pipefail

