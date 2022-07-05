#!/usr/bin/env bash
set -Eeuo pipefail

source '/scripts/requirements.sh'

function install_application() {
    local project_dir
    project_dir="${1}"

    if [ -z "${project_dir}" ];
    then

        printf 'A %s is expected as %s ("%s").%s' 'non-empty string' '1st argument' 'project directory' $'\n'

        return 1

    fi

    mv "$(lein uberjar | sed -n 's/^Created \(.*standalone\.jar\)/\1/p')" highlights-snapshots-standalone.jar

    chown -R worker. highlights-snapshots-standalone.jar
}

function install_dependencies() {
    local project_dir
    project_dir="${1}"

    if [ -z "${project_dir}" ];
    then

        printf 'A %s is expected as %s ("%s").%s' 'non-empty string' '1st argument' 'project directory' $'\n'

        return 1

    fi

    cd "${project_dir}" || exit

    rm -rf "${project_dir}"/target/*

    lein deps
}

function set_file_permissions() {
    local project_dir
    project_dir="${1}"

    if [ -z "${project_dir}" ];
    then

        printf 'A %s is expected as %s ("%s").%s' 'non-empty string' '1st argument' 'project directory' $'\n'

        return 1

    fi

    remove_distributed_version_control_system_files_git "${project_dir}"

    chown -R "${WORKER_UID}:${WORKER_GID}" \
        /scripts \
        "${project_dir}"

    chmod     o-rwx /scripts
    chmod -R ug+rx  /scripts
    chmod -R  u+w   /scripts

    find "${project_dir}"  \
        -maxdepth 1 \
        -executable \
        -readable \
        -type d \
        -not -path "${project_dir}"'/provisioning/volumes' \
        -exec /bin/bash -c 'export file_path="{}" && \chown --recursive '"${WORKER_UID}"':'"${WORKER_GID}"' "${file_path}"' \; \
        -exec /bin/bash -c 'export file_path="{}" && \chmod --recursive og-rwx "${file_path}"' \; \
        -exec /bin/bash -c 'export file_path="{}" && \chmod --recursive g+rx "${file_path}"' \; && \
        printf '%s.%s' 'Successfully changed directories permissions' $'\n'

    find "${project_dir}" \
        -maxdepth 2 \
        -type d \
        -executable \
        -readable \
        -regex '.+/var.+' \
        -not -path "${project_dir}"'/var/log' \
        -exec /bin/bash -c 'export file_path="{}" && \chmod --recursive ug+w "${file_path}"' \; && \
        printf '%s.%s' 'Successfully made var directories writable' $'\n'

    find "${project_dir}" \
        -type f \
        -readable \
        -not -path "${project_dir}"'/provisioning/volumes' \
        -exec /bin/bash -c 'export file_path="{}" && \chown '"${WORKER_UID}"':'"${WORKER_GID}"' "${file_path}"' \; \
        -exec /bin/bash -c 'export file_path="{}" && \chmod og-rwx "${file_path}"' \; \
        -exec /bin/bash -c 'export file_path="{}" && \chmod  g+r   "${file_path}"' \; && \
        printf '%s.%s' 'Successfully changed files permissions' $'\n'
}

function remove_distributed_version_control_system_files_git() {
    local project_dir
    project_dir="${1}"

    if [ -z "${project_dir}" ];
    then

        printf 'A %s is expected as %s ("%s").%s' 'non-empty string' '1st argument' 'project directory' $'\n'

        return 1

    fi

    if [ ! -d "${project_dir}/.git" ];
    then
        rm --force --verbose "${project_dir}/.git"
    fi
}

function install_tracing() {
    wget -O /var/www/dd-java-agent.jar https://dtdg.co/latest-java-tracer
    chown worker. /var/www/dd-java-agent.jar
}

function install_app_requirements() {
    local WORKER_UID
    local WORKER_GID

    if [ -z "${WORKER}" ];
    then

      printf 'A %s is expected as %s ("%s").%s' 'non-empty string' 'environment variable' 'WORKER' $'\n'

      return 1

    fi

    local project_dir
    project_dir='/var/www/'${WORKER}

    source "${project_dir}/.env"

    if [ -z "${WORKER_UID}" ];
    then

        printf 'A %s is expected as %s ("%s").%s' 'non-empty numeric' 'system user uid' 'WORKER_UID' $'\n'

        return 1

    fi

    if [ -z "${WORKER_GID}" ];
    then

        printf 'A %s is expected as %s ("%s").%s' 'non-empty numeric' 'system user gid' 'WORKER_GID' $'\n'

        return 1

    fi

    install_dependencies "${project_dir}"
    remove_distributed_version_control_system_files_git "${project_dir}"
    set_file_permissions "${project_dir}"
    install_application "${project_dir}"
    install_tracing

    if [ -d "${project_dir}/target" ];
    then
        chown -R worker. "${project_dir}/target"
    fi
}
install_app_requirements

set +Eeuo pipefail
