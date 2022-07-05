#!/usr/bin/env bash
set -Eeuo pipefail

function add_system_user_group() {
    if [ $(cat /etc/group | grep "${WORKER_GID}" -c) -eq 0 ]; then
        groupadd \
            --gid "${WORKER_GID}" \
            worker
    fi

    useradd \
        --gid ${WORKER_GID} \
        --home-dir=/var/www \
        --create-home \
        --no-user-group \
        --non-unique \
        --shell /usr/sbin/nologin \
        --uid ${WORKER_UID} \
        worker
}

function clear_package_management_system_cache() {
    # Remove packages installed with apt except for tini
    apt-get remove --assume-yes build-essential gcc build-essential wget
    apt-get autoremove --assume-yes
    apt-get purge --assume-yes
    apt-get clean
    rm -rf /var/lib/apt/lists/*
}

function create_log_files_when_non_existing() {
    prefix="${1}"
    local prefix="${1}"

    if [ -z "${prefix}" ];
    then
        printf 'A %s is expected (%s).%s' 'non empty string' 'log file' $'\n'

        return 1
    fi

    mkdir \
      --verbose \
      --parents \
      "/var/www/${WORKER}/var/log"

    if [ ! -e "/var/www/${WORKER}/var/log/${prefix}.log" ];
    then

        touch "/var/www/${WORKER}/var/log/${prefix}.log"

        printf '%s "%s".%s' 'Created file located at' "/var/www/${WORKER}/var/log/${prefix}.log" $'\n'

    fi

    if [ ! -e "/var/www/${WORKER}/var/log/${prefix}.error.log" ];
    then

        touch "/var/www/${WORKER}/var/log/${prefix}.error.log"

        printf '%s "%s".%s' 'Created file located at' "/var/www/${WORKER}/var/log/${prefix}.error.log" $'\n'

    fi
}

function set_permissions() {
    chown -R  worker.   /var/www/"${WORKER}"/var/log/* \
                        /var/www \
                        /start.sh

    chmod     ug+x      /start.sh
}

function install_system_packages() {
    # Update package source repositories
    apt-get update

    # Install packages with package management system frontend (apt)
    apt-get install --assume-yes \
        apt-utils \
        ca-certificates \
        git \
        libcurl4-gnutls-dev \
        libpq-dev \
        make \
        procps \
        tini \
        unzip \
        wget
}

function install_tracing() {
    wget -O /var/www/dd-java-agent.jar https://dtdg.co/latest-java-tracer
    chown worker. /var/www/dd-java-agent.jar
}

set -Eeuo pipefail

