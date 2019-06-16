#!/bin/bash

lein_bin=/usr/local/bin/lein

function require_project_directory() {
    if [ -z "${CLJ_PROJECT_DIR}" ];
    then
        echo 'Please provide with a path the project directory e.g.'
        echo 'export CLJ_PROJECT_DIR=`pwd`'
        return 1
    fi

    return 0
}

function run_command() {
    local command="${1}"
    local success_message="${2}"

    project_dir_is_unavailable="$(require_project_directory)"
    if [ "${project_dir_is_unavailable}" == "1" ];
    then
        return
    fi

    local from="$CLJ_PROJECT_DIR"
    echo "${from}"
    cd "${from}"

    /bin/bash -c "${command}" 2>> ./logs/highlights.error.log >> ./logs/highlights.out.log && \
    now="$(date)" && \
    echo "${success_message}$(date)"'"' >> ./logs/highlights.out.log || \
    echo 'Something went horribly horribly wrong' >> ./logs/highlights.out.log
}

function refresh_highlights() {
    local now="$(date)"
    local command="echo 'Started to record popularity of tweets at "${now}"' &&
    ${lein_bin} run save-highlights `date -I` &&
    ${lein_bin} run record-popularity-of-highlights `date -I` "

    local success_message='Finished recording popularity of tweets at "'
    run_command "${command}" "${success_message}"
}

function save_highlights_for_all_aggregates() {
    local now="$(date)"
    local command="echo 'Started to record popularity of tweets at "${now}"' &&
    ${lein_bin} run save-highlights-for-all-aggregates  `date -I`"

    local success_message='Finished saving highlights for all aggregates at "'
    run_command "${command}" "${success_message}"
}

function build_clojure_container() {
    docker build -t devobs-clojure .
}

function remove_clojure_container {
    if [ `docker ps -a | grep 'devobs-clojure' | grep -c ''` -gt 0 ];
    then
        docker rm -f `docker ps -a | grep devobs-clojure | awk '{print $1}'`
    fi
}

function get_docker_network() {
    echo 'press-review-network'
}

function get_network_option() {
    network='--network '`get_docker_network`' '
    if [ ! -z "${NO_DOCKER_NETWORK}" ];
    then
        network=''
    fi

    echo "${network}";
}

function run_clojure_container() {
    local arguments="${COMMAND}"
    remove_clojure_container

    local network=`get_network_option`
    local command='docker run -it --hostname clojure \
        --hostname devobs.clojure '"${network}"' \
        --rm --name devobs-clojure devobs-clojure '"${arguments}"
    echo "About to run: \"${command}\""
    /bin/bash -c "${command}""${arguments}"
}

alias refresh-highlights=refresh_highlights
alias save-highlights-for-all-aggregates=save_highlights_for_all_aggregates
