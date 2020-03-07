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

function get_image_name() {
    local image_name

    local work_directory
    work_directory="$(pwd)"

    image_name="$(echo "${work_directory}" | sha1sum | tail -c12 | awk '{print $1}')"
    echo "devobs-api-${image_name}"
}

function build_clojure_container() {
    local image_name
    image_name=$(get_image_name)

    docker build -t "${image_name}" .
}

function get_container_name() {
    local suffix
    suffix="${1}"

    echo 'devobs-api-'"${suffix}"
}

function get_docker_network() {
    if [ -n "${NETWORK}" ]; then
        echo "${NETWORK}"
        return
    fi

    echo 'devobs-api-network'
}

function create_network() {
    local network
    network=$(get_docker_network)
    /bin/bash -c 'docker network create '"${network}"
}

function get_network_option() {
    local network

    network='--network '$(get_docker_network)' '
    if [ ! -z "${NO_DOCKER_NETWORK}" ];
    then
        network=''
    fi

    echo "${network}";
}

function remove_clojure_container {
    local suffix
    suffix="${1}"

    local container_name
    container_name="$(get_container_name "${suffix}")"

    if [ "$(docker ps -a | grep "${container_name}" | grep -c '')" -gt 0 ];
    then
        docker rm -f "$(docker ps -a | grep "${container_name}" | awk '{print $1}')"
    fi
}

function run_clojure_container() {
    local arguments
    arguments="${COMMAND}"

    local prefix
    prefix="$(pwd)"

    local container_name_suffix=
    container_name_suffix="$(echo "${prefix}${arguments}" | sha1sum  | awk '{print $1}')"

    local container_name
    container_name="$(get_container_name "${container_name_suffix}")"

    remove_clojure_container "${container_name_suffix}"

    local interactive_mode_option=''
    if [ -z "${arguments}" ];
    then
        interactive_mode_option=' -ti'
    fi

    local image_name
    image_name=`get_image_name`

    local network
    network=`get_network_option`

    local command
    command='docker run '"${interactive_mode_option}"' \
        --hostname devobs.clojure '"${network}"' \
        --rm --name '"${container_name}"' '"${image_name}"' \
        java -jar devobs-standalone.jar '"${arguments}"
    echo "About to run: \"${command}\""
    /bin/bash -c "${command}"
}

function run_command() {
    local command
    command="${1}"

    local before_message
    before_message="${2}"

    local success_message
    success_message="${3}"

    local project_dir_is_unavailable
    project_dir_is_unavailable="$(require_project_directory)"
    if [ $? -eq 1 ];
    then
        echo "${project_dir_is_unavailable}"
        return
    fi

    local from
    from="$CLJ_PROJECT_DIR"
    echo 'About to run command from "'"${from}"'"'
    cd "${from}" || exit

    echo '-> About to run "'"${command}"'"'

    echo "${before_message}" && \
    export COMMAND="${command}" && run_clojure_container \
    2>> ./logs/highlights.error.log \
    >> ./logs/highlights.out.log && \
    now="$(date)" && \
    echo "${success_message}$(date)"'"' >> ./logs/highlights.out.log || \
    echo 'Something went horribly horribly wrong' >> ./logs/highlights.out.log
}

function refresh_highlights() {
    local now
    now="$(date)"

    local before_message
    before_message='Started to refresh highlights at '"${now}"

    local success_message
    success_message='Finished refreshing highlights at "'

    local command
    command='save-highlights '"$(date -I)"

    run_command "${command}" "${before_message}" "${success_message}"

    now="$(date)"
    before_message='Started to record popularity of highlights at '"${now}"
    success_message='Finished recording popularity of highlights at "'
    command='record-popularity-of-highlights '"$(date -I)"

    run_command "${command}" "${before_message}" "${success_message}"
}

function save_highlights_for_all_aggregates() {
    local command
    command="save-highlights-for-all-aggregates $(date -I)"

    local now
    now="$(date)"

    local before_message
    before_message='Started to record popularity of tweets at '"${now}"

    local success_message
    success_message='Finished saving highlights for all aggregates at "'

    run_command "${command}" "${before_message}" "${success_message}"
}

alias refresh-highlights=refresh_highlights
alias save-highlights-for-all-aggregates=save_highlights_for_all_aggregates
