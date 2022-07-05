#!/bin/bash

lein_bin=/usr/local/bin/lein

function require_project_directory() {
    if [ -z "${WORKER}" ];
    then

        printf '%s' 'Please provide with a path the project directory e.g.' 1>&2
        printf '%s' 'export ${WORKER}=$(pwd)' 1>&2

        return 1

    fi

    return 0
}

function run_clojure_container() {
    echo "About to run: \"${COMMAND}\""
    make start
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
    from="/var/www/${WORKER}"
    echo 'About to run command from "'"${from}"'"'
    cd "${from}" || exit

    echo '-> About to run "'"${command}"'"'

    echo "${before_message}" && \
    export COMMAND="${command}" && run_clojure_container \
    >> "./var/log/${WORKER}.log" \
    2>> "./var/log/${WORKER}.error.log" && \
    now="$(date)" && \
    echo "${success_message}$(date)"'"' >> "./var/log/${WORKER}.log" || \
    echo 'Something went horribly horribly wrong' >> "./var/log/${WORKER}.log"
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
