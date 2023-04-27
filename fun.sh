#!/usr/bin/env bash
set -Eeuo pipefail

function load_configuration_parameters() {
    if [ ! -e ./.env ]; then
        cp --verbose ./.env{.dist,}
    fi

    if [ ! -e ./provisioning/containers/docker-compose.override.yaml ]; then
        cp ./provisioning/containers/docker-compose.override.yaml{.dist,}
    fi

    source ./.env

    validate_docker_compose_configuration
    guard_against_missing_variables
}

function _set_file_permissions() {
    local temporary_directory
    temporary_directory="${1}"

    if [ -z "${temporary_directory}" ];
    then
        printf 'A %s is expected as %s (%s).%s' 'non-empty string' '1st argument' 'temporary directory file path' $'\n'

        return 1;
    fi

    if [ ! -d "${temporary_directory}" ];
    then
        printf 'A %s is expected as %s (%s).%s' 'directory' '1st argument' 'temporary directory file path' $'\n'

        return 1;
    fi

    local project_name
    project_name="$(get_project_name)"

    docker compose \
        --project-name="${project_name}" \
        -f ./provisioning/containers/docker-compose.yaml \
        -f ./provisioning/containers/docker-compose.override.yaml \
        run \
        --rm \
        --user root \
        --volume "${temporary_directory}:/tmp/remove-me" \
        app \
        /bin/bash -c 'chmod -R ug+w /tmp/remove-me'
}

function build() {
    local DEBUG
    local WORKER
    local WORKER_OWNER_UID
    local WORKER_OWNER_GID

    load_configuration_parameters

    if [ $? -gt 1 ];
    then

        printf '%s.%s' 'Invalid configuration files' $'\n' 1>&2

        return 1;

    fi

    local project_name
    project_name="$(get_project_name)"

    if [ -n "${DEBUG}" ];
    then

        clean ''

        docker compose \
            --project-name="${project_name}" \
            --file=./provisioning/containers/docker-compose.yaml \
            --file=./provisioning/containers/docker-compose.override.yaml \
            build \
            --no-cache \
            --build-arg "WORKER=${WORKER}" \
            --build-arg "OWNER_UID=${WORKER_OWNER_UID}" \
            --build-arg "OWNER_GID=${WORKER_OWNER_GID}" \
            app \
            worker

    else

        docker compose \
            --project-name="${project_name}" \
            --file=./provisioning/containers/docker-compose.yaml \
            --file=./provisioning/containers/docker-compose.override.yaml \
            build \
            --build-arg "WORKER=${WORKER}" \
            --build-arg "OWNER_UID=${WORKER_OWNER_UID}" \
            --build-arg "OWNER_GID=${WORKER_OWNER_GID}" \
            app \
            worker

    fi
}

function guard_against_missing_variables() {
    if [ -z "${WORKER}" ];
    then

        printf 'A %s is expected as %s ("%s" environment variable).%s' 'non-empty string' 'worker name e.g. worker.example.com' 'WORKER' $'\n'

        exit 1

    fi

    if [ "${WORKER}" = 'org.example.highlights' ];
    then

        printf 'Have you picked a satisfying worker name ("%s" environment variable - "%s" as default value is not accepted).%s' 'WORKER' 'highlights.example.org' $'\n'

        exit 1

    fi

    if [ -z "${WORKER_OWNER_UID}" ];
    then

 OWNER_       printf 'A %s is expected as %s ("%s").%s' 'non-empty numeric' 'system user uid' 'WORKER_OWNER_UID' $'\n'

     OWNER_   exit 1

    fi

    if [ -z "${WORKER_OWNER_GID}" ];
    then

        printf 'A %s is expected as %s ("%s").%s' 'non-empty numeric' 'system user gid' 'WORKER_OWNER_GID' $'\n'

        exit 1

    fi
}

function remove_running_container_and_image_in_debug_mode() {
    local container_name
    container_name="${1}"

    if [ -z "${container_name}" ];
    then

        printf 'A %s is expected as %s ("%s").%s' 'non-empty string' '1st argument' 'container name' $'\n'

        return 1

    fi

    local DEBUG
    local WORKER_OWNER_UID
    local WORKER_OWNER_GID
    local WORKER
    local COMPOSE_PROJECT_NAME

    load_configuration_parameters

    if [ $? -gt 1 ];
    then

        printf '%s.%s' 'Invalid configuration files' $'\n' 1>&2

        return 1;

    fi

    local project_name
    project_name="$(get_project_name)"

    cat <<- CMD
      docker ps -a |
      \grep "${project_name}" |
      \grep "${container_name}" |
      awk '{print \$1}' |
      xargs -I{} docker rm -f {}
CMD

    docker ps -a |
        \grep "${project_name}" |
        \grep "${container_name}" |
        awk '{print $1}' |
        xargs -I{} docker rm -f {}

    if [ -n "${DEBUG}" ];
    then

        cat <<- CMD
        docker images -a |
        \grep "${project_name}" |
        \grep "${container_name}" |
        awk '{print \$3}' |
        xargs -I{} docker rmi -f {}
CMD

        docker images -a |
            \grep "${project_name}" |
            \grep "${container_name}" |
            awk '{print $3}' |
            xargs -I{} docker rmi -f {}

    fi
}

function clean() {
    local temporary_directory
    temporary_directory="${1}"

    if [ -n "${temporary_directory}" ];
    then
        printf 'About to remove "%s".%s' "${temporary_directory}" $'\n'

        _set_file_permissions "${temporary_directory}"

        return 0
    fi

    remove_running_container_and_image_in_debug_mode 'app'
    remove_running_container_and_image_in_debug_mode 'worker'
}

function install() {
    local DEBUG
    local WORKER_OWNER_UID
    local WORKER_OWNER_GID
    local WORKER

    load_configuration_parameters

    if [ $? -gt 1 ];
    then

        printf '%s.%s' 'Invalid configuration files' $'\n' 1>&2

        return 1;

    fi

    local project_name
    project_name="$(get_project_name)"

    docker compose \
        --project-name="${project_name}" \
        -f ./provisioning/containers/docker-compose.yaml \
        -f ./provisioning/containers/docker-compose.override.yaml \
        run \
        --env WORKER="${WORKER}" \
        --user root \
        --rm \
        --no-TTY \
        app \
        /bin/bash -c 'source /scripts/install-app-requirements.sh'
}

function get_project_name() {
    if [ -z "${COMPOSE_PROJECT_NAME}" ];
    then

      printf 'A %s is expected as %s ("%s").%s' 'non-empty string' '"COMPOSE_PROJECT_NAME" environment variable' 'docker compose project name' $'\n'

      return 1;

    fi

    echo "${COMPOSE_PROJECT_NAME}"
}

function get_worker_shell() {
    if ! command -v jq >> /dev/null 2>&1;
    then
        printf 'Is %s (%s) installed?%s' 'command-line JSON processor' 'jq' $'\n'

        return 1
    fi

    local project_name
    project_name="$(get_project_name)"

    docker exec -ti "$(
        docker ps -a \
        | \grep "${project_name}" \
        | \grep 'worker' \
        | awk '{print $1}'
    )" bash
}

function start() {
    local DEBUG
    local WORKER
    local WORKER_OWNER_UID
    local WORKER_OWNER_GID

    load_configuration_parameters

    if [ $? -gt 1 ];
    then

        printf '%s.%s' 'Invalid configuration files' $'\n' 1>&2

        return 1;

    fi

    if [ -z "${CMD}" ];
    then

        CMD='save-highlights-from-date-for-publishers-list'

    fi

    if [ -z "${DATE}" ];
    then

        DATE="$(date -I)"

    fi

    if [ -z "${LIST}" ];
    then

        LIST="${LIST_NAME}"

    fi

    local agent
    agent=''

    if [ -n "${DD_AGENT_HOST}" ];
    then
        agent='-javaagent:/var/www/dd-java-agent.jar '
    fi

    local project_name
    project_name="$(get_project_name)"

    cmd="$(
        cat <<-START
				docker compose \
        --project-name="${project_name}" \
				--file=./provisioning/containers/docker-compose.yaml \
				--file=./provisioning/containers/docker-compose.override.yaml \
				run \
				--detach \
				--rm \
				worker \
				java \
				${agent}-jar ./highlights-standalone.jar \
				'${CMD}' '${DATE}' '${LIST}'
START
)"
    printf '%s.%s' 'About to run the following command' $'\n' 1>&2
    printf '%s%s' "${cmd}" $'\n'                              1>&2

    printf '%s:%s' 'For list' $'\n'                           1>&2
    printf '%s%s' "${LIST}" $'\n'                             1>&2

    printf '%s:%s' 'For date' $'\n'                           1>&2
    printf '%s%s' "${DATE}" $'\n'                             1>&2

    container_name="$(/bin/bash -c "${cmd}")"
    docker logs -f "${container_name}" 2>&1 | grep -v environ >> "./var/log/${WORKER}.log"
}

function stop() {
    guard_against_missing_variables

    remove_running_container_and_image_in_debug_mode 'worker'
}

function test() {
  (
    for param in $(\cat ./.env.test.dist | sed -E "s/='([^']*)'/=\1/g");
        do export $param;
    done

    env | grep -E '^DATABASE_' | grep -v 'PASSWORD'
    lein test
  )
}

function validate_docker_compose_configuration() {
    local project_name
    project_name="$(get_project_name)"

    docker compose \
        --project-name="${project_name}" \
        -f ./provisioning/containers/docker-compose.yaml \
        -f ./provisioning/containers/docker-compose.override.yaml \
        config -q
}

set +Eeuo pipefail
