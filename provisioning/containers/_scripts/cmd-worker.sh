#!/usr/bin/env bash
set -Eeo pipefail

function start() {
    printf '%s.%s' 'Changing directories' $'\n' 1>&2
    printf '%s%s' "/var/www/${WORKER}" $'\n' 1>&2

    cd "/var/www/${WORKER}" || exit

    if [ -e ./highlights-revuedepresse-standalone.jar ];
    then

        printf '%s.%s' 'About to run application' $'\n' 1>&2
        tail -F /dev/null

    else

        printf '%s.%s' 'About to install application' $'\n' 1>&2
        source '/scripts/install-app-requirements.sh'

    fi
}
start $@

set +Eeo pipefail
