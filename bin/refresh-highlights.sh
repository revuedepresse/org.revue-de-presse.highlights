#!/bin/bash

function refresh_highlights() {
    if [ -z "${CLJ_PROJECT_DIR}" ];
    then
        echo 'Please provide with a path the project directory e.g.'
        echo 'export CLJ_PROJECT_DIR=`pwd`'
    fi

    local from="$CLJ_PROJECT_DIR"
    local lein_bin=/usr/local/bin/lein
    local now="$(date)"
    local command="echo 'Started to record popularity of tweets at "${now}"' &&
    ${lein_bin} run save-highlights `date -I` &&
    ${lein_bin} run record-popularity-of-highlights `date -I` "

    echo "${from}"
    cd "${from}"

    /bin/bash -c "${command}" 2>> ./logs/highlights.error.log >> ./logs/highlights.out.log && echo 'Finished recording popularity of tweets at "'"$(date)"'"' >> ./logs/highlights.out.log || \
    echo 'Something went horribly horribly wrong' >> ./logs/highlights.out.log
}

refresh_highlights