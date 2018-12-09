#!/bin/bash

function refresh_highlights() {
    local lein_bin=/usr/local/bin/lein

    /bin/bash -c "${lein_bin} run save-highlights `date -I` && \
    ${lein_bin} run record-popularity-of-highlights `date -I`" 2>> ./logs/highlights.error.log \
    >> ./logs/highlights.out.log
}

refresh_highlights