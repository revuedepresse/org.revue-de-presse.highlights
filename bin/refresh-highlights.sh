#!/bin/bash

function refresh_highlights() {
    local lein_bin=/usr/local/bin/lein
    local now="$(date)"
    /bin/bash -c "echo 'Started to record popularity of tweets at "${now}"' && \
    ${lein_bin} run save-highlights `date -I` && \
    ${lein_bin} run record-popularity-of-highlights `date -I` " 2>> ./logs/highlights.error.log >> ./logs/highlights.out.log && \
    echo 'Finished recording popularity of tweets at "'"$(date)"'"' >> ./logs/highlights.out.log || \
    echo 'Something went horribly horribly wrong' >> ./logs/highlights.out.log
}

refresh_highlights