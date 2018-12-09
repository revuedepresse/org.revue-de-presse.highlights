#!/bin/bash

function refresh_highlights() {
    /bin/bash -c '/usr/local/bin/lein run save-highlights `date -I` && \
    lein run record-popularity-of-highlights `date -I` ' 2>> ./logs/highlights.error.log \
    >> ./logs/highlights.out.log
}

refresh_highlights