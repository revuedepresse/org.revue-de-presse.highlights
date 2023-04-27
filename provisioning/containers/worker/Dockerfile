FROM        clojure:openjdk-11-lein-2.9.8-slim-bullseye

ARG         OWNER_UID
ARG         OWNER_GID
ARG         WORKER

RUN         test -n "${OWNER_UID}" && \
            test -n "${OWNER_GID}" && \
            test -n "${WORKER}"

ENV         WORKER_OWNER_UID=${OWNER_UID}
ENV         WORKER_OWNER_GID=${OWNER_GID}

ARG         WORKER_DIR="${WORKER}"

COPY        --chown=${WORKER_OWNER_UID}:${WORKER_OWNER_GID} \
            ./provisioning/containers/_scripts \
            /scripts

COPY        --chown=${WORKER_OWNER_UID}:${WORKER_OWNER_GID} \
            ./provisioning/containers/_scripts/cmd-worker.sh \
            /start.sh

RUN         /bin/bash -c 'source /scripts/install-worker-requirements.sh'

USER        ${WORKER_OWNER_UID}:${WORKER_OWNER_GID}

VOLUME      ["/scripts", "/var/www/shared", "/var/www/${WORKER_DIR}"]

WORKDIR     "/var/www/${WORKER_DIR}"

ENTRYPOINT  ["/usr/bin/tini", "--"]

CMD         ["/start.sh"]
