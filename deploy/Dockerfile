# The litecd image: the static binary plus the tools manifest generation
# runs (git, helm, kustomize), as Argo CD's repo server does.
FROM alpine:3.22
# ssh refuses to run for a uid /etc/passwd does not name: 65532 gets one.
RUN apk add --no-cache ca-certificates git openssh-client helm kustomize jsonnet \
    && adduser -D -H -h /tmp/litecd -u 65532 litecd
COPY litecd /usr/local/bin/litecd
USER 65532
ENTRYPOINT ["/usr/local/bin/litecd", "controller"]
