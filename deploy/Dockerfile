# The litecd image: the static binary plus the tools manifest generation
# runs (git, helm, kustomize), as Argo CD's repo server does.
FROM alpine:3.22
RUN apk add --no-cache ca-certificates git openssh-client helm kustomize jsonnet
COPY litecd /usr/local/bin/litecd
USER 65532
ENTRYPOINT ["/usr/local/bin/litecd", "controller"]
