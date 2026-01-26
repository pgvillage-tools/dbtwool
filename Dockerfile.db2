ARG VERSION=v0.0.0

FROM --platform=amd64 golang:bookworm AS buildstage
ARG IBM_HOME=/opt/ibm/db2
ARG IBM_DB_HOME=$IBM_HOME/clidriver
RUN set -x && export SHELLOPTS && \
    uname -a && \
    mkdir -p "$IBM_HOME" && \
    cd $IBM_HOME && \
    git clone https://github.com/ibmdb/go_ibm_db/ && \
    go env GOPATH && \
    cd go_ibm_db/installer && \
    go run ./setup.go

ENV CGO_CFLAGS=-I$IBM_DB_HOME/include \
    CGO_LDFLAGS=-L$IBM_DB_HOME/lib \
    LD_LIBRARY_PATH=$IBM_DB_HOME/lib

WORKDIR /usr/src/app/

COPY go.* /usr/src/app/
COPY cmd /usr/src/app/cmd/
COPY internal /usr/src/app/internal/
COPY pkg /usr/src/app/pkg/

RUN go mod tidy
RUN go build -v -a -ldflags="-X 'github.com/pgvillage-tools/dbtwool/internal/version/main.appVersion=${VERSION}'" -o ./dbtwool ./cmd/dbtwool

FROM --platform=amd64 debian:bookworm

ENV CGO_CFLAGS=-I$IBM_DB_HOME/include \
    CGO_LDFLAGS=-L$IBM_DB_HOME/lib \
    LD_LIBRARY_PATH=$IBM_DB_HOME/lib

COPY --from=buildstage "$IBM_DB_HOME" "$IBM_DB_HOME"
COPY --from=buildstage /usr/src/app/dbtwool /dbtwool

ENTRYPOINT ["/dbtwool"]
