FROM --platform=${BUILDPLATFORM} icr.io/db2_community/db2 AS client

FROM rockylinux:9 AS stage
ENV LICENSE=accept
ENV DB2_HOME=/opt/ibm/db2/V12.1

COPY --from=client /opt/ibm/db2/V12.1/include /opt/ibm/db2/V12.1/include
COPY --from=client /opt/ibm/db2/V12.1/lib64 /opt/ibm/db2/V12.1/lib64
COPY --from=client /opt/ibm/db2/V12.1/bin /opt/ibm/db2/V12.1/bin

WORKDIR /usr/src/app
COPY cmd internal pkg /usr/src/app/ 

RUN dnf install -y epel-release && \
    dnf install -y golang git && \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -v -a -ldflags="-X 'github.com/pgvillage-tools/dbtwool/internal/version/main.appVersion=${VERSION}'" -o /usr/local/bin/dbtwool ./cmd/dbtwool