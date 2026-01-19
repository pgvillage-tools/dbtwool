FROM rockylinux:9

RUN dnf -y update && dnf -y install \
    git \
    gcc \
    gcc-c++ \
    make \
    wget \
    curl-minimal \
    libxcrypt-compat \
    tar \
    xz \
    which \
    ca-certificates \
    && dnf clean all


RUN curl -sSL https://go.dev/dl/go1.25.4.linux-amd64.tar.gz -o /tmp/go.tar.gz && \
    rm -rf /usr/local/go && \
    tar -C /usr/local -xzf /tmp/go.tar.gz && \
    rm /tmp/go.tar.gz


ENV GOPATH=/root/go
ENV PATH=/usr/local/go/bin:$GOPATH/bin:$PATH

RUN go install github.com/ibmdb/go_ibm_db/installer@latest

ENV IBM_DB_HOME=/root/go/pkg/mod/github.com/ibmdb/clidriver
ENV CGO_CFLAGS="-I${IBM_DB_HOME}/include"
ENV CGO_LDFLAGS="-L${IBM_DB_HOME}/lib"
ENV LD_LIBRARY_PATH="${IBM_DB_HOME}/lib:${LD_LIBRARY_PATH}

RUN /root/go/bin/installer || true

WORKDIR /app


CMD ["/bin/bash"]