# This Dockerfile and image, ghcr.io/dask/dask-gateway-server, is used by the
# dask-gateway Helm chart, by the api pod and the controller pod.
#
# The pods are started with different commands:
#
# - api pod command:        dask-gateway-server ...
# - controller pod command: dask-gateway-server kube-controller ...
#

ARG MYAPP_IMAGE=htcondor/submit:latest
FROM ${MYAPP_IMAGE}

## Install dependencies required for Python installation
RUN yum update -y && yum groupinstall -y "Development Tools" && \
    yum install -y \
    wget \
    zlib-devel \
    ncurses-devel \
    gdbm-devel \
    openssl-devel \
    readline-devel \
    libffi-devel \
    sqlite-devel \
    && yum clean all \
    && rm -rf /var/cache/yum

## Download and install Python 3.13
WORKDIR /tmp
RUN wget https://www.python.org/ftp/python/3.13.1/Python-3.13.1.tgz \
    && tar xzf Python-3.13.1.tgz \
    && cd Python-3.13.1 \
    && ./configure --enable-optimizations \
    && make -j $(nproc) \
    && make install \
    && cd .. \
    && rm -rf Python-3.13.1 Python-3.13.1.tgz

## Verify Python installation
RUN python3.13 --version

## Install pip for Python 3.13
RUN wget https://bootstrap.pypa.io/get-pip.py \
    && python3.13 get-pip.py \
    && rm get-pip.py

## Make Python 3.13 the default python3
#RUN alternatives --install /usr/bin/python3 python3 /usr/local/bin/python3.13 1 && \
#    alternatives --install /usr/bin/python python /usr/local/bin/python3.13 1 && \
#    alternatives --set python /usr/local/bin/python3.13

## Create symlinks for pip commands
RUN ln -sf /usr/local/bin/pip3.13 /usr/bin/pip && \
    ln -sf /usr/local/bin/pip3.13 /usr/bin/pip3


## Clean up
WORKDIR /





## Set labels based on the Open Containers Initiative (OCI):
## https://github.com/opencontainers/image-spec/blob/main/annotations.md#pre-defined-annotation-keys
##
LABEL org.opencontainers.image.source="https://github.com/dask/dask-gateway"
LABEL org.opencontainers.image.url="https://github.com/dask/dask-gateway/blob/HEAD/dask-gateway-server/Dockerfile"
#
## Install tini and upgrade linux packages are updated to patch known
## vulnerabilities. Use EPEL repository for tini.
RUN yum -y install epel-release \
 && yum -y update \
 && yum -y install \
        tini \
 && yum install -y golang \
 && yum install -y sudo \
 && yum clean all \
 && rm -rf /var/cache/yum

## Create a non-root user to run as
RUN useradd --create-home --user-group --uid 1002 dask
#
## To continue testing the slurm backend, it wants this directory to write tls
## certs to.
USER dask:dask
ENV PATH=/home/dask/.local/bin:$PATH
WORKDIR /home/dask/

## Install dask-gateway-server
##
## The Golang proxy binary isn't built as the dask-gateway Helm chart relies on
## Traefik as a proxy instead to run in its dedicated pod.
##
COPY --chown=dask:dask . /opt/dask-gateway-server


RUN DASK_GATEWAY_SERVER__NO_PROXY=false pip install --no-cache-dir  \
        -r /opt/dask-gateway-server/Dockerfile.requirements.txt

# USER root

ENTRYPOINT ["tini", "-g", "--"]
CMD ["dask-gateway-server", "--config", "/etc/dask-gateway/dask_gateway_config.py"]
