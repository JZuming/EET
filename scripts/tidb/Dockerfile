FROM ubuntu:20.04
ENV DEBIAN_FRONTEND=noninteractive

ARG TOOL_NAME=EET

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y apt-utils
RUN apt-get install -y vim cmake curl wget git tmux

# build golang
WORKDIR /root/
RUN wget https://go.dev/dl/go1.21.3.linux-amd64.tar.gz
RUN rm -rf /usr/local/go && tar -C /usr/local -xzf go1.21.3.linux-amd64.tar.gz
ENV PATH=$PATH:/usr/local/go/bin

# install latest tidb-server
# build tidb from source
RUN apt-get install -y mysql-client
RUN apt-get install -y libmysqlclient-dev
COPY tidb /root/tidb
WORKDIR /root/tidb
RUN make -j

# get tool
USER root
RUN apt-get update
RUN apt-get install -y build-essential autoconf autoconf-archive libpqxx-dev libboost-regex-dev
RUN apt-get install -y python3 pip
RUN pip install psutil
WORKDIR /root
COPY $TOOL_NAME /root/$TOOL_NAME
WORKDIR /root/$TOOL_NAME
RUN autoreconf -if
RUN ./configure 
RUN make clean; make -j

WORKDIR /root
RUN mkdir test
ENV ASAN_OPTIONS=detect_leaks=0
ENTRYPOINT bash