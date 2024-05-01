FROM ubuntu:20.04
ENV DEBIAN_FRONTEND=noninteractive

ARG TOOL_NAME=EET

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y apt-utils
RUN apt-get install -y vim cmake curl wget git tmux

# build sqlite
RUN apt-get install -y tclsh
WORKDIR /root
# RUN git clone https://github.com/sqlite/sqlite.git
# use copy to prevent using old version (docker will directly skip if some instructions is executed previously)
COPY sqlite /root/sqlite
RUN mv sqlite sqlite_source/
WORKDIR /root/sqlite_source
RUN mkdir build
WORKDIR /root/sqlite_source/build
RUN ../configure --disable-tcl
RUN make -j
RUN make install

# get tool
USER root
RUN apt-get update
RUN apt-get install -y build-essential autoconf autoconf-archive libpqxx-dev libboost-regex-dev
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