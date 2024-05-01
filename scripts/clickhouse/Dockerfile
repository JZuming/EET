FROM ubuntu:20.04
ENV DEBIAN_FRONTEND=noninteractive

ARG TOOL_NAME=EET

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y apt-utils
RUN apt-get install -y vim cmake curl wget git tmux

WORKDIR /root
RUN apt-get install -y git cmake ccache python3 ninja-build nasm yasm gawk
RUN apt-get install -y software-properties-common
RUN add-apt-repository -y ppa:ubuntu-toolchain-r/test

# get llvm 16
WORKDIR /root
RUN wget https://apt.llvm.org/llvm.sh
RUN bash llvm.sh 16
ENV CC=clang-16
ENV CXX=clang++-16

# get latest cmake
WORKDIR /root
RUN wget https://github.com/Kitware/CMake/archive/refs/tags/v3.26.3.tar.gz
RUN tar -xf v3.26.3.tar.gz
WORKDIR /root/CMake-3.26.3
RUN apt-get install -y libssl-dev
RUN ./bootstrap && make -j && make install
RUN apt-get remove -y cmake
RUN cp bin/cmake /usr/bin/

# build Clickhouse
WORKDIR /root
COPY ClickHouse /root/ClickHouse
WORKDIR /root/ClickHouse
# RUN git submodule update --init
# RUN git pull --recurse-submodules
RUN mkdir build
RUN cmake -S . -B build
RUN cmake --build build
RUN cp build/programs/clickhouse /usr/bin/

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