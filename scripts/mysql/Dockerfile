FROM ubuntu:20.04
ENV DEBIAN_FRONTEND=noninteractive

ARG TOOL_NAME=EET

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y apt-utils
RUN apt-get install -y vim cmake curl wget git

# prepare for mysql
RUN apt-get install -y libssl-dev g++ libncurses5-dev
RUN apt-get install -y pkg-config bison 
RUN useradd -ms /bin/bash mysql
WORKDIR /home/mysql
COPY mysql_source mysql_source/
RUN chown -R mysql:mysql mysql_source/
USER mysql

# build mysql
WORKDIR /home/mysql/mysql_source
RUN mkdir build
USER root
WORKDIR /home/mysql/mysql_source/build
RUN cmake -DWITH_UNIT_TESTS=OFF \
        -DDOWNLOAD_BOOST=1 -DWITH_BOOST=/home/mysql ../ 
RUN make -j$(nproc)
# install mysql
USER root
RUN make install
RUN chown -R mysql:mysql /usr/local/mysql/
USER mysql
WORKDIR /usr/local/mysql/
RUN ./bin/mysqld --initialize-insecure
# RUN ./bin/mysqld --initialize-insecure --thread_stack=512k
# ENV LD_LIBRARY_PATH /usr/local/mysql/lib

# get txfuzz
USER root
RUN apt-get update
RUN apt-get install -y tmux
RUN apt-get install -y build-essential autoconf autoconf-archive libpqxx-dev libboost-regex-dev libsqlite3-dev
RUN apt-get install -y libmysqlclient-dev
WORKDIR /home/mysql
COPY $TOOL_NAME /home/mysql/$TOOL_NAME
RUN chown -R mysql:mysql /home/mysql/$TOOL_NAME
USER mysql
WORKDIR /home/mysql/$TOOL_NAME
RUN autoreconf -if
RUN ./configure
RUN make clean; make -j

WORKDIR /home/mysql
USER root
RUN mkdir test
ENV ASAN_OPTIONS=detect_leaks=0
ENTRYPOINT bash
