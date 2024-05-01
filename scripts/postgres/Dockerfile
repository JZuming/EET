FROM ubuntu:20.04
ENV DEBIAN_FRONTEND=noninteractive

ARG TOOL_NAME=EET
ARG USER_NAME=zuming

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y apt-utils
RUN apt-get install -y vim cmake curl wget git tmux

# build postgres
RUN useradd -ms /bin/bash $USER_NAME
RUN apt-get install -y libicu-le-hb-dev libreadline-dev
RUN apt-get install -y flex bison
# RUN git clone https://github.com/postgres/postgres.git
COPY postgres /home/$USER_NAME/postgres
RUN chown -R $USER_NAME /home/$USER_NAME/postgres
USER $USER_NAME
WORKDIR /home/$USER_NAME/postgres

# # with asan
# USER root
# RUN apt-get install -y clang
# USER $USER_NAME
# RUN ./configure CC=clang CFLAGS="-fsanitize=address" LDFLAGS="-fsanitize=address"
# ENV ASAN_OPTIONS=detect_leaks=0
# without asan
RUN ./configure

RUN make -j
USER root
RUN make install
RUN mkdir -p /usr/local/pgsql/data
RUN chown $USER_NAME /usr/local/pgsql/data
USER $USER_NAME
RUN /usr/local/pgsql/bin/initdb -D /usr/local/pgsql/data

# get tool
USER root
RUN apt-get update
RUN apt-get install -y build-essential autoconf autoconf-archive libpqxx-dev libboost-regex-dev
COPY $TOOL_NAME /home/$USER_NAME/$TOOL_NAME
RUN chown -R $USER_NAME /home/$USER_NAME/$TOOL_NAME
USER $USER_NAME
WORKDIR /home/$USER_NAME/$TOOL_NAME
RUN autoreconf -if
RUN ./configure 
RUN make clean; make -j

USER root
WORKDIR /home/$USER_NAME
RUN mkdir test
ENTRYPOINT bash