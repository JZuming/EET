#!/bin/bash

DOCKER_DIR="."
TOOL_DIR="eet"
TOOL_SRC_DIR="../"$TOOL_DIR

echo "copy the mysql "$TOOL_DIR" scripts"
cp $TOOL_SRC_DIR/scripts/mysql/* $DOCKER_DIR/
cd $DOCKER_DIR

echo "check if there is mysql source code"
if [ -d mysql_source ]; then
    echo "there is a mysql source code tar"
else
    echo "download mysql 8.0.34 ..."
    wget https://github.com/mysql/mysql-server/archive/refs/tags/mysql-8.0.34.tar.gz
    tar -zxf mysql-8.0.34.tar.gz
    mv mysql-server-mysql-8.0.34 mysql_source/
    rm mysql-8.0.34.tar.gz
fi

mode="default"
if [ $# -ge 1 ]; then
    mode=$1
fi

if [ "$mode" == "large" ]; then
    ./build_docker.sh n y 8 1 n
else
    ./build_docker.sh n y 1 1 n
fi
