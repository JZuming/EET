#!/bin/bash

DOCKER_DIR="."
TOOL_DIR="EET"
TOOL_SRC_DIR="../"$TOOL_DIR

set -e

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

docker_num=1
test_each_docker=1

if [ $# -ge 1 ]; then
    docker_num=$1
fi

if [ $# -ge 2 ]; then
    test_each_docker=$2
fi

set +e

./build_docker.sh n y $docker_num $test_each_docker n