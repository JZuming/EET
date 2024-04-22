#!/bin/bash

DOCKER_DIR="."
TOOL_DIR="qit"
TOOL_SRC_DIR="../"$TOOL_DIR

echo "copy the mysql qit scripts"
cp $TOOL_SRC_DIR/scripts/mysql/* $DOCKER_DIR/
cd $DOCKER_DIR

echo "check if there is mysql source code"
if [ -f *mysql*tar.gz ]; then
    echo "there is a mysql source code tar"
else
    echo "download mysql 8.0.34 ..."
    wget https://github.com/mysql/mysql-server/archive/refs/tags/mysql-8.0.34.tar.gz
    echo "downloaded"
fi

./build_docker.sh
