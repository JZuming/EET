#!/bin/bash

DOCKER_DIR="."
TOOL_DIR="EET"
TOOL_SRC_DIR="../"$TOOL_DIR

set -e

echo "copy the sqlite "$TOOL_DIR" scripts"
cp $TOOL_SRC_DIR/scripts/sqlite/* $DOCKER_DIR/
cd $DOCKER_DIR

echo "check if there is sqlite source code"
if [ -d sqlite ]; then
    echo "there is a sqlite source code"
else
    echo "git clone sqlite ..."
    git clone https://github.com/sqlite/sqlite.git
    echo "reset to commit 20e09ba"
    cd sqlite
    git checkout 20e09ba
    cd ..
    echo "done"
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
