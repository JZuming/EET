#!/bin/bash

DOCKER_DIR="."
TOOL_DIR="EET"
TOOL_SRC_DIR="../"$TOOL_DIR

set -e

echo "copy the tidb "$TOOL_DIR" scripts"
cp $TOOL_SRC_DIR/scripts/tidb/* $DOCKER_DIR/
cd $DOCKER_DIR

echo "check if there is tidb source code"
if [ -d tidb ]; then
    echo "there is a tidb source code"
else
    echo "git clone tidb ..."
    git clone https://github.com/pingcap/tidb.git
    echo "reset to commit f5ca27e"
    cd tidb
    git checkout f5ca27e
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
