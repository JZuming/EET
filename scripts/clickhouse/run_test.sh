#!/bin/bash

DOCKER_DIR="."
TOOL_DIR="EET"
TOOL_SRC_DIR="../"$TOOL_DIR

set -e

echo "copy the clickhouse qit scripts"
cp $TOOL_SRC_DIR/scripts/clickhouse/* $DOCKER_DIR/
cd $DOCKER_DIR

echo "check if there is clickhouse source code"
if [ -d ClickHouse ]; then
    echo "there is a clickhouse source code"
else
    echo "git clone clickhouse ..."
    git clone https://github.com/ClickHouse/ClickHouse.git
    echo "reset to commit 30464b9"
    cd ClickHouse
    git checkout 30464b9
    echo "init submodules"
    git submodule update --init
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
