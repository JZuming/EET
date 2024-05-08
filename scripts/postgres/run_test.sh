#!/bin/bash

DOCKER_DIR="."
TOOL_DIR="EET"
TOOL_SRC_DIR="../"$TOOL_DIR

set -e

echo "copy the postgres "$TOOL_DIR" scripts"
cp $TOOL_SRC_DIR/scripts/postgres/* $DOCKER_DIR/
cd $DOCKER_DIR

echo "check if there is postgres source code"
if [ -d postgres ]; then
    echo "there is a postgres source code"
else
    echo "git clone postgres ..."
    git clone https://github.com/postgres/postgres
    echo "reset to commit 3f1aaaa"
    cd postgres
    git checkout 3f1aaaa
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
