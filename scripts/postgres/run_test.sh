#!/bin/bash

DOCKER_DIR="."
TOOL_DIR="qit"
TOOL_SRC_DIR="../"$TOOL_DIR

echo "copy the postgres qit scripts"
cp $TOOL_SRC_DIR/scripts/postgres/* $DOCKER_DIR/
cd $DOCKER_DIR

echo "check if there is postgres source code"
if [ -d postgres ]; then
    echo "there is a postgres source code"
else
    echo "git clone postgres ..."
    git clone https://github.com/postgres/postgres
    echo "done"
fi

./build_docker.sh
