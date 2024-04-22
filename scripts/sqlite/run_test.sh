#!/bin/bash

DOCKER_DIR="."
TOOL_DIR="qit"
TOOL_SRC_DIR="../"$TOOL_DIR

echo "copy the sqlite qit scripts"
cp $TOOL_SRC_DIR/scripts/sqlite/* $DOCKER_DIR/
cd $DOCKER_DIR

echo "check if there is sqlite source code"
if [ -d sqlite ]; then
    echo "there is a sqlite source code"
else
    echo "git clone sqlite ..."
    git clone https://github.com/sqlite/sqlite.git
    echo "done"
fi

./build_docker.sh
