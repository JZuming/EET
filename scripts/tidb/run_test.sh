#!/bin/bash

DOCKER_DIR="."
TOOL_DIR="qit"
TOOL_SRC_DIR="../"$TOOL_DIR

echo "copy the tidb qit scripts"
cp $TOOL_SRC_DIR/scripts/tidb/* $DOCKER_DIR/
cd $DOCKER_DIR

echo "check if there is tidb source code"
if [ -d tidb ]; then
    echo "there is a tidb source code"
else
    echo "git clone tidb ..."
    git clone https://github.com/pingcap/tidb.git
    echo "done"
fi

./build_docker.sh
