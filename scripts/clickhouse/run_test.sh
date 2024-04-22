#!/bin/bash

DOCKER_DIR="."
TOOL_DIR="qit"
TOOL_SRC_DIR="../"$TOOL_DIR

echo "copy the clickhouse qit scripts"
cp $TOOL_SRC_DIR/scripts/clickhouse/* $DOCKER_DIR/
cd $DOCKER_DIR

echo "check if there is clickhouse source code"
if [ -d ClickHouse ]; then
    echo "there is a clickhouse source code"
else
    echo "git clone clickhouse ..."
    git clone https://github.com/ClickHouse/ClickHouse.git
    echo "done"
fi

./build_docker.sh
