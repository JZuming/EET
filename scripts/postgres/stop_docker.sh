#!/bin/bash

TOOL_DIR="eet"
DOCKER_CONTAINER_NAME=$TOOL_DIR"-development-postgres"

echo "stop and remove the previous container"
docker stop $(docker ps  -a --format "table {{.Names}}" | grep $DOCKER_CONTAINER_NAME)
docker rm $(docker ps  -a --format "table {{.Names}}" | grep $DOCKER_CONTAINER_NAME)