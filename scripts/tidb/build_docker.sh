#!/bin/bash -v

DOCKER_DIR="."
TOOL_DIR="qit"
TOOL_SRC_DIR="../"$TOOL_DIR
DOCKER_CONTAINER_NAME=$TOOL_DIR"-development-tidb"
DOCKER_IMAGE_NAME=$DOCKER_CONTAINER_NAME
DOCKER_VOLUME_LOCATION="/zdata/zuming/qit/tidb_docker_disk"

echo "stop and remove the previous container"
docker stop $(docker ps  -a --format "table {{.Names}}" | grep $DOCKER_CONTAINER_NAME)
docker rm $(docker ps  -a --format "table {{.Names}}" | grep $DOCKER_CONTAINER_NAME)

echo "copy the qit source code"
rm $DOCKER_DIR/$TOOL_DIR -rf
cp -r $TOOL_SRC_DIR $DOCKER_DIR
cd $DOCKER_DIR

echo "build the qit test image"
DOCKER_BUILDKIT=1 docker build -t $DOCKER_CONTAINER_NAME .

rm $DOCKER_DIR/$TOOL_DIR -rf
docker rmi $(docker image ls -f dangling=true -q)

# run a container
docker run -itd -m 128g --name $DOCKER_CONTAINER_NAME $DOCKER_IMAGE_NAME

# cp and run test_setup.sh
# docker cp test_setup.sh $DOCKER_CONTAINER_NAME:/root
docker cp setup_and_monitor_tidb.py $DOCKER_CONTAINER_NAME:/root
# docker exec -it $DOCKER_CONTAINER_NAME sh -v test_setup.sh
