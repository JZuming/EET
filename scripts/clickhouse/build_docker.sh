#!/bin/bash

DOCKER_DIR="."
TOOL_DIR="EET"
TOOL_SRC_DIR="../"$TOOL_DIR
DOCKER_CONTAINER_NAME="eet-development-clickhouse"
DOCKER_IMAGE_NAME=$DOCKER_CONTAINER_NAME
MEM_LIMIT=10g

echo "stop and remove the previous container"
./stop_docker.sh

echo "copy the qit source code"
rm $DOCKER_DIR/$TOOL_DIR -rf
cp -r $TOOL_SRC_DIR $DOCKER_DIR
cd $DOCKER_DIR

echo "build the "$TOOL_DIR" test image"
DOCKERFILE="Dockerfile"

echo "/****************************/"
if [ $# -ge 1 ]; then
    echo "build database engine with asan (y/n)? : "$1
fi

if [ $# -ge 2 ]; then
    echo "run the test now (y/n)? : "$2
fi

if [ $# -ge 3 ]; then
    echo "docker number (max 8): "$3
fi

if [ $# -ge 4 ]; then
    echo "test number in each docker: "$4
fi

if [ $# -ge 5 ]; then
    echo "test with --ignore-crash (y/n)? "$5
fi
echo "/****************************/"

if [ $# -ge 1 ]; then
    answer=$1
else
    read -p "build database engine with asan (y/n)? " answer
fi

if [ "$answer" == "y" ]; then
    echo "use Dockerfile_asan"
    DOCKERFILE="Dockerfile_asan"
fi

set -e
DOCKER_BUILDKIT=1 docker build -t $DOCKER_IMAGE_NAME -f $DOCKERFILE .
set +e

rm $DOCKER_DIR/$TOOL_DIR -rf
docker rmi $(docker image ls -f dangling=true -q)

if [ $# -ge 2 ]; then
    answer=$2
else
    read -p "run the test now (y/n)? " answer
fi

if [ "$answer" != "y" ]; then
    echo "do not test it now, exit the script"
    exit
fi

if [ $# -ge 3 ]; then
    answer=$3
else
    read -p "docker number: " answer
fi
docker_num=$answer

if [ $# -ge 4 ]; then
    answer=$4
else
    read -p "test instance number in each docker: " answer
fi
test_num=$answer

if [ $# -ge 5 ]; then
    answer=$5
else
    read -p "test with --ignore-crash (y/n)? " answer
fi
ignore_crash=$answer

# cp and run test_setup.sh
set -e
n=1
while [ $n -le $docker_num ]
do
    echo "run docker container: "$DOCKER_CONTAINER_NAME-$n
    docker run -itd -m $MEM_LIMIT --name $DOCKER_CONTAINER_NAME-$n $DOCKER_IMAGE_NAME
    docker cp test_setup.sh $DOCKER_CONTAINER_NAME-$n:/root/test
    docker exec -w /root/test -it $DOCKER_CONTAINER_NAME-$n bash test_setup.sh $test_num $ignore_crash
    n=$(( $n + 1))
done
set +e
