#!/bin/bash

DOCKER_DIR="."
TOOL_DIR="qit"
TOOL_SRC_DIR="../"$TOOL_DIR
DOCKER_CONTAINER_NAME=$TOOL_DIR"-development-sqlite"
DOCKER_IMAGE_NAME=$DOCKER_CONTAINER_NAME
DOCKER_VOLUME_LOCATION="/zdata/zuming/qit/sqlite_docker_disk"

echo "stop and remove the previous container"
docker stop $(docker ps  -a --format "table {{.Names}}" | grep $DOCKER_CONTAINER_NAME)
docker rm $(docker ps  -a --format "table {{.Names}}" | grep $DOCKER_CONTAINER_NAME)

echo "copy the qit source code"
rm $DOCKER_DIR/$TOOL_DIR -rf
cp -r $TOOL_SRC_DIR $DOCKER_DIR
cd $DOCKER_DIR

echo "build the qit test image"
DOCKERFILE="Dockerfile"
read -p "build database engine with asan (y/n)?" answer
if [ "$answer" == "y" ]; then
    echo "use Dockerfile_asan"
    DOCKERFILE="Dockerfile_asan"
fi
DOCKER_BUILDKIT=1 docker build -t $DOCKER_IMAGE_NAME -f $DOCKERFILE .

rm $DOCKER_DIR/$TOOL_DIR -rf
docker rmi $(docker image ls -f dangling=true -q)

read -p "run the test now (y/n)?" answer
if [ "$answer" != "y" ]; then
    echo "do not test it now, exit the script"
    exit
fi

read -p "docker number (max 8): " answer
docker_num=$answer

read -p "test number in each docker: " answer
test_num=$answer

read -p "test with --ignore-crash (y/n)?" answer
ignore_crash=$answer

# cp and run test_setup.sh
n=1
while [ $n -le $docker_num ]
do
    echo "run docker container: "$DOCKER_CONTAINER_NAME-$n
    docker run -itd -m 20g --name $DOCKER_CONTAINER_NAME-$n -v $DOCKER_VOLUME_LOCATION-$n:/root/test $DOCKER_IMAGE_NAME
    docker cp test_setup.sh $DOCKER_CONTAINER_NAME-$n:/root/test
    docker exec -w /root/test -it $DOCKER_CONTAINER_NAME-$n bash test_setup.sh $test_num $ignore_crash
    n=$(( $n + 1))
done


# SQLITE_EXE=sqlite-version-3.44.0/sqlite3
# rm testdb; $SQLITE_EXE testdb < test/minimized/minimized_db_record.sql ; $SQLITE_EXE testdb < test/minimized/select_origin.sql ; $SQLITE_EXE testdb < test/minimized/select_qit.sql
