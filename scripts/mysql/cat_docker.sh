#!/bin/sh

n=1
m=1
test_num=8
docker_num=8
container=qit-development-mysql
log_file=log
DIR=/home/mysql/test

echo "current running qcn: "$(( $(ps -aux | grep "qcn" |grep "log" | wc -l)))

while [ $m -le $docker_num ]
do
	echo $container-$m" trigger bug: "
	while [ $n -le $test_num ]
	do
		if test ! -z "$(docker exec -w $DIR -it $container-$m tail -30 test$n/$log_file | grep 'trigger')"; then
			echo test$n
			echo "-----------------------------------------"
			echo "*****************************************"
			docker exec -w $DIR -it $container-$m tail -20 test$n/$log_file
			echo ""
			echo "*****************************************"
			echo "-----------------------------------------"
			echo ""
			echo ""
		fi
		n=$(( $n + 1))
	done
	m=$(( $m + 1))
	n=1
done

