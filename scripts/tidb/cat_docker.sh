#!/bin/sh

n=0
test_num=15
container_name=qit-development-tidb

echo "current running qcn: "$(( $(ps -aux | grep qit | wc -l) - 1 ))

while [ $n -le $test_num ]
do
	echo "--------------"
    echo test$n
    echo "--------------"
    docker exec -it $container_name tail -10 test$n/tester_log
    n=$(( $n + 1))
done

echo "Trigger bug: "
while [ $n -le $test_num ]
do
    if test ! -z "$(docker exec -it $container_name tail -30 test$n/nohup.out | grep "trigger")"; then
	    echo test$n
	    docker exec -it $container_name tail -30 test$n/tester_log
    fi
    n=$(( $n + 1))
done

