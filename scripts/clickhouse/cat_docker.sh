#!/bin/sh

n=1
test_num=$1
container=qit-development-clickhouse
log_file=log

echo "current running qcn: "$(( $(ps -aux | grep "qcn" |grep "log" | wc -l)))

# while [ $n -le $test_num ]
# do
# 	echo "--------------"
#     echo test$n
#     echo "--------------"
#     docker exec -it $container tail -10 test$n/nohup.out
#     n=$(( $n + 1))
# done

echo "Trigger bug: "
n=1
while [ $n -le $test_num ]
do
    if test ! -z "$(docker exec -w /root/test -it $container tail -30 test$n/$log_file | grep 'trigger')"; then
	    echo test$n
	    echo "-----------------------------------------"
	    echo "*****************************************"
	    docker exec -w /root/test -it $container tail -8 test$n/$log_file
	    echo "*****************************************"
	    echo "-----------------------------------------"
    fi
    n=$(( $n + 1))
done

