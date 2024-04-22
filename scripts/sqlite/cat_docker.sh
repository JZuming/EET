#!/bin/sh

n=1
test_num=$1
container=qit-development-sqlite-1

echo "current running qcn: "$(( $(ps -aux | grep qcn | grep sqlite | wc -l) - 1 ))

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
    if test ! -z "$(docker exec -it $container tail -30 test/test$n/log | grep 'trigger')"; then
	    echo "=============="
        echo test$n
	    docker exec -it $container tail -8 test/test$n/log
	    echo "=============="
    fi
    n=$(( $n + 1))
done

