#!/bin/bash

n=1

nohup /root/tidb/bin/tidb-server >> server_log 2>&1 &
sleep 10s

echo "test number: "$1
test_num=$1

echo "ignore crash: "$2
IGNORE_CRASH=""
if [ "$2" == "y" ]; then
    IGNORE_CRASH="--ignore-crash"
fi

while [ $n -le $test_num ]
do
    mkdir test$n
    cd test$n
    pwd
    tmux new -d -s test$n "/root/EET/eet $IGNORE_CRASH --tidb-db=testdb$n --tidb-port=4000 2>&1 |tee log"
    sleep 1s
    cd ../
    n=$(( $n + 1))
done
