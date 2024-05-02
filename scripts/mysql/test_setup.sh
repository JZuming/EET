#!/bin/bash
n=1
export ASAN_OPTIONS=detect_leaks=0
nohup /usr/local/mysql/bin/mysqld_safe &
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
    tmux new -d -s test$n "/home/mysql/EET/eet $IGNORE_CRASH --mysql-db=testdb$n --mysql-port=3306 2>&1 |tee log"
    sleep 1s
    cd ../
    n=$(( $n + 1))
done
