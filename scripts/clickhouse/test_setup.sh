#!/bin/sh

n=1

directory=$(pwd)
if [ -e "$directory" ]; then 
    files=$(find "$directory" -mindepth 1 -type f -not -name ".*")
    if [ -n "$files" ]; then
        echo "$directory contains other files"
            echo "Deleting the files..."
            rm -rf ./*
            echo "All files deleted."
    else
        echo "$directory is empty."
    fi
else
    echo "$directory does not exist."
    exit 1;
fi

mkdir server
cd server
tmux new -d -s server 'clickhouse server -L server_log'
sleep 10s
cd ../

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
    tmux new -d -s test$n "/root/EET/eet $IGNORE_CRASH --clickhouse-db=testdb$n --clickhouse-port=9000 2>&1 |tee log"
    sleep 1s
    cd ../
    n=$(( $n + 1))
done
