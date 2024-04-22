#!/bin/sh -v

if [ $# -ne 4 ]; then 
    echo "tested SQL file: "$1
    echo "targeted table: "$2
    echo "test database: "$3
    echo "backup file: "$4
    return
fi

clickhouse client -q "drop database $3"
clickhouse client -q "create database $3"
clickhouse client -d $3 --queries-file $4
clickhouse client -d $3 -q "select * from $2" | sort > o_result
clickhouse client --mutations_sync 1 --compile_expression 0 -d $3 --queries-file $1
clickhouse client -d $3 -q "select * from $2" | sort > q_result
diff q_result o_result | grep ">"
